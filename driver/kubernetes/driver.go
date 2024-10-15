package kubernetes

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/docker/buildx/driver"
	"github.com/docker/buildx/driver/kubernetes/execconn"
	"github.com/docker/buildx/driver/kubernetes/manifest"
	"github.com/docker/buildx/driver/kubernetes/podchooser"
	"github.com/docker/buildx/store"
	"github.com/docker/buildx/util/platformutil"
	"github.com/docker/buildx/util/progress"
	"github.com/docker/go-units"
	"github.com/moby/buildkit/client"
	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	// applyappsv1 "k8s.io/client-go/applyconfigurations/apps/v1"

	// scalev1 "k8s.io/client-go/applyconfigurations/autoscaling/v1"
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"

	// clientappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	clientcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

const (
	DriverName = "kubernetes"
)

const (
	// valid values for driver-opt loadbalance
	LoadbalanceRandom = "random"
	LoadbalanceSticky = "sticky"
)

type Bootstrapper interface {
	Apply(context.Context, metav1.ApplyOptions) error
	Stop(context.Context) error
	Rm(context.Context, metav1.DeleteOptions) error
}

type Driver struct {
	driver.InitConfig
	factory      driver.Factory
	clientConfig ClientConfig

	// if you add fields, remember to update docs:
	// https://github.com/docker/docs/blob/main/content/build/drivers/kubernetes.md
	minReplicas int
	controller  string
	// deployment        *applyappsv1.DeploymentApplyConfiguration
	// statefulset       *applyappsv1.StatefulSetApplyConfiguration
	configMaps []*applycorev1.ConfigMapApplyConfiguration
	clientset  *kubernetes.Clientset
	// deploymentClient  clientappsv1.DeploymentInterface
	// statefulsetClient clientappsv1.StatefulSetInterface
	podClient       clientcorev1.PodInterface
	configMapClient clientcorev1.ConfigMapInterface
	podChooser      podchooser.PodChooser
	labelSelector   *metav1.LabelSelector
	applier         Bootstrapper
	defaultLoad     bool
	timeout         time.Duration
}

func (d *Driver) IsMobyDriver() bool {
	return false
}

func (d *Driver) Config() driver.InitConfig {
	return d.InitConfig
}

func (d *Driver) Bootstrap(ctx context.Context, l progress.Logger) error {
	return progress.Wrap("[internal] booting buildkit", l, func(sub progress.SubLogger) error {
		err := sub.Wrap("applying manifests", func() error {
			err := d.applier.Apply(ctx, metav1.ApplyOptions{FieldManager: "buildx"})
			if err != nil {
				return errors.Wrap(err, "failed to apply manifests")
			}
			return nil
		})
		if err != nil {
			return err
		}
		return sub.Wrap(
			fmt.Sprintf("waiting for %d pods to be ready, timeout: %s", d.minReplicas, units.HumanDuration(d.timeout)),
			func() error {
				return d.wait(ctx)
			})
	})
}

func (d *Driver) wait(ctx context.Context) error {
	timeoutChan := time.After(d.timeout)
	watcher, err := d.podClient.Watch(ctx, metav1.ListOptions{
		LabelSelector: metav1.FormatLabelSelector(d.labelSelector),
	})
	if err != nil {
		return errors.Wrap(err, "failed to watch pods")
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "context cancelled while watching pods")
		case <-timeoutChan:
			return err
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return errors.New("watcher channel closed")
			}
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				return errors.Errorf("unexpected object type %T", event.Object)
			}
			if pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" {
				return nil
			}
		}
	}
}

func (d *Driver) Info(ctx context.Context) (*driver.Info, error) {
	pods, err := podchooser.ListRunningPods(ctx, d.podClient, d.labelSelector)
	if err != nil {
		return nil, err
	}
	var dynNodes []store.Node
	for _, p := range pods {
		node := store.Node{
			Name: p.Name,
			// Other fields are unset (TODO: detect real platforms)
		}

		if p.Annotations != nil {
			if p, ok := p.Annotations[manifest.AnnotationPlatform]; ok {
				ps, err := platformutil.Parse(strings.Split(p, ","))
				if err == nil {
					node.Platforms = ps
				}
			}
		}

		dynNodes = append(dynNodes, node)
	}
	status := driver.Running
	if len(dynNodes) == 0 {
		status = driver.Stopped
	}
	return &driver.Info{
		Status:       status,
		DynamicNodes: dynNodes,
	}, nil
}

func (d *Driver) Version(ctx context.Context) (string, error) {
	return "", nil
}

func (d *Driver) Stop(ctx context.Context, force bool) error {
	err := d.applier.Stop(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to stop")
	}

	return nil
}

func (d *Driver) Rm(ctx context.Context, force, rmVolume, rmDaemon bool) error {
	if !rmDaemon {
		return nil
	}

	options := metav1.DeleteOptions{}
	if force {
		options = *metav1.NewDeleteOptions(0)
	}
	err := d.applier.Rm(ctx, options)
	if err != nil {
		return errors.Wrap(err, "Failed to remove")
	}

	return nil
}

func (d *Driver) Dial(ctx context.Context) (net.Conn, error) {
	restClient := d.clientset.CoreV1().RESTClient()
	restClientConfig, err := d.clientConfig.ClientConfig()
	if err != nil {
		return nil, err
	}
	pod, err := d.podChooser.ChoosePod(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to choose pod")
	}
	if len(pod.Spec.Containers) == 0 {
		return nil, errors.Errorf("pod %s does not have any container", pod.Name)
	}
	containerName := pod.Spec.Containers[0].Name
	cmd := []string{"buildctl", "dial-stdio"}
	conn, err := execconn.ExecConn(ctx, restClient, restClientConfig, pod.Namespace, pod.Name, containerName, cmd)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (d *Driver) Client(ctx context.Context, opts ...client.ClientOpt) (*client.Client, error) {
	opts = append([]client.ClientOpt{
		client.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return d.Dial(ctx)
		}),
	}, opts...)
	return client.New(ctx, "", opts...)
}

func (d *Driver) Factory() driver.Factory {
	return d.factory
}

func (d *Driver) Features(_ context.Context) map[driver.Feature]bool {
	return map[driver.Feature]bool{
		driver.OCIExporter:    true,
		driver.DockerExporter: d.DockerAPI != nil,
		driver.CacheExport:    true,
		driver.MultiPlatform:  true, // Untested (needs multiple Driver instances)
		driver.DefaultLoad:    d.defaultLoad,
	}
}

func (d *Driver) HostGatewayIP(_ context.Context) (net.IP, error) {
	return nil, errors.New("host-gateway is not supported by the kubernetes driver")
}
