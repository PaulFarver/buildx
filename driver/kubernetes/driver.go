package kubernetes

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/docker/buildx/driver"
	"github.com/docker/buildx/driver/kubernetes/execconn"
	"github.com/docker/buildx/driver/kubernetes/manifest"
	"github.com/docker/buildx/driver/kubernetes/podchooser"
	"github.com/docker/buildx/store"
	"github.com/docker/buildx/util/platformutil"
	"github.com/docker/buildx/util/progress"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/util/tracing/detect"
	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"
	clientappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
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

type Driver struct {
	driver.InitConfig
	factory           driver.Factory
	minReplicas       int
	controller        string
	deployment        *appsv1.Deployment
	statefulset       *appsv1.StatefulSet
	configMaps        []*corev1.ConfigMap
	clientset         *kubernetes.Clientset
	deploymentClient  clientappsv1.DeploymentInterface
	statefulsetClient clientappsv1.StatefulSetInterface
	podClient         clientcorev1.PodInterface
	configMapClient   clientcorev1.ConfigMapInterface
	podChooser        podchooser.PodChooser
	labelSelector     *metav1.LabelSelector
}

func (d *Driver) IsMobyDriver() bool {
	return false
}

func (d *Driver) Config() driver.InitConfig {
	return d.InitConfig
}

func (d *Driver) Bootstrap(ctx context.Context, l progress.Logger) error {
	return progress.Wrap("[internal] booting buildkit", l, func(sub progress.SubLogger) error {
		for _, cm := range d.configMaps {
			// Apply configuration
			_, err := d.configMapClient.Create(ctx, cm, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				return errors.Wrapf(err, "failed to create configmap %s", cm.Name)
			}
			d.configMapClient.Update(ctx, cm, metav1.UpdateOptions{})
		}
		switch d.controller {
		case "deployment":
			_, err := d.deploymentClient.Create(ctx, d.deployment, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				return errors.Wrap(err, "failed to create deployment")
			}
		case "statefulset":
			_, err := d.statefulsetClient.Create(ctx, d.statefulset, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				return errors.Wrap(err, "failed to create statefulset")
			}
		default:
			return errors.Errorf("unknown controller %s", d.controller)
		}
		// Look for a pod to use
		// pod, err := d.podChooser.ChoosePod(ctx)
		// if err != nil {
		// If no pod exists, create the controller
		// return err
		// }
		// _, err := d.deploymentClient.Get(ctx, d.deployment.Name, metav1.GetOptions{})
		// if err != nil {
		// 	if !apierrors.IsNotFound(err) {
		// 		// If the deployment is found
		// 		return errors.Wrapf(err, "error for bootstrap %q", d.deployment.Name)
		// 	}

		// 	for _, cfg := range d.configMaps {
		// 		// create ConfigMap first if exists
		// 		_, err = d.configMapClient.Create(ctx, cfg, metav1.CreateOptions{})
		// 		if err != nil {
		// 			if !apierrors.IsAlreadyExists(err) {
		// 				return errors.Wrapf(err, "error while calling configMapClient.Create for %q", cfg.Name)
		// 			}
		// 			_, err = d.configMapClient.Update(ctx, cfg, metav1.UpdateOptions{})
		// 			if err != nil {
		// 				return errors.Wrapf(err, "error while calling configMapClient.Update for %q", cfg.Name)
		// 			}
		// 		}
		// 	}

		// 	_, err = d.deploymentClient.Create(ctx, d.deployment, metav1.CreateOptions{})
		// 	if err != nil {
		// 		return errors.Wrapf(err, "error while calling deploymentClient.Create for %q", d.deployment.Name)
		// 	}
		// }
		return sub.Wrap(
			fmt.Sprintf("waiting for %d pods to be ready", d.minReplicas),
			func() error {
				if err := d.wait(ctx); err != nil {
					return err
				}
				return nil
			})
	})
}

func (d *Driver) wait(ctx context.Context) error {
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
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return errors.New("watcher channel closed")
			}
			pod, ok := event.Object.(*corev1.Pod)
			if !ok {
				return errors.Errorf("unexpected object type %T", event.Object)
			}
			fmt.Printf("pod: %s, status: %s", pod.Name, pod.Status.Phase)
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
	return &driver.Info{
		Status:       driver.Running,
		DynamicNodes: dynNodes,
	}, nil
}

func (d *Driver) Version(ctx context.Context) (string, error) {
	return "", nil
}

func (d *Driver) Stop(ctx context.Context, force bool) error {
	// future version may scale the replicas to zero here
	return nil
}

func (d *Driver) Rm(ctx context.Context, force, rmVolume, rmDaemon bool) error {
	if !rmDaemon {
		return nil
	}

	if err := d.deploymentClient.Delete(ctx, d.deployment.Name, metav1.DeleteOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			return errors.Wrapf(err, "error while calling deploymentClient.Delete for %q", d.deployment.Name)
		}
	}
	for _, cfg := range d.configMaps {
		if err := d.configMapClient.Delete(ctx, cfg.Name, metav1.DeleteOptions{}); err != nil {
			if !apierrors.IsNotFound(err) {
				return errors.Wrapf(err, "error while calling configMapClient.Delete for %q", cfg.Name)
			}
		}
	}
	return nil
}

func (d *Driver) Client(ctx context.Context) (*client.Client, error) {
	restClient := d.clientset.CoreV1().RESTClient()
	restClientConfig, err := d.KubeClientConfig.ClientConfig()
	if err != nil {
		return nil, err
	}
	pod, err := d.podChooser.ChoosePod(ctx)
	if err != nil {
		return nil, err
	}
	if len(pod.Spec.Containers) == 0 {
		return nil, errors.Errorf("pod %s does not have any container", pod.Name)
	}
	containerName := pod.Spec.Containers[0].Name
	cmd := []string{"buildctl", "dial-stdio"}
	conn, err := execconn.ExecConn(restClient, restClientConfig,
		pod.Namespace, pod.Name, containerName, cmd)
	if err != nil {
		return nil, err
	}

	exp, err := detect.Exporter()
	if err != nil {
		return nil, err
	}

	td, _ := exp.(client.TracerDelegate)

	return client.New(ctx, "", client.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return conn, nil
	}), client.WithTracerDelegate(td))
}

func (d *Driver) Factory() driver.Factory {
	return d.factory
}

func (d *Driver) Features() map[driver.Feature]bool {
	return map[driver.Feature]bool{
		driver.OCIExporter:    true,
		driver.DockerExporter: d.DockerAPI != nil,

		driver.CacheExport:   true,
		driver.MultiPlatform: true, // Untested (needs multiple Driver instances)
	}
}
