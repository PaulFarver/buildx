package kubernetes

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/docker/buildx/driver"
	"github.com/docker/buildx/driver/bkimage"
	ctxkube "github.com/docker/buildx/driver/kubernetes/context"
	"github.com/docker/buildx/driver/kubernetes/manifest"
	"github.com/docker/buildx/driver/kubernetes/podchooser"
	dockerclient "github.com/docker/docker/client"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	prioritySupported   = 40
	priorityUnsupported = 80
	defaultTimeout      = 120 * time.Second
)

type ClientConfig interface {
	ClientConfig() (*rest.Config, error)
	Namespace() (string, bool, error)
}

type ClientConfigInCluster struct{}

func (k ClientConfigInCluster) ClientConfig() (*rest.Config, error) {
	return rest.InClusterConfig()
}

func (k ClientConfigInCluster) Namespace() (string, bool, error) {
	namespace, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return "", false, err
	}
	return strings.TrimSpace(string(namespace)), true, nil
}

func init() {
	driver.Register(&factory{})
}

type factory struct {
	cc ClientConfig // used for testing
}

func (*factory) Name() string {
	return DriverName
}

func (*factory) Usage() string {
	return DriverName
}

func (*factory) Priority(ctx context.Context, endpoint string, api dockerclient.APIClient, dialMeta map[string][]string) int {
	if api == nil {
		return priorityUnsupported
	}
	return prioritySupported
}

func (f *factory) New(ctx context.Context, cfg driver.InitConfig) (driver.Driver, error) {
	var err error
	var cc ClientConfig
	if f.cc != nil {
		cc = f.cc
	} else {
		cc, err = ctxkube.ConfigFromEndpoint(cfg.EndpointAddr, cfg.ContextStore)
		if err != nil {
			// err is returned if cfg.EndpointAddr is non-context name like "unix:///var/run/docker.sock".
			// try again with name="default".
			// FIXME(@AkihiroSuda): cfg should retain real context name.
			cc, err = ctxkube.ConfigFromEndpoint("default", cfg.ContextStore)
			if err != nil {
				logrus.Error(err)
			}
		}
		tryToUseConfigInCluster := false
		if cc == nil {
			tryToUseConfigInCluster = true
		} else {
			if _, err := cc.ClientConfig(); err != nil {
				tryToUseConfigInCluster = true
			}
		}
		if tryToUseConfigInCluster {
			ccInCluster := ClientConfigInCluster{}
			if _, err := ccInCluster.ClientConfig(); err == nil {
				logrus.Debug("using kube config in cluster")
				cc = ccInCluster
			}
		}
		if cc == nil {
			return nil, errors.Errorf("%s driver requires kubernetes API access", DriverName)
		}
	}
	k8sName, err := buildxNameToK8sName(cfg.Name)
	if err != nil {
		return nil, err
	}
	namespace, _, err := cc.Namespace()
	if err != nil {
		return nil, errors.Wrap(err, "cannot determine Kubernetes namespace, specify manually")
	}
	restClientConfig, err := cc.ClientConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(restClientConfig)
	if err != nil {
		return nil, err
	}

	d := &Driver{
		factory:      f,
		clientConfig: cc,
		InitConfig:   cfg,
		clientset:    clientset,
	}

	deploymentOpt, loadbalance, namespace, defaultLoad, timeout, err := f.processDriverOpts(k8sName, namespace, cfg)
	if nil != err {
		return nil, err
	}

	d.defaultLoad = defaultLoad
	d.timeout = timeout
	controllerType, ok := d.DriverOpts["controller"]
	if !ok {
		controllerType = "Deployment"
	}
	d.controller = controllerType
	switch controllerType {
	case "deployment":
		applier, err := manifest.NewDeploymentApplier(d.clientset, namespace, deploymentOpt)
		if err != nil {
			return nil, err
		}
		d.applier = applier
		d.labelSelector = applier.LabelSelector()

		break

		// deployment, configMaps, err := manifest.NewDeployment(deploymentOpt)
		// if err != nil {
		// 	return nil, err
		// }

		// d.configMaps = configMaps
		// d.minReplicas = deploymentOpt.Replicas
		// d.labelSelector = &metav1.LabelSelector{
		// 	MatchLabels: deployment.Spec.Selector.MatchLabels,
		// }

		// d.deployment = deployment

		// d.deploymentClient = clientset.AppsV1().Deployments(namespace)
		// break
	case "statefulset":
		applier, err := manifest.NewStatefulSetApplier(d.clientset, namespace, deploymentOpt)
		if err != nil {
			return nil, err
		}
		d.applier = applier
		d.labelSelector = applier.LabelSelector()

		break

		// statefulset, configMaps, err := manifest.NewStatefulSet(deploymentOpt)
		// if err != nil {
		// 	return nil, err
		// }

		// d.configMaps = configMaps
		// d.minReplicas = deploymentOpt.Replicas
		// d.labelSelector = &metav1.LabelSelector{
		// 	MatchLabels: statefulset.Spec.Selector.MatchLabels,
		// }

		// d.statefulset = statefulset

		// d.statefulsetClient = clientset.AppsV1().StatefulSets(namespace)
		// break
	default:
		return nil, errors.Errorf("unsupported controller type %s", controllerType)
	}

	d.podClient = clientset.CoreV1().Pods(namespace)
	d.configMapClient = clientset.CoreV1().ConfigMaps(namespace)

	switch loadbalance {
	case LoadbalanceSticky:
		d.podChooser = &podchooser.StickyPodChooser{
			Key:       cfg.ContextPathHash,
			PodClient: d.podClient,
			Selector:  d.labelSelector,
		}
	case LoadbalanceRandom:
		d.podChooser = &podchooser.RandomPodChooser{
			PodClient: d.podClient,
			Selector:  d.labelSelector,
		}
	}
	return d, nil
}

func (f *factory) processDriverOpts(deploymentName string, namespace string, cfg driver.InitConfig) (*manifest.DeploymentOpt, string, string, bool, time.Duration, error) {
	deploymentOpt := &manifest.DeploymentOpt{
		Name:          deploymentName,
		Image:         bkimage.DefaultImage,
		Replicas:      1,
		BuildkitFlags: cfg.BuildkitdFlags,
		Rootless:      false,
		Platforms:     cfg.Platforms,
		ConfigFiles:   cfg.Files,
	}

	defaultLoad := false
	timeout := defaultTimeout

	deploymentOpt.Qemu.Image = bkimage.QemuImage

	loadbalance := LoadbalanceSticky
	var err error

	for k, v := range cfg.DriverOpts {
		switch k {
		case "image":
			if v != "" {
				deploymentOpt.Image = v
			}
		case "namespace":
			namespace = v
		case "replicas":
			deploymentOpt.Replicas, err = strconv.Atoi(v)
			if err != nil {
				return nil, "", "", false, 0, err
			}
		case "requests.cpu":
			r, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, "", "", false, 0, err
			}
			deploymentOpt.RequestsCPU = r
		case "requests.memory":
			r, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, "", "", false, 0, err
			}
			deploymentOpt.RequestsMemory = r
		case "requests.ephemeral-storage":
			r, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, "", "", false, 0, err
			}
			deploymentOpt.RequestsEphemeralStorage = r
		case "limits.cpu":
			r, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, "", "", false, 0, err
			}
			deploymentOpt.LimitsCPU = r
		case "limits.memory":
			r, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, "", "", false, 0, err
			}
			deploymentOpt.LimitsMemory = r
		case "limits.ephemeral-storage":
			r, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, "", "", false, 0, err
			}
			deploymentOpt.LimitsEphemeralStorage = r
		case "rootless":
			deploymentOpt.Rootless, err = strconv.ParseBool(v)
			if err != nil {
				return nil, "", "", false, 0, err
			}
			if _, isImage := cfg.DriverOpts["image"]; !isImage {
				deploymentOpt.Image = bkimage.DefaultRootlessImage
			}
		case "schedulername":
			deploymentOpt.SchedulerName = v
		case "serviceaccount":
			deploymentOpt.ServiceAccountName = v
		case "nodeselector":
			deploymentOpt.NodeSelector, err = splitMultiValues(v, ",", "=")
			if err != nil {
				return nil, "", "", false, 0, errors.Wrap(err, "cannot parse node selector")
			}
		case "annotations":
			deploymentOpt.CustomAnnotations, err = splitMultiValues(v, ",", "=")
			if err != nil {
				return nil, "", "", false, 0, errors.Wrap(err, "cannot parse annotations")
			}
		case "labels":
			deploymentOpt.CustomLabels, err = splitMultiValues(v, ",", "=")
			if err != nil {
				return nil, "", "", false, 0, errors.Wrap(err, "cannot parse labels")
			}
		case "tolerations":
			ts := strings.Split(v, ";")
			deploymentOpt.Tolerations = []*applycorev1.TolerationApplyConfiguration{}
			// deploymentOpt.Tolerations = []corev1.Toleration{}
			for i := range ts {
				kvs := strings.Split(ts[i], ",")

				t := applycorev1.Toleration()

				for j := range kvs {
					kv := strings.Split(kvs[j], "=")
					if len(kv) == 2 {
						switch kv[0] {
						case "key":
							t = t.WithKey(kv[1])
						case "operator":
							t = t.WithOperator(corev1.TolerationOperator(kv[1]))
						case "value":
							t = t.WithValue(kv[1])
						case "effect":
							t = t.WithEffect(corev1.TaintEffect(kv[1]))
						case "tolerationSeconds":
							seconds, err := strconv.Atoi(kv[1])
							if nil != err {
								return nil, "", "", false, 0, err
							}
							t = t.WithTolerationSeconds(int64(seconds))
						default:
							return nil, "", "", false, 0, errors.Errorf("invalid tolaration %q", v)
						}
					}
				}

				deploymentOpt.Tolerations = append(deploymentOpt.Tolerations, t)
			}
		case "loadbalance":
			switch v {
			case LoadbalanceSticky:
			case LoadbalanceRandom:
			default:
				return nil, "", "", false, 0, errors.Errorf("invalid loadbalance %q", v)
			}
			loadbalance = v
		case "qemu.install":
			deploymentOpt.Qemu.Install, err = strconv.ParseBool(v)
			if err != nil {
				return nil, "", "", false, 0, err
			}
		case "qemu.image":
			if v != "" {
				deploymentOpt.Qemu.Image = v
			}
		case "default-load":
			defaultLoad, err = strconv.ParseBool(v)
			if err != nil {
				return nil, "", "", false, 0, err
			}
		case "timeout":
			timeout, err = time.ParseDuration(v)
			if err != nil {
				return nil, "", "", false, 0, errors.Wrap(err, "cannot parse timeout")
			}
		case "controller":
			break
		default:
			return nil, "", "", false, 0, errors.Errorf("invalid driver option %s for driver %s", k, DriverName)
		}
	}

	return deploymentOpt, loadbalance, namespace, defaultLoad, timeout, nil
}

func splitMultiValues(in string, itemsep string, kvsep string) (map[string]string, error) {
	kvs := strings.Split(strings.Trim(in, `"`), itemsep)
	s := map[string]string{}
	for i := range kvs {
		kv := strings.Split(kvs[i], kvsep)
		if len(kv) != 2 {
			return nil, errors.Errorf("invalid key-value pair: %s", kvs[i])
		}
		s[kv[0]] = kv[1]
	}
	return s, nil
}

func (f *factory) AllowsInstances() bool {
	return true
}

// buildxNameToK8sName converts buildx name to Kubernetes Deployment or Statefulset name.
//
// eg. "buildx_buildkit_loving_mendeleev0" -> "loving-mendeleev0"
func buildxNameToK8sName(bx string) (string, error) {
	// TODO: commands.util.go should not pass "buildx_buildkit_" prefix to drivers
	s, err := driver.ParseBuilderName(bx)
	if err != nil {
		return "", err
	}
	s = strings.ReplaceAll(s, "_", "-")
	return s, nil
}
