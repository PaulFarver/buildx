package kubernetes

import (
	"context"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	applycorev1 "k8s.io/client-go/applyconfigurations/core/v1"

	"github.com/docker/buildx/driver"
	"github.com/docker/buildx/driver/bkimage"
	"github.com/docker/buildx/driver/kubernetes/manifest"
	"github.com/docker/buildx/driver/kubernetes/podchooser"
	dockerclient "github.com/docker/docker/client"
	"github.com/pkg/errors"
	"k8s.io/client-go/kubernetes"
)

const (
	prioritySupported   = 40
	priorityUnsupported = 80
)

func init() {
	driver.Register(&factory{})
}

type factory struct{}

func (*factory) Name() string {
	return DriverName
}

func (*factory) Usage() string {
	return DriverName
}

func (*factory) Priority(ctx context.Context, endpoint string, api dockerclient.APIClient) int {
	if api == nil {
		return priorityUnsupported
	}
	return prioritySupported
}

func (f *factory) New(ctx context.Context, cfg driver.InitConfig) (driver.Driver, error) {
	if cfg.KubeClientConfig == nil {
		return nil, errors.Errorf("%s driver requires kubernetes API access", DriverName)
	}
	k8sName, err := buildxNameToK8sName(cfg.Name)
	if err != nil {
		return nil, err
	}
	namespace, _, err := cfg.KubeClientConfig.Namespace()
	if err != nil {
		return nil, errors.Wrap(err, "cannot determine Kubernetes namespace, specify manually")
	}
	restClientConfig, err := cfg.KubeClientConfig.ClientConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(restClientConfig)
	if err != nil {
		return nil, err
	}

	d := &Driver{
		factory:    f,
		InitConfig: cfg,
		clientset:  clientset,
	}

	deploymentOpt, loadbalance, namespace, err := f.processDriverOpts(k8sName, namespace, cfg)
	if nil != err {
		return nil, err
	}

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

func (f *factory) processDriverOpts(deploymentName string, namespace string, cfg driver.InitConfig) (*manifest.DeploymentOpt, string, string, error) {
	deploymentOpt := &manifest.DeploymentOpt{
		Name:          deploymentName,
		Image:         bkimage.DefaultImage,
		Replicas:      1,
		BuildkitFlags: cfg.BuildkitFlags,
		Rootless:      false,
		Platforms:     cfg.Platforms,
		ConfigFiles:   cfg.Files,
	}

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
				return nil, "", "", err
			}
		case "requests.cpu":
			r, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, "", "", err
			}
			deploymentOpt.RequestsCPU = r
		case "requests.memory":
			r, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, "", "", err
			}
			deploymentOpt.RequestsMemory = r
		case "limits.cpu":
			r, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, "", "", err
			}
			deploymentOpt.LimitsCPU = r
		case "limits.memory":
			r, err := resource.ParseQuantity(v)
			if err != nil {
				return nil, "", "", err
			}
			deploymentOpt.LimitsMemory = r
		case "rootless":
			deploymentOpt.Rootless, err = strconv.ParseBool(v)
			if err != nil {
				return nil, "", "", err
			}
			if _, isImage := cfg.DriverOpts["image"]; !isImage {
				deploymentOpt.Image = bkimage.DefaultRootlessImage
			}
		case "nodeselector":
			kvs := strings.Split(strings.Trim(v, `"`), ",")
			s := map[string]string{}
			for i := range kvs {
				kv := strings.Split(kvs[i], "=")
				if len(kv) == 2 {
					s[kv[0]] = kv[1]
				}
			}
			deploymentOpt.NodeSelector = s
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
								return nil, "", "", err
							}
							t = t.WithTolerationSeconds(int64(seconds))
						default:
							return nil, "", "", errors.Errorf("invalid tolaration %q", v)
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
				return nil, "", "", errors.Errorf("invalid loadbalance %q", v)
			}
			loadbalance = v
		case "qemu.install":
			deploymentOpt.Qemu.Install, err = strconv.ParseBool(v)
			if err != nil {
				return nil, "", "", err
			}
		case "qemu.image":
			if v != "" {
				deploymentOpt.Qemu.Image = v
			}
		case "controller":
			break
		default:
			return nil, "", "", errors.Errorf("invalid driver option %s for driver %s", k, DriverName)
		}
	}

	return deploymentOpt, loadbalance, namespace, nil
}

func (f *factory) AllowsInstances() bool {
	return true
}

// buildxNameToK8sName converts buildx name to Kubernetes Deployment or Statefulset name.
//
// eg. "buildx_buildkit_loving_mendeleev0" -> "loving-mendeleev0"
func buildxNameToK8sName(bx string) (string, error) {
	// TODO: commands.util.go should not pass "buildx_buildkit_" prefix to drivers
	if !strings.HasPrefix(bx, "buildx_buildkit_") {
		return "", errors.Errorf("expected a string with \"buildx_buildkit_\", got %q", bx)
	}
	s := strings.TrimPrefix(bx, "buildx_buildkit_")
	s = strings.ReplaceAll(s, "_", "-")
	return s, nil
}
