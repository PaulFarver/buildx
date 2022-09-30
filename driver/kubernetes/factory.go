package kubernetes

import (
	"context"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

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
	switch controllerType {
	case "Deployment":
		deployment, configMaps, err := manifest.NewDeployment(deploymentOpt)
		if err != nil {
			return nil, err
		}

		d.configMaps = configMaps
		d.minReplicas = deploymentOpt.Replicas
		d.labelSelector = deployment.Spec.Selector

		d.deploymentClient = clientset.AppsV1().Deployments(namespace)
		break
	case "StatefulSet":
		statefulSet, configMaps, err := manifest.NewStatefulSet(deploymentOpt)
		if err != nil {
			return nil, err
		}

		d.configMaps = configMaps
		d.minReplicas = deploymentOpt.Replicas
		d.labelSelector = statefulSet.Spec.Selector
		// d.statefulSetClient = clientset.AppsV1().StatefulSets(namespace)
		break
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
			deploymentOpt.RequestsCPU = v
		case "requests.memory":
			deploymentOpt.RequestsMemory = v
		case "limits.cpu":
			deploymentOpt.LimitsCPU = v
		case "limits.memory":
			deploymentOpt.LimitsMemory = v
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
			deploymentOpt.Tolerations = []corev1.Toleration{}
			for i := range ts {
				kvs := strings.Split(ts[i], ",")

				t := corev1.Toleration{}

				for j := range kvs {
					kv := strings.Split(kvs[j], "=")
					if len(kv) == 2 {
						switch kv[0] {
						case "key":
							t.Key = kv[1]
						case "operator":
							t.Operator = corev1.TolerationOperator(kv[1])
						case "value":
							t.Value = kv[1]
						case "effect":
							t.Effect = corev1.TaintEffect(kv[1])
						case "tolerationSeconds":
							c, err := strconv.Atoi(kv[1])
							if nil != err {
								return nil, "", "", err
							}
							c64 := int64(c)
							t.TolerationSeconds = &c64
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
