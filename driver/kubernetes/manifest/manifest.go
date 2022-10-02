package manifest

import (
	"fmt"
	"path"
	"strings"

	"github.com/docker/buildx/util/platformutil"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	// corev1 "k8s.io/api/core/v1"
	av1 "k8s.io/api/apps/v1"
	kv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	// metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	appsv1 "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1 "k8s.io/client-go/applyconfigurations/core/v1"
	metav1 "k8s.io/client-go/applyconfigurations/meta/v1"
)

type DeploymentOpt struct {
	Namespace string
	Name      string
	Image     string
	Replicas  int

	// Qemu
	Qemu struct {
		// when true, will install binfmt
		Install bool
		Image   string
	}

	BuildkitFlags []string
	// files mounted at /etc/buildkitd
	ConfigFiles map[string][]byte

	Rootless       bool
	NodeSelector   map[string]string
	Tolerations    []*corev1.TolerationApplyConfiguration
	RequestsCPU    resource.Quantity
	RequestsMemory resource.Quantity
	LimitsCPU      resource.Quantity
	LimitsMemory   resource.Quantity
	Platforms      []v1.Platform

	VolumeSize             resource.Quantity
	VolumeStorageClassName string
}

const (
	containerName                 = "buildkitd"
	AnnotationPlatform            = "buildx.docker.com/platform"
	defaultVolumeSize             = "1Gi"
	defaultVolumeStorageClassName = "hostpath"
)

func NewDeployment(opt *DeploymentOpt) (*appsv1.DeploymentApplyConfiguration, []*corev1.ConfigMapApplyConfiguration, error) {
	labels := map[string]string{
		"app": opt.Name,
	}
	annotations := map[string]string{}

	if len(opt.Platforms) > 0 {
		annotations[AnnotationPlatform] = strings.Join(platformutil.Format(opt.Platforms), ",")
	}
	if opt.Rootless {
		annotations["container.apparmor.security.beta.kubernetes.io/"+containerName] = "unconfined"
	}

	configMaps, volumes, mounts := configMaps(opt, labels, annotations)

	deployment := appsv1.Deployment(opt.Name, opt.Namespace).
		WithLabels(labels).
		WithAnnotations(annotations).
		WithSpec(appsv1.DeploymentSpec().
			WithReplicas(int32(opt.Replicas)).
			WithSelector(metav1.LabelSelector().WithMatchLabels(labels)).
			WithTemplate(corev1.PodTemplateSpec().
				WithLabels(labels).
				WithAnnotations(annotations).
				WithSpec(podSpec(opt, volumes, mounts)),
			),
		)
	return deployment, configMaps, nil
}

const cacheVolumeName = "buildkit-cache"

func NewStatefulSet(opt *DeploymentOpt) (*appsv1.StatefulSetApplyConfiguration, []*corev1.ConfigMapApplyConfiguration, error) {
	labels := map[string]string{
		"app": opt.Name,
	}
	annotations := map[string]string{}

	if len(opt.Platforms) > 0 {
		annotations[AnnotationPlatform] = strings.Join(platformutil.Format(opt.Platforms), ",")
	}
	if opt.Rootless {
		annotations["container.apparmor.security.beta.kubernetes.io/"+containerName] = "unconfined"
	}

	configMaps, volumes, mounts := configMaps(opt, labels, annotations)

	mounts = append(mounts, corev1.VolumeMount().
		WithName(cacheVolumeName).
		WithMountPath("/var/lib/buildkit"))

	if opt.VolumeSize.Value() == 0 {
		opt.VolumeSize = resource.MustParse(defaultVolumeSize)
	}

	if opt.VolumeStorageClassName == "" {
		opt.VolumeStorageClassName = defaultVolumeStorageClassName
	}

	statefulset := appsv1.StatefulSet(opt.Name, opt.Namespace).
		WithLabels(labels).
		WithAnnotations(annotations).
		WithSpec(appsv1.StatefulSetSpec().
			WithReplicas(int32(opt.Replicas)).
			WithSelector(metav1.LabelSelector().WithMatchLabels(labels)).
			WithTemplate(corev1.PodTemplateSpec().
				WithLabels(labels).
				WithAnnotations(annotations).
				WithSpec(podSpec(opt, volumes, mounts)),
			).
			WithPodManagementPolicy(av1.ParallelPodManagement).
			WithVolumeClaimTemplates(corev1.PersistentVolumeClaim(cacheVolumeName, "").
				WithLabels(labels).
				WithSpec(corev1.PersistentVolumeClaimSpec().
					WithAccessModes(kv1.ReadWriteOnce).
					WithStorageClassName(opt.VolumeStorageClassName).
					WithResources(corev1.ResourceRequirements().
						WithRequests(kv1.ResourceList{kv1.ResourceStorage: opt.VolumeSize}),
					),
				),
			),
		)

	return statefulset, configMaps, nil
}

func configMaps(opt *DeploymentOpt, labels, annotations map[string]string) ([]*corev1.ConfigMapApplyConfiguration, []*corev1.VolumeApplyConfiguration, []*corev1.VolumeMountApplyConfiguration) {
	configs := splitConfigFiles(opt.ConfigFiles)
	configMaps := make([]*corev1.ConfigMapApplyConfiguration, len(configs))
	volumes := make([]*corev1.VolumeApplyConfiguration, len(configs))
	mounts := make([]*corev1.VolumeMountApplyConfiguration, len(configs))
	for i, cfg := range splitConfigFiles(opt.ConfigFiles) {
		name := fmt.Sprintf("%s-%s", opt.Name, cfg.name)
		configMaps[i] = corev1.ConfigMap(name, opt.Namespace).
			WithLabels(labels).
			WithAnnotations(annotations).
			WithData(cfg.files)

		volumes[i] = corev1.Volume().
			WithName(name).
			WithConfigMap(corev1.ConfigMapVolumeSource().
				WithName(name),
			)

		mounts[i] = corev1.VolumeMount().
			WithName(name).
			WithMountPath(path.Join("/etc/buildkit", cfg.path))
	}
	return configMaps, volumes, mounts
}

func podSpec(opt *DeploymentOpt, volumes []*corev1.VolumeApplyConfiguration, volumeMounts []*corev1.VolumeMountApplyConfiguration) *corev1.PodSpecApplyConfiguration {
	args := append(opt.BuildkitFlags, fmt.Sprintf("--oci-worker-no-process-sandbox=%v", opt.Rootless))

	seccompProfileType := kv1.SeccompProfileTypeRuntimeDefault
	if opt.Rootless {
		seccompProfileType = kv1.SeccompProfileTypeUnconfined
	}

	initContainer := corev1.Container().
		WithName("qemu").
		WithImage(opt.Qemu.Image).
		WithCommand("--install", "all").
		WithSecurityContext(corev1.SecurityContext().
			WithPrivileged(true),
		)

	container := corev1.Container().
		WithName(containerName).
		WithImage(opt.Image).
		WithArgs(args...).
		WithResources(corev1.ResourceRequirements().
			WithRequests(kv1.ResourceList{
				kv1.ResourceCPU:    opt.RequestsCPU,
				kv1.ResourceMemory: opt.RequestsMemory,
			}).
			WithLimits(kv1.ResourceList{
				kv1.ResourceCPU:    opt.LimitsCPU,
				kv1.ResourceMemory: opt.LimitsMemory,
			}),
		).
		WithSecurityContext(corev1.SecurityContext().
			WithPrivileged(!opt.Rootless).
			WithSeccompProfile(corev1.SeccompProfile().WithType(seccompProfileType)),
		).
		WithReadinessProbe(corev1.Probe().
			WithExec(corev1.ExecAction().WithCommand("buildctl", "debug", "workers")),
		)

	container = container.WithVolumeMounts(volumeMounts...)

	podSpec := corev1.PodSpec().
		WithContainers(container).
		WithNodeSelector(opt.NodeSelector).
		WithTolerations(opt.Tolerations...).
		WithVolumes(volumes...)

	if opt.Qemu.Install {
		podSpec = podSpec.WithInitContainers(initContainer)
	}

	return podSpec
}

type config struct {
	name  string
	path  string
	files map[string]string
}

func splitConfigFiles(m map[string][]byte) []config {
	var c []config
	idx := map[string]int{}
	nameIdx := 0
	for k, v := range m {
		dir := path.Dir(k)
		i, ok := idx[dir]
		if !ok {
			idx[dir] = len(c)
			i = len(c)
			name := "config"
			if dir != "." {
				nameIdx++
				name = fmt.Sprintf("%s-%d", name, nameIdx)
			}
			c = append(c, config{
				path:  dir,
				name:  name,
				files: map[string]string{},
			})
		}
		c[i].files[path.Base(k)] = string(v)
	}
	return c
}
