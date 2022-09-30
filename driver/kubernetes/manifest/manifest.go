package manifest

import (
	"fmt"
	"path"
	"strings"

	"github.com/docker/buildx/util/platformutil"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"

	// corev1 "k8s.io/api/core/v1"
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
	Tolerations    []corev1.Toleration
	RequestsCPU    string
	RequestsMemory string
	LimitsCPU      string
	LimitsMemory   string
	Platforms      []v1.Platform

	VolumeSize             string
	VolumeStorageClassName *string
}

const (
	containerName      = "buildkitd"
	AnnotationPlatform = "buildx.docker.com/platform"
	defaultVolumeSize  = "100Gi"
	apiVersion         = "apps/v1"
	kind               = "Deployment"
)

func strtp(s string) *string {
	return &s
}

func NewDeployment(opt *DeploymentOpt) (d *appsv1.DeploymentApplyConfiguration, c []*corev1.ConfigMapApplyConfiguration, err error) {
	labels := map[string]string{
		"app": opt.Name,
	}
	annotations := map[string]string{}
	replicas := int32(opt.Replicas)
	privileged := true
	args := opt.BuildkitFlags

	if len(opt.Platforms) > 0 {
		annotations[AnnotationPlatform] = strings.Join(platformutil.Format(opt.Platforms), ",")
	}

	d = &appsv1.DeploymentApplyConfiguration{
		TypeMetaApplyConfiguration: metav1.TypeMetaApplyConfiguration{
			APIVersion: strtp(apiVersion),
			Kind:       strtp(kind),
		},
		ObjectMetaApplyConfiguration: &metav1.ObjectMetaApplyConfiguration{
			Name:        &opt.Name,
			Namespace:   &opt.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: &appsv1.DeploymentSpecApplyConfiguration{
			Replicas: &replicas,
			Selector: &metav1.LabelSelectorApplyConfiguration{
				MatchLabels: labels,
			},
			Template: &corev1.PodTemplateSpecApplyConfiguration{
				ObjectMetaApplyConfiguration: &metav1.ObjectMetaApplyConfiguration{
					Labels:      labels,
					Annotations: annotations,
				},
				Spec: &corev1.PodSpecApplyConfiguration{
					Containers: []corev1.ContainerApplyConfiguration{
						{
							Name:  strtp(containerName),
							Image: &opt.Image,
							Args:  args,
							SecurityContext: &corev1.SecurityContextApplyConfiguration{
								Privileged: &privileged,
							},
							ReadinessProbe: &corev1.ProbeApplyConfiguration{
								HandlerApplyConfiguration: corev1.HandlerApplyConfiguration{
									Exec: &corev1.ExecActionApplyConfiguration{
										Command: []string{"buildctl", "debug", "workers"},
									},
								},
							},
							Resources: corev1.ResourceRequirementsApplyConfiguration{
								Requests: corev1.ResourceList{},
								Limits:   corev1.ResourceList{},
							},
						},
					},
				},
			},
		},
	}
	for _, cfg := range splitConfigFiles(opt.ConfigFiles) {
		cc := &corev1.ConfigMap{
			TypeMeta: metav1.TypeMeta{
				APIVersion: corev1.SchemeGroupVersion.String(),
				Kind:       "ConfigMap",
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   opt.Namespace,
				Name:        opt.Name + "-" + cfg.name,
				Annotations: annotations,
			},
			Data: cfg.files,
		}

		d.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
			Name:      cfg.name,
			MountPath: path.Join("/etc/buildkit", cfg.path),
		}}

		d.Spec.Template.Spec.Volumes = []corev1.Volume{{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: cc.Name,
					},
				},
			},
		}}
		c = append(c, cc)
	}

	if opt.Qemu.Install {
		d.Spec.Template.Spec.InitContainers = []corev1.Container{
			{
				Name:  "qemu",
				Image: opt.Qemu.Image,
				Args:  []string{"--install", "all"},
				SecurityContext: &corev1.SecurityContext{
					Privileged: &privileged,
				},
			},
		}
	}

	if opt.Rootless {
		if err := toRootless(d); err != nil {
			return nil, nil, err
		}
	}

	if len(opt.NodeSelector) > 0 {
		d.Spec.Template.Spec.NodeSelector = opt.NodeSelector
	}

	if len(opt.Tolerations) > 0 {
		d.Spec.Template.Spec.Tolerations = opt.Tolerations
	}

	if opt.RequestsCPU != "" {
		reqCPU, err := resource.ParseQuantity(opt.RequestsCPU)
		if err != nil {
			return nil, nil, err
		}
		d.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = reqCPU
	}

	if opt.RequestsMemory != "" {
		reqMemory, err := resource.ParseQuantity(opt.RequestsMemory)
		if err != nil {
			return nil, nil, err
		}
		d.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = reqMemory
	}

	if opt.LimitsCPU != "" {
		limCPU, err := resource.ParseQuantity(opt.LimitsCPU)
		if err != nil {
			return nil, nil, err
		}
		d.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = limCPU
	}

	if opt.LimitsMemory != "" {
		limMemory, err := resource.ParseQuantity(opt.LimitsMemory)
		if err != nil {
			return nil, nil, err
		}
		d.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = limMemory
	}

	return
}

func NewStatefulSet(opt *DeploymentOpt) (d *appsv1.StatefulSet, c []*corev1.ConfigMap, err error) {
	labels := map[string]string{
		"app": opt.Name,
	}
	annotations := map[string]string{}
	replicas := int32(opt.Replicas)
	privileged := true
	args := opt.BuildkitFlags

	volumeSize := resource.MustParse(defaultVolumeSize)
	if opt.VolumeSize != "" {
		volumeSize, err = resource.ParseQuantity(opt.VolumeSize)
		if err != nil {
			return nil, nil, errors.Wrapf(err, "failed to parse volume size '%s'", opt.VolumeSize)
		}
	}

	if len(opt.Platforms) > 0 {
		annotations[AnnotationPlatform] = strings.Join(platformutil.Format(opt.Platforms), ",")
	}

	d = &appsv1.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: appsv1.SchemeGroupVersion.String(),
			Kind:       "StatefulSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   opt.Namespace,
			Name:        opt.Name,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: annotations,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  containerName,
							Image: opt.Image,
							Args:  args,
							SecurityContext: &corev1.SecurityContext{
								Privileged: &privileged,
							},
							ReadinessProbe: &corev1.Probe{
								Handler: corev1.Handler{
									Exec: &corev1.ExecAction{
										Command: []string{"buildctl", "debug", "workers"},
									},
								},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{},
								Limits:   corev1.ResourceList{},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "cache",
									MountPath: "/var/lib/buildkit",
								},
							},
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "cache",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: opt.VolumeStorageClassName,
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: volumeSize,
							},
						},
					},
				},
			},
		},
	}
	for _, cfg := range splitConfigFiles(opt.ConfigFiles) {

		cc := &corev1.ConfigMap{
			TypeMeta: metav1.TypeMeta{
				APIVersion: corev1.SchemeGroupVersion.String(),
				Kind:       "ConfigMap",
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   opt.Namespace,
				Name:        opt.Name + "-" + cfg.name,
				Annotations: annotations,
			},
			Data: cfg.files,
		}
		d.Spec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{
			Name:      cfg.name,
			MountPath: path.Join("/etc/buildkit", cfg.path),
		}}

		d.Spec.Template.Spec.Volumes = []corev1.Volume{{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: cc.Name,
					},
				},
			},
		}}
		c = append(c, cc)
	}

	if opt.Qemu.Install {
		d.Spec.Template.Spec.InitContainers = []corev1.Container{
			{
				Name:  "qemu",
				Image: opt.Qemu.Image,
				Args:  []string{"--install", "all"},
				SecurityContext: &corev1.SecurityContext{
					Privileged: &privileged,
				},
			},
		}
	}

	if opt.Rootless {
		if err := toRootlessSts(d); err != nil {
			return nil, nil, err
		}
	}

	if len(opt.NodeSelector) > 0 {
		d.Spec.Template.Spec.NodeSelector = opt.NodeSelector
	}

	if len(opt.Tolerations) > 0 {
		d.Spec.Template.Spec.Tolerations = opt.Tolerations
	}

	if opt.RequestsCPU != "" {
		reqCPU, err := resource.ParseQuantity(opt.RequestsCPU)
		if err != nil {
			return nil, nil, err
		}
		d.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = reqCPU
	}

	if opt.RequestsMemory != "" {
		reqMemory, err := resource.ParseQuantity(opt.RequestsMemory)
		if err != nil {
			return nil, nil, err
		}
		d.Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = reqMemory
	}

	if opt.LimitsCPU != "" {
		limCPU, err := resource.ParseQuantity(opt.LimitsCPU)
		if err != nil {
			return nil, nil, err
		}
		d.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = limCPU
	}

	if opt.LimitsMemory != "" {
		limMemory, err := resource.ParseQuantity(opt.LimitsMemory)
		if err != nil {
			return nil, nil, err
		}
		d.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory] = limMemory
	}

	return
}

func toRootless(d *appsv1.Deployment) error {
	d.Spec.Template.Spec.Containers[0].Args = append(
		d.Spec.Template.Spec.Containers[0].Args,
		"--oci-worker-no-process-sandbox",
	)
	d.Spec.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeUnconfined,
		},
	}
	if d.Spec.Template.ObjectMeta.Annotations == nil {
		d.Spec.Template.ObjectMeta.Annotations = make(map[string]string, 1)
	}
	d.Spec.Template.ObjectMeta.Annotations["container.apparmor.security.beta.kubernetes.io/"+containerName] = "unconfined"
	return nil
}

func toRootlessSts(d *appsv1.StatefulSet) error {
	d.Spec.Template.Spec.Containers[0].Args = append(
		d.Spec.Template.Spec.Containers[0].Args,
		"--oci-worker-no-process-sandbox",
	)
	d.Spec.Template.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeUnconfined,
		},
	}
	if d.Spec.Template.ObjectMeta.Annotations == nil {
		d.Spec.Template.ObjectMeta.Annotations = make(map[string]string, 1)
	}
	d.Spec.Template.ObjectMeta.Annotations["container.apparmor.security.beta.kubernetes.io/"+containerName] = "unconfined"
	return nil
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
