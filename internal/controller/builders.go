package controller

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const labelPrefix = "zfsreplication.example.com"

type runObjects struct {
	BaseName     string
	RunName      string
	SecretName   string
	ReceiverName string
	SenderName   string
	Labels       map[string]string
}

func dataMoverJob(namespace, name, image string, labels map[string]string, nodeName, command string, env []corev1.EnvVar, secretName string, readiness bool) *batchv1.Job {
	backoffLimit := int32(0)
	ttl := int32(86400)
	privileged := true
	automount := false
	env = append(env,
		corev1.EnvVar{Name: "EXPECTED_NODE_NAME", Value: nodeName},
		corev1.EnvVar{
			Name: "ACTUAL_NODE_NAME",
			ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "spec.nodeName",
			}},
		},
	)
	container := corev1.Container{
		Name:            "datamover",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{command},
		Env:             env,
		SecurityContext: &corev1.SecurityContext{Privileged: &privileged},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "dev-zfs", MountPath: "/dev/zfs"},
		},
	}
	volumes := []corev1.Volume{
		{Name: "dev-zfs", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/dev/zfs"}}},
	}
	if secretName != "" {
		mode := int32(0400)
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "ssh", MountPath: "/var/run/zfsrep/ssh", ReadOnly: true})
		volumes = append(volumes, corev1.Volume{Name: "ssh", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: secretName, DefaultMode: &mode}}})
	}
	if readiness {
		container.ReadinessProbe = &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(2222)},
			},
		}
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: &automount,
					NodeName:                     nodeName,
					Containers:                   []corev1.Container{container},
					Volumes:                      volumes,
				},
			},
		},
	}
}

func cloneLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
