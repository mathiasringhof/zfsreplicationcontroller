package controller

import (
	"fmt"
	"maps"
	"strconv"
	"strings"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/syncoid"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	senderContainerName         = "sender"
	senderExecutable            = "/usr/local/bin/zfsrep-sender"
	senderSSHMountPath          = "/var/run/zfsrep/ssh"
	senderSSHKeyFile            = senderSSHMountPath + "/id_rsa"
	senderKnownHostsFile        = senderSSHMountPath + "/known_hosts"
	senderPodHostname           = "zfsrep-sender"
	senderTerminationMessage    = "/dev/termination-log"
	senderFinishedJobTTLSeconds = int32(86400)
)

func senderJob(run *zfsv1.ZFSReplicationRun, releaseImage string, endpoint zfsv1.ReceiveTaskEndpoint) (*batchv1.Job, error) {
	names := objectNamesForRun(run.Name)
	labels := map[string]string{labelPrefix + "/role": "sender"}

	contract, err := syncoid.Translate(run)
	if err != nil {
		return nil, fmt.Errorf("translate Syncoid Replication Contract: %w", err)
	}
	env := append([]corev1.EnvVar(nil), contract.SenderEnvironment...)
	env = append(env, syncoid.ConnectionEnvironment(syncoid.Connection{
		TargetHost:     fmt.Sprintf("zfs-recv@%s", endpoint.Host),
		SSHKeyFile:     senderSSHKeyFile,
		KnownHostsFile: senderKnownHostsFile,
		SSHPort:        strconv.FormatInt(int64(endpoint.Port), 10),
	})...)

	backoffLimit := int32(0)
	finishedJobTTLSeconds := senderFinishedJobTTLSeconds
	automountServiceAccountToken := false
	privileged := true
	sshSecretMode := int32(0400)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      names.SenderName,
			Namespace: run.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &finishedJobTTLSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: maps.Clone(labels)},
				Spec: corev1.PodSpec{
					Hostname:                     senderPodHostname,
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: &automountServiceAccountToken,
					NodeName:                     run.Spec.Source.NodeName,
					Containers: []corev1.Container{
						{
							Name:                     senderContainerName,
							Image:                    releaseImage,
							ImagePullPolicy:          senderImagePullPolicy(releaseImage),
							Command:                  []string{senderExecutable},
							Env:                      env,
							SecurityContext:          &corev1.SecurityContext{Privileged: &privileged},
							TerminationMessagePath:   senderTerminationMessage,
							TerminationMessagePolicy: corev1.TerminationMessageReadFile,
							VolumeMounts: []corev1.VolumeMount{
								{Name: "dev-zfs", MountPath: "/dev/zfs"},
								{Name: "ssh", MountPath: senderSSHMountPath, ReadOnly: true},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "dev-zfs",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{Path: "/dev/zfs"},
							},
						},
						{
							Name: "ssh",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  names.SecretName,
									DefaultMode: &sshSecretMode,
								},
							},
						},
					},
				},
			},
		},
	}, nil
}

func senderImagePullPolicy(image string) corev1.PullPolicy {
	if strings.Contains(image, "@sha256:") {
		return corev1.PullIfNotPresent
	}
	if tag := senderImageTag(image); tag == "" || tag == "latest" || tag == "main" {
		return corev1.PullAlways
	}
	return corev1.PullIfNotPresent
}

func senderImageTag(image string) string {
	lastSlash := strings.LastIndex(image, "/")
	lastColon := strings.LastIndex(image, ":")
	if lastColon <= lastSlash {
		return ""
	}
	return image[lastColon+1:]
}
