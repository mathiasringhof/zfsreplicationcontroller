package controller

import (
	"fmt"
	"strconv"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const labelPrefix = "zfsreplication.example.com"

type runObjects struct {
	BaseName     string
	RunName      string
	SnapshotName string
	SecretName   string
	ReceiverName string
	SenderName   string
	ServiceName  string
	Labels       map[string]string
}

func objectNames(rep *zfsv1.ZFSReplication) runObjects {
	rn := runName(rep.Name, rep.Spec.RunID)
	labels := map[string]string{
		labelPrefix + "/name":   rep.Name,
		labelPrefix + "/run-id": rep.Spec.RunID,
	}
	return runObjects{
		BaseName:     baseName(rep.Name),
		RunName:      rn,
		SnapshotName: snapshotPrefix(rep.Spec.SnapshotPrefix) + "-" + rep.Spec.RunID,
		SecretName:   sanitizeName(rn, "token"),
		ReceiverName: sanitizeName(rn, "receiver"),
		SenderName:   sanitizeName(rn, "sender"),
		ServiceName:  sanitizeName(rn, "receiver"),
		Labels:       labels,
	}
}

func tokenSecret(rep *zfsv1.ZFSReplication, names runObjects, token string) *corev1.Secret {
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "token"
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: names.SecretName, Namespace: rep.Namespace, Labels: labels},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"token": []byte(token)},
	}
}

func receiverService(rep *zfsv1.ZFSReplication, names runObjects) *corev1.Service {
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "receiver"
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: names.ServiceName, Namespace: rep.Namespace, Labels: labels},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Name:       "stream",
				Port:       8080,
				TargetPort: intstr.FromInt(8080),
			}},
		},
	}
}

func receiverJob(rep *zfsv1.ZFSReplication, names runObjects, image string) *batchv1.Job {
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "receiver"
	env := []corev1.EnvVar{
		{Name: "ZFSREP_ROLE", Value: "receiver"},
		{Name: "RUN_ID", Value: rep.Spec.RunID},
		{Name: "SNAPSHOT_NAME", Value: names.SnapshotName},
		{Name: "DST_DATASET", Value: rep.Spec.Target.Dataset},
		{Name: "TOKEN_FILE", Value: "/var/run/zfsrep/token/token"},
		{Name: "BOOTSTRAP_MODE", Value: bootstrapMode(rep.Spec.Bootstrap.Mode)},
		{Name: "RECEIVE_UNMOUNTED", Value: strconv.FormatBool(boolDefault(rep.Spec.Receive.ReceiveUnmounted, true))},
		{Name: "RECEIVE_RESUMABLE", Value: strconv.FormatBool(boolDefault(rep.Spec.Receive.Resumable, true))},
		{Name: "LISTEN_ADDR", Value: ":8080"},
	}
	return dataMoverJob(rep, names.ReceiverName, image, labels, rep.Spec.Target.NodeName, "/usr/local/bin/zfsrep-receiver", env, names.SecretName)
}

func senderJob(rep *zfsv1.ZFSReplication, names runObjects, image string) *batchv1.Job {
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "sender"
	receiverURL := fmt.Sprintf("http://%s.%s.svc:8080/receive", names.ServiceName, rep.Namespace)
	env := []corev1.EnvVar{
		{Name: "ZFSREP_ROLE", Value: "sender"},
		{Name: "RUN_ID", Value: rep.Spec.RunID},
		{Name: "SNAPSHOT_PREFIX", Value: snapshotPrefix(rep.Spec.SnapshotPrefix)},
		{Name: "SNAPSHOT_NAME", Value: names.SnapshotName},
		{Name: "SRC_DATASET", Value: rep.Spec.Source.Dataset},
		{Name: "DST_DATASET", Value: rep.Spec.Target.Dataset},
		{Name: "BASE_SNAPSHOT", Value: rep.Status.LastSuccessfulSnapshot},
		{Name: "RECEIVER_URL", Value: receiverURL},
		{Name: "TOKEN_FILE", Value: "/var/run/zfsrep/token/token"},
		{Name: "BOOTSTRAP_MODE", Value: bootstrapMode(rep.Spec.Bootstrap.Mode)},
	}
	return dataMoverJob(rep, names.SenderName, image, labels, rep.Spec.Source.NodeName, "/usr/local/bin/zfsrep-sender", env, names.SecretName)
}

func dataMoverJob(rep *zfsv1.ZFSReplication, name, image string, labels map[string]string, nodeName, command string, env []corev1.EnvVar, secretName string) *batchv1.Job {
	backoffLimit := int32(0)
	ttl := int32(86400)
	privileged := true
	automount := false
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: rep.Namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: &automount,
					NodeSelector:                 map[string]string{"kubernetes.io/hostname": nodeName},
					Containers: []corev1.Container{{
						Name:            "datamover",
						Image:           image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Command:         []string{command},
						Env:             env,
						SecurityContext: &corev1.SecurityContext{Privileged: &privileged},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "dev-zfs", MountPath: "/dev/zfs"},
							{Name: "token", MountPath: "/var/run/zfsrep/token", ReadOnly: true},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "dev-zfs", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/dev/zfs"}}},
						{Name: "token", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: secretName}}},
					},
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
