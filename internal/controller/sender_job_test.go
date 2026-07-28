package controller

import (
	"testing"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/syncoid"
	corev1 "k8s.io/api/core/v1"
)

func TestSenderJobBuildsConcreteSyncoidManifest(t *testing.T) {
	run := replicationRun("direct-sender")
	run.Spec.Syncoid.DeleteTargetSnapshots = ptr(true)
	endpoint := zfsv1.ReceiveTaskEndpoint{Host: "10.0.0.42", Port: 2205}

	job, err := senderJob(run, "registry.local:5000/zfsreplicationcontroller:v0.4.0", endpoint)
	if err != nil {
		t.Fatal(err)
	}

	names := objectNamesForRun(run.Name)
	if job.Name != names.SenderName || job.Namespace != run.Namespace {
		t.Fatalf("sender Job identity = %s/%s, want %s/%s", job.Namespace, job.Name, run.Namespace, names.SenderName)
	}
	if job.Labels[labelPrefix+"/run"] != run.Name || job.Labels[labelPrefix+"/role"] != "sender" {
		t.Fatalf("sender Job labels = %#v", job.Labels)
	}
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Fatalf("backoff limit = %v, want 0", job.Spec.BackoffLimit)
	}
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != 86400 {
		t.Fatalf("finished Job TTL = %v, want 86400", job.Spec.TTLSecondsAfterFinished)
	}

	pod := job.Spec.Template.Spec
	if pod.NodeName != run.Spec.Source.NodeName {
		t.Fatalf("nodeName = %q, want source node %q", pod.NodeName, run.Spec.Source.NodeName)
	}
	if pod.Hostname != "zfsrep-sender" {
		t.Fatalf("hostname = %q, want zfsrep-sender", pod.Hostname)
	}
	if pod.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("restart policy = %q, want Never", pod.RestartPolicy)
	}
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken {
		t.Fatalf("automount service account token = %v, want false", pod.AutomountServiceAccountToken)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(pod.Containers))
	}

	container := pod.Containers[0]
	if container.Name != "sender" {
		t.Fatalf("container name = %q, want sender", container.Name)
	}
	if container.Image != "registry.local:5000/zfsreplicationcontroller:v0.4.0" {
		t.Fatalf("image = %q", container.Image)
	}
	if container.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Fatalf("image pull policy = %q, want IfNotPresent", container.ImagePullPolicy)
	}
	if len(container.Command) != 1 || container.Command[0] != "/usr/local/bin/zfsrep-sender" {
		t.Fatalf("command = %#v", container.Command)
	}
	if container.SecurityContext == nil || container.SecurityContext.Privileged == nil || !*container.SecurityContext.Privileged {
		t.Fatalf("security context = %#v, want privileged", container.SecurityContext)
	}
	if container.TerminationMessagePath != "/dev/termination-log" ||
		container.TerminationMessagePolicy != corev1.TerminationMessageReadFile {
		t.Fatalf("termination message settings = (%q, %q)", container.TerminationMessagePath, container.TerminationMessagePolicy)
	}
	for name, want := range map[string]string{
		"DST_HOST":         "zfs-recv@10.0.0.42",
		"SSH_PORT":         "2205",
		"SSH_KEY_FILE":     "/var/run/zfsrep/ssh/id_rsa",
		"KNOWN_HOSTS_FILE": "/var/run/zfsrep/ssh/known_hosts",
		"SRC_DATASET":      run.Spec.Source.Dataset,
		"DST_DATASET":      run.Spec.Target.Dataset,
	} {
		if got := envValue(job, name); got != want {
			t.Fatalf("%s = %q, want %q", name, got, want)
		}
	}
	contract, err := syncoid.Translate(run)
	if err != nil {
		t.Fatal(err)
	}
	wantEnvironmentCount := len(contract.SenderEnvironment) + len(syncoid.ConnectionEnvironment(syncoid.Connection{}))
	if len(container.Env) != wantEnvironmentCount {
		t.Fatalf("environment entries = %d, want only %d Syncoid contract and connection entries", len(container.Env), wantEnvironmentCount)
	}
	assertSenderMount(t, container.VolumeMounts, "dev-zfs", "/dev/zfs", false)
	assertSenderMount(t, container.VolumeMounts, "ssh", "/var/run/zfsrep/ssh", true)
	assertSenderVolumes(t, pod.Volumes, names.SecretName)
}

func assertSenderMount(t *testing.T, mounts []corev1.VolumeMount, name, path string, readOnly bool) {
	t.Helper()
	for _, mount := range mounts {
		if mount.Name == name {
			if mount.MountPath != path || mount.ReadOnly != readOnly {
				t.Fatalf("mount %s = %#v", name, mount)
			}
			return
		}
	}
	t.Fatalf("missing mount %s", name)
}

func assertSenderVolumes(t *testing.T, volumes []corev1.Volume, secretName string) {
	t.Helper()
	var foundZFS, foundSSH bool
	for _, volume := range volumes {
		switch volume.Name {
		case "dev-zfs":
			foundZFS = volume.HostPath != nil && volume.HostPath.Path == "/dev/zfs"
		case "ssh":
			foundSSH = volume.Secret != nil &&
				volume.Secret.SecretName == secretName &&
				volume.Secret.DefaultMode != nil &&
				*volume.Secret.DefaultMode == 0400
		}
	}
	if !foundZFS || !foundSSH {
		t.Fatalf("volumes = %#v, want /dev/zfs host path and per-run SSH Secret %q", volumes, secretName)
	}
}
