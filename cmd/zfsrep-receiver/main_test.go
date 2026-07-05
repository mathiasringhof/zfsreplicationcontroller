package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRenderSSHDConfigAllowsRootMappedReceiverUser(t *testing.T) {
	cfg := receiverConfig{
		AuthorizedKeysFile: "/run/zfs-receiver/authorized_keys",
		SSHPort:            2222,
	}

	config := renderSSHDConfig(cfg)

	for _, want := range []string{
		"PermitRootLogin prohibit-password",
		"AllowUsers zfs-recv",
		"PasswordAuthentication no",
		"KbdInteractiveAuthentication no",
		"AuthorizedKeysFile /run/zfs-receiver/authorized_keys",
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("sshd_config missing %q:\n%s", want, config)
		}
	}
	if strings.Contains(config, "PermitRootLogin no") {
		t.Fatalf("sshd_config rejects the root-mapped zfs-recv account:\n%s", config)
	}
}

func TestReceiveTaskAuthorizationForcesPolicyCommand(t *testing.T) {
	task := &zfsv1.ZFSReceiveTask{
		ObjectMeta: metav1.ObjectMeta{Name: "recv-1", Namespace: "storage"},
		Spec: zfsv1.ZFSReceiveTaskSpec{
			Destination: zfsv1.ReceiveDestination{Dataset: "tank/dst"},
			SSH: zfsv1.ReceiveTaskSSHSpec{
				AuthorizedPublicKey: "ssh-rsa AAAATEST zfsreplication-controller",
			},
			Policy: zfsv1.ReceiveTaskPolicy{
				ReceiveUnmounted:         true,
				ReceiveResumable:         true,
				AllowSyncSnapshotDestroy: true,
				Compression:              "none",
			},
		},
	}
	cfg := receiverConfig{AuthorizedKeysFile: "/run/zfs-receiver/authorized_keys"}

	auth := receiveTaskAuthorization(cfg, task)

	wantCommand := regexp.MustCompile(`^restrict,command="/usr/local/bin/zfsrep-receiver exec --policy-id policy-[a-f0-9]{32}"`)
	if !wantCommand.MatchString(auth.AuthorizedKey) {
		t.Fatalf("authorized key = %q, want command pattern %q", auth.AuthorizedKey, wantCommand)
	}
	if !regexp.MustCompile(`^policy-[a-f0-9]{32}$`).MatchString(auth.PolicyID) {
		t.Fatalf("policy ID = %q, want opaque shell-safe ID", auth.PolicyID)
	}
	if filepath.Base(auth.PolicyPath) != auth.PolicyID+".json" {
		t.Fatalf("policy path = %q, want file for policy ID %q", auth.PolicyPath, auth.PolicyID)
	}
	if !strings.HasSuffix(auth.AuthorizedKey, " ssh-rsa AAAATEST zfsreplication-controller") {
		t.Fatalf("authorized key = %q, want public key suffix", auth.AuthorizedKey)
	}
	if auth.Policy.TargetDataset != "tank/dst" {
		t.Fatalf("policy target = %q, want tank/dst", auth.Policy.TargetDataset)
	}
	if !auth.Policy.ReceiveUnmounted || !auth.Policy.ReceiveResumable || !auth.Policy.AllowSyncSnapshotDestroy {
		t.Fatalf("policy flags = %#v, want receive flags and sync snapshot destroy", auth.Policy)
	}
	if auth.Policy.ReceiveTaskPolicy != task.Spec.Policy {
		t.Fatalf("embedded receive task policy = %#v, want %#v", auth.Policy.ReceiveTaskPolicy, task.Spec.Policy)
	}
	data, err := json.Marshal(auth.Policy)
	if err != nil {
		t.Fatal(err)
	}
	var roundTripped receiverCommandPolicy
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatal(err)
	}
	if roundTripped.TargetDataset != auth.Policy.TargetDataset {
		t.Fatalf("round-tripped target dataset = %q, want %q", roundTripped.TargetDataset, auth.Policy.TargetDataset)
	}
	if roundTripped.ReceiveTaskPolicy != auth.Policy.ReceiveTaskPolicy {
		t.Fatalf("round-tripped policy = %#v, want %#v", roundTripped.ReceiveTaskPolicy, auth.Policy.ReceiveTaskPolicy)
	}
}

func TestReceiveTaskAuthorizationDoesNotEmbedUserControlledNames(t *testing.T) {
	task := &zfsv1.ZFSReceiveTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:      `recv-1"; touch /tmp/pwn; #`,
			Namespace: "storage $(id)",
		},
		Spec: zfsv1.ZFSReceiveTaskSpec{
			Destination: zfsv1.ReceiveDestination{Dataset: "tank/dst"},
			SSH: zfsv1.ReceiveTaskSSHSpec{
				AuthorizedPublicKey: "ssh-rsa AAAATEST zfsreplication-controller",
			},
			Policy: zfsv1.ReceiveTaskPolicy{
				ReceiveUnmounted: true,
				ReceiveResumable: true,
				Compression:      "none",
			},
		},
	}
	cfg := receiverConfig{AuthorizedKeysFile: "/run/zfs-receiver/authorized_keys"}

	auth := receiveTaskAuthorization(cfg, task)

	commandPrefix := strings.Split(auth.AuthorizedKey, " ssh-rsa ")[0]
	commandPrefix = strings.TrimPrefix(commandPrefix, `restrict,command="`)
	commandPrefix = strings.TrimSuffix(commandPrefix, `"`)
	for _, disallowed := range []string{"storage", "recv-1", "tank/dst", ";", "$", "(", ")", `"`, "\n", "\r", "<", ">", "*", "?"} {
		if strings.Contains(commandPrefix, disallowed) {
			t.Fatalf("forced command %q contains user-controlled or shell-sensitive fragment %q", commandPrefix, disallowed)
		}
	}
}

func TestPatchTaskReadyDoesNotReopenTerminalTask(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := zfsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	terminal := &zfsv1.ZFSReceiveTask{
		ObjectMeta: metav1.ObjectMeta{Name: "recv-1", Namespace: "storage"},
		Status: zfsv1.ZFSReceiveTaskStatus{
			Phase: zfsv1.ReceiveTaskPhaseCompleted,
		},
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&zfsv1.ZFSReceiveTask{}).
		WithObjects(terminal).
		Build()
	stale := terminal.DeepCopy()
	stale.Status = zfsv1.ZFSReceiveTaskStatus{}

	err := patchTaskReady(context.Background(), kubeClient, stale, receiverConfig{
		PodName: "zfs-receiver",
		PodUID:  "uid",
		PodIP:   "10.0.0.42",
		SSHPort: 2222,
	}, "ssh-ed25519 AAAATEST receiver")
	if err != nil {
		t.Fatal(err)
	}

	var got zfsv1.ZFSReceiveTask
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Name: terminal.Name, Namespace: terminal.Namespace}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != zfsv1.ReceiveTaskPhaseCompleted {
		t.Fatalf("phase after stale ready patch = %s, want %s", got.Status.Phase, zfsv1.ReceiveTaskPhaseCompleted)
	}
}
