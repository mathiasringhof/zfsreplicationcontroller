package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

const validReceiverPublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOOBMEh4NBNCYArCdegKrXOfyIVEEhfvFoOYNYjsBP41 receiver"

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
				AuthorizedPublicKey: validReceiverPublicKey,
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

	auth, err := receiveTaskAuthorization(cfg, task)
	if err != nil {
		t.Fatal(err)
	}

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
	if strings.Count(auth.AuthorizedKey, "\n") != 0 {
		t.Fatalf("authorized key contains a newline: %q", auth.AuthorizedKey)
	}
	if !strings.HasSuffix(auth.AuthorizedKey, " ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOOBMEh4NBNCYArCdegKrXOfyIVEEhfvFoOYNYjsBP41") {
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

func TestReceiveTaskAuthorizationRejectsUnsafeAuthorizedPublicKeys(t *testing.T) {
	for _, tt := range []struct {
		name string
		key  string
	}{
		{name: "extra line", key: validReceiverPublicKey + "\n" + validReceiverPublicKey},
		{name: "authorized_keys options", key: `command="sh" ` + validReceiverPublicKey},
		{name: "invalid key", key: "ssh-rsa AAAATEST zfsreplication-controller"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			task := &zfsv1.ZFSReceiveTask{
				ObjectMeta: metav1.ObjectMeta{Name: "recv-1", Namespace: "storage"},
				Spec: zfsv1.ZFSReceiveTaskSpec{
					Destination: zfsv1.ReceiveDestination{Dataset: "tank/dst"},
					SSH: zfsv1.ReceiveTaskSSHSpec{
						AuthorizedPublicKey: tt.key,
					},
					Policy: zfsv1.ReceiveTaskPolicy{Compression: "none"},
				},
			}

			if _, err := receiveTaskAuthorization(receiverConfig{AuthorizedKeysFile: "/run/zfs-receiver/authorized_keys"}, task); err == nil {
				t.Fatal("receiveTaskAuthorization() error = nil, want rejection")
			}
		})
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
				AuthorizedPublicKey: validReceiverPublicKey,
			},
			Policy: zfsv1.ReceiveTaskPolicy{
				ReceiveUnmounted: true,
				ReceiveResumable: true,
				Compression:      "none",
			},
		},
	}
	cfg := receiverConfig{AuthorizedKeysFile: "/run/zfs-receiver/authorized_keys"}

	auth, err := receiveTaskAuthorization(cfg, task)
	if err != nil {
		t.Fatal(err)
	}

	commandPrefix := strings.Split(auth.AuthorizedKey, " ssh-ed25519 ")[0]
	commandPrefix = strings.TrimPrefix(commandPrefix, `restrict,command="`)
	commandPrefix = strings.TrimSuffix(commandPrefix, `"`)
	for _, disallowed := range []string{"storage", "recv-1", "tank/dst", ";", "$", "(", ")", `"`, "\n", "\r", "<", ">", "*", "?"} {
		if strings.Contains(commandPrefix, disallowed) {
			t.Fatalf("forced command %q contains user-controlled or shell-sensitive fragment %q", commandPrefix, disallowed)
		}
	}
}

func TestReconcileReceiveTasksRevokesExpiredTaskBeforeReturningPatchError(t *testing.T) {
	scheme := newReceiverTestScheme(t)
	dir := t.TempDir()
	cfg := receiverConfig{
		NodeName:           "worker-b",
		PodName:            "zfs-receiver",
		PodUID:             "uid",
		PodIP:              "10.0.0.42",
		SSHPort:            2222,
		AuthorizedKeysFile: filepath.Join(dir, "authorized_keys"),
	}
	task := &zfsv1.ZFSReceiveTask{
		ObjectMeta: metav1.ObjectMeta{Name: "recv-1", Namespace: "storage"},
		Spec: zfsv1.ZFSReceiveTaskSpec{
			NodeName:    cfg.NodeName,
			Destination: zfsv1.ReceiveDestination{Dataset: "tank/dst"},
			SSH: zfsv1.ReceiveTaskSSHSpec{
				AuthorizedPublicKey: validReceiverPublicKey,
				ExpiresAt:           metav1.NewTime(time.Now().Add(-time.Hour)),
			},
			Policy: zfsv1.ReceiveTaskPolicy{
				ReceiveUnmounted: true,
				Compression:      "none",
			},
		},
	}
	patchFailures := 1
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&zfsv1.ZFSReceiveTask{}).
		WithObjects(task).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if subResourceName == "status" && patchFailures > 0 {
					patchFailures--
					return errors.New("status patch failure")
				}
				return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	if err := writeAuthorizedKeys(cfg.AuthorizedKeysFile, []string{"stale-key"}); err != nil {
		t.Fatal(err)
	}
	stalePolicyID := "policy-0123456789abcdef0123456789abcdef"
	if err := writeReceiverPolicies(receiverPolicyDir(cfg), map[string]receiverCommandPolicy{
		stalePolicyID: testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{Compression: "none"}),
	}); err != nil {
		t.Fatal(err)
	}

	err := reconcileReceiveTasks(context.Background(), kubeClient, cfg, validReceiverPublicKey)
	if err == nil || !strings.Contains(err.Error(), "status patch failure") {
		t.Fatalf("reconcileReceiveTasks() error = %v, want status patch failure", err)
	}
	data, err := os.ReadFile(cfg.AuthorizedKeysFile)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(data)) != "" {
		t.Fatalf("authorized_keys = %q, want stale key revoked", string(data))
	}
	entries, err := os.ReadDir(receiverPolicyDir(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("receiver policy entries = %v, want stale policies removed", entries)
	}
}

func TestPatchTaskReadyDoesNotReopenTerminalTask(t *testing.T) {
	scheme := newReceiverTestScheme(t)
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

func newReceiverTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := zfsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}
