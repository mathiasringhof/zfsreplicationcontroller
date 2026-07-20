package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/receiverauthorization"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

const validReceiverPublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOOBMEh4NBNCYArCdegKrXOfyIVEEhfvFoOYNYjsBP41 receiver"
const otherReceiverPublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPFmCq6yib3eYpmpYpK91ZyY8LfFdU2GWDhP9f7k7j8H unrelated"

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

func TestReceiveTaskCandidateMapsAPINeutralAuthorityValues(t *testing.T) {
	expiresAt := time.Now().Add(10 * time.Minute)
	task := &zfsv1.ZFSReceiveTask{
		ObjectMeta: metav1.ObjectMeta{Name: "recv-1", Namespace: "storage", UID: types.UID("11111111-2222-3333-4444-555555555555")},
		Spec: zfsv1.ZFSReceiveTaskSpec{
			Destination: zfsv1.ReceiveDestination{Dataset: "tank/dst"},
			SSH: zfsv1.ReceiveTaskSSHSpec{
				AuthorizedPublicKey: validReceiverPublicKey,
				ExpiresAt:           metav1.NewTime(expiresAt),
			},
			Policy: zfsv1.ReceiveTaskPolicy{
				ReceiveUnmounted:         true,
				ReceiveResumable:         true,
				AllowRollback:            true,
				AllowDestroy:             true,
				AllowSyncSnapshotDestroy: true,
				SyncSnapshotIdentifier:   "rel123",
				Compression:              "none",
			},
		},
	}
	candidate := receiveTaskCandidate(receiverConfig{AllowedPrefixes: []string{"tank"}}, task)
	if candidate.TaskUID != string(task.UID) || candidate.AuthorizedPublicKey != validReceiverPublicKey ||
		!candidate.ExpiresAt.Equal(expiresAt) || candidate.TargetDataset != "tank/dst" ||
		len(candidate.ReceiverDatasetPrefixes) != 1 || candidate.ReceiverDatasetPrefixes[0] != "tank" ||
		!candidate.ReceiveUnmounted || !candidate.ReceiveResumable || !candidate.AllowRollback ||
		!candidate.AllowDestroy || !candidate.AllowSyncSnapshotDestroy ||
		candidate.SyncSnapshotIdentifier != "rel123" || candidate.Compression != "none" {
		t.Fatalf("candidate = %#v, want exact API-neutral task authority", candidate)
	}
}

func TestActivateInitialReceiverAuthorizationPublishesEmptySnapshot(t *testing.T) {
	dir := t.TempDir()
	cfg := receiverConfig{
		NodeName:           "worker-b",
		AuthorizedKeysFile: filepath.Join(dir, "authorized_keys"),
	}
	kubeClient := fake.NewClientBuilder().WithScheme(newReceiverTestScheme(t)).Build()

	reconciler := receiverAuthorizationReconciler{
		client:         kubeClient,
		apiReader:      kubeClient,
		cfg:            cfg,
		authorization:  receiverauthorization.New(cfg.AuthorizedKeysFile),
		now:            time.Now,
		initialTrigger: make(chan event.GenericEvent, 1),
		startupGate:    make(chan struct{}),
		initialResult:  make(chan error, 1),
	}
	reconcileDone := make(chan error, 1)
	go func() {
		_, err := reconciler.Reconcile(context.Background(), receiverAuthorizationRequest)
		reconcileDone <- err
	}()
	if err := reconciler.StartInitial(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-reconcileDone; err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(cfg.AuthorizedKeysFile)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "# receiver-authorization-snapshot snapshot-") {
		t.Fatalf("initial authorized_keys = %q, want canonical empty snapshot", data)
	}
}

func TestReconcileReceiveTasksRejectsEveryDuplicateCanonicalKeyAndKeepsUnrelatedGrant(t *testing.T) {
	scheme := newReceiverTestScheme(t)
	dir := t.TempDir()
	cfg := receiverConfig{
		NodeName:           "worker-b",
		PodName:            "zfs-receiver",
		PodUID:             "receiver-pod-uid",
		PodIP:              "10.0.0.42",
		SSHPort:            2222,
		AuthorizedKeysFile: filepath.Join(dir, "authorized_keys"),
		AllowedPrefixes:    []string{"tank"},
	}
	expiresAt := metav1.NewTime(time.Now().Add(10 * time.Minute))
	tasks := []*zfsv1.ZFSReceiveTask{
		testReceiveTask("duplicate-a", "11111111-2222-3333-4444-555555555555", validReceiverPublicKey, expiresAt, cfg.NodeName),
		testReceiveTask("duplicate-b", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", strings.TrimSuffix(validReceiverPublicKey, " receiver")+" another", expiresAt, cfg.NodeName),
		testReceiveTask("unrelated", "99999999-8888-7777-6666-555555555555", otherReceiverPublicKey, expiresAt, cfg.NodeName),
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&zfsv1.ZFSReceiveTask{}).
		WithObjects(tasks[0], tasks[1], tasks[2]).
		Build()

	if _, err := newReceiverAuthorizationTestReconciler(kubeClient, cfg, validReceiverPublicKey).Reconcile(context.Background(), receiverAuthorizationRequest); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"duplicate-a", "duplicate-b"} {
		var got zfsv1.ZFSReceiveTask
		if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: "storage", Name: name}, &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Phase != zfsv1.ReceiveTaskPhaseFailed || !strings.Contains(got.Status.Error, "ambiguous") {
			t.Fatalf("duplicate task %s status = %#v, want isolated ambiguity rejection", name, got.Status)
		}
	}
	var unrelated zfsv1.ZFSReceiveTask
	if err := kubeClient.Get(context.Background(), types.NamespacedName{Namespace: "storage", Name: "unrelated"}, &unrelated); err != nil {
		t.Fatal(err)
	}
	if unrelated.Status.Phase != zfsv1.ReceiveTaskPhaseReady {
		t.Fatalf("unrelated task status = %#v, want ready", unrelated.Status)
	}
	data, err := os.ReadFile(cfg.AuthorizedKeysFile)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(strings.TrimSpace(string(data)), "\n") + 1; lines != 2 {
		t.Fatalf("authorized_keys = %q, want exactly one unrelated grant", data)
	}
}

func testReceiveTask(name, uid, publicKey string, expiresAt metav1.Time, nodeName string) *zfsv1.ZFSReceiveTask {
	return &zfsv1.ZFSReceiveTask{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "storage", UID: types.UID(uid)},
		Spec: zfsv1.ZFSReceiveTaskSpec{
			NodeName:    nodeName,
			Destination: zfsv1.ReceiveDestination{Dataset: "tank/dst"},
			SSH: zfsv1.ReceiveTaskSSHSpec{
				AuthorizedPublicKey: publicKey,
				ExpiresAt:           expiresAt,
			},
			Policy: zfsv1.ReceiveTaskPolicy{ReceiveUnmounted: true, Compression: "none"},
		},
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
		ObjectMeta: metav1.ObjectMeta{Name: "recv-1", Namespace: "storage", UID: types.UID("11111111-2222-3333-4444-555555555555")},
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
	if err := activateReceiverPolicyFixture(dir, testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{Compression: "none"})); err != nil {
		t.Fatal(err)
	}

	_, err := newReceiverAuthorizationTestReconciler(kubeClient, cfg, validReceiverPublicKey).Reconcile(context.Background(), receiverAuthorizationRequest)
	if err == nil || !strings.Contains(err.Error(), "status patch failure") {
		t.Fatalf("Reconcile() error = %v, want status patch failure", err)
	}
	data, err := os.ReadFile(cfg.AuthorizedKeysFile)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "# receiver-authorization-snapshot snapshot-") {
		t.Fatalf("authorized_keys = %q, want empty active snapshot", string(data))
	}
	entries, err := os.ReadDir(filepath.Join(dir, "receiver-authorization", "generations"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("receiver authorization generations = %v, want only active empty snapshot", entries)
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

	err := patchTaskReady(context.Background(), kubeClient, kubeClient, stale, receiverConfig{
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

func TestPatchTaskReadyDoesNotPatchRecreatedTask(t *testing.T) {
	scheme := newReceiverTestScheme(t)
	current := testReceiveTask("recv-1", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", validReceiverPublicKey, metav1.NewTime(time.Now().Add(10*time.Minute)), "worker-b")
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&zfsv1.ZFSReceiveTask{}).
		WithObjects(current).
		Build()
	stale := current.DeepCopy()
	stale.UID = types.UID("11111111-2222-3333-4444-555555555555")

	err := patchTaskReady(context.Background(), kubeClient, kubeClient, stale, receiverConfig{
		PodName: "zfs-receiver",
		PodUID:  "receiver-pod-uid",
		PodIP:   "10.0.0.42",
		SSHPort: 2222,
	}, "ssh-ed25519 AAAATEST receiver")
	if err != nil {
		t.Fatal(err)
	}

	var got zfsv1.ZFSReceiveTask
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(current), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "" {
		t.Fatalf("recreated task phase = %q, want untouched", got.Status.Phase)
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

func newReceiverAuthorizationTestReconciler(kubeClient client.Client, cfg receiverConfig, hostKey string) *receiverAuthorizationReconciler {
	return &receiverAuthorizationReconciler{
		client:        kubeClient,
		apiReader:     kubeClient,
		cfg:           cfg,
		hostKey:       hostKey,
		authorization: receiverauthorization.New(cfg.AuthorizedKeysFile),
		now:           time.Now,
		startupGate:   closedStartupGate(),
	}
}
