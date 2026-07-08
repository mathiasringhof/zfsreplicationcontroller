package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/datamover"
	"golang.org/x/crypto/ssh"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const testReceiverHostKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOOBMEh4NBNCYArCdegKrXOfyIVEEhfvFoOYNYjsBP41 receiver"

func TestRunReconcileSenderJobUsesSyncoidOptions(t *testing.T) {
	run := replicationRun("manual-1")
	names := objectNamesForRun(run.Name)
	r := newRunReconciler(t, run, readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey))
	if _, err := r.Reconcile(context.Background(), request("manual-1")); err != nil {
		t.Fatal(err)
	}
	sender := getJob(t, r.Client, "zfsrep-manual-1-sender")
	if got := sender.Spec.Template.Spec.Hostname; got != "zfsrep-sender" {
		t.Fatalf("sender pod hostname = %q, want zfsrep-sender", got)
	}
	if got := envValue(sender, "SRC_DATASET"); got != "tank/src" {
		t.Fatalf("SRC_DATASET = %q", got)
	}
	if got := envValue(sender, "DST_HOST"); got != "zfs-recv@10.0.0.42" {
		t.Fatalf("DST_HOST = %q", got)
	}
	if got := envValue(sender, "KNOWN_HOSTS_FILE"); got != "/var/run/zfsrep/ssh/known_hosts" {
		t.Fatalf("KNOWN_HOSTS_FILE = %q", got)
	}
	if got := envValue(sender, "SYNCOID_NO_SYNC_SNAP"); got != "true" {
		t.Fatalf("SYNCOID_NO_SYNC_SNAP = %q", got)
	}
	if got := envValue(sender, "SYNCOID_COMPRESS"); got != "zstd" {
		t.Fatalf("SYNCOID_COMPRESS = %q", got)
	}
	if got := envValue(sender, "SYNCOID_IDENTIFIER"); got == "" || strings.ContainsAny(got, " \t\r\n;|&`$()<>\\") {
		t.Fatalf("SYNCOID_IDENTIFIER = %q, want non-empty shell-safe identifier", got)
	}
	if got := envValue(sender, "SYNCOID_INCLUDE_SNAPS"); got != "^snap-.*\n^manual$" {
		t.Fatalf("SYNCOID_INCLUDE_SNAPS = %q", got)
	}
	if got := envValue(sender, "SYNCOID_EXCLUDE_SNAPS"); got != ".*-tmp$" {
		t.Fatalf("SYNCOID_EXCLUDE_SNAPS = %q", got)
	}
	cfg := datamover.SenderConfigFromLookup(func(name string) string {
		return envValue(sender, name)
	})
	if cfg.SrcDataset != run.Spec.Source.Dataset {
		t.Fatalf("round-tripped SrcDataset = %q, want %q", cfg.SrcDataset, run.Spec.Source.Dataset)
	}
	if cfg.DstDataset != run.Spec.Target.Dataset {
		t.Fatalf("round-tripped DstDataset = %q, want %q", cfg.DstDataset, run.Spec.Target.Dataset)
	}
	if cfg.DstHost != "zfs-recv@10.0.0.42" {
		t.Fatalf("round-tripped DstHost = %q", cfg.DstHost)
	}
	if !cfg.NoSyncSnap || !cfg.NoRollback || cfg.ForceDelete || cfg.Compress != "zstd" {
		t.Fatalf("round-tripped Syncoid config = %#v", cfg)
	}
	if cfg.ReceiveUnmounted || cfg.ReceiveResumable {
		t.Fatalf("round-tripped receive flags = %#v, want both false", cfg)
	}
	if strings.Join(cfg.IncludeSnaps, "\n") != strings.Join(run.Spec.Syncoid.IncludeSnaps, "\n") {
		t.Fatalf("round-tripped IncludeSnaps = %#v", cfg.IncludeSnaps)
	}
	if strings.Join(cfg.ExcludeSnaps, "\n") != strings.Join(run.Spec.Syncoid.ExcludeSnaps, "\n") {
		t.Fatalf("round-tripped ExcludeSnaps = %#v", cfg.ExcludeSnaps)
	}
	var secret corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Name: names.SecretName, Namespace: run.Namespace}, &secret); err != nil {
		t.Fatal(err)
	}
	gotKnownHosts := secret.Data["known_hosts"]
	if got := string(gotKnownHosts); !strings.HasPrefix(got, "[10.0.0.42]:2222 ssh-ed25519 ") {
		t.Fatalf("known_hosts = %q, want bracketed receiver endpoint", got)
	}
	_, hosts, parsedKey, comment, rest, err := ssh.ParseKnownHosts(gotKnownHosts)
	if err != nil {
		t.Fatalf("parse known_hosts: %v", err)
	}
	if len(hosts) != 1 || hosts[0] != "[10.0.0.42]:2222" {
		t.Fatalf("known_hosts hosts = %v, want receiver endpoint", hosts)
	}
	if parsedKey.Type() != "ssh-ed25519" {
		t.Fatalf("known_hosts key type = %q", parsedKey.Type())
	}
	if comment != "receiver" {
		t.Fatalf("known_hosts comment = %q, want receiver", comment)
	}
	if len(rest) != 0 {
		t.Fatalf("known_hosts rest = %q, want empty", rest)
	}
}

func TestKnownHostsLineRejectsInvalidHostKey(t *testing.T) {
	if _, err := knownHostsLine("10.0.0.42", 2222, "ssh-ed25519 not-base64 receiver"); err == nil {
		t.Fatal("knownHostsLine() error = nil, want invalid host key rejection")
	}
}

func TestRunReconcileCreatesReceiveTaskBeforeSenderJob(t *testing.T) {
	run := replicationRun("manual-1")
	names := objectNamesForRun(run.Name)
	r := newRunReconciler(t, run)

	result, err := r.Reconcile(context.Background(), request("manual-1"))
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("RequeueAfter = %v, want receiver wait", result.RequeueAfter)
	}

	var task zfsv1.ZFSReceiveTask
	if err := r.Get(context.Background(), types.NamespacedName{Name: names.ReceiveTaskName, Namespace: run.Namespace}, &task); err != nil {
		t.Fatal(err)
	}
	if task.Spec.RunRef.Name != run.Name {
		t.Fatalf("task runRef = %#v", task.Spec.RunRef)
	}
	if task.Spec.NodeName != run.Spec.Target.NodeName {
		t.Fatalf("task nodeName = %q", task.Spec.NodeName)
	}
	if task.Spec.Destination.Dataset != run.Spec.Target.Dataset {
		t.Fatalf("task destination = %#v", task.Spec.Destination)
	}
	if task.Spec.SSH.AuthorizedPublicKey == "" {
		t.Fatal("task authorized public key is empty")
	}
	if task.Spec.Policy.AllowRollback {
		t.Fatal("task allows rollback by default")
	}
	if task.Spec.Policy.ReceiveResumable {
		t.Fatal("task allows resumable receive when the run disabled it")
	}
	if !task.Spec.Policy.AllowMount {
		t.Fatal("task does not allow mounted receive when the run disabled receiveUnmounted")
	}
	if task.Spec.Policy.AllowSyncSnapshotDestroy {
		t.Fatal("task allows Syncoid snapshot pruning when noSyncSnap is true")
	}
	if task.Spec.Policy.Compression != "zstd" {
		t.Fatalf("task compression = %q, want zstd", task.Spec.Policy.Compression)
	}
	if task.Spec.Policy.SyncSnapshotIdentifier == "" || strings.ContainsAny(task.Spec.Policy.SyncSnapshotIdentifier, " \t\r\n;|&`$()<>\\") {
		t.Fatalf("task sync snapshot identifier = %q, want non-empty shell-safe identifier", task.Spec.Policy.SyncSnapshotIdentifier)
	}
	assertObjectDeleted(t, r.Client, &batchv1.Job{}, names.SenderName)
}

func TestRunReconcileLogsReceiverAndSenderLifecycle(t *testing.T) {
	run := replicationRun("manual-logs")
	names := objectNamesForRun(run.Name)
	r := newRunReconciler(t, run, readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey))
	ctx, logs := captureRunLogger()

	if _, err := r.Reconcile(ctx, request(run.Name)); err != nil {
		t.Fatal(err)
	}

	baseFields := map[string]string{
		"namespace":         run.Namespace,
		"run":               run.Name,
		"sourceDataset":     run.Spec.Source.Dataset,
		"targetDataset":     run.Spec.Target.Dataset,
		"senderJob":         names.SenderName,
		"receiveTask":       names.ReceiveTaskName,
		"syncoidIdentifier": syncSnapshotIdentifierForRun(run),
	}
	assertNoLogEntry(t, logs, "reconciling replication run")
	assertLogEntry(t, logs, "accepted replication run", baseFields)
	receiverFields := cloneStringMap(baseFields)
	receiverFields["receiverPod"] = "zfs-receiver-worker-b"
	receiverFields["receiverPodIP"] = "10.0.0.42"
	assertLogEntry(t, logs, "replication receiver is ready", receiverFields)
	assertLogEntry(t, logs, "created sender job", receiverFields)
}

func TestRunReconcileDoesNotRecheckSenderJobImmediatelyAfterCreate(t *testing.T) {
	run := replicationRun("manual-create-cache")
	names := objectNamesForRun(run.Name)
	jobCreated := false
	hideCreatedJobGets := 1
	r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetName() == names.SenderName {
				jobCreated = true
			}
			return c.Create(ctx, obj, opts...)
		},
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == names.SenderName && jobCreated && hideCreatedJobGets > 0 {
				hideCreatedJobGets--
				return apierrors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}, run, readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey))
	ctx, logs := captureRunLogger()

	result, err := r.Reconcile(ctx, request(run.Name))
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want nil after sender job create", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("RequeueAfter = %v, want cache-visible sender job recheck", result.RequeueAfter)
	}
	assertLogEntry(t, logs, "replication receiver is ready", map[string]string{
		"namespace":     run.Namespace,
		"run":           run.Name,
		"senderJob":     names.SenderName,
		"receiveTask":   names.ReceiveTaskName,
		"receiverPod":   "zfs-receiver-worker-b",
		"receiverPodIP": "10.0.0.42",
	})
	assertLogEntry(t, logs, "created sender job", map[string]string{
		"namespace":     run.Namespace,
		"run":           run.Name,
		"senderJob":     names.SenderName,
		"receiveTask":   names.ReceiveTaskName,
		"receiverPod":   "zfs-receiver-worker-b",
		"receiverPodIP": "10.0.0.42",
	})

	ctx, logs = captureRunLogger()
	if _, err := r.Reconcile(ctx, request(run.Name)); err != nil {
		t.Fatalf("second Reconcile() error = %v", err)
	}
	assertNoLogEntry(t, logs, "replication receiver is ready")
	assertNoLogEntry(t, logs, "created sender job")
}

func TestRunReconcileTreatsSenderJobCreateAlreadyExistsAsSuccess(t *testing.T) {
	run := replicationRun("manual-create-exists")
	names := objectNamesForRun(run.Name)
	returnAlreadyExists := true
	r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetName() == names.SenderName && returnAlreadyExists {
				returnAlreadyExists = false
				if err := c.Create(ctx, obj, opts...); err != nil {
					return err
				}
				return apierrors.NewAlreadyExists(schema.GroupResource{Group: "batch", Resource: "jobs"}, obj.GetName())
			}
			return c.Create(ctx, obj, opts...)
		},
	}, run, readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey))

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatalf("Reconcile() error = %v, want nil after AlreadyExists sender job create", err)
	}
	assertObjectExists(t, r.Client, &batchv1.Job{}, names.SenderName)
}

func TestRunReconcileTreatsEphemeralCreateAlreadyExistsAsSuccess(t *testing.T) {
	for _, tt := range []struct {
		name          string
		alreadyExists func(client.Object) bool
		resource      schema.GroupResource
		assertObject  client.Object
		assertName    string
	}{
		{
			name: "secret",
			alreadyExists: func(obj client.Object) bool {
				_, ok := obj.(*corev1.Secret)
				return ok
			},
			resource:     schema.GroupResource{Resource: "secrets"},
			assertObject: &corev1.Secret{},
		},
		{
			name: "receive task",
			alreadyExists: func(obj client.Object) bool {
				_, ok := obj.(*zfsv1.ZFSReceiveTask)
				return ok
			},
			resource:     schema.GroupResource{Group: zfsv1.Group, Resource: "zfsreceivetasks"},
			assertObject: &zfsv1.ZFSReceiveTask{},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := replicationRun("manual-" + strings.ReplaceAll(tt.name, " ", "-") + "-exists")
			names := objectNamesForRun(run.Name)
			if tt.assertName == "" {
				switch tt.name {
				case "secret":
					tt.assertName = names.SecretName
				case "receive task":
					tt.assertName = names.ReceiveTaskName
				}
			}
			returnAlreadyExists := true
			r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if returnAlreadyExists && tt.alreadyExists(obj) {
						returnAlreadyExists = false
						if err := c.Create(ctx, obj, opts...); err != nil {
							return err
						}
						return apierrors.NewAlreadyExists(tt.resource, obj.GetName())
					}
					return c.Create(ctx, obj, opts...)
				},
			}, run)

			if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
				t.Fatalf("Reconcile() error = %v, want nil after AlreadyExists %s create", err, tt.name)
			}
			assertObjectExists(t, r.Client, tt.assertObject, tt.assertName)
		})
	}
}

func TestRunReconcileDoesNotReadSecretFromCacheImmediatelyAfterCreate(t *testing.T) {
	run := replicationRun("manual-secret-cache")
	names := objectNamesForRun(run.Name)
	secretCreated := false
	r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetName() == names.SecretName {
				secretCreated = true
			}
			return c.Create(ctx, obj, opts...)
		},
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == names.SecretName && secretCreated {
				return apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}, run)

	result, err := r.Reconcile(context.Background(), request(run.Name))
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want nil after secret create", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("RequeueAfter = %v, want receiver wait", result.RequeueAfter)
	}
	assertObjectExists(t, r.Client, &zfsv1.ZFSReceiveTask{}, names.ReceiveTaskName)
}

func TestRunReconcileDoesNotReadReceiveTaskFromCacheImmediatelyAfterCreate(t *testing.T) {
	run := replicationRun("manual-task-cache")
	names := objectNamesForRun(run.Name)
	taskCreated := false
	r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetName() == names.ReceiveTaskName {
				taskCreated = true
			}
			return c.Create(ctx, obj, opts...)
		},
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == names.ReceiveTaskName && taskCreated {
				return apierrors.NewNotFound(schema.GroupResource{Group: zfsv1.Group, Resource: "zfsreceivetasks"}, key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}, run, runSSHSecretForTest(run, names))

	result, err := r.Reconcile(context.Background(), request(run.Name))
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want nil after receive task create", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("RequeueAfter = %v, want receiver wait", result.RequeueAfter)
	}
}

func TestRunReconcileDoesNotLogAcceptedBeforeInitialStatusPersists(t *testing.T) {
	run := replicationRun("manual-accept-persist")
	r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*corev1.Secret); ok {
				return errors.New("temporary secret create failure")
			}
			return c.Create(ctx, obj, opts...)
		},
	}, run)
	ctx, logs := captureRunLogger()

	if _, err := r.Reconcile(ctx, request(run.Name)); err == nil || !strings.Contains(err.Error(), "temporary secret create failure") {
		t.Fatalf("Reconcile() error = %v, want temporary secret create failure", err)
	}
	assertNoLogEntry(t, logs, "accepted replication run")
}

func TestWaitForReplicationReceiverUsesFreshStatusForTransitionLogs(t *testing.T) {
	run := replicationRun("manual-fresh-wait")
	names := objectNamesForRun(run.Name)
	fresh := run.DeepCopy()
	now := metav1.Now()
	fresh.Status.StartedAt = &now
	fresh.Status.Phase = zfsv1.PhaseStartingReceiver
	fillRunStatusNames(&fresh.Status, names)
	r := newRunReconciler(t, run)
	r.APIReader = fake.NewClientBuilder().
		WithScheme(newTestScheme(t)).
		WithStatusSubresource(&zfsv1.ZFSReplicationRun{}).
		WithObjects(fresh).
		Build()
	ctx, logs := captureRunLogger()

	if _, err := r.waitForReplicationReceiver(ctx, run, names); err != nil {
		t.Fatal(err)
	}

	assertNoLogEntry(t, logs, "waiting for replication receiver")
	assertNoLogEntry(t, logs, "accepted replication run")
}

func TestRunReconcileLogsReceiverWait(t *testing.T) {
	run := replicationRun("manual-wait")
	names := objectNamesForRun(run.Name)
	r := newRunReconciler(t, run)
	ctx, logs := captureRunLogger()

	result, err := r.Reconcile(ctx, request(run.Name))
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("RequeueAfter = %v, want receiver wait", result.RequeueAfter)
	}

	assertLogEntry(t, logs, "waiting for replication receiver", map[string]string{
		"namespace":     run.Namespace,
		"run":           run.Name,
		"sourceDataset": run.Spec.Source.Dataset,
		"targetDataset": run.Spec.Target.Dataset,
		"senderJob":     names.SenderName,
		"receiveTask":   names.ReceiveTaskName,
	})
	assertLogEntry(t, logs, "accepted replication run", map[string]string{
		"namespace":     run.Namespace,
		"run":           run.Name,
		"sourceDataset": run.Spec.Source.Dataset,
		"targetDataset": run.Spec.Target.Dataset,
		"senderJob":     names.SenderName,
		"receiveTask":   names.ReceiveTaskName,
	})

	ctx, logs = captureRunLogger()
	result, err = r.Reconcile(ctx, request(run.Name))
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("second RequeueAfter = %v, want receiver wait", result.RequeueAfter)
	}
	assertNoLogEntry(t, logs, "accepted replication run")
	assertNoLogEntry(t, logs, "waiting for replication receiver")
}

func TestRunReconcileLogsSenderSuccess(t *testing.T) {
	run := replicationRun("manual-success")
	names := objectNamesForRun(run.Name)
	run.Status.ReceiverPodName = "zfs-receiver-worker-b"
	run.Status.ReceiverPodIP = "10.0.0.42"
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	sender := runSenderJob(run, names, "datamover:test", "10.0.0.42")
	sender.Status.Succeeded = 1
	r := newRunReconciler(t, run, task, sender)
	ctx, logs := captureRunLogger()

	if _, err := r.Reconcile(ctx, request(run.Name)); err != nil {
		t.Fatal(err)
	}

	assertLogEntry(t, logs, "sender job succeeded", map[string]string{
		"namespace":     run.Namespace,
		"run":           run.Name,
		"senderJob":     names.SenderName,
		"receiveTask":   names.ReceiveTaskName,
		"receiverPod":   "zfs-receiver-worker-b",
		"receiverPodIP": "10.0.0.42",
	})
}

func TestRunReconcileLogsSenderJobAlreadyPresent(t *testing.T) {
	run := replicationRun("manual-present")
	names := objectNamesForRun(run.Name)
	run.Status.ReceiverPodName = "zfs-receiver-worker-b"
	run.Status.ReceiverPodIP = "10.0.0.42"
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	sender := runSenderJob(run, names, "datamover:test", "10.0.0.42")
	r := newRunReconciler(t, run, task, sender)
	ctx, logs := captureRunLogger()

	if _, err := r.Reconcile(ctx, request(run.Name)); err != nil {
		t.Fatal(err)
	}

	assertNoLogEntry(t, logs, "sender job already present")
}

func TestRunReconcileLogsSenderFailure(t *testing.T) {
	run := replicationRun("manual-failure")
	names := objectNamesForRun(run.Name)
	run.Status.ReceiverPodName = "zfs-receiver-worker-b"
	run.Status.ReceiverPodIP = "10.0.0.42"
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	sender := runSenderJob(run, names, "datamover:test", "10.0.0.42")
	sender.Status.Failed = 1
	sender.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "syncoid exited with status 1"},
	}
	r := newRunReconciler(t, run, task, sender)
	ctx, logs := captureRunLogger()

	if _, err := r.Reconcile(ctx, request(run.Name)); err != nil {
		t.Fatal(err)
	}

	assertLogEntry(t, logs, "sender job failed", map[string]string{
		"namespace": run.Namespace,
		"run":       run.Name,
		"reason":    "syncoid exited with status 1",
	})
}

func TestRunReconcileBoundsSenderPodLogLastError(t *testing.T) {
	run := replicationRun("manual-log-tail")
	names := objectNamesForRun(run.Name)
	run.Status.ReceiverPodName = "zfs-receiver-worker-b"
	run.Status.ReceiverPodIP = "10.0.0.42"
	sender := runSenderJob(run, names, "datamover:test", "10.0.0.42")
	sender.Status.Failed = 1
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sender-pod",
			Namespace: run.Namespace,
			Labels: map[string]string{
				"job-name": names.SenderName,
			},
		},
	}
	oldOutput := `old-output-marker --sshkey="/var/run/zfsrep/ssh/id_rsa"`
	tailOutput := `tail-output-marker --sshkey="/var/run/zfsrep/ssh/id_rsa"`
	r := newRunReconciler(t, run, sender, pod)
	r.PodLogs = fakePodLogs{
		"sender-pod": "sender completed result=failure error=\"" + oldOutput + strings.Repeat("x", 70*1024) + tailOutput + "\"\n",
	}

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}

	var got zfsv1.ZFSReplicationRun
	if err := r.Get(context.Background(), request(run.Name).NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != zfsv1.PhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	if strings.Contains(got.Status.LastError, "old-output-marker") {
		t.Fatalf("lastError contains beginning of huge log line: %q", got.Status.LastError)
	}
	if !strings.Contains(got.Status.LastError, `tail-output-marker --sshkey=<redacted>`) {
		t.Fatalf("lastError missing redacted tail marker: %q", got.Status.LastError)
	}
	if strings.Contains(got.Status.LastError, "--sshkey=/var/run/zfsrep/ssh/id_rsa") {
		t.Fatalf("lastError contains unredacted ssh key path: %q", got.Status.LastError)
	}
}

func TestFailureMessageFromLogsKeepsConciseCriticalError(t *testing.T) {
	logs := "syncoid stderr CRITICAL ERROR: zfs send -w tank/src@syncoid_new_2026 | ssh -i /var/run/zfsrep/ssh/id_rsa zfs-recv@10.42.2.11 zfs receive -s -F -u missingpool/dst 2>&1 failed: 256\n"

	got := failureMessageFromLogs(logs)
	if !strings.Contains(got, "CRITICAL ERROR") {
		t.Fatalf("failureMessageFromLogs() = %q, want CRITICAL ERROR", got)
	}
	if !strings.Contains(got, "missingpool/dst") {
		t.Fatalf("failureMessageFromLogs() = %q, want target dataset", got)
	}
	if strings.Contains(got, "target=2>&1") {
		t.Fatalf("failureMessageFromLogs() chose shell redirection as target: %q", got)
	}
	if strings.Contains(got, "zfs send") || strings.Contains(got, "/var/run/zfsrep/ssh/id_rsa") {
		t.Fatalf("failureMessageFromLogs() contains raw pipeline or private key path: %q", got)
	}
}

func TestRunReconcileRedactsQuotedJobFailedConditionWithoutPodLogs(t *testing.T) {
	run := replicationRun("manual-fallback-redaction")
	names := objectNamesForRun(run.Name)
	run.Status.ReceiverPodName = "zfs-receiver-worker-b"
	run.Status.ReceiverPodIP = "10.0.0.42"
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	sender := runSenderJob(run, names, "datamover:test", "10.0.0.42")
	sender.Status.Failed = 1
	sender.Status.Conditions = []batchv1.JobCondition{
		{
			Type:    batchv1.JobFailed,
			Status:  corev1.ConditionTrue,
			Message: `syncoid exited with status 1 --sshkey=\"/var/run/zfsrep/ssh/id_rsa\"`,
		},
	}
	r := newRunReconciler(t, run, task, sender)
	ctx, logs := captureRunLogger()

	if _, err := r.Reconcile(ctx, request(run.Name)); err != nil {
		t.Fatal(err)
	}

	var got zfsv1.ZFSReplicationRun
	if err := r.Get(context.Background(), request(run.Name).NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.LastError != `syncoid exited with status 1 --sshkey=<redacted>` {
		t.Fatalf("lastError = %q", got.Status.LastError)
	}
	assertLogEntry(t, logs, "sender job failed", map[string]string{
		"namespace": run.Namespace,
		"run":       run.Name,
		"reason":    `syncoid exited with status 1 --sshkey=<redacted>`,
	})
}

func TestRunReconcileLogsDestinationWaitOnlyOnTransition(t *testing.T) {
	run := replicationRun("manual-destination-wait")
	other := replicationRun("manual-active")
	other.Status.Phase = zfsv1.PhaseRunning
	names := objectNamesForRun(run.Name)
	r := newRunReconciler(t, run, other)
	ctx, logs := captureRunLogger()

	result, err := r.Reconcile(ctx, request(run.Name))
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("RequeueAfter = %v, want destination wait", result.RequeueAfter)
	}
	wantReason := "waiting for active run manual-active to finish receiving into tank/dst on worker-b"
	assertLogEntry(t, logs, "waiting for replication destination", map[string]string{
		"namespace":     run.Namespace,
		"run":           run.Name,
		"sourceDataset": run.Spec.Source.Dataset,
		"targetDataset": run.Spec.Target.Dataset,
		"senderJob":     names.SenderName,
		"receiveTask":   names.ReceiveTaskName,
		"reason":        wantReason,
	})

	var got zfsv1.ZFSReplicationRun
	if err := r.Get(context.Background(), request(run.Name).NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != zfsv1.PhasePending || got.Status.LastError != wantReason {
		t.Fatalf("status = phase %q lastError %q, want Pending/%q", got.Status.Phase, got.Status.LastError, wantReason)
	}
	assertObjectDeleted(t, r.Client, &batchv1.Job{}, names.SenderName)

	ctx, logs = captureRunLogger()
	result, err = r.Reconcile(ctx, request(run.Name))
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("second RequeueAfter = %v, want destination wait", result.RequeueAfter)
	}
	assertNoLogEntry(t, logs, "waiting for replication destination")
}

func TestRunReconcileLogsTerminalCleanup(t *testing.T) {
	run := replicationRun("manual-terminal")
	run.Status.Phase = zfsv1.PhaseSucceeded
	r := newRunReconciler(t, run)
	ctx, logs := captureRunLogger()

	if _, err := r.Reconcile(ctx, request(run.Name)); err != nil {
		t.Fatal(err)
	}

	assertLogEntry(t, logs, "cleaning up terminal replication run", map[string]string{
		"namespace": run.Namespace,
		"run":       run.Name,
		"phase":     string(zfsv1.PhaseSucceeded),
	})
}

func TestRunReconcileRetriesCleanupForTerminalRun(t *testing.T) {
	for _, phase := range []zfsv1.Phase{zfsv1.PhaseSucceeded, zfsv1.PhaseFailed} {
		t.Run(string(phase)+"/secret delete failure", func(t *testing.T) {
			run := replicationRun("manual-" + strings.ToLower(string(phase)) + "-secret")
			run.Status.Phase = phase
			names := objectNamesForRun(run.Name)
			receiveTask := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
			sshSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: names.SecretName, Namespace: run.Namespace}}
			deleteSecretFailures := 1
			r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if obj.GetName() == names.SecretName && deleteSecretFailures > 0 {
						deleteSecretFailures--
						return errors.New("temporary secret delete failure")
					}
					return c.Delete(ctx, obj, opts...)
				},
			}, run, receiveTask, sshSecret)

			if _, err := r.Reconcile(context.Background(), request(run.Name)); err == nil || !strings.Contains(err.Error(), "temporary secret delete failure") {
				t.Fatalf("Reconcile() error = %v, want cleanup secret delete error", err)
			}
			assertObjectExists(t, r.Client, &corev1.Secret{}, names.SecretName)

			if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
				t.Fatalf("second Reconcile() error = %v, want nil", err)
			}
			assertObjectDeleted(t, r.Client, &corev1.Secret{}, names.SecretName)
			assertReceiveTaskPhase(t, r.Client, names.ReceiveTaskName, phase.ReceiveTaskTerminalPhase())
		})

		t.Run(string(phase)+"/receiver pod delete failure", func(t *testing.T) {
			run := replicationRun("manual-" + strings.ToLower(string(phase)))
			run.Status.Phase = phase
			names := objectNamesForRun(run.Name)
			receiveTask := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
			sshSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: names.SecretName, Namespace: run.Namespace}}
			receiverPod := runReceiverPod(run, "10.0.0.42")
			deleteReceiverPodFailures := 1
			r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if obj.GetName() == receiverPod.Name && deleteReceiverPodFailures > 0 {
						deleteReceiverPodFailures--
						return errors.New("temporary receiver pod delete failure")
					}
					return c.Delete(ctx, obj, opts...)
				},
			}, run, receiveTask, sshSecret, receiverPod)

			if _, err := r.Reconcile(context.Background(), request(run.Name)); err == nil || !strings.Contains(err.Error(), "temporary receiver pod delete failure") {
				t.Fatalf("Reconcile() error = %v, want cleanup pod delete error", err)
			}
			assertObjectExists(t, r.Client, &corev1.Pod{}, receiverPod.Name)
			assertObjectDeleted(t, r.Client, &corev1.Secret{}, names.SecretName)

			if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
				t.Fatalf("second Reconcile() error = %v, want nil", err)
			}
			assertObjectDeleted(t, r.Client, &corev1.Pod{}, receiverPod.Name)
			assertObjectDeleted(t, r.Client, &corev1.Secret{}, names.SecretName)
			assertReceiveTaskPhase(t, r.Client, names.ReceiveTaskName, phase.ReceiveTaskTerminalPhase())
		})
	}
}

func TestRunReconcileCleansUpReceiverPodForTerminalRun(t *testing.T) {
	run := replicationRun("manual-cleanup")
	run.Status.Phase = zfsv1.PhaseSucceeded
	names := objectNamesForRun(run.Name)
	receiveTask := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	sshSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: names.SecretName, Namespace: run.Namespace}}
	receiverPod := runReceiverPod(run, "10.0.0.42")
	r := newRunReconciler(t, run, receiveTask, sshSecret, receiverPod)

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	assertObjectDeleted(t, r.Client, &corev1.Pod{}, receiverPod.Name)
	assertObjectDeleted(t, r.Client, &corev1.Secret{}, names.SecretName)
}

func TestRunReconcileCleansUpOrphanReceiverPodForTerminalRun(t *testing.T) {
	run := replicationRun("manual-orphan-cleanup")
	run.Status.Phase = zfsv1.PhaseSucceeded
	receiverPod := runReceiverPod(run, "10.0.0.42")
	r := newRunReconciler(t, run, receiverPod)

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	assertObjectDeleted(t, r.Client, &corev1.Pod{}, receiverPod.Name)
}

func TestRunValidationAllowsSameDatasetOnDifferentNodes(t *testing.T) {
	spec := replicationRun("manual-1").Spec
	spec.Target.Dataset = spec.Source.Dataset

	if err := validateRunSpec(spec); err != nil {
		t.Fatalf("validateRunSpec() error = %v, want nil", err)
	}
}

func TestRunValidationRejectsSameDatasetOnSameNode(t *testing.T) {
	spec := replicationRun("manual-1").Spec
	spec.Target.NodeName = spec.Source.NodeName
	spec.Target.Dataset = spec.Source.Dataset

	err := validateRunSpec(spec)
	if err == nil || err.Error() != "source and target must not reference the same dataset on the same node" {
		t.Fatalf("validateRunSpec() error = %v", err)
	}
}

func TestRunValidationRejectsReceiverUnsafeDatasets(t *testing.T) {
	for _, dataset := range []string{
		"tank/a#b",
		"tank/a*b",
		"tank/a\"b",
		"tank/a[b",
		"tank/a\x01b",
	} {
		t.Run(dataset, func(t *testing.T) {
			spec := replicationRun("manual-1").Spec
			spec.Target.Dataset = dataset

			err := validateRunSpec(spec)
			if err == nil || !strings.Contains(err.Error(), "spec.target.dataset") {
				t.Fatalf("validateRunSpec() error = %v, want target dataset rejection", err)
			}
		})
	}
}

func TestDestinationLockedHandlesOverlappingTargetDatasets(t *testing.T) {
	for _, tt := range []struct {
		name               string
		targetDataset      string
		otherTargetNode    string
		otherTargetDataset string
		wantLocked         bool
	}{
		{
			name:               "same dataset",
			targetDataset:      "tank/dst",
			otherTargetNode:    "worker-b",
			otherTargetDataset: "tank/dst",
			wantLocked:         true,
		},
		{
			name:               "active parent blocks child",
			targetDataset:      "tank/dst/child",
			otherTargetNode:    "worker-b",
			otherTargetDataset: "tank/dst",
			wantLocked:         true,
		},
		{
			name:               "active child blocks parent",
			targetDataset:      "tank/dst",
			otherTargetNode:    "worker-b",
			otherTargetDataset: "tank/dst/child",
			wantLocked:         true,
		},
		{
			name:               "siblings do not block",
			targetDataset:      "tank/dst/a",
			otherTargetNode:    "worker-b",
			otherTargetDataset: "tank/dst/b",
			wantLocked:         false,
		},
		{
			name:               "same hierarchy on different node does not block",
			targetDataset:      "tank/dst/child",
			otherTargetNode:    "worker-c",
			otherTargetDataset: "tank/dst",
			wantLocked:         false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := replicationRun("manual-1")
			run.Spec.Target.Dataset = tt.targetDataset
			other := replicationRun("manual-2")
			other.Spec.Target.NodeName = tt.otherTargetNode
			other.Spec.Target.Dataset = tt.otherTargetDataset
			other.Status.Phase = zfsv1.PhaseRunning
			r := newRunReconciler(t, run, other)

			locked, _, err := r.destinationLocked(context.Background(), run)
			if err != nil {
				t.Fatal(err)
			}
			if locked != tt.wantLocked {
				t.Fatalf("destinationLocked() locked = %v, want %v", locked, tt.wantLocked)
			}
		})
	}
}

func replicationRun(name string) *zfsv1.ZFSReplicationRun {
	return &zfsv1.ZFSReplicationRun{
		TypeMeta:   metav1.TypeMeta{APIVersion: zfsv1.Group + "/" + zfsv1.Version, Kind: "ZFSReplicationRun"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "storage"},
		Spec: zfsv1.ZFSReplicationRunSpec{
			Source: zfsv1.DatasetRef{NodeName: "worker-a", Dataset: "tank/src"},
			Target: zfsv1.DatasetRef{NodeName: "worker-b", Dataset: "tank/dst"},
			Syncoid: zfsv1.SyncoidSpec{
				NoSyncSnap:       ptr(true),
				NoRollback:       ptr(true),
				Compress:         "zstd",
				ReceiveUnmounted: ptr(false),
				ReceiveResumable: ptr(false),
				IncludeSnaps:     []string{"^snap-.*", "^manual$"},
				ExcludeSnaps:     []string{".*-tmp$"},
			},
		},
	}
}

func runReceiverPod(run *zfsv1.ZFSReplicationRun, ip string) *corev1.Pod {
	names := objectNamesForRun(run.Name)
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "receiver"
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "receiver-pod", Namespace: run.Namespace, Labels: labels},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: ip,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func readyReceiveTask(run *zfsv1.ZFSReplicationRun, names runObjects, host, hostKey string) *zfsv1.ZFSReceiveTask {
	return &zfsv1.ZFSReceiveTask{
		TypeMeta: metav1.TypeMeta{APIVersion: zfsv1.Group + "/" + zfsv1.Version, Kind: "ZFSReceiveTask"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      names.ReceiveTaskName,
			Namespace: run.Namespace,
			Labels:    cloneLabels(names.Labels),
		},
		Spec: zfsv1.ZFSReceiveTaskSpec{
			RunRef:      zfsv1.LocalObjectReference{Name: run.Name},
			NodeName:    run.Spec.Target.NodeName,
			Destination: zfsv1.ReceiveDestination{Dataset: run.Spec.Target.Dataset},
			SSH: zfsv1.ReceiveTaskSSHSpec{
				AuthorizedPublicKey: "ssh-rsa AAAATEST zfsreplication-controller",
				ExpiresAt:           metav1.NewTime(time.Now().Add(time.Hour)),
			},
			Policy: zfsv1.ReceiveTaskPolicy{
				ReceiveUnmounted: true,
			},
		},
		Status: zfsv1.ZFSReceiveTaskStatus{
			Phase: zfsv1.ReceiveTaskPhaseReady,
			Endpoint: zfsv1.ReceiveTaskEndpoint{
				Host: host,
				Port: 2222,
			},
			SSH: zfsv1.ReceiveTaskSSHStatus{HostKey: hostKey},
			ReceiverPod: zfsv1.ReceiveTaskPodStatus{
				Name: "zfs-receiver-worker-b",
				UID:  "pod-uid",
			},
		},
	}
}

func runSSHSecretForTest(run *zfsv1.ZFSReplicationRun, names runObjects) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      names.SecretName,
			Namespace: run.Namespace,
			Labels:    cloneLabels(names.Labels),
		},
		Data: map[string][]byte{
			"id_rsa":     []byte("test-private-key"),
			"id_rsa.pub": []byte("ssh-rsa AAAATEST zfsreplication-controller"),
		},
	}
}

func newRunReconciler(t *testing.T, objs ...client.Object) *ZFSReplicationRunReconciler {
	t.Helper()
	scheme := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&zfsv1.ZFSReplicationRun{}, &zfsv1.ZFSReceiveTask{}).WithObjects(objs...).Build()
	return &ZFSReplicationRunReconciler{Client: c, Scheme: scheme, DataMoverImage: "datamover:test"}
}

func newRunReconcilerWithInterceptors(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) *ZFSReplicationRunReconciler {
	t.Helper()
	scheme := newTestScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&zfsv1.ZFSReplicationRun{}, &zfsv1.ZFSReceiveTask{}).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
	return &ZFSReplicationRunReconciler{Client: c, Scheme: scheme, DataMoverImage: "datamover:test"}
}

func newScheduleReconciler(t *testing.T, now time.Time, objs ...client.Object) *ZFSReplicationScheduleReconciler {
	t.Helper()
	scheme := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&zfsv1.ZFSReplicationSchedule{}).WithObjects(objs...).Build()
	return &ZFSReplicationScheduleReconciler{Client: c, Scheme: scheme, Now: func() time.Time { return now }}
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := zfsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return scheme
}

type capturedLogs struct {
	entries []map[string]any
}

type fakePodLogs map[string]string

func (f fakePodLogs) Logs(_ context.Context, _, podName string) (string, error) {
	return f[podName], nil
}

func captureRunLogger() (context.Context, *capturedLogs) {
	var logs capturedLogs
	logger := funcr.NewJSON(func(obj string) {
		entry := map[string]any{}
		if err := json.Unmarshal([]byte(obj), &entry); err == nil {
			logs.entries = append(logs.entries, entry)
		}
	}, funcr.Options{})
	return log.IntoContext(context.Background(), logger), &logs
}

func assertLogEntry(t *testing.T, logs *capturedLogs, msg string, fields map[string]string) {
	t.Helper()
	for _, entry := range logs.entries {
		if entry["msg"] != msg {
			continue
		}
		if logEntryHasFields(entry, fields) {
			return
		}
	}
	t.Fatalf("logs did not contain %q with fields %#v; got %#v", msg, fields, logs.entries)
}

func assertNoLogEntry(t *testing.T, logs *capturedLogs, msg string) {
	t.Helper()
	for _, entry := range logs.entries {
		if entry["msg"] == msg {
			t.Fatalf("logs contained %q: %#v", msg, logs.entries)
		}
	}
}

func logEntryHasFields(entry map[string]any, fields map[string]string) bool {
	for key, want := range fields {
		got, ok := entry[key]
		if !ok || got != want {
			return false
		}
	}
	return true
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func request(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "storage"}}
}

func getJob(t *testing.T, c client.Client, name string) *batchv1.Job {
	t.Helper()
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "storage"}, &job); err != nil {
		t.Fatal(err)
	}
	return &job
}

func assertObjectExists(t *testing.T, c client.Client, obj client.Object, name string) {
	t.Helper()
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "storage"}, obj); err != nil {
		t.Fatal(err)
	}
}

func assertObjectDeleted(t *testing.T, c client.Client, obj client.Object, name string) {
	t.Helper()
	err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "storage"}, obj)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("Get(%s) error = %v, want not found", name, err)
	}
}

func assertReceiveTaskPhase(t *testing.T, c client.Client, name string, phase zfsv1.ReceiveTaskPhase) {
	t.Helper()
	var task zfsv1.ZFSReceiveTask
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "storage"}, &task); err != nil {
		t.Fatal(err)
	}
	if task.Status.Phase != phase {
		t.Fatalf("task phase = %q, want %q", task.Status.Phase, phase)
	}
}

func envValue(job *batchv1.Job, name string) string {
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == name {
			return env.Value
		}
	}
	return ""
}

func ptr[T any](v T) *T { return &v }
