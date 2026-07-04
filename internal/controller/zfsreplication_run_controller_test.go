package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestRunReconcileSenderJobUsesSyncoidOptions(t *testing.T) {
	run := replicationRun("manual-1")
	names := objectNamesForRun(run.Name)
	r := newRunReconciler(t, run, readyReceiveTask(run, names, "10.0.0.42", "ssh-ed25519 AAAATEST receiver"))
	if _, err := r.Reconcile(context.Background(), request("manual-1")); err != nil {
		t.Fatal(err)
	}
	sender := getJob(t, r.Client, "zfsrep-manual-1-sender")
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
	if got := envValue(sender, "SYNCOID_INCLUDE_SNAPS"); got != "^snap-.*\n^manual$" {
		t.Fatalf("SYNCOID_INCLUDE_SNAPS = %q", got)
	}
	if got := envValue(sender, "SYNCOID_EXCLUDE_SNAPS"); got != ".*-tmp$" {
		t.Fatalf("SYNCOID_EXCLUDE_SNAPS = %q", got)
	}
	if hasEnv(sender, "ZFSREP_MANAGED_SNAPSHOT") {
		t.Fatalf("sender still has stale ZFSREP_MANAGED_SNAPSHOT env")
	}
	var secret corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Name: names.SecretName, Namespace: run.Namespace}, &secret); err != nil {
		t.Fatal(err)
	}
	if got := string(secret.Data["known_hosts"]); got != "[10.0.0.42]:2222 ssh-ed25519 AAAATEST receiver\n" {
		t.Fatalf("known_hosts = %q", got)
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
	if task.Spec.Policy.AllowSyncSnapshotDestroy {
		t.Fatal("task allows Syncoid snapshot pruning when noSyncSnap is true")
	}
	if task.Spec.Policy.Compression != "zstd" {
		t.Fatalf("task compression = %q, want zstd", task.Spec.Policy.Compression)
	}
	assertObjectDeleted(t, r.Client, &batchv1.Job{}, names.SenderName)
	assertObjectDeleted(t, r.Client, &batchv1.Job{}, names.ReceiverName)
}

func TestRunReconcileRetriesCleanupForTerminalRun(t *testing.T) {
	for _, phase := range []zfsv1.Phase{zfsv1.PhaseSucceeded, zfsv1.PhaseFailed} {
		t.Run(string(phase)+"/secret delete failure", func(t *testing.T) {
			run := replicationRun("manual-" + strings.ToLower(string(phase)) + "-secret")
			run.Status.Phase = phase
			names := objectNamesForRun(run.Name)
			receiveTask := readyReceiveTask(run, names, "10.0.0.42", "ssh-ed25519 AAAATEST receiver")
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
			assertReceiveTaskPhase(t, r.Client, names.ReceiveTaskName, receiveTaskTerminalPhase(phase))
		})

		t.Run(string(phase)+"/receiver pod delete failure", func(t *testing.T) {
			run := replicationRun("manual-" + strings.ToLower(string(phase)))
			run.Status.Phase = phase
			names := objectNamesForRun(run.Name)
			receiveTask := readyReceiveTask(run, names, "10.0.0.42", "ssh-ed25519 AAAATEST receiver")
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
			assertReceiveTaskPhase(t, r.Client, names.ReceiveTaskName, receiveTaskTerminalPhase(phase))
		})
	}
}

func TestRunReconcileCleansUpReceiverPodForTerminalRun(t *testing.T) {
	run := replicationRun("manual-cleanup")
	run.Status.Phase = zfsv1.PhaseSucceeded
	names := objectNamesForRun(run.Name)
	receiveTask := readyReceiveTask(run, names, "10.0.0.42", "ssh-ed25519 AAAATEST receiver")
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

func TestScheduleReconcileCreatesRunForDueSchedule(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 7, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.CreationTimestamp = metav1.NewTime(now.Add(-2 * time.Hour))
	r := newScheduleReconciler(t, now, schedule)

	if _, err := r.Reconcile(context.Background(), request("hourly")); err != nil {
		t.Fatal(err)
	}

	runName := scheduledRunName("hourly", time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC))
	var run zfsv1.ZFSReplicationRun
	if err := r.Get(context.Background(), types.NamespacedName{Name: runName, Namespace: "storage"}, &run); err != nil {
		t.Fatal(err)
	}
	if run.Spec.Source.Dataset != "tank/src" || run.Spec.Target.Dataset != "tank/dst" {
		t.Fatalf("run spec = %#v", run.Spec)
	}
	if len(run.OwnerReferences) != 1 || run.OwnerReferences[0].Name != "hourly" {
		t.Fatalf("ownerReferences = %#v", run.OwnerReferences)
	}
	if run.Labels[labelPrefix+"/schedule"] != "hourly" {
		t.Fatalf("labels = %#v", run.Labels)
	}
	var got zfsv1.ZFSReplicationSchedule
	if err := r.Get(context.Background(), types.NamespacedName{Name: "hourly", Namespace: "storage"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.LastScheduleTime == nil || !got.Status.LastScheduleTime.Time.Equal(time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("LastScheduleTime = %v", got.Status.LastScheduleTime)
	}
	if got.Status.LastRunName != runName {
		t.Fatalf("LastRunName = %q, want %q", got.Status.LastRunName, runName)
	}
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

func replicationSchedule(name string) *zfsv1.ZFSReplicationSchedule {
	return &zfsv1.ZFSReplicationSchedule{
		TypeMeta:   metav1.TypeMeta{APIVersion: zfsv1.Group + "/" + zfsv1.Version, Kind: "ZFSReplicationSchedule"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "storage"},
		Spec: zfsv1.ZFSReplicationScheduleSpec{
			Schedule:          "0 * * * *",
			ConcurrencyPolicy: zfsv1.ConcurrencyPolicyForbid,
			RunTemplate:       replicationRun("template").Spec,
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

func hasEnv(job *batchv1.Job, name string) bool {
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == name {
			return true
		}
	}
	return false
}

func ptr[T any](v T) *T { return &v }
