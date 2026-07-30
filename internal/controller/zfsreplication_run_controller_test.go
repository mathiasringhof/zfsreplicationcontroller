package controller

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/syncoid"
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

func TestRunReconcileSenderJobEmbedsSyncoidTranslation(t *testing.T) {
	run := replicationRun("manual-1")
	run.Spec.Syncoid.DeleteTargetSnapshots = ptr(true)
	names := objectNamesForRun(run.Name)
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	task.Status.Endpoint.Port = 2205
	pod := readyReceiverPodForTask(task)
	r := newRunReconciler(t, run, task, runSSHSecretForTest(run, names), pod)
	r.APIReader = newRunAPIReader(t, run, task, pod)
	if _, err := r.Reconcile(context.Background(), request("manual-1")); err != nil {
		t.Fatal(err)
	}
	sender := getJob(t, r.Client, "zfsrep-manual-1-sender")
	if got := sender.Spec.Template.Spec.Hostname; got != "zfsrep-sender" {
		t.Fatalf("sender pod hostname = %q, want zfsrep-sender", got)
	}
	container := sender.Spec.Template.Spec.Containers[0]
	if container.Name != "sender" {
		t.Fatalf("sender container name = %q, want sender", container.Name)
	}
	if container.Image != r.ReleaseImage {
		t.Fatalf("sender image = %q, want release image %q", container.Image, r.ReleaseImage)
	}
	if container.TerminationMessagePath != "/dev/termination-log" {
		t.Fatalf("termination message path = %q", container.TerminationMessagePath)
	}
	if container.TerminationMessagePolicy != corev1.TerminationMessageReadFile {
		t.Fatalf("termination message policy = %q, want File", container.TerminationMessagePolicy)
	}
	contract, err := syncoid.Translate(run)
	if err != nil {
		t.Fatal(err)
	}
	wantArguments, err := contract.SenderArguments(syncoid.Connection{
		TargetHost:     "zfs-recv@10.0.0.42",
		SSHKeyFile:     "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile: "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:        2205,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(container.Args, wantArguments) {
		t.Fatalf("sender arguments = %#v, want translated arguments %#v", container.Args, wantArguments)
	}
	if len(container.Env) != 0 {
		t.Fatalf("sender environment = %#v, want no private Sender protocol", container.Env)
	}
	var secret corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Name: names.SecretName, Namespace: run.Namespace}, &secret); err != nil {
		t.Fatal(err)
	}
	gotKnownHosts := secret.Data["known_hosts"]
	if got := string(gotKnownHosts); !strings.HasPrefix(got, "[10.0.0.42]:2205 ssh-ed25519 ") {
		t.Fatalf("known_hosts = %q, want bracketed receiver endpoint", got)
	}
	_, hosts, parsedKey, comment, rest, err := ssh.ParseKnownHosts(gotKnownHosts)
	if err != nil {
		t.Fatalf("parse known_hosts: %v", err)
	}
	if len(hosts) != 1 || hosts[0] != "[10.0.0.42]:2205" {
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
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	run := replicationRun("manual-1")
	run.Spec.Syncoid.DeleteTargetSnapshots = ptr(true)
	names := objectNamesForRun(run.Name)
	r := newRunReconciler(t, run)
	r.now = func() time.Time { return now }

	result, err := r.Reconcile(context.Background(), request("manual-1"))
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("RequeueAfter = %v, want receiver wait", result.RequeueAfter)
	}
	if _, err := r.Reconcile(context.Background(), request("manual-1")); err != nil {
		t.Fatal(err)
	}

	var task zfsv1.ZFSReceiveTask
	if err := r.Get(context.Background(), types.NamespacedName{Name: names.ReceiveTaskName, Namespace: run.Namespace}, &task); err != nil {
		t.Fatal(err)
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
	if want := now.Add(30 * time.Minute); !task.Spec.SSH.ExpiresAt.Time.Equal(want) {
		t.Fatalf("task expiresAt = %s, want controller time plus 30 minutes %s", task.Spec.SSH.ExpiresAt.Time, want)
	}
	contract, err := syncoid.Translate(run)
	if err != nil {
		t.Fatal(err)
	}
	if task.Spec.Policy != contract.ReceiverPolicy() {
		t.Fatalf("task policy = %#v, want translated policy %#v", task.Spec.Policy, contract.ReceiverPolicy())
	}
	assertObjectDeleted(t, r.Client, &batchv1.Job{}, names.SenderName)
	assertRunPhase(t, r.Client, run.Name, zfsv1.PhaseStartingReceiver)
}

func TestRunReconcileCreatesOwnedChildrenWithoutRunLabels(t *testing.T) {
	run := replicationRun("owned-children")
	names := objectNamesForRun(run.Name)
	r := newRunReconciler(t, run)

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}

	var secret corev1.Secret
	if err := r.Get(context.Background(), types.NamespacedName{Name: names.SecretName, Namespace: run.Namespace}, &secret); err != nil {
		t.Fatal(err)
	}
	var task zfsv1.ZFSReceiveTask
	if err := r.Get(context.Background(), types.NamespacedName{Name: names.ReceiveTaskName, Namespace: run.Namespace}, &task); err != nil {
		t.Fatal(err)
	}
	assertControlledByRun(t, &secret, run)
	assertControlledByRun(t, &task, run)
	assertOnlyRoleLabel(t, &secret, "ssh")
	assertOnlyRoleLabel(t, &task, "receiver")

	task.UID = "task-owned-children"
	if err := r.Update(context.Background(), &task); err != nil {
		t.Fatal(err)
	}
	task.Status = readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey).Status
	if err := r.Status().Update(context.Background(), &task); err != nil {
		t.Fatal(err)
	}
	pod := readyReceiverPodForTask(&task)
	if err := r.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}

	var job batchv1.Job
	if err := r.Get(context.Background(), types.NamespacedName{Name: names.SenderName, Namespace: run.Namespace}, &job); err != nil {
		t.Fatal(err)
	}
	assertControlledByRun(t, &job, run)
	assertOnlyRoleLabel(t, &job, "sender")
	if _, exists := job.Spec.Template.Labels[labelPrefix+"/run"]; exists {
		t.Fatalf("sender Pod template labels retain per-run relationship: %#v", job.Spec.Template.Labels)
	}
}

func TestRunReconcileFailsForForeignSameNameChildWithoutMutation(t *testing.T) {
	for _, kind := range []string{"Secret", "Receive Task", "sender Job"} {
		t.Run(kind, func(t *testing.T) {
			run := replicationRun("foreign-" + strings.ReplaceAll(strings.ToLower(kind), " ", "-"))
			names := objectNamesForRun(run.Name)
			foreignOwner := replicationRun("foreign-owner")
			foreignOwner.UID = "foreign-owner-uid"

			var foreign client.Object
			switch kind {
			case "Secret":
				foreign = runSSHSecretForTest(run, names)
				foreign.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(foreignOwner, zfsv1.SchemeGroupVersion.WithKind("ZFSReplicationRun"))})
				foreign.SetLabels(map[string]string{"keep": "secret"})
			case "Receive Task":
				foreign = readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
				foreign.SetOwnerReferences(nil)
				foreign.SetLabels(map[string]string{"keep": "task"})
			case "sender Job":
				foreign = mustSenderJob(t, run, "sender:test", "10.0.0.42")
				foreign.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(foreignOwner, zfsv1.SchemeGroupVersion.WithKind("ZFSReplicationRun"))})
				foreign.SetLabels(map[string]string{"keep": "job"})
			}
			r := newRunReconciler(t, run, foreign)

			if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
				t.Fatal(err)
			}

			var got zfsv1.ZFSReplicationRun
			if err := r.Get(context.Background(), request(run.Name).NamespacedName, &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.Phase != zfsv1.PhaseFailed || !strings.Contains(got.Status.LastError, "is not controlled by Replication Run") {
				t.Fatalf("run status = %#v, want foreign-owner Sender Failure Message", got.Status)
			}
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(foreign), foreign); err != nil {
				t.Fatalf("foreign %s was deleted: %v", kind, err)
			}
			if foreign.GetLabels()["keep"] == "" {
				t.Fatalf("foreign %s was mutated: %#v", kind, foreign)
			}
		})
	}
}

func TestRunReconcileRecoversPreSenderChildrenSafely(t *testing.T) {
	for _, tt := range []struct {
		name          string
		secret        bool
		task          bool
		wantPhase     zfsv1.Phase
		wantSecret    bool
		wantTask      bool
		wantPublicKey string
		reconciles    int
	}{
		{name: "both present", secret: true, task: true, wantPhase: zfsv1.PhaseStartingReceiver, wantSecret: true, wantTask: true, wantPublicKey: "ssh-rsa AAAATEST zfsreplication-controller", reconciles: 1},
		{name: "Secret only", secret: true, wantPhase: zfsv1.PhaseStartingReceiver, wantSecret: true, wantTask: true, wantPublicKey: "ssh-rsa AAAATEST zfsreplication-controller", reconciles: 1},
		{name: "neither present", wantPhase: zfsv1.PhaseStartingReceiver, wantSecret: true, wantTask: true, reconciles: 2},
		{name: "Receive Task only", task: true, wantPhase: zfsv1.PhaseFailed, wantTask: true, reconciles: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := replicationRun("recovery-" + strings.ReplaceAll(strings.ToLower(tt.name), " ", "-"))
			names := objectNamesForRun(run.Name)
			var objects []client.Object
			objects = append(objects, run)
			if tt.secret {
				objects = append(objects, runSSHSecretForTest(run, names))
			}
			if tt.task {
				task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
				task.Status = zfsv1.ZFSReceiveTaskStatus{}
				objects = append(objects, task)
			}
			r := newRunReconciler(t, objects...)

			for i := 0; i < tt.reconciles; i++ {
				if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
					t.Fatal(err)
				}
			}

			assertRunPhase(t, r.Client, run.Name, tt.wantPhase)
			if tt.wantSecret {
				assertObjectExists(t, r.Client, &corev1.Secret{}, names.SecretName)
			} else {
				assertObjectDeleted(t, r.Client, &corev1.Secret{}, names.SecretName)
			}
			if tt.wantTask {
				var task zfsv1.ZFSReceiveTask
				if err := r.Get(context.Background(), types.NamespacedName{Name: names.ReceiveTaskName, Namespace: run.Namespace}, &task); err != nil {
					t.Fatal(err)
				}
				if tt.wantPublicKey != "" && task.Spec.SSH.AuthorizedPublicKey != tt.wantPublicKey {
					t.Fatalf("recovered public key = %q, want %q", task.Spec.SSH.AuthorizedPublicKey, tt.wantPublicKey)
				}
			} else {
				assertObjectDeleted(t, r.Client, &zfsv1.ZFSReceiveTask{}, names.ReceiveTaskName)
			}
		})
	}
}

func TestReceiveTaskLeaseSchedulesAndRenewsAtPolicyDeadline(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name        string
		remaining   time.Duration
		wantExpiry  time.Time
		wantRequeue time.Duration
	}{
		{
			name:        "schedule before renewal window",
			remaining:   11 * time.Minute,
			wantExpiry:  now.Add(11 * time.Minute),
			wantRequeue: time.Minute,
		},
		{
			name:        "renew at ten minutes remaining",
			remaining:   10 * time.Minute,
			wantExpiry:  now.Add(30 * time.Minute),
			wantRequeue: 20 * time.Minute,
		},
		{
			name:        "renew inside renewal window",
			remaining:   9 * time.Minute,
			wantExpiry:  now.Add(30 * time.Minute),
			wantRequeue: 20 * time.Minute,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := replicationRun("lease-" + strings.ReplaceAll(tt.name, " ", "-"))
			names := objectNamesForRun(run.Name)
			task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
			task.Spec.SSH.ExpiresAt = metav1.NewTime(now.Add(tt.remaining))
			r := newRunReconciler(t, run, task)
			r.now = func() time.Time { return now }

			requeueAfter, msg, err := r.reconcileRunReceiveTaskLease(context.Background(), run, names)
			if err != nil {
				t.Fatal(err)
			}
			if msg != "" {
				t.Fatalf("lease message = %q, want empty", msg)
			}
			if requeueAfter != tt.wantRequeue {
				t.Fatalf("lease RequeueAfter = %v, want %v", requeueAfter, tt.wantRequeue)
			}
			var got zfsv1.ZFSReceiveTask
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(task), &got); err != nil {
				t.Fatal(err)
			}
			if !got.Spec.SSH.ExpiresAt.Time.Equal(tt.wantExpiry) {
				t.Fatalf("expiresAt = %s, want %s", got.Spec.SSH.ExpiresAt.Time, tt.wantExpiry)
			}
		})
	}
}

func TestReceiveTaskLeaseRefusesLapsedAndTerminalTasks(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name   string
		phase  zfsv1.ReceiveTaskPhase
		expiry time.Time
	}{
		{name: "lapsed", phase: zfsv1.ReceiveTaskPhaseReady, expiry: now},
		{name: "failed", phase: zfsv1.ReceiveTaskPhaseFailed, expiry: now.Add(5 * time.Minute)},
		{name: "completed", phase: zfsv1.ReceiveTaskPhaseCompleted, expiry: now.Add(5 * time.Minute)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := replicationRun("refuse-" + tt.name)
			names := objectNamesForRun(run.Name)
			task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
			task.Status.Phase = tt.phase
			task.Spec.SSH.ExpiresAt = metav1.NewTime(tt.expiry)
			r := newRunReconciler(t, run, task)
			r.now = func() time.Time { return now }

			_, msg, err := r.reconcileRunReceiveTaskLease(context.Background(), run, names)
			if err != nil {
				t.Fatal(err)
			}
			if msg == "" {
				t.Fatal("lease message is empty, want renewal refusal")
			}
			var got zfsv1.ZFSReceiveTask
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(task), &got); err != nil {
				t.Fatal(err)
			}
			if !got.Spec.SSH.ExpiresAt.Time.Equal(tt.expiry) {
				t.Fatalf("refused task expiresAt = %s, want unchanged %s", got.Spec.SSH.ExpiresAt.Time, tt.expiry)
			}
		})
	}
}

func TestRunReconcileDoesNotRenewTaskForTerminalRun(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	run := replicationRun("terminal-lease")
	run.Status.Phase = zfsv1.PhaseSucceeded
	names := objectNamesForRun(run.Name)
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	task.Spec.SSH.ExpiresAt = metav1.NewTime(now.Add(5 * time.Minute))
	r := newRunReconciler(t, run, task)
	r.now = func() time.Time { return now }

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}
	var got zfsv1.ZFSReceiveTask
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(task), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Spec.SSH.ExpiresAt.Time.Equal(task.Spec.SSH.ExpiresAt.Time) {
		t.Fatalf("terminal run renewed task to %s, want unchanged %s", got.Spec.SSH.ExpiresAt.Time, task.Spec.SSH.ExpiresAt.Time)
	}
}

func TestReceiveTaskLeaseUsesFreshRunStateToRefuseTerminalRun(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	cachedRun := replicationRun("fresh-terminal-lease")
	names := objectNamesForRun(cachedRun.Name)
	task := readyReceiveTask(cachedRun, names, "10.0.0.42", testReceiverHostKey)
	task.Spec.SSH.ExpiresAt = metav1.NewTime(now.Add(5 * time.Minute))
	freshRun := cachedRun.DeepCopy()
	freshRun.Status.Phase = zfsv1.PhaseSucceeded
	r := newRunReconciler(t, cachedRun, task)
	r.APIReader = newRunAPIReader(t, freshRun, task)
	r.now = func() time.Time { return now }

	requeueAfter, msg, err := r.reconcileRunReceiveTaskLease(context.Background(), cachedRun, names)
	if err != nil {
		t.Fatal(err)
	}
	if requeueAfter != 0 || msg != "" {
		t.Fatalf("terminal fresh run lease result = (%v, %q), want no renewal schedule", requeueAfter, msg)
	}
	var got zfsv1.ZFSReceiveTask
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(task), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Spec.SSH.ExpiresAt.Time.Equal(task.Spec.SSH.ExpiresAt.Time) {
		t.Fatalf("fresh terminal run renewed task to %s, want unchanged %s", got.Spec.SSH.ExpiresAt.Time, task.Spec.SSH.ExpiresAt.Time)
	}
}

func TestRunningRunRequeuesAtExplicitLeaseRenewalDeadline(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	run := replicationRun("scheduled-renewal")
	run.Status.Phase = zfsv1.PhaseRunning
	names := objectNamesForRun(run.Name)
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	task.Spec.SSH.ExpiresAt = metav1.NewTime(now.Add(15 * time.Minute))
	sender := mustSenderJob(t, run, "sender:test", task.Status.Endpoint.Host)
	r := newRunReconciler(t, run, task, sender)
	r.now = func() time.Time { return now }

	result, err := r.Reconcile(context.Background(), request(run.Name))
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 5*time.Minute {
		t.Fatalf("RequeueAfter = %v, want explicit renewal deadline in 5m", result.RequeueAfter)
	}
}

func TestRunReconcileRechecksLeaseImmediatelyBeforeSenderCreation(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	run := replicationRun("sender-lease-recheck")
	names := objectNamesForRun(run.Name)
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	task.Spec.SSH.ExpiresAt = metav1.NewTime(now.Add(15 * time.Minute))
	pod := readyReceiverPodForTask(task)
	r := newRunReconciler(t, run, task, runSSHSecretForTest(run, names), pod)
	r.now = func() time.Time { return now }
	freshReads := 0
	r.APIReader = fake.NewClientBuilder().
		WithScheme(newTestScheme(t)).
		WithStatusSubresource(&zfsv1.ZFSReplicationRun{}, &zfsv1.ZFSReceiveTask{}).
		WithObjects(run, task, pod).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if got, ok := obj.(*zfsv1.ZFSReceiveTask); ok {
					freshReads++
					if freshReads == 3 {
						copy := task.DeepCopy()
						copy.Spec.SSH.ExpiresAt = metav1.NewTime(now)
						*got = *copy
						return nil
					}
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).Build()

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}
	if freshReads < 3 {
		t.Fatalf("fresh receive task reads = %d, want final pre-sender check", freshReads)
	}
	assertObjectDeleted(t, r.Client, &batchv1.Job{}, names.SenderName)
	assertRunPhase(t, r.Client, run.Name, zfsv1.PhaseFailed)
}

func TestRunReconcileGatesSenderCreationOnFreshExactReadyReceiverPod(t *testing.T) {
	for _, tt := range []struct {
		name       string
		mutateTask func(*zfsv1.ZFSReceiveTask)
		pod        func(*zfsv1.ZFSReceiveTask) *corev1.Pod
		wantPhase  zfsv1.Phase
	}{
		{
			name:      "missing Pod",
			wantPhase: zfsv1.PhaseStartingReceiver,
		},
		{
			name: "missing task UID",
			mutateTask: func(task *zfsv1.ZFSReceiveTask) {
				task.UID = ""
			},
			wantPhase: zfsv1.PhaseFailed,
		},
		{
			name: "Pod UID mismatch",
			pod: func(task *zfsv1.ZFSReceiveTask) *corev1.Pod {
				pod := readyReceiverPodForTask(task)
				pod.UID = "replacement-pod-uid"
				return pod
			},
			wantPhase: zfsv1.PhaseStartingReceiver,
		},
		{
			name: "Pod unready after container restart",
			pod: func(task *zfsv1.ZFSReceiveTask) *corev1.Pod {
				pod := readyReceiverPodForTask(task)
				pod.Status.Conditions[0].Status = corev1.ConditionFalse
				return pod
			},
			wantPhase: zfsv1.PhaseStartingReceiver,
		},
		{
			name: "Pod Ready condition missing",
			pod: func(task *zfsv1.ZFSReceiveTask) *corev1.Pod {
				pod := readyReceiverPodForTask(task)
				pod.Status.Conditions = nil
				return pod
			},
			wantPhase: zfsv1.PhaseStartingReceiver,
		},
		{
			name: "expired task lease",
			mutateTask: func(task *zfsv1.ZFSReceiveTask) {
				task.Spec.SSH.ExpiresAt = metav1.NewTime(time.Now().Add(-time.Second))
			},
			pod:       readyReceiverPodForTask,
			wantPhase: zfsv1.PhaseFailed,
		},
		{
			name: "missing endpoint host",
			mutateTask: func(task *zfsv1.ZFSReceiveTask) {
				task.Status.Endpoint.Host = ""
			},
			wantPhase: zfsv1.PhaseFailed,
		},
		{
			name: "missing endpoint port",
			mutateTask: func(task *zfsv1.ZFSReceiveTask) {
				task.Status.Endpoint.Port = 0
			},
			wantPhase: zfsv1.PhaseFailed,
		},
		{
			name: "missing host key",
			mutateTask: func(task *zfsv1.ZFSReceiveTask) {
				task.Status.SSH.HostKey = ""
			},
			wantPhase: zfsv1.PhaseFailed,
		},
		{
			name: "missing receiver Pod name",
			mutateTask: func(task *zfsv1.ZFSReceiveTask) {
				task.Status.ReceiverPod.Name = ""
			},
			wantPhase: zfsv1.PhaseFailed,
		},
		{
			name: "missing receiver Pod UID",
			mutateTask: func(task *zfsv1.ZFSReceiveTask) {
				task.Status.ReceiverPod.UID = ""
			},
			wantPhase: zfsv1.PhaseFailed,
		},
		{
			name: "terminating Pod",
			pod: func(task *zfsv1.ZFSReceiveTask) *corev1.Pod {
				pod := readyReceiverPodForTask(task)
				now := metav1.Now()
				pod.DeletionTimestamp = &now
				pod.Finalizers = []string{"test.example/finalizer"}
				return pod
			},
			wantPhase: zfsv1.PhaseStartingReceiver,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := replicationRun("manual-gate-" + strings.ReplaceAll(tt.name, " ", "-"))
			names := objectNamesForRun(run.Name)
			task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
			if tt.mutateTask != nil {
				tt.mutateTask(task)
			}
			objects := []client.Object{run, task, runSSHSecretForTest(run, names)}
			if tt.pod != nil {
				objects = append(objects, tt.pod(task))
			}
			r := newRunReconciler(t, objects...)

			if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
				t.Fatal(err)
			}

			assertObjectDeleted(t, r.Client, &batchv1.Job{}, names.SenderName)
			var got zfsv1.ZFSReplicationRun
			if err := r.Get(context.Background(), request(run.Name).NamespacedName, &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.Phase != tt.wantPhase {
				t.Fatalf("run phase = %q, want safe path %q", got.Status.Phase, tt.wantPhase)
			}
		})
	}
}

func TestRunReconcileRejectsStaleReadyTaskIncarnation(t *testing.T) {
	run := replicationRun("manual-stale-task")
	names := objectNamesForRun(run.Name)
	stale := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	live := stale.DeepCopy()
	live.UID = "replacement-task-uid"
	live.Status = zfsv1.ZFSReceiveTaskStatus{Phase: zfsv1.ReceiveTaskPhasePending}
	pod := readyReceiverPodForTask(stale)
	r := newRunReconciler(t, run, stale, runSSHSecretForTest(run, names), pod)
	r.APIReader = newRunAPIReader(t, run, live, pod)

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}

	assertObjectDeleted(t, r.Client, &batchv1.Job{}, names.SenderName)
	assertRunPhase(t, r.Client, run.Name, zfsv1.PhaseStartingReceiver)
}

func TestRunReconcileUsesFreshReceiverPodRead(t *testing.T) {
	run := replicationRun("manual-fresh-pod")
	names := objectNamesForRun(run.Name)
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	cachedReadyPod := readyReceiverPodForTask(task)
	liveUnreadyPod := cachedReadyPod.DeepCopy()
	liveUnreadyPod.Status.Conditions[0].Status = corev1.ConditionFalse
	r := newRunReconciler(t, run, task, runSSHSecretForTest(run, names), cachedReadyPod)
	r.APIReader = newRunAPIReader(t, run, task, liveUnreadyPod)

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}

	assertObjectDeleted(t, r.Client, &batchv1.Job{}, names.SenderName)
	assertRunPhase(t, r.Client, run.Name, zfsv1.PhaseStartingReceiver)
}

func TestRunReconcileGetsExactReceiverPodByNamespace(t *testing.T) {
	run := replicationRun("manual-exact-pod-get")
	names := objectNamesForRun(run.Name)
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	pod := readyReceiverPodForTask(task)
	r := newRunReconciler(t, run, task, runSSHSecretForTest(run, names), pod)
	r.APIReader = fake.NewClientBuilder().
		WithScheme(newTestScheme(t)).
		WithStatusSubresource(&zfsv1.ZFSReplicationRun{}, &zfsv1.ZFSReceiveTask{}).
		WithObjects(run, task, pod).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return errors.New("cluster-wide Pod list is not authorized")
			},
		}).
		Build()

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}

	assertObjectExists(t, r.Client, &batchv1.Job{}, names.SenderName)
}

func TestRunReconcileFailsClosedWithoutFreshReader(t *testing.T) {
	run := replicationRun("manual-no-fresh-reader")
	names := objectNamesForRun(run.Name)
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	pod := readyReceiverPodForTask(task)
	r := newRunReconciler(t, run, task, pod)
	r.APIReader = nil

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err == nil || !strings.Contains(err.Error(), "fresh API reader") {
		t.Fatalf("Reconcile() error = %v, want missing fresh API reader", err)
	}

	assertObjectDeleted(t, r.Client, &batchv1.Job{}, names.SenderName)
}

func TestRunReconcileLogsReceiverAndSenderLifecycle(t *testing.T) {
	run := replicationRun("manual-logs")
	names := objectNamesForRun(run.Name)
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	r := newRunReconciler(t, run, task, runSSHSecretForTest(run, names), readyReceiverPodForTask(task))
	ctx, logs := captureRunLogger()

	if _, err := r.Reconcile(ctx, request(run.Name)); err != nil {
		t.Fatal(err)
	}

	baseFields := map[string]string{
		"namespace":     run.Namespace,
		"run":           run.Name,
		"sourceNode":    run.Spec.Source.NodeName,
		"sourceDataset": run.Spec.Source.Dataset,
		"targetNode":    run.Spec.Target.NodeName,
		"targetDataset": run.Spec.Target.Dataset,
	}
	assertNoLogEntry(t, logs, "reconciling replication run")
	assertLogEntry(t, logs, "accepted replication run", baseFields)
	assertLogEntryExcludesFields(t, logs, "accepted replication run", "senderJob", "receiveTask", "sshSecret", "syncoidIdentifier", "receiverPod")
	receiverFields := cloneStringMap(baseFields)
	receiverFields["receiveTask"] = names.ReceiveTaskName
	receiverFields["receiverPod"] = "zfs-receiver-worker-b"
	assertLogEntry(t, logs, "replication receiver is ready", receiverFields)
	senderFields := cloneStringMap(baseFields)
	senderFields["senderJob"] = names.SenderName
	senderFields["receiverPod"] = "zfs-receiver-worker-b"
	assertLogEntry(t, logs, "created sender job", senderFields)
}

func TestRunReconcileDoesNotRecheckSenderJobImmediatelyAfterCreate(t *testing.T) {
	run := replicationRun("manual-create-cache")
	names := objectNamesForRun(run.Name)
	jobCreated := false
	hideCreatedJobGets := 1
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
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
	}, run, task, runSSHSecretForTest(run, names), readyReceiverPodForTask(task))
	ctx, logs := captureRunLogger()

	result, err := r.Reconcile(ctx, request(run.Name))
	if err != nil {
		t.Fatalf("Reconcile() error = %v, want nil after sender job create", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatalf("RequeueAfter = %v, want cache-visible sender job recheck", result.RequeueAfter)
	}
	assertLogEntry(t, logs, "replication receiver is ready", map[string]string{
		"namespace":   run.Namespace,
		"run":         run.Name,
		"receiveTask": names.ReceiveTaskName,
		"receiverPod": "zfs-receiver-worker-b",
	})
	assertLogEntry(t, logs, "created sender job", map[string]string{
		"namespace":   run.Namespace,
		"run":         run.Name,
		"senderJob":   names.SenderName,
		"receiverPod": "zfs-receiver-worker-b",
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
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
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
	}, run, task, runSSHSecretForTest(run, names), readyReceiverPodForTask(task))

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
			objects := []client.Object{run}
			if tt.name == "receive task" {
				objects = append(objects, runSSHSecretForTest(run, names))
			}
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
			}, objects...)

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
	if !secretCreated {
		t.Fatal("SSH Secret was not created")
	}
	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReceiveTask{}, names.ReceiveTaskName)
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
	initializeRunStatus(&fresh.Status, names)
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
		"receiveTask":   names.ReceiveTaskName,
	})
	assertLogEntry(t, logs, "accepted replication run", map[string]string{
		"namespace":     run.Namespace,
		"run":           run.Name,
		"sourceDataset": run.Spec.Source.Dataset,
		"targetDataset": run.Spec.Target.Dataset,
	})
	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}
	var task zfsv1.ZFSReceiveTask
	if err := r.Get(context.Background(), types.NamespacedName{Name: names.ReceiveTaskName, Namespace: run.Namespace}, &task); err != nil {
		t.Fatal(err)
	}
	task.UID = "task-uid"
	if err := r.Update(context.Background(), &task); err != nil {
		t.Fatal(err)
	}

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
	sender := mustSenderJob(t, run, "sender:test", "10.0.0.42")
	sender.Status.Succeeded = 1
	r := newRunReconciler(t, run, task, sender)
	ctx, logs := captureRunLogger()

	if _, err := r.Reconcile(ctx, request(run.Name)); err != nil {
		t.Fatal(err)
	}

	assertLogEntry(t, logs, "replication succeeded", map[string]string{
		"namespace":   run.Namespace,
		"run":         run.Name,
		"senderJob":   names.SenderName,
		"receiverPod": "zfs-receiver-worker-b",
	})
}

func TestRunReconcileBindsAndEnforcesSenderJobUID(t *testing.T) {
	t.Run("discovered Job binds UID and continues", func(t *testing.T) {
		run := replicationRun("bind-job-uid")
		names := objectNamesForRun(run.Name)
		sender := mustSenderJob(t, run, "sender:test", "10.0.0.42")
		r := newRunReconciler(t, run, sender)

		if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
			t.Fatal(err)
		}

		var got zfsv1.ZFSReplicationRun
		if err := r.Get(context.Background(), request(run.Name).NamespacedName, &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Phase != zfsv1.PhaseRunning ||
			got.Status.SenderJobName != names.SenderName ||
			got.Status.SenderJobUID != string(sender.UID) {
			t.Fatalf("run status = %#v, want running with exact Job identity", got.Status)
		}
	})

	t.Run("matching recorded UID continues", func(t *testing.T) {
		run := replicationRun("matching-job-uid")
		sender := mustSenderJob(t, run, "sender:test", "10.0.0.42")
		run.Status.Phase = zfsv1.PhaseRunning
		run.Status.SenderJobName = sender.Name
		run.Status.SenderJobUID = string(sender.UID)
		r := newRunReconciler(t, run, sender)

		if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
			t.Fatal(err)
		}
		assertRunPhase(t, r.Client, run.Name, zfsv1.PhaseRunning)
	})

	t.Run("replacement UID fails without deleting replacement", func(t *testing.T) {
		run := replicationRun("replacement-job-uid")
		sender := mustSenderJob(t, run, "sender:test", "10.0.0.42")
		run.Status.Phase = zfsv1.PhaseRunning
		run.Status.SenderJobName = sender.Name
		run.Status.SenderJobUID = "original-job-uid"
		r := newRunReconciler(t, run, sender)

		if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
			t.Fatal(err)
		}

		var got zfsv1.ZFSReplicationRun
		if err := r.Get(context.Background(), request(run.Name).NamespacedName, &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Phase != zfsv1.PhaseFailed || !strings.Contains(got.Status.LastError, "differs from recorded UID original-job-uid") {
			t.Fatalf("run status = %#v, want replacement Job Sender Failure Message", got.Status)
		}
		if got.Status.SenderJobUID != "original-job-uid" {
			t.Fatalf("senderJobUID = %q, want write-once original UID", got.Status.SenderJobUID)
		}
		assertObjectExists(t, r.Client, &batchv1.Job{}, sender.Name)
	})

	t.Run("missing active Job fails", func(t *testing.T) {
		run := replicationRun("missing-active-job")
		names := objectNamesForRun(run.Name)
		run.Status.Phase = zfsv1.PhaseRunning
		run.Status.SenderJobName = names.SenderName
		run.Status.SenderJobUID = "missing-job-uid"
		r := newRunReconciler(t, run)

		if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
			t.Fatal(err)
		}

		var got zfsv1.ZFSReplicationRun
		if err := r.Get(context.Background(), request(run.Name).NamespacedName, &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Phase != zfsv1.PhaseFailed || !strings.Contains(got.Status.LastError, "with UID missing-job-uid is missing") {
			t.Fatalf("run status = %#v, want missing Job Sender Failure Message", got.Status)
		}
	})

	t.Run("terminal Run retains identity after TTL deletion", func(t *testing.T) {
		run := replicationRun("terminal-job-ttl")
		names := objectNamesForRun(run.Name)
		run.Status.Phase = zfsv1.PhaseSucceeded
		run.Status.SenderJobName = names.SenderName
		run.Status.SenderJobUID = "historical-job-uid"
		r := newRunReconciler(t, run)

		if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
			t.Fatal(err)
		}

		var got zfsv1.ZFSReplicationRun
		if err := r.Get(context.Background(), request(run.Name).NamespacedName, &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Phase != zfsv1.PhaseSucceeded ||
			got.Status.SenderJobName != names.SenderName ||
			got.Status.SenderJobUID != "historical-job-uid" {
			t.Fatalf("terminal run status = %#v, want retained Job identity", got.Status)
		}
	})
}

func TestRunReconcileTerminalSenderObservationRevokesAuthorityAndKeyMaterial(t *testing.T) {
	run := replicationRun("sender-terminal-cleanup")
	names := objectNamesForRun(run.Name)
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	secret := runSSHSecretForTest(run, names)
	sender := mustSenderJob(t, run, "sender:test", "10.0.0.42")
	sender.Status.Succeeded = 1
	r := newRunReconciler(t, run, task, secret, sender)

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}

	var got zfsv1.ZFSReplicationRun
	if err := r.Get(context.Background(), request(run.Name).NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != zfsv1.PhaseSucceeded ||
		got.Status.SenderJobName != sender.Name ||
		got.Status.SenderJobUID != string(sender.UID) {
		t.Fatalf("run status = %#v, want succeeded with exact Job identity", got.Status)
	}
	assertObjectDeleted(t, r.Client, &corev1.Secret{}, names.SecretName)
	assertReceiveTaskPhase(t, r.Client, names.ReceiveTaskName, zfsv1.ReceiveTaskPhaseCompleted)
	assertObjectExists(t, r.Client, &batchv1.Job{}, names.SenderName)
}

func TestRunReconcileObservesTerminalSenderBeforeLeaseRenewal(t *testing.T) {
	now := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	run := replicationRun("sender-terminal-before-renewal")
	run.Status.Phase = zfsv1.PhaseRunning
	names := objectNamesForRun(run.Name)
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	task.Spec.SSH.ExpiresAt = metav1.NewTime(now.Add(5 * time.Minute))
	sender := mustSenderJob(t, run, "sender:test", "10.0.0.42")
	sender.Status.Succeeded = 1
	renewalAttempts := 0
	r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*zfsv1.ZFSReceiveTask); ok {
				renewalAttempts++
				return errors.New("receive task update forbidden")
			}
			return c.Update(ctx, obj, opts...)
		},
	}, run, task, sender)
	r.now = func() time.Time { return now }

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}

	if renewalAttempts != 0 {
		t.Fatalf("receive task renewal attempts = %d, want none after sender completion", renewalAttempts)
	}
	assertRunPhase(t, r.Client, run.Name, zfsv1.PhaseSucceeded)
}

func TestRunReconcileLogsSenderJobAlreadyPresent(t *testing.T) {
	run := replicationRun("manual-present")
	names := objectNamesForRun(run.Name)
	run.Status.ReceiverPodName = "zfs-receiver-worker-b"
	run.Status.ReceiverPodIP = "10.0.0.42"
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	sender := mustSenderJob(t, run, "sender:test", "10.0.0.42")
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
	sender := mustSenderJob(t, run, "sender:test", "10.0.0.42")
	sender.Status.Failed = 1
	sender.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "syncoid exited with status 1"},
	}
	r := newRunReconciler(t, run, task, sender)
	ctx, logs := captureRunLogger()

	if _, err := r.Reconcile(ctx, request(run.Name)); err != nil {
		t.Fatal(err)
	}

	assertLogEntry(t, logs, "replication failed", map[string]string{
		"namespace":   run.Namespace,
		"run":         run.Name,
		"senderJob":   names.SenderName,
		"receiverPod": "zfs-receiver-worker-b",
		"reason":      "syncoid exited with status 1",
	})
	assertNoLogEntry(t, logs, "sender job failed")
}

func TestRunReconcilePreservesTerminationMessageFromExactSenderJob(t *testing.T) {
	run := replicationRun("manual-termination-message")
	run.Status.ReceiverPodName = "zfs-receiver-worker-b"
	run.Status.ReceiverPodIP = "10.0.0.42"
	sender := mustSenderJob(t, run, "sender:test", "10.0.0.42")
	sender.UID = "sender-job-uid"
	sender.Status.Failed = 1
	finished := metav1.NewTime(time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC))
	pod := terminatedSenderPod(run.Namespace, "sender-pod", sender, finished,
		`cannot receive tank/archive --sshkey=/var/run/zfsrep/ssh/id_rsa`)
	staleJob := sender.DeepCopy()
	staleJob.UID = "stale-job-uid"
	stale := terminatedSenderPod(run.Namespace, "stale-sender-pod", staleJob, metav1.NewTime(finished.Add(time.Minute)),
		"stale job evidence must not win")
	r := newRunReconciler(t, run, sender, pod, stale)

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
	if got.Status.LastError != `cannot receive tank/archive --sshkey=/var/run/zfsrep/ssh/id_rsa` {
		t.Fatalf("lastError = %q", got.Status.LastError)
	}
}

func TestRunReconcileSelectsNewestTerminatedSenderDeterministically(t *testing.T) {
	base := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		configure func(older, newer *corev1.Pod)
	}{
		{
			name: "finish time",
			configure: func(older, newer *corev1.Pod) {
				older.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.NewTime(base)
				newer.Status.ContainerStatuses[0].State.Terminated.FinishedAt = metav1.NewTime(base.Add(time.Minute))
			},
		},
		{
			name: "pod creation time",
			configure: func(older, newer *corev1.Pod) {
				finished := metav1.NewTime(base)
				older.Status.ContainerStatuses[0].State.Terminated.FinishedAt = finished
				newer.Status.ContainerStatuses[0].State.Terminated.FinishedAt = finished
				older.CreationTimestamp = metav1.NewTime(base.Add(-2 * time.Minute))
				newer.CreationTimestamp = metav1.NewTime(base.Add(-time.Minute))
			},
		},
		{
			name: "pod name",
			configure: func(older, newer *corev1.Pod) {
				finished := metav1.NewTime(base)
				created := metav1.NewTime(base.Add(-time.Minute))
				older.Status.ContainerStatuses[0].State.Terminated.FinishedAt = finished
				newer.Status.ContainerStatuses[0].State.Terminated.FinishedAt = finished
				older.CreationTimestamp = created
				newer.CreationTimestamp = created
				older.Name = "sender-a"
				newer.Name = "sender-b"
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := replicationRun("manual-newest-" + strings.ReplaceAll(tt.name, " ", "-"))
			sender := mustSenderJob(t, run, "release:test", "10.0.0.42")
			sender.UID = types.UID("sender-job-uid-" + tt.name)
			sender.Status.Failed = 1
			finished := metav1.NewTime(base)
			older := terminatedSenderPod(run.Namespace, "sender-old", sender, finished, "older message")
			newer := terminatedSenderPod(run.Namespace, "sender-new", sender, finished, "newest message")
			tt.configure(older, newer)
			r := newRunReconciler(t, run, sender, older, newer)

			if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
				t.Fatal(err)
			}

			var got zfsv1.ZFSReplicationRun
			if err := r.Get(context.Background(), request(run.Name).NamespacedName, &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.LastError != "newest message" {
				t.Fatalf("lastError = %q", got.Status.LastError)
			}
		})
	}
}

func TestRunReconcileFallsBackToTerminationReasonAndExitCode(t *testing.T) {
	run := replicationRun("manual-termination-reason")
	sender := mustSenderJob(t, run, "release:test", "10.0.0.42")
	sender.UID = "sender-job-uid"
	sender.Status.Failed = 1
	pod := terminatedSenderPod(run.Namespace, "sender-pod", sender, metav1.Now(), "")
	pod.Status.ContainerStatuses[0].State.Terminated.Reason = "OOMKilled"
	pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 137
	r := newRunReconciler(t, run, sender, pod)

	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}

	var got zfsv1.ZFSReplicationRun
	if err := r.Get(context.Background(), request(run.Name).NamespacedName, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.LastError != "sender terminated: reason OOMKilled, exit code 137" {
		t.Fatalf("lastError = %q", got.Status.LastError)
	}
}

func TestRunReconcilePreservesJobFailedConditionWithoutPodLogs(t *testing.T) {
	run := replicationRun("manual-fallback-redaction")
	names := objectNamesForRun(run.Name)
	run.Status.ReceiverPodName = "zfs-receiver-worker-b"
	run.Status.ReceiverPodIP = "10.0.0.42"
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	sender := mustSenderJob(t, run, "sender:test", "10.0.0.42")
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
	if got.Status.LastError != `syncoid exited with status 1 --sshkey=\"/var/run/zfsrep/ssh/id_rsa\"` {
		t.Fatalf("lastError = %q", got.Status.LastError)
	}
	assertLogEntry(t, logs, "replication failed", map[string]string{
		"namespace": run.Namespace,
		"run":       run.Name,
		"reason":    `syncoid exited with status 1 --sshkey=\"/var/run/zfsrep/ssh/id_rsa\"`,
	})
}

func TestJobFailedUsesNonLogEvidenceOrder(t *testing.T) {
	for _, tt := range []struct {
		name               string
		terminationMessage string
		terminationReason  string
		conditionMessage   string
		want               string
	}{
		{
			name:               "termination message before every fallback",
			terminationMessage: "Sender Failure Message",
			terminationReason:  "Error",
			conditionMessage:   "Job condition message",
			want:               "Sender Failure Message",
		},
		{
			name:              "termination reason before Job condition",
			terminationReason: "OOMKilled",
			conditionMessage:  "Job condition message",
			want:              "sender terminated: reason OOMKilled, exit code 137",
		},
		{
			name:             "Job condition before generic fallback",
			conditionMessage: "Job condition message",
			want:             "Job condition message",
		},
		{
			name: "generic fallback",
			want: "sender Job failed",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "sender",
					Namespace: "storage",
					UID:       "sender-job-uid",
				},
				Status: batchv1.JobStatus{Failed: 1},
			}
			if tt.conditionMessage != "" {
				job.Status.Conditions = []batchv1.JobCondition{{
					Type:    batchv1.JobFailed,
					Status:  corev1.ConditionTrue,
					Message: tt.conditionMessage,
				}}
			}
			var objects []client.Object
			if tt.terminationMessage != "" || tt.terminationReason != "" {
				pod := terminatedSenderPod(job.Namespace, "sender-pod", job, metav1.Now(), tt.terminationMessage)
				pod.Status.ContainerStatuses[0].State.Terminated.Reason = tt.terminationReason
				if tt.terminationReason == "OOMKilled" {
					pod.Status.ContainerStatuses[0].State.Terminated.ExitCode = 137
				}
				objects = append(objects, pod)
			}
			reconciler := newRunReconciler(t, objects...)

			failed, message, err := reconciler.jobFailed(context.Background(), job, "sender Job failed")
			if err != nil {
				t.Fatal(err)
			}
			if !failed || message != tt.want {
				t.Fatalf("jobFailed() = (%t, %q), want (true, %q)", failed, message, tt.want)
			}
		})
	}
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
			sshSecret := runSSHSecretForTest(run, names)
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
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "storage", UID: types.UID("run-" + name)},
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

func mustSenderJob(t *testing.T, run *zfsv1.ZFSReplicationRun, image, receiverHost string) *batchv1.Job {
	t.Helper()
	job, err := senderJob(run, image, zfsv1.ReceiveTaskEndpoint{Host: receiverHost, Port: 2222})
	if err != nil {
		t.Fatal(err)
	}
	job.UID = types.UID("job-" + run.Name)
	job.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(
		run,
		zfsv1.SchemeGroupVersion.WithKind("ZFSReplicationRun"),
	)}
	return job
}

func readyReceiveTask(run *zfsv1.ZFSReplicationRun, names runObjects, host, hostKey string) *zfsv1.ZFSReceiveTask {
	return &zfsv1.ZFSReceiveTask{
		TypeMeta: metav1.TypeMeta{APIVersion: zfsv1.Group + "/" + zfsv1.Version, Kind: "ZFSReceiveTask"},
		ObjectMeta: metav1.ObjectMeta{
			Name:            names.ReceiveTaskName,
			Namespace:       run.Namespace,
			Labels:          map[string]string{labelPrefix + "/role": "receiver"},
			UID:             "task-uid",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(run, zfsv1.SchemeGroupVersion.WithKind("ZFSReplicationRun"))},
		},
		Spec: zfsv1.ZFSReceiveTaskSpec{
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

func readyReceiverPodForTask(task *zfsv1.ZFSReceiveTask) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      task.Status.ReceiverPod.Name,
			Namespace: "zfsreplication-system",
			UID:       types.UID(task.Status.ReceiverPod.UID),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			PodIP: task.Status.Endpoint.Host,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func runSSHSecretForTest(run *zfsv1.ZFSReplicationRun, names runObjects) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            names.SecretName,
			Namespace:       run.Namespace,
			Labels:          map[string]string{labelPrefix + "/role": "ssh"},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(run, zfsv1.SchemeGroupVersion.WithKind("ZFSReplicationRun"))},
		},
		Data: map[string][]byte{
			"id_rsa":     []byte("test-private-key"),
			"id_rsa.pub": []byte("ssh-rsa AAAATEST zfsreplication-controller"),
		},
	}
}

func terminatedSenderPod(namespace, name string, job *batchv1.Job, finished metav1.Time, message string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         namespace,
			CreationTimestamp: metav1.NewTime(finished.Add(-time.Minute)),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, batchv1.SchemeGroupVersion.WithKind("Job")),
			},
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{
				Name: "sender",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   1,
					Reason:     "Error",
					Message:    message,
					FinishedAt: finished,
				}},
			},
		}},
	}
}

func newRunReconciler(t *testing.T, objs ...client.Object) *ZFSReplicationRunReconciler {
	t.Helper()
	scheme := newTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&zfsv1.ZFSReplicationRun{}, &zfsv1.ZFSReceiveTask{}).WithObjects(objs...).Build()
	return &ZFSReplicationRunReconciler{
		Client:            c,
		APIReader:         c,
		Scheme:            scheme,
		ReleaseImage:      "sender:test",
		ReceiverNamespace: "zfsreplication-system",
	}
}

func newRunAPIReader(t *testing.T, objs ...client.Object) client.Reader {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newTestScheme(t)).
		WithStatusSubresource(&zfsv1.ZFSReplicationRun{}, &zfsv1.ZFSReceiveTask{}).
		WithObjects(objs...).
		Build()
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
	return &ZFSReplicationRunReconciler{
		Client:            c,
		APIReader:         c,
		Scheme:            scheme,
		ReleaseImage:      "sender:test",
		ReceiverNamespace: "zfsreplication-system",
	}
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

func assertLogEntryExcludesFields(t *testing.T, logs *capturedLogs, msg string, fields ...string) {
	t.Helper()
	for _, entry := range logs.entries {
		if entry["msg"] != msg {
			continue
		}
		for _, field := range fields {
			if _, ok := entry[field]; ok {
				t.Fatalf("log %q unexpectedly contained field %q: %#v", msg, field, entry)
			}
		}
		return
	}
	t.Fatalf("logs did not contain %q: %#v", msg, logs.entries)
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

func assertRunPhase(t *testing.T, c client.Client, name string, phase zfsv1.Phase) {
	t.Helper()
	var run zfsv1.ZFSReplicationRun
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "storage"}, &run); err != nil {
		t.Fatal(err)
	}
	if run.Status.Phase != phase {
		t.Fatalf("run phase = %q, want %q", run.Status.Phase, phase)
	}
}

func assertControlledByRun(t *testing.T, child client.Object, run *zfsv1.ZFSReplicationRun) {
	t.Helper()
	owner := metav1.GetControllerOf(child)
	if owner == nil || owner.UID != run.UID || owner.Kind != "ZFSReplicationRun" {
		t.Fatalf("%T controller owner = %#v, want Replication Run UID %q", child, owner, run.UID)
	}
}

func assertOnlyRoleLabel(t *testing.T, child client.Object, role string) {
	t.Helper()
	labels := child.GetLabels()
	if len(labels) != 1 || labels[labelPrefix+"/role"] != role {
		t.Fatalf("%T labels = %#v, want only role %q", child, labels, role)
	}
}

func ptr[T any](v T) *T { return &v }
