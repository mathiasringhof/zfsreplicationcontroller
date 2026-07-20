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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestAuthorizationRequestCoalescesEveryTaskEvent(t *testing.T) {
	requests := receiverAuthorizationRequests(context.Background(), &zfsv1.ZFSReceiveTask{
		ObjectMeta: metav1.ObjectMeta{Namespace: "storage", Name: "one"},
	})
	if len(requests) != 1 || requests[0] != receiverAuthorizationRequest {
		t.Fatalf("requests = %#v, want one shared receiver authorization request", requests)
	}
}

func TestAuthorizationDeadlineResultSchedulesFutureAndDueDeadlines(t *testing.T) {
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		next time.Time
		want ctrl.Result
	}{
		{name: "none", want: ctrl.Result{}},
		{name: "future", next: now.Add(10 * time.Minute), want: ctrl.Result{RequeueAfter: 10 * time.Minute}},
		{name: "exact boundary", next: now, want: ctrl.Result{Requeue: true}},
		{name: "overdue after clock sample", next: now.Add(-time.Nanosecond), want: ctrl.Result{Requeue: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := deadlineResult(now, tt.next); got != tt.want {
				t.Fatalf("deadlineResult() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestSynchronizedAuthorizedSSHDStartup(t *testing.T) {
	activationErr := errors.New("initial observation failed")
	tests := []struct {
		name          string
		cacheSynced   bool
		activationErr error
		wantOrder     string
		wantErr       error
		wantErrText   string
	}{
		{name: "success", cacheSynced: true, wantOrder: "cache,activate,sshd"},
		{name: "cache synchronization failure", wantOrder: "cache", wantErrText: "synchronize"},
		{name: "initial activation failure", cacheSynced: true, activationErr: activationErr, wantOrder: "cache,activate", wantErr: activationErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			order := []string{}
			_, err := startSynchronizedAuthorizedSSHD(
				context.Background(), receiverConfig{},
				func(context.Context) bool {
					order = append(order, "cache")
					return tt.cacheSynced
				},
				func(context.Context) error {
					order = append(order, "activate")
					return tt.activationErr
				},
				func(context.Context, receiverConfig) (<-chan error, error) {
					order = append(order, "sshd")
					return make(chan error), nil
				},
			)
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("startup error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErrText != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErrText)) {
				t.Fatalf("startup error = %v, want text %q", err, tt.wantErrText)
			}
			if tt.wantErr == nil && tt.wantErrText == "" && err != nil {
				t.Fatal(err)
			}
			if got := strings.Join(order, ","); got != tt.wantOrder {
				t.Fatalf("startup order = %q, want %q", got, tt.wantOrder)
			}
		})
	}
}

func TestQueuedInitialReconciliationDistinguishesActivationFromLaterReporting(t *testing.T) {
	warning := errors.New("retry retired-generation cleanup")
	publicationErr := errors.New("pre-commit publication failure")
	tests := []struct {
		name           string
		activation     receiverauthorization.Activation
		publicationErr error
		wantStartErr   error
		wantRunErr     error
	}{
		{name: "post-commit warning does not block readiness", activation: receiverauthorization.Activation{Changed: true, Warning: warning}, wantRunErr: warning},
		{name: "pre-commit error blocks readiness", publicationErr: publicationErr, wantStartErr: publicationErr, wantRunErr: publicationErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := receiverAuthorizationReconciler{
				client:         fake.NewClientBuilder().WithScheme(newReceiverTestScheme(t)).Build(),
				cfg:            receiverConfig{NodeName: "worker-b"},
				authorization:  staticAuthorizationPublisher{activation: tt.activation, err: tt.publicationErr},
				now:            time.Now,
				initialTrigger: make(chan event.GenericEvent, 1),
				startupGate:    make(chan struct{}),
				initialResult:  make(chan error, 1),
			}
			r.apiReader = r.client
			reconcileDone := make(chan error, 1)
			go func() {
				_, err := r.Reconcile(context.Background(), receiverAuthorizationRequest)
				reconcileDone <- err
			}()
			startErr := r.StartInitial(context.Background())
			if !errors.Is(startErr, tt.wantStartErr) || (tt.wantStartErr == nil && startErr != nil) {
				t.Fatalf("StartInitial() error = %v, want %v", startErr, tt.wantStartErr)
			}
			runErr := <-reconcileDone
			if !errors.Is(runErr, tt.wantRunErr) {
				t.Fatalf("Reconcile() error = %v, want %v", runErr, tt.wantRunErr)
			}
		})
	}
}

func TestReceiverAuthorizationReconcilerUsesCompleteNodeLocalViewsAndDeadline(t *testing.T) {
	now := time.Now()
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
	local := testReceiveTask("local", "11111111-2222-3333-4444-555555555555", validReceiverPublicKey, metav1.NewTime(now.Add(3*time.Minute)), cfg.NodeName)
	remote := testReceiveTask("remote", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", otherReceiverPublicKey, metav1.NewTime(now.Add(2*time.Minute)), "worker-c")
	kubeClient := fake.NewClientBuilder().
		WithScheme(newReceiverTestScheme(t)).
		WithStatusSubresource(&zfsv1.ZFSReceiveTask{}).
		WithObjects(local, remote).
		Build()
	reconciler := receiverAuthorizationReconciler{
		client:        kubeClient,
		apiReader:     kubeClient,
		cfg:           cfg,
		hostKey:       validReceiverPublicKey,
		authorization: receiverauthorization.New(cfg.AuthorizedKeysFile),
		now:           func() time.Time { return now },
		startupGate:   closedStartupGate(),
	}

	result, err := reconciler.Reconcile(context.Background(), receiverAuthorizationRequest)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter <= 2*time.Minute || result.RequeueAfter > 3*time.Minute {
		t.Fatalf("RequeueAfter = %v, want local lease deadline", result.RequeueAfter)
	}
	assertAuthorizedKeyLines(t, cfg.AuthorizedKeysFile, 2)

	var current zfsv1.ZFSReceiveTask
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(local), &current); err != nil {
		t.Fatal(err)
	}
	current.Status.Phase = zfsv1.ReceiveTaskPhaseCompleted
	if err := kubeClient.Status().Update(context.Background(), &current); err != nil {
		t.Fatal(err)
	}
	result, err = reconciler.Reconcile(context.Background(), receiverAuthorizationRequest)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("terminal complete view RequeueAfter = %v, want none", result.RequeueAfter)
	}
	assertAuthorizedKeyLines(t, cfg.AuthorizedKeysFile, 1)

	added := testReceiveTask("added", "99999999-8888-7777-6666-555555555555", otherReceiverPublicKey, metav1.NewTime(now.Add(4*time.Minute)), cfg.NodeName)
	if err := kubeClient.Create(context.Background(), added); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), receiverAuthorizationRequest); err != nil {
		t.Fatal(err)
	}
	beforeUpdate, err := os.ReadFile(cfg.AuthorizedKeysFile)
	if err != nil {
		t.Fatal(err)
	}
	var updated zfsv1.ZFSReceiveTask
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(added), &updated); err != nil {
		t.Fatal(err)
	}
	updated.Spec.Destination.Dataset = "tank/updated"
	if err := kubeClient.Update(context.Background(), &updated); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), receiverAuthorizationRequest); err != nil {
		t.Fatal(err)
	}
	afterUpdate, err := os.ReadFile(cfg.AuthorizedKeysFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(afterUpdate) == string(beforeUpdate) {
		t.Fatal("authority-bearing task update did not change the complete snapshot")
	}
	if err := kubeClient.Delete(context.Background(), &updated); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), receiverAuthorizationRequest); err != nil {
		t.Fatal(err)
	}
	assertAuthorizedKeyLines(t, cfg.AuthorizedKeysFile, 1)

	expired := testReceiveTask("expired", "77777777-8888-9999-aaaa-bbbbbbbbbbbb", otherReceiverPublicKey, metav1.NewTime(time.Now().Add(-time.Minute)), cfg.NodeName)
	if err := kubeClient.Create(context.Background(), expired); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), receiverAuthorizationRequest); err != nil {
		t.Fatal(err)
	}
	assertAuthorizedKeyLines(t, cfg.AuthorizedKeysFile, 1)
	var expiredStatus zfsv1.ZFSReceiveTask
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(expired), &expiredStatus); err != nil {
		t.Fatal(err)
	}
	if expiredStatus.Status.Phase != zfsv1.ReceiveTaskPhaseFailed {
		t.Fatalf("expired task phase = %q, want Failed", expiredStatus.Status.Phase)
	}
}

func TestStatusPatchUsesResourceVersionToAvoidReopeningConcurrentTerminalTask(t *testing.T) {
	task := testReceiveTask("local", "11111111-2222-3333-4444-555555555555", validReceiverPublicKey, metav1.NewTime(time.Now().Add(10*time.Minute)), "worker-b")
	transitioned := false
	kubeClient := fake.NewClientBuilder().
		WithScheme(newReceiverTestScheme(t)).
		WithStatusSubresource(&zfsv1.ZFSReceiveTask{}).
		WithObjects(task).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if subResourceName == "status" && !transitioned {
					transitioned = true
					var current zfsv1.ZFSReceiveTask
					if err := c.Get(ctx, client.ObjectKeyFromObject(task), &current); err != nil {
						return err
					}
					current.Status.Phase = zfsv1.ReceiveTaskPhaseCompleted
					if err := c.Status().Update(ctx, &current); err != nil {
						return err
					}
				}
				return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	err := patchTaskReady(context.Background(), kubeClient, kubeClient, task, receiverConfig{PodName: "receiver", PodUID: "pod-uid", PodIP: "10.0.0.42", SSHPort: 2222}, validReceiverPublicKey)
	if err == nil {
		t.Fatal("status patch succeeded across a concurrent terminal transition")
	}
	var got zfsv1.ZFSReceiveTask
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(task), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != zfsv1.ReceiveTaskPhaseCompleted {
		t.Fatalf("concurrent terminal phase = %q, want Completed", got.Status.Phase)
	}
}

func TestReceiverAuthorizationReconcilerRetriesStatusWithoutRollingAuthorityBack(t *testing.T) {
	now := time.Now()
	dir := t.TempDir()
	cfg := receiverConfig{NodeName: "worker-b", PodName: "receiver", PodUID: "pod-uid", PodIP: "10.0.0.42", SSHPort: 2222, AuthorizedKeysFile: filepath.Join(dir, "authorized_keys")}
	task := testReceiveTask("local", "11111111-2222-3333-4444-555555555555", validReceiverPublicKey, metav1.NewTime(now.Add(10*time.Minute)), cfg.NodeName)
	patchFailures := 1
	kubeClient := fake.NewClientBuilder().
		WithScheme(newReceiverTestScheme(t)).
		WithStatusSubresource(&zfsv1.ZFSReceiveTask{}).
		WithObjects(task).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if subResourceName == "status" && patchFailures > 0 {
					patchFailures--
					return errors.New("temporary status failure")
				}
				return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	reconciler := receiverAuthorizationReconciler{client: kubeClient, apiReader: kubeClient, cfg: cfg, hostKey: validReceiverPublicKey, authorization: receiverauthorization.New(cfg.AuthorizedKeysFile), now: time.Now, startupGate: closedStartupGate()}

	if _, err := reconciler.Reconcile(context.Background(), receiverAuthorizationRequest); err == nil || !strings.Contains(err.Error(), "temporary status failure") {
		t.Fatalf("first Reconcile() error = %v, want status retry", err)
	}
	firstManifest, err := os.ReadFile(cfg.AuthorizedKeysFile)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), receiverAuthorizationRequest); err != nil {
		t.Fatal(err)
	}
	secondManifest, err := os.ReadFile(cfg.AuthorizedKeysFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(secondManifest) != string(firstManifest) {
		t.Fatal("status retry rewrote or rolled back active authority")
	}
	var got zfsv1.ZFSReceiveTask
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(task), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != zfsv1.ReceiveTaskPhaseReady {
		t.Fatalf("retried task phase = %q, want Ready", got.Status.Phase)
	}
}

func TestReceiverAuthorizationLeaseLapseRevokesBeforeStatusPersistence(t *testing.T) {
	startedAt := time.Now().UTC().Truncate(time.Second)
	now := startedAt
	dir := t.TempDir()
	cfg := receiverConfig{NodeName: "worker-b", PodName: "receiver", PodUID: "pod-uid", PodIP: "10.0.0.42", SSHPort: 2222, AuthorizedKeysFile: filepath.Join(dir, "authorized_keys")}
	task := testReceiveTask("local", "11111111-2222-3333-4444-555555555555", validReceiverPublicKey, metav1.NewTime(startedAt.Add(10*time.Minute)), cfg.NodeName)
	statusFailure := false
	kubeClient := fake.NewClientBuilder().
		WithScheme(newReceiverTestScheme(t)).
		WithStatusSubresource(&zfsv1.ZFSReceiveTask{}).
		WithObjects(task).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if subResourceName == "status" && statusFailure {
					return errors.New("status unavailable")
				}
				return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	module := receiverauthorization.New(cfg.AuthorizedKeysFile)
	reconciler := receiverAuthorizationReconciler{
		client: kubeClient, apiReader: kubeClient, cfg: cfg, hostKey: validReceiverPublicKey,
		authorization: module, now: func() time.Time { return now }, startupGate: closedStartupGate(),
	}

	result, err := reconciler.Reconcile(context.Background(), receiverAuthorizationRequest)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 10*time.Minute {
		t.Fatalf("initial RequeueAfter = %v, want exact activation deadline", result.RequeueAfter)
	}
	assertAuthorizedKeyLines(t, cfg.AuthorizedKeysFile, 2)

	var expiring zfsv1.ZFSReceiveTask
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(task), &expiring); err != nil {
		t.Fatal(err)
	}
	expiring.Spec.SSH.ExpiresAt = metav1.NewTime(time.Now().Add(-time.Second))
	if err := kubeClient.Update(context.Background(), &expiring); err != nil {
		t.Fatal(err)
	}
	now = startedAt.Add(10 * time.Minute)
	statusFailure = true
	result, err = reconciler.Reconcile(context.Background(), receiverAuthorizationRequest)
	if err == nil || !strings.Contains(err.Error(), "status unavailable") {
		t.Fatalf("lapse Reconcile() error = %v, want status retry", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("lapsed RequeueAfter = %v, want no active lease deadline", result.RequeueAfter)
	}
	assertAuthorizedKeyLines(t, cfg.AuthorizedKeysFile, 1)

	var renewed zfsv1.ZFSReceiveTask
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(task), &renewed); err != nil {
		t.Fatal(err)
	}
	renewed.Spec.SSH.ExpiresAt = metav1.NewTime(now.Add(10 * time.Minute))
	if err := kubeClient.Update(context.Background(), &renewed); err != nil {
		t.Fatal(err)
	}
	statusFailure = false
	if _, err := reconciler.Reconcile(context.Background(), receiverAuthorizationRequest); err != nil {
		t.Fatal(err)
	}
	assertAuthorizedKeyLines(t, cfg.AuthorizedKeysFile, 1)
	if err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(task), &renewed); err != nil {
		t.Fatal(err)
	}
	if renewed.Status.Phase != zfsv1.ReceiveTaskPhaseFailed || !strings.Contains(renewed.Status.Error, "lapsed") {
		t.Fatalf("renewed lapsed task status = %#v, want terminal same-process rejection", renewed.Status)
	}
}

func TestReceiverRestartActivatesDurablyRenewedFreshTask(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	dir := t.TempDir()
	cfg := receiverConfig{
		NodeName:           "worker-b",
		PodName:            "receiver",
		PodUID:             "receiver-pod-uid",
		PodIP:              "10.0.0.42",
		SSHPort:            2222,
		AuthorizedKeysFile: filepath.Join(dir, "authorized_keys"),
		AllowedPrefixes:    []string{"tank"},
	}
	expired := testReceiveTask("local", "11111111-2222-3333-4444-555555555555", validReceiverPublicKey, metav1.NewTime(now.Add(-time.Minute)), cfg.NodeName)
	oldClient := fake.NewClientBuilder().
		WithScheme(newReceiverTestScheme(t)).
		WithStatusSubresource(&zfsv1.ZFSReceiveTask{}).
		WithObjects(expired).
		Build()
	oldProcess := receiverauthorization.New(cfg.AuthorizedKeysFile)
	oldReconciler := receiverAuthorizationReconciler{
		client: oldClient, apiReader: oldClient, cfg: cfg, hostKey: validReceiverPublicKey,
		authorization: oldProcess, now: func() time.Time { return now }, startupGate: closedStartupGate(),
	}
	if _, err := oldReconciler.Reconcile(context.Background(), receiverAuthorizationRequest); err != nil {
		t.Fatal(err)
	}
	assertAuthorizedKeyLines(t, cfg.AuthorizedKeysFile, 1)

	renewed := expired.DeepCopy()
	renewed.Spec.SSH.ExpiresAt = metav1.NewTime(now.Add(30 * time.Minute))
	renewed.Status = zfsv1.ZFSReceiveTaskStatus{}
	freshClient := fake.NewClientBuilder().
		WithScheme(newReceiverTestScheme(t)).
		WithStatusSubresource(&zfsv1.ZFSReceiveTask{}).
		WithObjects(renewed).
		Build()
	newProcess := receiverauthorization.New(cfg.AuthorizedKeysFile)
	if err := newProcess.Reset(); err != nil {
		t.Fatal(err)
	}
	newReconciler := receiverAuthorizationReconciler{
		client: freshClient, apiReader: freshClient, cfg: cfg, hostKey: validReceiverPublicKey,
		authorization: newProcess, now: func() time.Time { return now }, startupGate: closedStartupGate(),
	}
	result, err := newReconciler.Reconcile(context.Background(), receiverAuthorizationRequest)
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter != 30*time.Minute {
		t.Fatalf("restart RequeueAfter = %v, want renewed lease deadline", result.RequeueAfter)
	}
	assertAuthorizedKeyLines(t, cfg.AuthorizedKeysFile, 2)
	manifest, err := os.ReadFile(cfg.AuthorizedKeysFile)
	if err != nil {
		t.Fatal(err)
	}
	canonicalKey := strings.Join(strings.Fields(validReceiverPublicKey)[:2], " ")
	if !strings.Contains(string(manifest), canonicalKey) {
		t.Fatalf("restart manifest = %q, want renewed task key", manifest)
	}
	var current zfsv1.ZFSReceiveTask
	if err := freshClient.Get(context.Background(), client.ObjectKeyFromObject(renewed), &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != zfsv1.ReceiveTaskPhaseReady {
		t.Fatalf("renewed task phase after restart = %q, want Ready", current.Status.Phase)
	}
}

func TestReceiverAuthorizationReconcilerDistinguishesDegradedFromUntrustedAuthority(t *testing.T) {
	publicationErr := errors.New("publish candidate")
	tests := []struct {
		name         string
		activeUsable bool
		wantFatal    bool
	}{
		{name: "last complete remains enforceable", activeUsable: true},
		{name: "active snapshot is untrusted", wantFatal: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fatal := make(chan error, 1)
			r := receiverAuthorizationReconciler{
				client:        fake.NewClientBuilder().WithScheme(newReceiverTestScheme(t)).Build(),
				cfg:           receiverConfig{NodeName: "worker-b"},
				authorization: failingAuthorizationPublisher{replaceErr: classifiedPublicationError{cause: publicationErr, activeUsable: tt.activeUsable}},
				fatal:         fatal,
				now:           time.Now,
				startupGate:   closedStartupGate(),
			}
			r.apiReader = r.client
			if _, err := r.Reconcile(context.Background(), reconcile.Request{}); !errors.Is(err, publicationErr) {
				t.Fatalf("Reconcile() error = %v, want publication failure", err)
			}
			select {
			case err := <-fatal:
				if !tt.wantFatal || !errors.Is(err, publicationErr) {
					t.Fatalf("fatal error = %v, wantFatal = %v", err, tt.wantFatal)
				}
			default:
				if tt.wantFatal {
					t.Fatal("untrusted active authority did not terminate receiver")
				}
			}
		})
	}
}

type failingAuthorizationPublisher struct {
	replaceErr error
}

func (f failingAuthorizationPublisher) Replace([]receiverauthorization.Candidate) (receiverauthorization.Activation, error) {
	return receiverauthorization.Activation{}, f.replaceErr
}

type staticAuthorizationPublisher struct {
	activation receiverauthorization.Activation
	err        error
}

func (p staticAuthorizationPublisher) Replace([]receiverauthorization.Candidate) (receiverauthorization.Activation, error) {
	return p.activation, p.err
}

type classifiedPublicationError struct {
	cause        error
	activeUsable bool
}

func (e classifiedPublicationError) Error() string               { return e.cause.Error() }
func (e classifiedPublicationError) Unwrap() error               { return e.cause }
func (e classifiedPublicationError) ActiveAuthorityUsable() bool { return e.activeUsable }

func assertAuthorizedKeyLines(t *testing.T, path string, want int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(strings.TrimSuffix(string(data), "\n"), "\n") + 1; got != want {
		t.Fatalf("authorized_keys lines = %d, want %d: %q", got, want, data)
	}
}

func closedStartupGate() chan struct{} {
	gate := make(chan struct{})
	close(gate)
	return gate
}
