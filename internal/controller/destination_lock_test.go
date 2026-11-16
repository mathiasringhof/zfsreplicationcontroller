package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestDestinationLockedAcrossNamespaces(t *testing.T) {
	for _, tt := range []struct {
		name, target, otherTarget, otherNode string
		wantLocked                           bool
	}{
		{"equal", "tank/dst", "tank/dst", "worker-b", true},
		{"parent", "tank/dst/child", "tank/dst", "worker-b", true},
		{"child", "tank/dst", "tank/dst/child", "worker-b", true},
		{"siblings", "tank/dst/a", "tank/dst/b", "worker-b", false},
		{"different node", "tank/dst", "tank/dst", "worker-c", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := replicationRun("same-name")
			run.Status.Phase = zfsv1.PhasePending
			run.Spec.Target.Dataset = tt.target
			other := replicationRun(run.Name)
			other.Namespace = "other-namespace"
			other.UID = "other-uid"
			other.Status.Phase = zfsv1.PhaseRunning
			other.Spec.Target.Dataset = tt.otherTarget
			other.Spec.Target.NodeName = tt.otherNode
			r := newRunReconciler(t, run, other)
			locked, msg, err := r.destinationLocked(context.Background(), run)
			if err != nil || locked != tt.wantLocked {
				t.Fatalf("destinationLocked() = (%v, %q, %v), want locked=%v", locked, msg, err, tt.wantLocked)
			}
			if locked && !strings.Contains(msg, other.Namespace+"/"+other.Name) {
				t.Fatalf("waiting reason %q does not identify the conflicting namespace/name", msg)
			}
		})
	}
}

func TestDestinationLeaseName(t *testing.T) {
	seen := make(map[string]string)
	for _, node := range []string{"worker-b", "Worker-B", "worker.b", "worker_b", strings.Repeat("a", 253), strings.Repeat("a", 252) + "b"} {
		name := destinationLeaseName(node)
		if problems := validation.IsDNS1123Subdomain(name); len(problems) != 0 {
			t.Errorf("Lease name %q: %v", name, problems)
		}
		if !strings.HasPrefix(name, "zfsrep-destination-") || name != destinationLeaseName(node) {
			t.Errorf("Lease name %q must be readable and deterministic", name)
		}
		if previous, ok := seen[name]; ok {
			t.Errorf("nodes %q and %q share Lease name %q", previous, node, name)
		}
		seen[name] = node
	}
}

func TestDestinationLockOverlap(t *testing.T) {
	for _, tt := range []struct {
		name, target, node string
		wantBlocked        bool
	}{
		{"equal", "tank/dst", "worker-b", true},
		{"child", "tank/dst/child", "worker-b", true},
		{"parent", "tank", "worker-b", true},
		{"sibling", "tank/dst-sibling", "worker-b", false},
		{"other node", "tank/dst", "worker-c", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			owner := replicationRun("owner")
			run := replicationRun("contender")
			run.Namespace = "other-namespace"
			run.Spec.Target.Dataset = tt.target
			run.Spec.Target.NodeName = tt.node
			r := newRunReconciler(t, owner, run)
			lock := testDestinationLock(r)
			if blocker, err := lock.acquire(context.Background(), owner); err != nil || blocker != nil {
				t.Fatalf("first acquire = (%v, %v)", blocker, err)
			}
			blocker, err := lock.acquire(context.Background(), run)
			if err != nil || (blocker != nil) != tt.wantBlocked {
				t.Fatalf("second acquire = (%v, %v), want blocked=%v", blocker, err, tt.wantBlocked)
			}
			if blocker != nil && (blocker.UID != owner.UID || blocker.Namespace != owner.Namespace || blocker.Name != owner.Name) {
				t.Fatalf("blocker = %#v, want exact owner", blocker)
			}
			claims := readDestinationClaims(t, r.APIReader, owner)
			want := 1
			if !tt.wantBlocked && tt.node == owner.Spec.Target.NodeName {
				want = 2
			}
			if len(claims) != want {
				t.Fatalf("claims = %#v, want %d", claims, want)
			}
			if tt.node != owner.Spec.Target.NodeName {
				claims = readDestinationClaims(t, r.APIReader, run)
				if len(claims) != 1 || claims[0].UID != run.UID {
					t.Fatalf("other node claims = %#v, want independent owner", claims)
				}
			}
		})
	}
}

func TestDestinationLockPrunesOnlyStaleClaims(t *testing.T) {
	for _, tt := range []struct {
		name               string
		phase              zfsv1.Phase
		deleted, recreated bool
		getError           error
		wantBlocked        bool
	}{
		{name: "deleted", deleted: true},
		{name: "recreated", recreated: true},
		{name: "succeeded", phase: zfsv1.PhaseSucceeded},
		{name: "failed", phase: zfsv1.PhaseFailed},
		{name: "pending", phase: zfsv1.PhasePending, wantBlocked: true},
		{name: "running", phase: zfsv1.PhaseRunning, wantBlocked: true},
		{name: "forbidden", getError: apierrors.NewForbidden(schema.GroupResource{Group: zfsv1.Group, Resource: "zfsreplicationruns"}, "owner", errors.New("denied")), wantBlocked: true},
		{name: "unavailable", getError: errors.New("API unavailable"), wantBlocked: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			owner := replicationRun("owner")
			owner.Namespace = "other-namespace"
			owner.CreationTimestamp = metav1.NewTime(time.Now().Add(-365 * 24 * time.Hour))
			owner.Status.Phase = tt.phase
			lease := destinationLeaseForTest(t, owner)
			run := replicationRun("contender")
			objects := []client.Object{run, lease}
			if tt.recreated {
				owner.UID = "new-incarnation"
			}
			if !tt.deleted {
				objects = append(objects, owner)
			}
			r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*zfsv1.ZFSReplicationRun); ok && key == client.ObjectKeyFromObject(owner) && tt.getError != nil {
						return tt.getError
					}
					return c.Get(ctx, key, obj, opts...)
				},
			}, objects...)
			blocker, err := testDestinationLock(r).acquire(context.Background(), run)
			if err != nil || (blocker != nil) != tt.wantBlocked {
				t.Fatalf("acquire = (%v, %v), want blocked=%v", blocker, err, tt.wantBlocked)
			}
			claims := readDestinationClaims(t, r.APIReader, owner)
			wantUID := run.UID
			if tt.wantBlocked {
				wantUID = "run-owner"
			}
			if len(claims) != 1 || claims[0].UID != wantUID {
				t.Fatalf("claims = %#v, want UID %s", claims, wantUID)
			}
		})
	}
}

func TestDestinationLockConflictRereadsClaims(t *testing.T) {
	owner := replicationRun("owner")
	run := replicationRun("contender")
	lease := destinationLeaseForTest(t, owner)
	lease.Annotations[destinationClaimsAnnotation] = "[]"
	updates := 0
	r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if current, ok := obj.(*coordinationv1.Lease); ok {
				updates++
				if updates == 1 {
					competing := destinationLeaseForTest(t, owner)
					competing.ResourceVersion = current.ResourceVersion
					if err := c.Update(ctx, competing); err != nil {
						return err
					}
					return apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, obj.GetName(), errors.New("concurrent acquisition"))
				}
			}
			return c.Update(ctx, obj, opts...)
		},
	}, owner, run, lease)
	blocker, err := testDestinationLock(r).acquire(context.Background(), run)
	if err != nil || blocker == nil || blocker.UID != owner.UID || updates != 1 {
		t.Fatalf("acquire after conflict = (%v, %v), updates=%d; want competing owner retained", blocker, err, updates)
	}
}

func TestDestinationLockReleaseRetriesConflictAndPreservesOtherClaims(t *testing.T) {
	owner := replicationRun("owner")
	other := replicationRun("other")
	other.Spec.Target.Dataset = "tank/sibling"
	lease := destinationLeaseForTest(t, owner, other)
	updates := 0
	r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*coordinationv1.Lease); ok {
				updates++
				if updates == 1 {
					return apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, obj.GetName(), errors.New("retry"))
				}
			}
			return c.Update(ctx, obj, opts...)
		},
	}, owner, other, lease)
	lock := testDestinationLock(r)
	previous := owner.DeepCopy()
	previous.UID = "previous-incarnation"
	if err := lock.release(context.Background(), previous); err != nil {
		t.Fatal(err)
	}
	if updates != 0 {
		t.Fatal("release by a different UID modified the current claim")
	}
	for range 2 {
		if err := lock.release(context.Background(), owner); err != nil {
			t.Fatal(err)
		}
	}
	claims := readDestinationClaims(t, r.APIReader, owner)
	if updates != 2 || len(claims) != 1 || claims[0].UID != other.UID {
		t.Fatalf("updates=%d, claims=%#v, want only other run after retry and idempotent release", updates, claims)
	}
	if err := r.Delete(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	if err := lock.release(context.Background(), owner); err != nil {
		t.Fatal(err)
	}
}

func TestDestinationLockReassertionAfterRestart(t *testing.T) {
	run := replicationRun("owner")
	run.Status.Phase = zfsv1.PhaseRunning
	run.CreationTimestamp = metav1.Now()
	older := replicationRun("older-pending")
	older.CreationTimestamp = metav1.NewTime(run.CreationTimestamp.Add(-time.Minute))
	older.Status.Phase = zfsv1.PhasePending
	names := objectNamesForRun(run.Name)
	task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	sender := mustSenderJob(t, run, "sender:test", task.Status.Endpoint.Host)
	leaseReads := 0
	server := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*coordinationv1.Lease); ok {
				leaseReads++
			}
			return c.Get(ctx, key, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*coordinationv1.Lease); ok {
				return errors.New("existing claim must not be rewritten")
			}
			return c.Update(ctx, obj, opts...)
		},
	}, run, older, task, sender, destinationLeaseForTest(t, run))
	for range 2 {
		// Each instance has no in-memory ownership state to recover.
		r := newRunReconciler(t)
		r.Client, r.APIReader = server.Client, server.APIReader
		if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
			t.Fatal(err)
		}
		assertRunPhase(t, r.Client, run.Name, zfsv1.PhaseRunning)
	}
	if leaseReads != 2 {
		t.Fatalf("Lease reads = %d, want one per reconcile", leaseReads)
	}
	claims := readDestinationClaims(t, server.APIReader, run)
	if len(claims) != 1 || claims[0].UID != run.UID {
		t.Fatalf("claims = %#v, want original owner", claims)
	}
}

func TestDestinationLockFailsClosed(t *testing.T) {
	for _, failure := range []string{"lease forbidden", "malformed claims", "incomplete claim", "missing reader", "missing namespace"} {
		t.Run(failure, func(t *testing.T) {
			run := replicationRun("owner")
			lease := destinationLeaseForTest(t, run)
			switch failure {
			case "malformed claims":
				lease.Annotations[destinationClaimsAnnotation] = "{"
			case "incomplete claim":
				lease.Annotations[destinationClaimsAnnotation] = `[{"uid":"someone"}]`
			}
			r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, ok := obj.(*coordinationv1.Lease); ok && failure == "lease forbidden" {
						return apierrors.NewForbidden(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, key.Name, errors.New("denied"))
					}
					return c.Get(ctx, key, obj, opts...)
				},
			}, run, lease)
			if failure == "missing reader" {
				r.APIReader = nil
			}
			if failure == "missing namespace" {
				r.DestinationLockNamespace = ""
			}
			if _, err := r.Reconcile(context.Background(), request(run.Name)); err == nil {
				t.Fatal("Reconcile() succeeded without a trustworthy Destination Lock")
			}
			names := objectNamesForRun(run.Name)
			assertObjectDeleted(t, r.Client, &zfsv1.ZFSReceiveTask{}, names.ReceiveTaskName)
			assertObjectDeleted(t, r.Client, &corev1.Secret{}, names.SecretName)
			assertObjectDeleted(t, r.Client, &batchv1.Job{}, names.SenderName)
		})
	}
}

func TestDestinationLockRetriesAreBounded(t *testing.T) {
	for _, operation := range []string{"create", "acquire update", "release update"} {
		t.Run(operation, func(t *testing.T) {
			run := replicationRun("owner")
			objects := []client.Object{run}
			if operation != "create" {
				lease := destinationLeaseForTest(t, run)
				if operation == "acquire update" {
					lease.Annotations[destinationClaimsAnnotation] = "[]"
				}
				objects = append(objects, lease)
			}
			attempts := 0
			r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*coordinationv1.Lease); ok {
						attempts++
						return apierrors.NewAlreadyExists(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, obj.GetName())
					}
					return c.Create(ctx, obj, opts...)
				},
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					if _, ok := obj.(*coordinationv1.Lease); ok {
						attempts++
						return apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, obj.GetName(), errors.New("busy"))
					}
					return c.Update(ctx, obj, opts...)
				},
			}, objects...)
			lock := testDestinationLock(r)
			var err error
			if operation == "release update" {
				err = lock.release(context.Background(), run)
			} else {
				_, err = lock.acquire(context.Background(), run)
			}
			if err == nil || attempts != destinationLockAttempts {
				t.Fatalf("error=%v attempts=%d, want error after %d bounded attempts", err, attempts, destinationLockAttempts)
			}
		})
	}
}

func TestRunReconcileRetriesDestinationLockRelease(t *testing.T) {
	run := replicationRun("owner")
	run.Status.Phase = zfsv1.PhaseSucceeded
	attempts := 0
	r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*coordinationv1.Lease); ok {
				attempts++
				if attempts == 1 {
					return errors.New("API unavailable")
				}
			}
			return c.Update(ctx, obj, opts...)
		},
	}, run, destinationLeaseForTest(t, run))
	if _, err := r.Reconcile(context.Background(), request(run.Name)); err == nil {
		t.Fatal("first release must report write failure")
	}
	if len(readDestinationClaims(t, r.APIReader, run)) != 1 {
		t.Fatal("failed release lost claim")
	}
	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}
	if len(readDestinationClaims(t, r.APIReader, run)) != 0 {
		t.Fatal("retried terminal cleanup retained claim")
	}
}

func TestRunReconcileNamespacedDestinationLock(t *testing.T) {
	run := replicationRun("owner")
	outside := replicationRun("outside")
	outside.Namespace = "other-namespace"
	outside.Status.Phase = zfsv1.PhaseRunning
	r := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			// Model the manager's namespace-scoped informer cache.
			return c.List(ctx, list, append(opts, client.InNamespace(run.Namespace))...)
		},
	}, run, outside)
	r.DestinationLockNamespace = run.Namespace
	if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
		t.Fatal(err)
	}
	assertRunPhase(t, r.Client, run.Name, zfsv1.PhaseStartingReceiver)
	var lease coordinationv1.Lease
	if err := r.APIReader.Get(context.Background(), types.NamespacedName{Namespace: run.Namespace, Name: destinationLeaseName(run.Spec.Target.NodeName)}, &lease); err != nil {
		t.Fatalf("no Destination Lock in watched namespace: %v", err)
	}
}

func TestRunReconcileDestinationLockHandoff(t *testing.T) {
	ctx := context.Background()
	owner, next := replicationRun("a-owner"), replicationRun("b-next")
	owner.Namespace = "other-namespace"
	r := newRunReconciler(t, owner, next)
	ownerRequest := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(owner)}
	if _, err := r.Reconcile(ctx, ownerRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, request(next.Name)); err != nil {
		t.Fatal(err)
	}
	assertRunPhase(t, r.Client, next.Name, zfsv1.PhasePending)
	if err := r.Get(ctx, client.ObjectKeyFromObject(owner), owner); err != nil {
		t.Fatal(err)
	}
	owner.Status.Phase = zfsv1.PhaseSucceeded
	if err := r.Status().Update(ctx, owner); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, ownerRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, request(next.Name)); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(next), next); err != nil {
		t.Fatal(err)
	}
	if next.Status.Phase != zfsv1.PhaseStartingReceiver || next.Status.LastError != "" {
		t.Fatalf("handoff status = %#v, want receiver start without obsolete waiting error", next.Status)
	}
	claims := readDestinationClaims(t, r.APIReader, next)
	if len(claims) != 1 || claims[0].UID != next.UID {
		t.Fatalf("handoff claims = %#v, want next run", claims)
	}
}

func TestRunReconcileReleasesDestinationLock(t *testing.T) {
	for _, outcome := range []string{"succeeded", "failed", "validation", "already succeeded", "already failed"} {
		t.Run(outcome, func(t *testing.T) {
			run := replicationRun("owner")
			lease := destinationLeaseForTest(t, run)
			names := objectNamesForRun(run.Name)
			task := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
			sender := mustSenderJob(t, run, "sender:test", task.Status.Endpoint.Host)
			switch outcome {
			case "succeeded":
				sender.Status.Succeeded = 1
			case "failed":
				sender.Status.Failed = 1
				sender.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "failed"}}
			case "validation":
				run.Spec.Source.Dataset = ""
			case "already succeeded":
				run.Status.Phase = zfsv1.PhaseSucceeded
			case "already failed":
				run.Status.Phase = zfsv1.PhaseFailed
			}
			r := newRunReconciler(t, run, lease, task, sender)
			if _, err := r.Reconcile(context.Background(), request(run.Name)); err != nil {
				t.Fatal(err)
			}
			if claims := readDestinationClaims(t, r.APIReader, run); len(claims) != 0 {
				t.Fatalf("terminal run retained claims: %#v", claims)
			}
			var got zfsv1.ZFSReplicationRun
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(run), &got); err != nil {
				t.Fatal(err)
			}
			if !got.Status.Phase.Terminal() {
				t.Fatalf("phase = %s, want terminal", got.Status.Phase)
			}
			if len(got.Finalizers) != 0 {
				t.Fatalf("run finalizers = %v, want none", got.Finalizers)
			}
		})
	}
}

func TestRunReconcileDestinationLockAtomicAcrossStaleCaches(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "create"
		if existing {
			name = "update"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			first, second := replicationRun("first"), replicationRun("second")
			second.Namespace = "other-namespace"
			second.Spec.Target.Dataset += "/child"
			objects := []client.Object{first, second}
			if existing {
				lease := destinationLeaseForTest(t, first)
				lease.Annotations[destinationClaimsAnnotation] = "[]"
				objects = append(objects, lease)
			}
			var attempts atomic.Int32
			ready := make(chan struct{})
			race := func() error {
				n := attempts.Add(1)
				if n == 2 {
					close(ready)
				}
				select {
				case <-ready:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			server := newRunReconcilerWithInterceptors(t, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					if _, ok := obj.(*coordinationv1.Lease); ok {
						if err := race(); err != nil {
							return err
						}
					}
					return c.Create(ctx, obj, opts...)
				},
				Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					if _, ok := obj.(*coordinationv1.Lease); ok {
						if err := race(); err != nil {
							return err
						}
					}
					return c.Update(ctx, obj, opts...)
				},
			}, objects...).Client
			api, ok := server.(client.WithWatch)
			if !ok {
				t.Fatal("fake API does not implement client.WithWatch")
			}
			type outcome struct {
				result ctrl.Result
				err    error
				run    *zfsv1.ZFSReplicationRun
			}
			results := make(chan outcome, 2)
			for _, run := range []*zfsv1.ZFSReplicationRun{first, second} {
				r := newRunReconciler(t)
				stale := newRunReconciler(t, run).Client
				r.APIReader = api
				r.Client = interceptor.NewClient(api, interceptor.Funcs{
					Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
						if _, ok := obj.(*coordinationv1.Lease); ok {
							return errors.New("Lease read through cached client")
						}
						return c.Get(ctx, key, obj, opts...)
					},
					List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
						if _, ok := list.(*zfsv1.ZFSReplicationRunList); ok {
							return stale.List(ctx, list, opts...)
						}
						return c.List(ctx, list, opts...)
					},
				})
				go func() {
					result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(run)})
					results <- outcome{result, err, run}
				}()
			}
			started, waiting := 0, 0
			for range 2 {
				out := <-results
				if out.err != nil {
					t.Fatal(out.err)
				}
				var got zfsv1.ZFSReplicationRun
				if err := api.Get(ctx, client.ObjectKeyFromObject(out.run), &got); err != nil {
					t.Fatal(err)
				}
				switch got.Status.Phase {
				case zfsv1.PhasePending:
					waiting++
					owner := first
					if out.run.UID == first.UID {
						owner = second
					}
					if out.result.RequeueAfter != 5*time.Second || !strings.Contains(got.Status.LastError, owner.Namespace+"/"+owner.Name) {
						t.Fatalf("waiting result=%#v, status=%#v", out.result, got.Status)
					}
					for _, obj := range []client.Object{&corev1.Secret{}, &zfsv1.ZFSReceiveTask{}, &batchv1.Job{}} {
						names := objectNamesForRun(out.run.Name)
						childName := names.SecretName
						switch obj.(type) {
						case *zfsv1.ZFSReceiveTask:
							childName = names.ReceiveTaskName
						case *batchv1.Job:
							childName = names.SenderName
						}
						if err := api.Get(ctx, types.NamespacedName{Namespace: out.run.Namespace, Name: childName}, obj); !apierrors.IsNotFound(err) {
							t.Fatalf("blocked run child %T: %v", obj, err)
						}
					}
				case zfsv1.PhaseStartingReceiver:
					started++
				}
			}
			if started != 1 || waiting != 1 || attempts.Load() != 2 {
				t.Fatalf("started=%d waiting=%d write attempts=%d, want one owner after two competing writes", started, waiting, attempts.Load())
			}
			if claims := readDestinationClaims(t, api, first); len(claims) != 1 {
				t.Fatalf("claims=%#v, want exactly one", claims)
			}
		})
	}
}

func testDestinationLock(r *ZFSReplicationRunReconciler) destinationLock {
	return destinationLock{client: r.Client, reader: r.APIReader, namespace: "zfsreplication-system"}
}

func destinationLeaseForTest(t *testing.T, runs ...*zfsv1.ZFSReplicationRun) *coordinationv1.Lease {
	t.Helper()
	claims := make([]destinationClaim, 0, len(runs))
	for _, run := range runs {
		claims = append(claims, destinationClaim{Namespace: run.Namespace, Name: run.Name, UID: run.UID, Dataset: run.Spec.Target.Dataset})
	}
	data, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: destinationLeaseName(runs[0].Spec.Target.NodeName), Namespace: "zfsreplication-system", Annotations: map[string]string{destinationClaimsAnnotation: string(data)}}}
}

func readDestinationClaims(t *testing.T, reader client.Reader, run *zfsv1.ZFSReplicationRun) []destinationClaim {
	t.Helper()
	var lease coordinationv1.Lease
	if err := reader.Get(context.Background(), types.NamespacedName{Namespace: "zfsreplication-system", Name: destinationLeaseName(run.Spec.Target.NodeName)}, &lease); err != nil {
		t.Fatal(err)
	}
	var claims []destinationClaim
	if err := json.Unmarshal([]byte(lease.Annotations[destinationClaimsAnnotation]), &claims); err != nil {
		t.Fatal(err)
	}
	if lease.Spec.HolderIdentity != nil {
		t.Fatalf("holderIdentity = %v, want unused", lease.Spec.HolderIdentity)
	}
	return claims
}

func TestDestinationFairnessAcrossNamespaces(t *testing.T) {
	now := time.Now()
	for _, tt := range []struct {
		name       string
		otherTime  time.Time
		wantLocked bool
	}{
		{"older pending", now.Add(-time.Minute), true},
		{"younger pending", now.Add(time.Minute), false},
		{"same time and name ordered by namespace", now, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := replicationRun("same-name")
			run.CreationTimestamp = metav1.NewTime(now)
			run.Status.Phase = zfsv1.PhasePending
			other := run.DeepCopy()
			other.Namespace = "other-namespace"
			other.UID = "other-uid"
			other.CreationTimestamp = metav1.NewTime(tt.otherTime)
			r := newRunReconciler(t, run, other)
			locked, _, err := r.destinationLocked(context.Background(), run)
			if err != nil || locked != tt.wantLocked {
				t.Fatalf("destinationLocked() = (%v, %v), want locked=%v", locked, err, tt.wantLocked)
			}
		})
	}
}
