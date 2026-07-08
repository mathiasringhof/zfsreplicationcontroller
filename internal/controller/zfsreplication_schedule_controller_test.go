package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

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

func TestScheduleReconcilePrunesTerminalRunHistoryWithDefaults(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 7, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.CreationTimestamp = metav1.NewTime(now)

	r := newScheduleReconciler(t, now,
		schedule,
		scheduledTerminalRun("success-1", "hourly", zfsv1.PhaseSucceeded, now.Add(-5*time.Hour)),
		scheduledTerminalRun("success-2", "hourly", zfsv1.PhaseSucceeded, now.Add(-4*time.Hour)),
		scheduledTerminalRun("success-3", "hourly", zfsv1.PhaseSucceeded, now.Add(-3*time.Hour)),
		scheduledTerminalRun("success-4", "hourly", zfsv1.PhaseSucceeded, now.Add(-2*time.Hour)),
		scheduledTerminalRun("failure-1", "hourly", zfsv1.PhaseFailed, now.Add(-5*time.Hour)),
		scheduledTerminalRun("failure-2", "hourly", zfsv1.PhaseFailed, now.Add(-4*time.Hour)),
	)

	if _, err := r.Reconcile(context.Background(), request("hourly")); err != nil {
		t.Fatal(err)
	}

	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "success-1")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "success-2")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "success-3")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "success-4")
	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "failure-1")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "failure-2")
}

func TestScheduleReconcilePrunesTerminalRunHistoryWithExplicitZero(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 7, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.CreationTimestamp = metav1.NewTime(now)
	schedule.Spec.SuccessfulRunsHistoryLimit = ptr(int32(0))
	schedule.Spec.FailedRunsHistoryLimit = ptr(int32(0))

	active := scheduledTerminalRun("active", "hourly", zfsv1.PhaseRunning, now.Add(-3*time.Hour))
	manual := replicationRun("manual-success")
	manual.Status.Phase = zfsv1.PhaseSucceeded
	completedAt := metav1.NewTime(now.Add(-3 * time.Hour))
	manual.Status.CompletedAt = &completedAt

	r := newScheduleReconciler(t, now,
		schedule,
		scheduledTerminalRun("success", "hourly", zfsv1.PhaseSucceeded, now.Add(-2*time.Hour)),
		scheduledTerminalRun("failure", "hourly", zfsv1.PhaseFailed, now.Add(-1*time.Hour)),
		active,
		manual,
	)

	if _, err := r.Reconcile(context.Background(), request("hourly")); err != nil {
		t.Fatal(err)
	}

	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "success")
	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "failure")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "active")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "manual-success")
}

func TestScheduleReconcilePrunesTerminalRunHistoryByCompletionCreationAndName(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 7, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.CreationTimestamp = metav1.NewTime(now)
	schedule.Spec.SuccessfulRunsHistoryLimit = ptr(int32(2))
	schedule.Spec.FailedRunsHistoryLimit = ptr(int32(10))

	withoutCompletedAt := scheduledTerminalRun("without-completed-at", "hourly", zfsv1.PhaseSucceeded, time.Time{})
	withoutCompletedAt.CreationTimestamp = metav1.NewTime(now.Add(-6 * time.Hour))

	sameCompletedAt := now.Add(-3 * time.Hour)
	nameTieLoser := scheduledTerminalRun("same-a", "hourly", zfsv1.PhaseSucceeded, sameCompletedAt)
	nameTieWinner := scheduledTerminalRun("same-b", "hourly", zfsv1.PhaseSucceeded, sameCompletedAt)
	sameCreationTime := metav1.NewTime(now.Add(-4 * time.Hour))
	nameTieLoser.CreationTimestamp = sameCreationTime
	nameTieWinner.CreationTimestamp = sameCreationTime

	newest := scheduledTerminalRun("newest", "hourly", zfsv1.PhaseSucceeded, now.Add(-1*time.Hour))

	r := newScheduleReconciler(t, now, schedule, withoutCompletedAt, nameTieLoser, nameTieWinner, newest)

	if _, err := r.Reconcile(context.Background(), request("hourly")); err != nil {
		t.Fatal(err)
	}

	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "without-completed-at")
	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "same-a")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "same-b")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "newest")
}

func TestScheduleReconcilePruneFailureReturnsAfterStatusUpdate(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 7, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.CreationTimestamp = metav1.NewTime(now.Add(-2 * time.Hour))
	schedule.Spec.SuccessfulRunsHistoryLimit = ptr(int32(0))
	oldRun := scheduledTerminalRun("old-success", "hourly", zfsv1.PhaseSucceeded, now.Add(-2*time.Hour))

	r := newScheduleReconcilerWithInterceptors(t, now, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetName() == "old-success" {
				return errors.New("temporary run delete failure")
			}
			return c.Delete(ctx, obj, opts...)
		},
	}, schedule, oldRun)

	_, err := r.Reconcile(context.Background(), request("hourly"))
	if err == nil || !strings.Contains(err.Error(), "temporary run delete failure") {
		t.Fatalf("Reconcile() error = %v, want temporary run delete failure", err)
	}

	var got zfsv1.ZFSReplicationSchedule
	if err := r.Get(context.Background(), types.NamespacedName{Name: "hourly", Namespace: "storage"}, &got); err != nil {
		t.Fatal(err)
	}
	wantDue := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	if got.Status.LastScheduleTime == nil || !got.Status.LastScheduleTime.Time.Equal(wantDue) {
		t.Fatalf("LastScheduleTime = %v, want %v", got.Status.LastScheduleTime, wantDue)
	}
	if got.Status.LastRunName != scheduledRunName("hourly", wantDue) {
		t.Fatalf("LastRunName = %q, want %q", got.Status.LastRunName, scheduledRunName("hourly", wantDue))
	}
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "old-success")
}

func TestScheduleReconcilePruneFailureStopsAtOldestRun(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 7, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.CreationTimestamp = metav1.NewTime(now.Add(-2 * time.Hour))
	schedule.Spec.SuccessfulRunsHistoryLimit = ptr(int32(0))

	oldest := scheduledTerminalRun("success-1", "hourly", zfsv1.PhaseSucceeded, now.Add(-3*time.Hour))
	later := scheduledTerminalRun("success-2", "hourly", zfsv1.PhaseSucceeded, now.Add(-2*time.Hour))

	r := newScheduleReconcilerWithInterceptors(t, now, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetName() == oldest.Name {
				return errors.New("temporary run delete failure")
			}
			return c.Delete(ctx, obj, opts...)
		},
	}, schedule, oldest, later)

	_, err := r.Reconcile(context.Background(), request("hourly"))
	if err == nil || !strings.Contains(err.Error(), "delete old scheduled run storage/success-1: temporary run delete failure") {
		t.Fatalf("Reconcile() error = %v, want wrapped temporary run delete failure", err)
	}

	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "success-1")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "success-2")
}

func TestScheduleReconcilePruneListFailureReturnsAfterStatusUpdate(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 7, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.CreationTimestamp = metav1.NewTime(now.Add(-2 * time.Hour))
	schedule.Spec.ConcurrencyPolicy = zfsv1.ConcurrencyPolicyAllow

	r := newScheduleReconcilerWithInterceptors(t, now, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			switch list.(type) {
			case *zfsv1.ZFSReplicationRunList:
				return errors.New("temporary run list failure")
			default:
				return c.List(ctx, list, opts...)
			}
		},
	}, schedule)

	_, err := r.Reconcile(context.Background(), request("hourly"))
	if err == nil || !strings.Contains(err.Error(), "temporary run list failure") {
		t.Fatalf("Reconcile() error = %v, want temporary run list failure", err)
	}

	wantDue := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	var got zfsv1.ZFSReplicationSchedule
	if err := r.Get(context.Background(), types.NamespacedName{Name: "hourly", Namespace: "storage"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.LastScheduleTime == nil || !got.Status.LastScheduleTime.Time.Equal(wantDue) {
		t.Fatalf("LastScheduleTime = %v, want %v", got.Status.LastScheduleTime, wantDue)
	}
	if got.Status.LastRunName != scheduledRunName("hourly", wantDue) {
		t.Fatalf("LastRunName = %q, want %q", got.Status.LastRunName, scheduledRunName("hourly", wantDue))
	}
	if got.Status.LastError != "" {
		t.Fatalf("LastError = %q, want empty", got.Status.LastError)
	}
}

func TestScheduleReconcileRejectsNegativeHistoryLimit(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 7, 0, 0, time.UTC)

	tests := []struct {
		name      string
		configure func(*zfsv1.ZFSReplicationSchedule)
		wantError string
	}{
		{
			name: "successful runs history limit",
			configure: func(schedule *zfsv1.ZFSReplicationSchedule) {
				schedule.Spec.SuccessfulRunsHistoryLimit = ptr(int32(-1))
			},
			wantError: "successfulRunsHistoryLimit must be greater than or equal to 0",
		},
		{
			name: "failed runs history limit",
			configure: func(schedule *zfsv1.ZFSReplicationSchedule) {
				schedule.Spec.FailedRunsHistoryLimit = ptr(int32(-1))
			},
			wantError: "failedRunsHistoryLimit must be greater than or equal to 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := replicationSchedule("hourly")
			tt.configure(schedule)
			r := newScheduleReconciler(t, now, schedule)

			_, err := r.Reconcile(context.Background(), request("hourly"))
			if err != nil {
				t.Fatalf("Reconcile() error = %v, want nil", err)
			}

			var got zfsv1.ZFSReplicationSchedule
			if err := r.Get(context.Background(), types.NamespacedName{Name: "hourly", Namespace: "storage"}, &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.LastError != tt.wantError {
				t.Fatalf("LastError = %q, want %q", got.Status.LastError, tt.wantError)
			}
		})
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

func scheduledTerminalRun(name, scheduleName string, phase zfsv1.Phase, completedAt time.Time) *zfsv1.ZFSReplicationRun {
	run := replicationRun(name)
	run.CreationTimestamp = metav1.NewTime(completedAt.Add(-time.Minute))
	run.Labels = map[string]string{
		labelPrefix + "/schedule": scheduleName,
	}
	run.Status.Phase = phase
	if !completedAt.IsZero() {
		t := metav1.NewTime(completedAt)
		run.Status.CompletedAt = &t
	}
	return run
}

func newScheduleReconcilerWithInterceptors(t *testing.T, now time.Time, funcs interceptor.Funcs, objs ...client.Object) *ZFSReplicationScheduleReconciler {
	t.Helper()
	scheme := newTestScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&zfsv1.ZFSReplicationSchedule{}).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
	return &ZFSReplicationScheduleReconciler{Client: c, Scheme: scheme, Now: func() time.Time { return now }}
}
