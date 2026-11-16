package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
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

	runName := scheduledRunName(schedule, time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC))
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

func TestScheduleReconcileCreatesDistinctRunsForSubminuteTicks(t *testing.T) {
	createdAt := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	schedule := replicationSchedule("frequent")
	schedule.CreationTimestamp = metav1.NewTime(createdAt)
	schedule.Spec.Schedule = "@every 10s"
	schedule.Spec.ConcurrencyPolicy = zfsv1.ConcurrencyPolicyAllow
	r := newScheduleReconciler(t, createdAt.Add(10*time.Second), schedule)

	for _, elapsed := range []time.Duration{10 * time.Second, 20 * time.Second} {
		r.Now = func() time.Time { return createdAt.Add(elapsed) }
		if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err != nil {
			t.Fatal(err)
		}
	}

	var runs zfsv1.ZFSReplicationRunList
	if err := r.List(context.Background(), &runs, client.InNamespace(schedule.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 2 {
		t.Fatalf("scheduled runs = %d, want one for each of two ticks", len(runs.Items))
	}
	wantInstants := map[string]bool{
		"2026-06-20T12:00:10Z": true,
		"2026-06-20T12:00:20Z": true,
	}
	for _, run := range runs.Items {
		instant := run.Annotations[labelPrefix+"/scheduled-at"]
		if !wantInstants[instant] {
			t.Fatalf("scheduled-at = %q, want distinct supported due instants", instant)
		}
		delete(wantInstants, instant)
	}
}

func TestScheduleReconcileDistinguishesScheduleIdentities(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		first  string
		second string
	}{
		{name: "normalization", first: "backup.daily", second: "backup-daily"},
		{name: "truncation", first: strings.Repeat("backup-", 30) + "daily", second: strings.Repeat("backup-", 30) + "weekly"},
		{name: "recreation", first: "hourly", second: "hourly"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first := replicationSchedule(tt.first)
			first.UID = "first-incarnation"
			first.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
			first.Spec.ConcurrencyPolicy = zfsv1.ConcurrencyPolicyAllow
			r := newScheduleReconciler(t, now, first)
			if _, err := r.Reconcile(context.Background(), request(first.Name)); err != nil {
				t.Fatal(err)
			}
			if first.Name == tt.second {
				if err := r.Delete(context.Background(), first); err != nil {
					t.Fatal(err)
				}
			}
			second := replicationSchedule(tt.second)
			second.UID = "second-incarnation"
			second.CreationTimestamp = first.CreationTimestamp
			second.Spec.ConcurrencyPolicy = zfsv1.ConcurrencyPolicyAllow
			if err := r.Create(context.Background(), second); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Reconcile(context.Background(), request(second.Name)); err != nil {
				t.Fatal(err)
			}

			var runs zfsv1.ZFSReplicationRunList
			if err := r.List(context.Background(), &runs, client.InNamespace(first.Namespace)); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 2 {
				t.Fatalf("scheduled runs = %d, want one per exact schedule incarnation", len(runs.Items))
			}
			owners := map[types.UID]bool{"first-incarnation": true, "second-incarnation": true}
			for _, run := range runs.Items {
				if problems := validation.IsDNS1123Label(run.Name); len(problems) > 0 {
					t.Fatalf("run name %q is invalid: %v", run.Name, problems)
				}
				for _, value := range run.Labels {
					if problems := validation.IsValidLabelValue(value); len(problems) > 0 {
						t.Fatalf("run label value %q is invalid: %v", value, problems)
					}
				}
				owner := metav1.GetControllerOf(&run)
				if owner == nil || !owners[owner.UID] {
					t.Fatalf("controller owner = %#v, want a distinct schedule incarnation", owner)
				}
				delete(owners, owner.UID)
			}
		})
	}
}

func TestScheduleReconcileLongNameCreatesValidRunMetadata(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	schedule := replicationSchedule(strings.Repeat("backup.", 25) + "daily")
	schedule.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
	schedule.Spec.SuccessfulRunsHistoryLimit = ptr(int32(0))
	r := newScheduleReconcilerWithInterceptors(t, now, interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			problems := validation.IsDNS1123Label(obj.GetName())
			for _, value := range obj.GetLabels() {
				problems = append(problems, validation.IsValidLabelValue(value)...)
			}
			if len(problems) > 0 {
				return errors.New("invalid run metadata: " + strings.Join(problems, "; "))
			}
			return c.Create(ctx, obj, opts...)
		},
	}, schedule)
	if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err != nil {
		t.Fatal(err)
	}
	var runs zfsv1.ZFSReplicationRunList
	if err := r.List(context.Background(), &runs, client.InNamespace(schedule.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("scheduled runs = %d, want one", len(runs.Items))
	}
	run := &runs.Items[0]
	run.Status.Phase = zfsv1.PhaseSucceeded
	if err := r.Update(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err != nil {
		t.Fatal(err)
	}
	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, run.Name)
}

func TestScheduleReconcileRejectsConflictingExistingRun(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		change func(*zfsv1.ZFSReplicationRun)
	}{
		{name: "previous schedule incarnation", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].UID = "previous-incarnation" }},
		{name: "no owner", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences = nil }},
		{name: "non-controller owner", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].Controller = ptr(false) }},
		{name: "wrong owner name", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].Name = "other" }},
		{name: "wrong owner kind", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].Kind = "ZFSReplicationRun" }},
		{name: "wrong owner API version", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].APIVersion = "other/v1" }},
		{name: "missing due instant", change: func(run *zfsv1.ZFSReplicationRun) { run.Annotations = nil }},
		{name: "malformed due instant", change: func(run *zfsv1.ZFSReplicationRun) { run.Annotations[labelPrefix+"/scheduled-at"] = "not-a-time" }},
		{name: "different due instant", change: func(run *zfsv1.ZFSReplicationRun) {
			run.Annotations[labelPrefix+"/scheduled-at"] = "2026-06-20T11:00:00Z"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := replicationSchedule("hourly")
			schedule.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
			schedule.Spec.ConcurrencyPolicy = zfsv1.ConcurrencyPolicyAllow
			r := newScheduleReconciler(t, now, schedule)
			if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err != nil {
				t.Fatal(err)
			}
			var got zfsv1.ZFSReplicationSchedule
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(schedule), &got); err != nil {
				t.Fatal(err)
			}
			var existing zfsv1.ZFSReplicationRun
			if err := r.Get(context.Background(), types.NamespacedName{Name: got.Status.LastRunName, Namespace: got.Namespace}, &existing); err != nil {
				t.Fatal(err)
			}
			tt.change(&existing)
			if err := r.Update(context.Background(), &existing); err != nil {
				t.Fatal(err)
			}
			got.Status = zfsv1.ZFSReplicationScheduleStatus{LastError: "previous error"}
			if err := r.Status().Update(context.Background(), &got); err != nil {
				t.Fatal(err)
			}

			if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err == nil {
				t.Fatal("Reconcile() succeeded for a conflicting existing run")
			}
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(schedule), &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.LastScheduleTime != nil || got.Status.LastRunName != "" || got.Status.LastError != "previous error" {
				t.Fatalf("schedule status advanced on conflict: %#v", got.Status)
			}
			var unchanged zfsv1.ZFSReplicationRun
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(&existing), &unchanged); err != nil {
				t.Fatal(err)
			}
			if unchanged.ResourceVersion != existing.ResourceVersion {
				t.Fatal("conflicting run was modified")
			}
		})
	}
}

func TestScheduleReconcileValidatesRunCreatedConcurrently(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name      string
		change    func(*zfsv1.ZFSReplicationRun)
		wantError bool
	}{
		{name: "same owner and instant", change: func(run *zfsv1.ZFSReplicationRun) {
			run.Annotations[labelPrefix+"/scheduled-at"] = "2026-06-20T14:00:00+02:00"
		}},
		{name: "foreign owner", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].UID = "foreign" }, wantError: true},
		{name: "another instant", change: func(run *zfsv1.ZFSReplicationRun) {
			run.Annotations[labelPrefix+"/scheduled-at"] = "2026-06-20T11:00:00Z"
		}, wantError: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := replicationSchedule("hourly")
			schedule.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
			r := newScheduleReconcilerWithInterceptors(t, now, interceptor.Funcs{
				Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
					source, ok := obj.(*zfsv1.ZFSReplicationRun)
					if !ok {
						return c.Create(ctx, obj, opts...)
					}
					run := source.DeepCopy()
					run.UID = "concurrent-run"
					tt.change(run)
					if err := c.Create(ctx, run, opts...); err != nil {
						return err
					}
					return apierrors.NewAlreadyExists(zfsv1.SchemeGroupVersion.WithResource("zfsreplicationruns").GroupResource(), run.Name)
				},
			}, schedule)
			_, err := r.Reconcile(context.Background(), request(schedule.Name))
			if (err != nil) != tt.wantError {
				t.Fatalf("Reconcile() error = %v, want error %t", err, tt.wantError)
			}
			var got zfsv1.ZFSReplicationSchedule
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(schedule), &got); err != nil {
				t.Fatal(err)
			}
			if tt.wantError {
				if got.Status.LastScheduleTime != nil || got.Status.LastRunName != "" {
					t.Fatalf("schedule status advanced on conflict: %#v", got.Status)
				}
			} else if got.Status.LastScheduleTime == nil || !got.Status.LastScheduleTime.Time.Equal(now) || got.Status.LastRunName == "" {
				t.Fatalf("matching existing run was not recorded: %#v", got.Status)
			}
			var runs zfsv1.ZFSReplicationRunList
			if err := r.List(context.Background(), &runs, client.InNamespace(schedule.Namespace)); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 1 || runs.Items[0].UID != "concurrent-run" {
				t.Fatalf("concurrently created run was replaced: %#v", runs.Items)
			}
		})
	}
}

func TestScheduleReconcileRecordsExistingTickAfterStatusPatchFailure(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
	schedule.Spec.ConcurrencyPolicy = ""
	failed := false
	r := newScheduleReconcilerWithInterceptors(t, now, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if !failed {
				failed = true
				return errors.New("temporary status patch failure")
			}
			return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
	}, schedule)
	if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err == nil || !strings.Contains(err.Error(), "temporary status patch failure") {
		t.Fatalf("first Reconcile() error = %v, want injected status failure", err)
	}
	var runs zfsv1.ZFSReplicationRunList
	if err := r.List(context.Background(), &runs, client.InNamespace(schedule.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 {
		t.Fatalf("created runs = %d, want one before status retry", len(runs.Items))
	}
	created := runs.Items[0]
	for range 2 {
		if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err != nil {
			t.Fatal(err)
		}
	}
	var got zfsv1.ZFSReplicationSchedule
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(schedule), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.LastScheduleTime == nil || !got.Status.LastScheduleTime.Time.Equal(now) || got.Status.LastRunName != created.Name || got.Status.LastError != "" {
		t.Fatalf("status retry did not record the existing tick: %#v", got.Status)
	}
	if err := r.List(context.Background(), &runs, client.InNamespace(schedule.Namespace)); err != nil {
		t.Fatal(err)
	}
	if len(runs.Items) != 1 || runs.Items[0].ResourceVersion != created.ResourceVersion {
		t.Fatalf("same tick created or changed runs on retry: %#v", runs.Items)
	}
}

func TestScheduleReconcilePrunesOnlyCurrentScheduleHistory(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		change func(*zfsv1.ZFSReplicationRun)
	}{
		{name: "previous schedule incarnation", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].UID = "previous-incarnation" }},
		{name: "no owner", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences = nil }},
		{name: "non-controller owner", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].Controller = ptr(false) }},
		{name: "unspecified controller", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].Controller = nil }},
		{name: "wrong owner name", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].Name = "other" }},
		{name: "wrong owner kind", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].Kind = "ZFSReplicationRun" }},
		{name: "wrong owner API version", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].APIVersion = "other/v1" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := replicationSchedule("hourly")
			schedule.Spec.Suspend = ptr(true)
			schedule.Spec.SuccessfulRunsHistoryLimit = ptr(int32(0))
			schedule.Spec.FailedRunsHistoryLimit = ptr(int32(0))
			foreignSuccess := scheduledTerminalRun("foreign-success", schedule.Name, zfsv1.PhaseSucceeded, now.Add(-time.Hour))
			foreignFailure := scheduledTerminalRun("foreign-failure", schedule.Name, zfsv1.PhaseFailed, now.Add(-time.Hour))
			tt.change(foreignSuccess)
			tt.change(foreignFailure)
			r := newScheduleReconciler(t, now, schedule, foreignSuccess, foreignFailure,
				scheduledTerminalRun("owned-success", schedule.Name, zfsv1.PhaseSucceeded, now),
				scheduledTerminalRun("owned-failure", schedule.Name, zfsv1.PhaseFailed, now),
			)
			if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err != nil {
				t.Fatal(err)
			}
			assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, foreignSuccess.Name)
			assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, foreignFailure.Name)
			assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "owned-success")
			assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "owned-failure")
		})
	}
}

func TestScheduleReconcileForbidCountsOnlyCurrentScheduleRuns(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		change      func(*zfsv1.ZFSReplicationRun)
		wantSkipped bool
	}{
		{name: "current schedule", change: func(*zfsv1.ZFSReplicationRun) {}, wantSkipped: true},
		{name: "previous schedule incarnation", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].UID = "previous-incarnation" }},
		{name: "no owner", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences = nil }},
		{name: "non-controller owner", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].Controller = ptr(false) }},
		{name: "unspecified controller", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].Controller = nil }},
		{name: "wrong owner name", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].Name = "other" }},
		{name: "wrong owner kind", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].Kind = "ZFSReplicationRun" }},
		{name: "wrong owner API version", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].APIVersion = "other/v1" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := replicationSchedule("hourly")
			schedule.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
			active := scheduledTerminalRun("active", schedule.Name, zfsv1.PhaseRunning, now.Add(-time.Hour))
			tt.change(active)
			r := newScheduleReconciler(t, now, schedule, active)
			if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err != nil {
				t.Fatal(err)
			}
			var got zfsv1.ZFSReplicationSchedule
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(schedule), &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.LastScheduleTime == nil || !got.Status.LastScheduleTime.Time.Equal(now) {
				t.Fatalf("LastScheduleTime = %v, want current tick", got.Status.LastScheduleTime)
			}
			if tt.wantSkipped {
				if got.Status.LastRunName != "" || got.Status.LastError != "skipped scheduled run because a previous run is still active" {
					t.Fatalf("owned active run did not block creation: %#v", got.Status)
				}
			} else if got.Status.LastRunName == "" || got.Status.LastError != "" {
				t.Fatalf("foreign active run blocked creation: %#v", got.Status)
			}
			var runs zfsv1.ZFSReplicationRunList
			if err := r.List(context.Background(), &runs, client.InNamespace(schedule.Namespace)); err != nil {
				t.Fatal(err)
			}
			wantRuns := 2
			if tt.wantSkipped {
				wantRuns = 1
			}
			if len(runs.Items) != wantRuns {
				t.Fatalf("runs = %d, want %d", len(runs.Items), wantRuns)
			}
			assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, active.Name)
		})
	}
}

func TestScheduleReconcileDoesNotDeleteReplacementRun(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.Spec.Suspend = ptr(true)
	schedule.Spec.SuccessfulRunsHistoryLimit = ptr(int32(0))
	old := scheduledTerminalRun("old-success", schedule.Name, zfsv1.PhaseSucceeded, now.Add(-time.Hour))
	replaced := false
	r := newScheduleReconcilerWithInterceptors(t, now, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if !replaced && obj.GetName() == old.Name {
				replaced = true
				if err := c.Delete(ctx, old); err != nil {
					return err
				}
				replacement := old.DeepCopy()
				replacement.UID = "replacement-run"
				replacement.ResourceVersion = ""
				replacement.OwnerReferences = nil
				if err := c.Create(ctx, replacement); err != nil {
					return err
				}
			}
			var current zfsv1.ZFSReplicationRun
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), &current); err != nil {
				return err
			}
			// The fake client does not enforce Kubernetes UID delete preconditions.
			deleteOptions := (&client.DeleteOptions{}).ApplyOptions(opts)
			if deleteOptions.Preconditions != nil && deleteOptions.Preconditions.UID != nil && *deleteOptions.Preconditions.UID != current.UID {
				return apierrors.NewConflict(zfsv1.SchemeGroupVersion.WithResource("zfsreplicationruns").GroupResource(), current.Name, errors.New("UID precondition failed"))
			}
			return c.Delete(ctx, obj, opts...)
		},
	}, schedule, old)
	_, reconcileErr := r.Reconcile(context.Background(), request(schedule.Name))
	var replacement zfsv1.ZFSReplicationRun
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(old), &replacement); err != nil {
		t.Fatalf("same-name replacement did not survive pruning: %v", err)
	}
	if replacement.UID != "replacement-run" {
		t.Fatalf("remaining run UID = %q, want replacement-run", replacement.UID)
	}
	if !apierrors.IsConflict(reconcileErr) {
		t.Fatalf("Reconcile() error = %v, want UID precondition conflict", reconcileErr)
	}
	if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err != nil {
		t.Fatalf("retry after replacement conflict: %v", err)
	}
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, old.Name)
}

func TestScheduleReconcileReusesMatchingLegacyRun(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	for _, policy := range []zfsv1.ConcurrencyPolicy{zfsv1.ConcurrencyPolicyAllow, zfsv1.ConcurrencyPolicyForbid} {
		t.Run(string(policy), func(t *testing.T) {
			schedule := replicationSchedule("hourly")
			schedule.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
			schedule.Spec.ConcurrencyPolicy = policy
			legacy := scheduledTerminalRun("zfsrep-hourly-20260620-1200", schedule.Name, zfsv1.PhaseRunning, now)
			legacy.Annotations = map[string]string{labelPrefix + "/scheduled-at": "2026-06-20T12:00:00Z"}
			r := newScheduleReconciler(t, now, schedule, legacy)
			for range 2 {
				if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err != nil {
					t.Fatal(err)
				}
			}
			var got zfsv1.ZFSReplicationSchedule
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(schedule), &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.LastScheduleTime == nil || !got.Status.LastScheduleTime.Time.Equal(now) || got.Status.LastRunName != legacy.Name || got.Status.LastError != "" {
				t.Fatalf("matching legacy tick was not recorded: %#v", got.Status)
			}
			var runs zfsv1.ZFSReplicationRunList
			if err := r.List(context.Background(), &runs, client.InNamespace(schedule.Namespace)); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 1 || runs.Items[0].Name != legacy.Name || runs.Items[0].UID != legacy.UID {
				t.Fatalf("legacy run was duplicated or replaced: %#v", runs.Items)
			}
		})
	}
}

func TestScheduleReconcileIgnoresConflictingLegacyRun(t *testing.T) {
	createdAt := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	now := createdAt.Add(20 * time.Second)
	tests := []struct {
		name   string
		change func(*zfsv1.ZFSReplicationRun)
	}{
		{name: "previous schedule incarnation", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].UID = "previous-incarnation" }},
		{name: "normalized schedule name", change: func(run *zfsv1.ZFSReplicationRun) {
			run.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(replicationSchedule("backup.daily"), zfsv1.SchemeGroupVersion.WithKind("ZFSReplicationSchedule"))}
		}},
		{name: "no owner", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences = nil }},
		{name: "non-controller owner", change: func(run *zfsv1.ZFSReplicationRun) { run.OwnerReferences[0].Controller = ptr(false) }},
		{name: "previous subminute tick", change: func(run *zfsv1.ZFSReplicationRun) {
			run.Annotations[labelPrefix+"/scheduled-at"] = "2026-06-20T12:00:10Z"
		}},
		{name: "missing due instant", change: func(run *zfsv1.ZFSReplicationRun) { run.Annotations = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schedule := replicationSchedule("backup-daily")
			schedule.CreationTimestamp = metav1.NewTime(createdAt)
			schedule.Spec.Schedule = "@every 10s"
			schedule.Spec.ConcurrencyPolicy = zfsv1.ConcurrencyPolicyAllow
			legacy := scheduledTerminalRun("zfsrep-backup-daily-20260620-1200", schedule.Name, zfsv1.PhaseRunning, now)
			legacy.Annotations = map[string]string{labelPrefix + "/scheduled-at": "2026-06-20T12:00:20Z"}
			tt.change(legacy)
			r := newScheduleReconciler(t, now, schedule, legacy)
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(legacy), legacy); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err != nil {
				t.Fatal(err)
			}
			var got zfsv1.ZFSReplicationSchedule
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(schedule), &got); err != nil {
				t.Fatal(err)
			}
			if got.Status.LastScheduleTime == nil || !got.Status.LastScheduleTime.Time.Equal(now) || got.Status.LastRunName == "" || got.Status.LastRunName == legacy.Name || got.Status.LastError != "" {
				t.Fatalf("conflicting legacy name suppressed the new tick: %#v", got.Status)
			}
			var runs zfsv1.ZFSReplicationRunList
			if err := r.List(context.Background(), &runs, client.InNamespace(schedule.Namespace)); err != nil {
				t.Fatal(err)
			}
			if len(runs.Items) != 2 {
				t.Fatalf("runs = %d, want the unchanged legacy run plus the new tick", len(runs.Items))
			}
			var unchanged zfsv1.ZFSReplicationRun
			if err := r.Get(context.Background(), client.ObjectKeyFromObject(legacy), &unchanged); err != nil {
				t.Fatal(err)
			}
			if unchanged.UID != legacy.UID || unchanged.ResourceVersion != legacy.ResourceVersion {
				t.Fatalf("legacy run was changed: %#v", unchanged.ObjectMeta)
			}
		})
	}
}

func TestScheduleReconcileNamesAreDeterministicAcrossTimeZones(t *testing.T) {
	due := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.CreationTimestamp = metav1.NewTime(due.Add(-time.Hour))
	var firstName string
	for _, zone := range []*time.Location{time.UTC, time.FixedZone("offset", 2*60*60)} {
		r := newScheduleReconciler(t, due.In(zone), schedule.DeepCopy())
		if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err != nil {
			t.Fatal(err)
		}
		var got zfsv1.ZFSReplicationSchedule
		if err := r.Get(context.Background(), client.ObjectKeyFromObject(schedule), &got); err != nil {
			t.Fatal(err)
		}
		if firstName == "" {
			firstName = got.Status.LastRunName
		}
		if got.Status.LastRunName == "" || got.Status.LastRunName != firstName {
			t.Fatalf("same tick run name = %q, want %q", got.Status.LastRunName, firstName)
		}
	}
}

func TestScheduleReconcileEmptyUIDDoesNotEstablishOwnership(t *testing.T) {
	now := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.UID = ""
	schedule.Spec.Suspend = ptr(true)
	schedule.Spec.SuccessfulRunsHistoryLimit = ptr(int32(0))
	run := scheduledTerminalRun("unowned-success", schedule.Name, zfsv1.PhaseSucceeded, now)
	run.OwnerReferences[0].UID = ""
	r := newScheduleReconciler(t, now, schedule, run)
	if _, err := r.Reconcile(context.Background(), request(schedule.Name)); err != nil {
		t.Fatal(err)
	}
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, run.Name)
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
	if got.Status.LastRunName != scheduledRunName(schedule, wantDue) {
		t.Fatalf("LastRunName = %q, want %q", got.Status.LastRunName, scheduledRunName(schedule, wantDue))
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
	if got.Status.LastRunName != scheduledRunName(schedule, wantDue) {
		t.Fatalf("LastRunName = %q, want %q", got.Status.LastRunName, scheduledRunName(schedule, wantDue))
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
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "storage", UID: types.UID("schedule-" + name)},
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
	run.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(replicationSchedule(scheduleName), zfsv1.SchemeGroupVersion.WithKind("ZFSReplicationSchedule"))}
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
