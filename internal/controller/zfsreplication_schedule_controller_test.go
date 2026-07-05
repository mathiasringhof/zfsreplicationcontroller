package controller

import (
	"context"
	"testing"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
