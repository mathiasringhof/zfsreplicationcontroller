package controller

import (
	"context"
	"testing"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestReconcileNoRunIDCreatesNoJobs(t *testing.T) {
	r := newReconciler(t, replication(""))
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	var jobs batchv1.JobList
	if err := r.List(context.Background(), &jobs, client.InNamespace("storage")); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("jobs created = %d", len(jobs.Items))
	}
}

func TestReconcileNewRunCreatesReceiverSecretAndService(t *testing.T) {
	r := newReconciler(t, replication("manual-1"))
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	assertExists[*corev1.Secret](t, r.Client, "zfsrep-rep-manual-1-token")
	assertExists[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-1-receiver")
	assertExists[*corev1.Service](t, r.Client, "zfsrep-rep-manual-1-receiver")
	assertMissing[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-1-sender")
}

func TestReconcileReceiverReadyCreatesSender(t *testing.T) {
	rep := replication("manual-1")
	pod := receiverReadyPod(rep)
	r := newReconciler(t, rep, pod)
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	assertExists[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-1-sender")
}

func TestReconcileSucceededUpdatesStatus(t *testing.T) {
	rep := replication("manual-1")
	r := newReconciler(t, rep, receiverReadyPod(rep), succeededJob("zfsrep-rep-manual-1-receiver"), succeededJob("zfsrep-rep-manual-1-sender"))
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	var got zfsv1.ZFSReplication
	if err := r.Get(context.Background(), types.NamespacedName{Name: "rep", Namespace: "storage"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != zfsv1.PhaseSucceeded || got.Status.LastSuccessfulRunID != "manual-1" {
		t.Fatalf("status = %#v", got.Status)
	}
	assertMissing[*corev1.Service](t, r.Client, "zfsrep-rep-manual-1-receiver")
	assertMissing[*corev1.Secret](t, r.Client, "zfsrep-rep-manual-1-token")
}

func TestReconcileSenderFailedDoesNotUpdateSuccess(t *testing.T) {
	rep := replication("manual-1")
	r := newReconciler(t, rep, receiverReadyPod(rep), succeededJob("zfsrep-rep-manual-1-receiver"), failedJob("zfsrep-rep-manual-1-sender"))
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	var got zfsv1.ZFSReplication
	if err := r.Get(context.Background(), types.NamespacedName{Name: "rep", Namespace: "storage"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != zfsv1.PhaseFailed || got.Status.LastSuccessfulRunID != "" {
		t.Fatalf("status = %#v", got.Status)
	}
}

func TestReconcileAlreadySucceededStartsNothing(t *testing.T) {
	rep := replication("manual-1")
	rep.Status.LastSuccessfulRunID = "manual-1"
	r := newReconciler(t, rep)
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	assertMissing[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-1-receiver")
}

func newReconciler(t *testing.T, objs ...client.Object) *ZFSReplicationReconciler {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := zfsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&zfsv1.ZFSReplication{}).WithObjects(objs...).Build()
	return &ZFSReplicationReconciler{Client: c, Scheme: scheme, DataMoverImage: "datamover:test"}
}

func replication(runID string) *zfsv1.ZFSReplication {
	return &zfsv1.ZFSReplication{
		TypeMeta:   metav1.TypeMeta{APIVersion: zfsv1.Group + "/" + zfsv1.Version, Kind: "ZFSReplication"},
		ObjectMeta: metav1.ObjectMeta{Name: "rep", Namespace: "storage"},
		Spec: zfsv1.ZFSReplicationSpec{
			RunID:          runID,
			Source:         zfsv1.DatasetRef{NodeName: "worker-a", Dataset: "tank/src"},
			Target:         zfsv1.DatasetRef{NodeName: "worker-b", Dataset: "tank/dst"},
			SnapshotPrefix: "zsync",
			Bootstrap:      zfsv1.BootstrapSpec{Mode: zfsv1.BootstrapDestroyTargetAndReceiveFull},
		},
	}
}

func request(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "storage"}}
}

func receiverReadyPod(rep *zfsv1.ZFSReplication) *corev1.Pod {
	names := objectNames(rep)
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "receiver"
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "receiver-pod", Namespace: "storage", Labels: labels},
		Status:     corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
}

func succeededJob(name string) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "storage"}, Status: batchv1.JobStatus{Succeeded: 1}}
}

func failedJob(name string) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "storage"}, Status: batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "boom"}}}}
}

func assertExists[T client.Object](t *testing.T, c client.Client, name string) {
	t.Helper()
	obj := newObject[T]()
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "storage"}, obj); err != nil {
		t.Fatalf("%s missing: %v", name, err)
	}
}

func assertMissing[T client.Object](t *testing.T, c client.Client, name string) {
	t.Helper()
	obj := newObject[T]()
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "storage"}, obj); err == nil {
		t.Fatalf("%s exists", name)
	}
}

func newObject[T client.Object]() T {
	var zero T
	switch any(zero).(type) {
	case *corev1.Secret:
		obj, ok := any(&corev1.Secret{}).(T)
		if !ok {
			panic("unsupported type")
		}
		return obj
	case *corev1.Service:
		obj, ok := any(&corev1.Service{}).(T)
		if !ok {
			panic("unsupported type")
		}
		return obj
	case *batchv1.Job:
		obj, ok := any(&batchv1.Job{}).(T)
		if !ok {
			panic("unsupported type")
		}
		return obj
	default:
		panic("unsupported type")
	}
}
