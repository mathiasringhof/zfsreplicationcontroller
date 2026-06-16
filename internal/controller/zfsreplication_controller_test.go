package controller

import (
	"context"
	"testing"
	"time"

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

func TestReconcileNewRunCreatesReceiverSecretAndReceiverJobOnly(t *testing.T) {
	r := newReconciler(t, replication("manual-1"))
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	assertExists[*corev1.Secret](t, r.Client, "zfsrep-rep-manual-1-token")
	assertExists[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-1-receiver")
	assertMissing[*corev1.Service](t, r.Client, "zfsrep-rep-manual-1-receiver")
	assertMissing[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-1-sender")
}

func TestReconcileDoesNotCreateSenderBeforeReceiverPodExists(t *testing.T) {
	rep := replication("manual-1")
	r := newReconciler(t, rep)
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	assertMissing[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-1-sender")
}

func TestReconcileDoesNotCreateSenderWhenReceiverPodHasNoIP(t *testing.T) {
	rep := replication("manual-1")
	pod := receiverPod(rep, corev1.PodRunning, "", true)
	r := newReconciler(t, rep, pod)
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	assertMissing[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-1-sender")
}

func TestReconcileDoesNotCreateSenderWhenReceiverPodIsNotReady(t *testing.T) {
	rep := replication("manual-1")
	pod := receiverPod(rep, corev1.PodRunning, "10.244.3.27", false)
	r := newReconciler(t, rep, pod)
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	assertMissing[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-1-sender")
}

func TestReconcileReceiverUsableCreatesSenderWithPodIP(t *testing.T) {
	rep := replication("manual-1")
	pod := receiverReadyPod(rep)
	r := newReconciler(t, rep, pod)
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	sender := getJob(t, r.Client, "zfsrep-rep-manual-1-sender")
	if got := envValue(sender, "RECEIVER_URL"); got != "http://10.244.3.27:8080/receive" {
		t.Fatalf("RECEIVER_URL = %q", got)
	}
	var got zfsv1.ZFSReplication
	if err := r.Get(context.Background(), types.NamespacedName{Name: "rep", Namespace: "storage"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.ReceiverPodName != "receiver-pod" || got.Status.ReceiverPodIP != "10.244.3.27" {
		t.Fatalf("receiver pod status = name %q ip %q", got.Status.ReceiverPodName, got.Status.ReceiverPodIP)
	}
}

func TestReconcileMultipleReadyReceiverPodsFails(t *testing.T) {
	rep := replication("manual-1")
	pod1 := receiverReadyPod(rep)
	pod2 := receiverReadyPod(rep)
	pod2.Name = "receiver-pod-2"
	pod2.Status.PodIP = "10.244.3.28"
	r := newReconciler(t, rep, pod1, pod2)
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	var got zfsv1.ZFSReplication
	if err := r.Get(context.Background(), types.NamespacedName{Name: "rep", Namespace: "storage"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != zfsv1.PhaseFailed || got.Status.LastError != "multiple ready receiver Pods found" {
		t.Fatalf("status = %#v", got.Status)
	}
	assertMissing[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-1-sender")
}

func TestReconcileFailedReceiverPodBeforeSenderFails(t *testing.T) {
	rep := replication("manual-1")
	pod := receiverPod(rep, corev1.PodFailed, "", false)
	r := newReconciler(t, rep, pod)
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	var got zfsv1.ZFSReplication
	if err := r.Get(context.Background(), types.NamespacedName{Name: "rep", Namespace: "storage"}, &got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != zfsv1.PhaseFailed || got.Status.LastError == "" {
		t.Fatalf("status = %#v", got.Status)
	}
	assertMissing[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-1-sender")
}

func TestDataMoverJobsUseRequestedNodesAndNoRestarts(t *testing.T) {
	rep := replication("manual-1")
	r := newReconciler(t, rep, receiverReadyPod(rep))
	if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
		t.Fatal(err)
	}
	receiver := getJob(t, r.Client, "zfsrep-rep-manual-1-receiver")
	sender := getJob(t, r.Client, "zfsrep-rep-manual-1-sender")
	assertJobPlacement(t, receiver, "worker-b")
	assertJobPlacement(t, sender, "worker-a")
	if receiver.Spec.Template.Spec.Containers[0].ReadinessProbe == nil {
		t.Fatalf("receiver readiness probe missing")
	}
	if sender.Spec.Template.Spec.Containers[0].ReadinessProbe != nil {
		t.Fatalf("sender readiness probe present")
	}
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

func TestReconcileActiveLeaseHeldByDifferentRunBlocksNewRun(t *testing.T) {
	rep := replication("manual-2")
	r := newReconciler(t, rep, lease("zfsrep-rep", "manual-1", "active"))
	result, err := r.Reconcile(context.Background(), request("rep"))
	if err != nil {
		t.Fatal(err)
	}
	if result.RequeueAfter < 14*time.Second || result.RequeueAfter > 16*time.Second {
		t.Fatalf("RequeueAfter = %s, want around 15s", result.RequeueAfter)
	}
	assertMissing[*corev1.Secret](t, r.Client, "zfsrep-rep-manual-2-token")
	assertMissing[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-2-receiver")
	assertMissing[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-2-sender")
}

func TestReconcileNonActiveLeaseCanBeReusedByNewRun(t *testing.T) {
	tests := []struct {
		name  string
		state string
	}{
		{name: "failed", state: "failed"},
		{name: "succeeded", state: "succeeded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rep := replication("manual-2")
			r := newReconciler(t, rep, lease("zfsrep-rep", "manual-1", tt.state))
			if _, err := r.Reconcile(context.Background(), request("rep")); err != nil {
				t.Fatal(err)
			}
			assertExists[*corev1.Secret](t, r.Client, "zfsrep-rep-manual-2-token")
			assertExists[*batchv1.Job](t, r.Client, "zfsrep-rep-manual-2-receiver")

			var got coordinationv1.Lease
			if err := r.Get(context.Background(), types.NamespacedName{Name: "zfsrep-rep", Namespace: "storage"}, &got); err != nil {
				t.Fatal(err)
			}
			if got.Spec.HolderIdentity == nil || *got.Spec.HolderIdentity != "manual-2" {
				t.Fatalf("HolderIdentity = %v, want manual-2", got.Spec.HolderIdentity)
			}
			if got.Annotations[leaseStateAnnotation] != "active" {
				t.Fatalf("lease state = %q, want active", got.Annotations[leaseStateAnnotation])
			}
		})
	}
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
	return receiverPod(rep, corev1.PodRunning, "10.244.3.27", true)
}

func receiverPod(rep *zfsv1.ZFSReplication, phase corev1.PodPhase, podIP string, ready bool) *corev1.Pod {
	names := objectNames(rep)
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "receiver"
	conditionStatus := corev1.ConditionFalse
	if ready {
		conditionStatus = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "receiver-pod", Namespace: "storage", Labels: labels},
		Status: corev1.PodStatus{
			Phase:      phase,
			PodIP:      podIP,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: conditionStatus}},
		},
	}
}

func succeededJob(name string) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "storage"}, Status: batchv1.JobStatus{Succeeded: 1}}
}

func failedJob(name string) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "storage"}, Status: batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Message: "boom"}}}}
}

func lease(name, holder, state string) *coordinationv1.Lease {
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   "storage",
			Annotations: map[string]string{leaseStateAnnotation: state},
		},
		Spec: coordinationv1.LeaseSpec{HolderIdentity: &holder},
	}
}

func getJob(t *testing.T, c client.Client, name string) *batchv1.Job {
	t.Helper()
	var job batchv1.Job
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "storage"}, &job); err != nil {
		t.Fatal(err)
	}
	return &job
}

func envValue(job *batchv1.Job, name string) string {
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == name {
			return env.Value
		}
	}
	return ""
}

func assertJobPlacement(t *testing.T, job *batchv1.Job, nodeName string) {
	t.Helper()
	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Fatalf("%s backoffLimit = %v", job.Name, job.Spec.BackoffLimit)
	}
	if job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatalf("%s restartPolicy = %s", job.Name, job.Spec.Template.Spec.RestartPolicy)
	}
	if job.Spec.Template.Spec.NodeName != nodeName {
		t.Fatalf("%s nodeName = %q", job.Name, job.Spec.Template.Spec.NodeName)
	}
	if got := envValue(job, "EXPECTED_NODE_NAME"); got != nodeName {
		t.Fatalf("%s EXPECTED_NODE_NAME = %q", job.Name, got)
	}
	foundActual := false
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "ACTUAL_NODE_NAME" && env.ValueFrom != nil && env.ValueFrom.FieldRef != nil && env.ValueFrom.FieldRef.FieldPath == "spec.nodeName" {
			foundActual = true
		}
	}
	if !foundActual {
		t.Fatalf("%s ACTUAL_NODE_NAME downward API env missing", job.Name)
	}
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
	case *coordinationv1.Lease:
		obj, ok := any(&coordinationv1.Lease{}).(T)
		if !ok {
			panic("unsupported type")
		}
		return obj
	default:
		panic("unsupported type")
	}
}
