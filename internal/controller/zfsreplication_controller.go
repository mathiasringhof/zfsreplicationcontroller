package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ZFSReplicationReconciler struct {
	client.Client
	APIReader      client.Reader
	Scheme         *runtime.Scheme
	DataMoverImage string
	PodLogs        PodLogReader
}

func (r *ZFSReplicationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var rep zfsv1.ZFSReplication
	if err := r.Get(ctx, req.NamespacedName, &rep); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if rep.Spec.RunID == "" {
		return ctrl.Result{}, r.patchStatus(ctx, &rep, func(st *zfsv1.ZFSReplicationStatus) {
			if st.Phase == "" {
				st.Phase = zfsv1.PhasePending
			}
		})
	}
	if rep.Status.LastSuccessfulRunID == rep.Spec.RunID {
		return ctrl.Result{}, nil
	}
	if rep.Status.Phase == zfsv1.PhaseFailed && rep.Status.LastAttemptedRunID == rep.Spec.RunID {
		return ctrl.Result{}, nil
	}
	if rep.Spec.Source.Dataset == rep.Spec.Target.Dataset {
		return ctrl.Result{}, r.fail(ctx, &rep, "source and target datasets must differ")
	}

	names := objectNames(&rep)
	ok, err := acquireLease(ctx, r.Client, &rep, names)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ok {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if err := r.ensureSecret(ctx, &rep, names); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureReceiverJob(ctx, &rep, names); err != nil {
		return ctrl.Result{}, err
	}
	if failed, msg, err := r.jobFailed(ctx, rep.Namespace, names.ReceiverName, "receiver Job failed"); err != nil || failed {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.failRun(ctx, &rep, names, msg)
	}

	senderExists, err := r.jobExists(ctx, rep.Namespace, names.SenderName)
	if err != nil {
		return ctrl.Result{}, err
	}
	receiverPodName := rep.Status.ReceiverPodName
	receiverPodIP := rep.Status.ReceiverPodIP
	if !senderExists {
		receiverPod, ready, msg, err := r.usableReceiverPod(ctx, &rep, names)
		if err != nil {
			return ctrl.Result{}, err
		}
		if msg != "" {
			return ctrl.Result{}, r.failRun(ctx, &rep, names, msg)
		}
		if !ready {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, r.patchStatus(ctx, &rep, func(st *zfsv1.ZFSReplicationStatus) {
				st.Phase = zfsv1.PhaseStartingReceiver
				st.ObservedRunID = rep.Spec.RunID
				st.LastAttemptedRunID = rep.Spec.RunID
				fillStatusNames(st, &rep, names)
			})
		}
		receiverPodName = receiverPod.Name
		receiverPodIP = receiverPod.Status.PodIP
		if err := r.patchStatus(ctx, &rep, func(st *zfsv1.ZFSReplicationStatus) {
			st.Phase = zfsv1.PhaseReceiverReady
			st.ObservedRunID = rep.Spec.RunID
			st.LastAttemptedRunID = rep.Spec.RunID
			st.ReceiverPodName = receiverPodName
			st.ReceiverPodIP = receiverPodIP
			fillStatusNames(st, &rep, names)
		}); err != nil {
			return ctrl.Result{}, err
		}

		if err := r.ensureSenderJob(ctx, &rep, names, receiverPodIP); err != nil {
			return ctrl.Result{}, err
		}
	}
	if failed, msg, err := r.jobFailed(ctx, rep.Namespace, names.SenderName, "sender Job failed"); err != nil || failed {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.failRun(ctx, &rep, names, msg)
	}
	if failed, msg, err := r.jobFailed(ctx, rep.Namespace, names.ReceiverName, "receiver Job failed"); err != nil || failed {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.failRun(ctx, &rep, names, msg)
	}

	senderDone, err := r.jobSucceeded(ctx, rep.Namespace, names.SenderName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if senderDone {
		guid, err := r.senderSnapshotGUID(ctx, &rep, names)
		if err != nil {
			return ctrl.Result{}, err
		}
		now := metav1.Now()
		err = r.patchStatus(ctx, &rep, func(st *zfsv1.ZFSReplicationStatus) {
			st.Phase = zfsv1.PhaseSucceeded
			st.ObservedRunID = rep.Spec.RunID
			st.LastAttemptedRunID = rep.Spec.RunID
			st.LastSuccessfulRunID = rep.Spec.RunID
			st.LastSuccessfulSnapshot = names.SnapshotName
			st.LastSuccessfulSnapshotGUID = guid
			st.ReceiverPodName = receiverPodName
			st.ReceiverPodIP = receiverPodIP
			st.CompletedAt = &now
			st.LastError = ""
			fillStatusNames(st, &rep, names)
		})
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.cleanupSucceededRun(ctx, &rep, names)
	}

	return ctrl.Result{RequeueAfter: 5 * time.Second}, r.patchStatus(ctx, &rep, func(st *zfsv1.ZFSReplicationStatus) {
		st.Phase = zfsv1.PhaseRunning
		st.ObservedRunID = rep.Spec.RunID
		st.LastAttemptedRunID = rep.Spec.RunID
		st.ReceiverPodName = receiverPodName
		st.ReceiverPodIP = receiverPodIP
		fillStatusNames(st, &rep, names)
	})
}

func (r *ZFSReplicationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&zfsv1.ZFSReplication{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}

func (r *ZFSReplicationReconciler) image() string {
	if r.DataMoverImage == "" {
		return "zfsreplicationcontroller:latest"
	}
	return r.DataMoverImage
}

func (r *ZFSReplicationReconciler) ensureSecret(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects) error {
	var secret corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Name: names.SecretName, Namespace: rep.Namespace}, &secret)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	key, err := generateSSHKeyMaterial()
	if err != nil {
		return err
	}
	secretObj := sshSecret(rep, names, key)
	if err := ctrl.SetControllerReference(rep, secretObj, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, secretObj)
}

func (r *ZFSReplicationReconciler) ensureReceiverJob(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects) error {
	return r.ensureJob(ctx, rep, receiverJob(rep, names, r.image()))
}

func (r *ZFSReplicationReconciler) ensureSenderJob(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects, receiverPodIP string) error {
	return r.ensureJob(ctx, rep, senderJob(rep, names, r.image(), receiverPodIP))
}

func (r *ZFSReplicationReconciler) ensureJob(ctx context.Context, rep *zfsv1.ZFSReplication, job *batchv1.Job) error {
	var existing batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	if err := ctrl.SetControllerReference(rep, job, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, job)
}

func (r *ZFSReplicationReconciler) jobExists(ctx context.Context, ns, name string) (bool, error) {
	var job batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &job)
	if err == nil {
		return true, nil
	}
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return false, err
}

func (r *ZFSReplicationReconciler) cleanupSucceededRun(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects) error {
	var errs []error
	if err := finishLease(ctx, r.Client, rep, names, "succeeded"); err != nil {
		errs = append(errs, fmt.Errorf("finish lease: %w", err))
	}
	errs = append(errs, r.cleanupEphemeralObjects(ctx, rep, names)...)
	return errors.Join(errs...)
}

func (r *ZFSReplicationReconciler) cleanupFailedRun(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects) error {
	var errs []error
	if err := finishLease(ctx, r.Client, rep, names, "failed"); err != nil {
		errs = append(errs, fmt.Errorf("finish lease: %w", err))
	}
	errs = append(errs, r.cleanupEphemeralObjects(ctx, rep, names)...)
	return errors.Join(errs...)
}

func (r *ZFSReplicationReconciler) cleanupEphemeralObjects(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects) []error {
	var errs []error
	for _, obj := range []client.Object{
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: names.ReceiverName, Namespace: rep.Namespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: names.SecretName, Namespace: rep.Namespace}},
	} {
		if err := r.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
			errs = append(errs, fmt.Errorf("delete %s/%s: %w", obj.GetNamespace(), obj.GetName(), err))
		}
	}
	return errs
}

func (r *ZFSReplicationReconciler) usableReceiverPod(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects) (*corev1.Pod, bool, string, error) {
	var pods corev1.PodList
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "receiver"
	if err := r.podReader().List(ctx, &pods, client.InNamespace(rep.Namespace), client.MatchingLabels(labels)); err != nil {
		return nil, false, "", err
	}
	var usable []*corev1.Pod
	hasTerminal := false
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded {
			hasTerminal = true
			continue
		}
		if pod.Status.Phase == corev1.PodRunning && pod.Status.PodIP != "" && podReady(pod) {
			usable = append(usable, pod)
		}
	}
	if len(usable) > 1 {
		return nil, false, "multiple ready receiver Pods found", nil
	}
	if len(usable) == 1 {
		return usable[0], true, "", nil
	}
	if hasTerminal {
		return nil, false, "receiver Pod completed before sender was created", nil
	}
	return nil, false, "", nil
}

func podReady(pod *corev1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *ZFSReplicationReconciler) jobSucceeded(ctx context.Context, ns, name string) (bool, error) {
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &job); err != nil {
		return false, err
	}
	return job.Status.Succeeded > 0, nil
}

func (r *ZFSReplicationReconciler) jobFailed(ctx context.Context, ns, name, fallback string) (bool, string, error) {
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &job); err != nil {
		return false, "", err
	}
	if job.Status.Failed == 0 {
		return false, "", nil
	}
	if msg := r.failedJobLogMessage(ctx, ns, name); msg != "" {
		return true, msg, nil
	}
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue && cond.Message != "" {
			return true, cond.Message, nil
		}
	}
	return true, fallback, nil
}

func (r *ZFSReplicationReconciler) failedJobLogMessage(ctx context.Context, ns, jobName string) string {
	if r.PodLogs == nil {
		return ""
	}
	var last string
	seen := map[string]bool{}
	for _, label := range []string{"job-name", "batch.kubernetes.io/job-name"} {
		var pods corev1.PodList
		if err := r.podReader().List(ctx, &pods, client.InNamespace(ns), client.MatchingLabels{label: jobName}); err != nil {
			continue
		}
		for _, pod := range pods.Items {
			if seen[pod.Name] {
				continue
			}
			seen[pod.Name] = true
			logs, err := r.PodLogs.Logs(ctx, ns, pod.Name)
			if err != nil {
				continue
			}
			if msg := failureMessageFromLogs(logs); msg != "" {
				last = msg
			}
		}
	}
	return last
}

func failureMessageFromLogs(logs string) string {
	var last string
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.Contains(line, "zfs-sim-event ") {
			continue
		}
		last = line
	}
	return last
}

func (r *ZFSReplicationReconciler) senderSnapshotGUID(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects) (string, error) {
	if r.PodLogs == nil {
		return "", nil
	}
	var pods corev1.PodList
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "sender"
	if err := r.podReader().List(ctx, &pods, client.InNamespace(rep.Namespace), client.MatchingLabels(labels)); err != nil {
		return "", err
	}
	for _, pod := range pods.Items {
		logs, err := r.PodLogs.Logs(ctx, rep.Namespace, pod.Name)
		if err != nil {
			return "", err
		}
		if guid := snapshotGUIDFromLogs(logs); guid != "" {
			return guid, nil
		}
	}
	return "", nil
}

func (r *ZFSReplicationReconciler) podReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *ZFSReplicationReconciler) fail(ctx context.Context, rep *zfsv1.ZFSReplication, msg string) error {
	names := objectNames(rep)
	return r.failRun(ctx, rep, names, msg)
}

func (r *ZFSReplicationReconciler) failRun(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects, msg string) error {
	now := metav1.Now()
	err := r.patchStatus(ctx, rep, func(st *zfsv1.ZFSReplicationStatus) {
		st.Phase = zfsv1.PhaseFailed
		st.ObservedRunID = rep.Spec.RunID
		st.LastAttemptedRunID = rep.Spec.RunID
		st.CompletedAt = &now
		st.LastError = msg
		fillStatusNames(st, rep, names)
	})
	if err != nil {
		return err
	}
	return r.cleanupFailedRun(ctx, rep, names)
}

func (r *ZFSReplicationReconciler) patchStatus(ctx context.Context, rep *zfsv1.ZFSReplication, mutate func(*zfsv1.ZFSReplicationStatus)) error {
	copy := rep.DeepCopy()
	mutate(&copy.Status)
	return r.Status().Patch(ctx, copy, client.MergeFrom(rep))
}

func fillStatusNames(st *zfsv1.ZFSReplicationStatus, rep *zfsv1.ZFSReplication, names runObjects) {
	if st.StartedAt == nil {
		now := metav1.Now()
		st.StartedAt = &now
	}
	st.SenderJobName = names.SenderName
	st.ReceiverJobName = names.ReceiverName
	st.SSHSecretName = names.SecretName
}
