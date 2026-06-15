package controller

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
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
	Scheme         *runtime.Scheme
	DataMoverImage string
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
	if err := r.ensureService(ctx, &rep, names); err != nil {
		return ctrl.Result{}, err
	}

	if failed, msg, err := r.jobFailed(ctx, rep.Namespace, names.ReceiverName, "receiver Job failed"); err != nil || failed {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.failRun(ctx, &rep, names, msg)
	}
	if ready, err := r.receiverReady(ctx, &rep, names); err != nil || !ready {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, r.patchStatus(ctx, &rep, func(st *zfsv1.ZFSReplicationStatus) {
			st.Phase = zfsv1.PhaseStartingReceiver
			fillStatusNames(st, &rep, names)
		})
	}

	if err := r.ensureSenderJob(ctx, &rep, names); err != nil {
		return ctrl.Result{}, err
	}
	if failed, msg, err := r.jobFailed(ctx, rep.Namespace, names.SenderName, "sender Job failed"); err != nil || failed {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.failRun(ctx, &rep, names, msg)
	}

	senderDone, err := r.jobSucceeded(ctx, rep.Namespace, names.SenderName)
	if err != nil {
		return ctrl.Result{}, err
	}
	receiverDone, err := r.jobSucceeded(ctx, rep.Namespace, names.ReceiverName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if senderDone && receiverDone {
		now := metav1.Now()
		err := r.patchStatus(ctx, &rep, func(st *zfsv1.ZFSReplicationStatus) {
			st.Phase = zfsv1.PhaseSucceeded
			st.ObservedRunID = rep.Spec.RunID
			st.LastAttemptedRunID = rep.Spec.RunID
			st.LastSuccessfulRunID = rep.Spec.RunID
			st.LastSuccessfulSnapshot = names.SnapshotName
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
		fillStatusNames(st, &rep, names)
	})
}

func (r *ZFSReplicationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&zfsv1.ZFSReplication{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.Service{}).
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
	token, err := randomToken()
	if err != nil {
		return err
	}
	secretObj := tokenSecret(rep, names, token)
	if err := ctrl.SetControllerReference(rep, secretObj, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, secretObj)
}

func (r *ZFSReplicationReconciler) ensureReceiverJob(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects) error {
	return r.ensureJob(ctx, rep, receiverJob(rep, names, r.image()))
}

func (r *ZFSReplicationReconciler) ensureSenderJob(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects) error {
	return r.ensureJob(ctx, rep, senderJob(rep, names, r.image()))
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

func (r *ZFSReplicationReconciler) ensureService(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects) error {
	var svc corev1.Service
	err := r.Get(ctx, types.NamespacedName{Name: names.ServiceName, Namespace: rep.Namespace}, &svc)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	obj := receiverService(rep, names)
	if err := ctrl.SetControllerReference(rep, obj, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, obj)
}

func (r *ZFSReplicationReconciler) cleanupSucceededRun(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects) error {
	var errs []error
	if err := finishLease(ctx, r.Client, rep, names, "succeeded"); err != nil {
		errs = append(errs, fmt.Errorf("finish lease: %w", err))
	}
	for _, obj := range []client.Object{
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: names.ServiceName, Namespace: rep.Namespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: names.SecretName, Namespace: rep.Namespace}},
	} {
		if err := r.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
			errs = append(errs, fmt.Errorf("delete %s/%s: %w", obj.GetNamespace(), obj.GetName(), err))
		}
	}
	return errors.Join(errs...)
}

func (r *ZFSReplicationReconciler) receiverReady(ctx context.Context, rep *zfsv1.ZFSReplication, names runObjects) (bool, error) {
	var pods corev1.PodList
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "receiver"
	if err := r.List(ctx, &pods, client.InNamespace(rep.Namespace), client.MatchingLabels(labels)); err != nil {
		return false, err
	}
	for _, pod := range pods.Items {
		for _, cond := range pod.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				return true, nil
			}
		}
	}
	return false, nil
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
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue && cond.Message != "" {
			return true, cond.Message, nil
		}
	}
	return true, fallback, nil
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
	return finishLease(ctx, r.Client, rep, names, "failed")
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
	st.ServiceName = names.ServiceName
	st.TokenSecretName = names.SecretName
}

func randomToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}
