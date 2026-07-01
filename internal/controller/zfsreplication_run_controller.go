package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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

type ZFSReplicationRunReconciler struct {
	client.Client
	APIReader      client.Reader
	Scheme         *runtime.Scheme
	DataMoverImage string
	PodLogs        PodLogReader
}

func (r *ZFSReplicationRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var run zfsv1.ZFSReplicationRun
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	names := objectNamesForRun(run.Name)
	if run.Status.Phase == zfsv1.PhaseSucceeded || run.Status.Phase == zfsv1.PhaseFailed {
		return ctrl.Result{}, r.cleanupRunEphemeralObjects(ctx, &run, names)
	}
	if err := validateRunSpec(run.Spec); err != nil {
		return ctrl.Result{}, r.failRunValidation(ctx, &run, err.Error())
	}

	if err := r.ensureRunSecret(ctx, &run, names); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureRunReceiverJob(ctx, &run, names); err != nil {
		return ctrl.Result{}, err
	}
	if failed, msg, err := r.jobFailed(ctx, run.Namespace, names.ReceiverName, "receiver Job failed"); err != nil || failed {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.failRunObject(ctx, &run, names, msg)
	}

	senderExists, err := r.jobExists(ctx, run.Namespace, names.SenderName)
	if err != nil {
		return ctrl.Result{}, err
	}
	receiverPodName := run.Status.ReceiverPodName
	receiverPodIP := run.Status.ReceiverPodIP
	if !senderExists {
		receiverPod, ready, msg, err := r.usableRunReceiverPod(ctx, &run, names)
		if err != nil {
			return ctrl.Result{}, err
		}
		if msg != "" {
			return ctrl.Result{}, r.failRunObject(ctx, &run, names, msg)
		}
		if !ready {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, r.patchRunStatus(ctx, &run, func(st *zfsv1.ZFSReplicationRunStatus) {
				st.Phase = zfsv1.PhaseStartingReceiver
				fillRunStatusNames(st, names)
			})
		}
		receiverPodName = receiverPod.Name
		receiverPodIP = receiverPod.Status.PodIP
		if err := r.patchRunStatus(ctx, &run, func(st *zfsv1.ZFSReplicationRunStatus) {
			st.Phase = zfsv1.PhaseReceiverReady
			st.ReceiverPodName = receiverPodName
			st.ReceiverPodIP = receiverPodIP
			fillRunStatusNames(st, names)
		}); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.ensureRunSenderJob(ctx, &run, names, receiverPodIP); err != nil {
			return ctrl.Result{}, err
		}
	}

	if failed, msg, err := r.jobFailed(ctx, run.Namespace, names.SenderName, "sender Job failed"); err != nil || failed {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.failRunObject(ctx, &run, names, msg)
	}
	if failed, msg, err := r.jobFailed(ctx, run.Namespace, names.ReceiverName, "receiver Job failed"); err != nil || failed {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.failRunObject(ctx, &run, names, msg)
	}

	senderDone, err := r.jobSucceeded(ctx, run.Namespace, names.SenderName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if senderDone {
		now := metav1.Now()
		if err := r.patchRunStatus(ctx, &run, func(st *zfsv1.ZFSReplicationRunStatus) {
			st.Phase = zfsv1.PhaseSucceeded
			st.ReceiverPodName = receiverPodName
			st.ReceiverPodIP = receiverPodIP
			st.CompletedAt = &now
			st.LastError = ""
			fillRunStatusNames(st, names)
		}); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.cleanupRunEphemeralObjects(ctx, &run, names)
	}

	return ctrl.Result{RequeueAfter: 5 * time.Second}, r.patchRunStatus(ctx, &run, func(st *zfsv1.ZFSReplicationRunStatus) {
		st.Phase = zfsv1.PhaseRunning
		st.ReceiverPodName = receiverPodName
		st.ReceiverPodIP = receiverPodIP
		fillRunStatusNames(st, names)
	})
}

func (r *ZFSReplicationRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&zfsv1.ZFSReplicationRun{}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}

func (r *ZFSReplicationRunReconciler) image() string {
	if r.DataMoverImage == "" {
		return "zfsreplicationcontroller:latest"
	}
	return r.DataMoverImage
}

func (r *ZFSReplicationRunReconciler) ensureRunSecret(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects) error {
	var secret corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Name: names.SecretName, Namespace: run.Namespace}, &secret)
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
	secretObj := runSSHSecret(run, names, key)
	if err := ctrl.SetControllerReference(run, secretObj, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, secretObj)
}

func (r *ZFSReplicationRunReconciler) ensureRunReceiverJob(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects) error {
	return r.ensureRunJob(ctx, run, runReceiverJob(run, names, r.image()))
}

func (r *ZFSReplicationRunReconciler) ensureRunSenderJob(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects, receiverPodIP string) error {
	return r.ensureRunJob(ctx, run, runSenderJob(run, names, r.image(), receiverPodIP))
}

func (r *ZFSReplicationRunReconciler) ensureRunJob(ctx context.Context, run *zfsv1.ZFSReplicationRun, job *batchv1.Job) error {
	var existing batchv1.Job
	err := r.Get(ctx, types.NamespacedName{Name: job.Name, Namespace: job.Namespace}, &existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	if err := ctrl.SetControllerReference(run, job, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, job)
}

func (r *ZFSReplicationRunReconciler) jobExists(ctx context.Context, ns, name string) (bool, error) {
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

func (r *ZFSReplicationRunReconciler) jobSucceeded(ctx context.Context, ns, name string) (bool, error) {
	var job batchv1.Job
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &job); err != nil {
		return false, err
	}
	return job.Status.Succeeded > 0, nil
}

func (r *ZFSReplicationRunReconciler) jobFailed(ctx context.Context, ns, name, fallback string) (bool, string, error) {
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

func (r *ZFSReplicationRunReconciler) failedJobLogMessage(ctx context.Context, ns, jobName string) string {
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

func (r *ZFSReplicationRunReconciler) cleanupRunEphemeralObjects(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects) error {
	var errs []error
	for _, obj := range []client.Object{
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: names.ReceiverName, Namespace: run.Namespace}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: names.SecretName, Namespace: run.Namespace}},
	} {
		if err := r.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
			errs = append(errs, fmt.Errorf("delete %s/%s: %w", obj.GetNamespace(), obj.GetName(), err))
		}
	}
	return errors.Join(errs...)
}

func (r *ZFSReplicationRunReconciler) usableRunReceiverPod(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects) (*corev1.Pod, bool, string, error) {
	var pods corev1.PodList
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "receiver"
	if err := r.podReader().List(ctx, &pods, client.InNamespace(run.Namespace), client.MatchingLabels(labels)); err != nil {
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

func (r *ZFSReplicationRunReconciler) podReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *ZFSReplicationRunReconciler) failRunObject(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects, msg string) error {
	now := metav1.Now()
	if err := r.patchRunStatus(ctx, run, func(st *zfsv1.ZFSReplicationRunStatus) {
		st.Phase = zfsv1.PhaseFailed
		st.CompletedAt = &now
		st.LastError = msg
		fillRunStatusNames(st, names)
	}); err != nil {
		return err
	}
	return r.cleanupRunEphemeralObjects(ctx, run, names)
}

func (r *ZFSReplicationRunReconciler) failRunValidation(ctx context.Context, run *zfsv1.ZFSReplicationRun, msg string) error {
	now := metav1.Now()
	return r.patchRunStatus(ctx, run, func(st *zfsv1.ZFSReplicationRunStatus) {
		st.Phase = zfsv1.PhaseFailed
		st.CompletedAt = &now
		st.LastError = msg
		if st.StartedAt == nil {
			st.StartedAt = &now
		}
	})
}

func (r *ZFSReplicationRunReconciler) patchRunStatus(ctx context.Context, run *zfsv1.ZFSReplicationRun, mutate func(*zfsv1.ZFSReplicationRunStatus)) error {
	copy := run.DeepCopy()
	mutate(&copy.Status)
	return r.Status().Patch(ctx, copy, client.MergeFrom(run))
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

func validateRunSpec(spec zfsv1.ZFSReplicationRunSpec) error {
	if spec.Source.NodeName == "" {
		return fmt.Errorf("spec.source.nodeName must not be empty")
	}
	if spec.Source.Dataset == "" {
		return fmt.Errorf("spec.source.dataset must not be empty")
	}
	if spec.Target.NodeName == "" {
		return fmt.Errorf("spec.target.nodeName must not be empty")
	}
	if spec.Target.Dataset == "" {
		return fmt.Errorf("spec.target.dataset must not be empty")
	}
	if spec.Source.NodeName == spec.Target.NodeName && spec.Source.Dataset == spec.Target.Dataset {
		return fmt.Errorf("source and target must not reference the same dataset on the same node")
	}
	return nil
}

func fillRunStatusNames(st *zfsv1.ZFSReplicationRunStatus, names runObjects) {
	if st.StartedAt == nil {
		now := metav1.Now()
		st.StartedAt = &now
	}
	st.SenderJobName = names.SenderName
	st.ReceiverJobName = names.ReceiverName
	st.SSHSecretName = names.SecretName
}

func objectNamesForRun(runName string) runObjects {
	labels := map[string]string{
		labelPrefix + "/run": runName,
	}
	return runObjects{
		BaseName:     sanitizeName("zfsrep", runName),
		RunName:      runName,
		SecretName:   sanitizeName("zfsrep", runName, "ssh"),
		ReceiverName: sanitizeName("zfsrep", runName, "receiver"),
		SenderName:   sanitizeName("zfsrep", runName, "sender"),
		Labels:       labels,
	}
}

func runSSHSecret(run *zfsv1.ZFSReplicationRun, names runObjects, key sshKeyMaterial) *corev1.Secret {
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "ssh"
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      names.SecretName,
			Namespace: run.Namespace,
			Labels:    labels,
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"id_rsa":          key.PrivateKeyPEM,
			"id_rsa.pub":      key.PublicKey,
			"authorized_keys": key.AuthorizedKeys,
		},
	}
}

func runReceiverJob(run *zfsv1.ZFSReplicationRun, names runObjects, image string) *batchv1.Job {
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "receiver"
	env := []corev1.EnvVar{
		{Name: "ZFSREP_ROLE", Value: "receiver"},
		{Name: "SSH_AUTHORIZED_KEYS_FILE", Value: "/var/run/zfsrep/ssh/authorized_keys"},
		{Name: "SSH_LISTEN_PORT", Value: "2222"},
	}
	return dataMoverJobForRun(run, names.ReceiverName, image, labels, run.Spec.Target.NodeName, "/usr/local/bin/zfsrep-ssh-receiver", env, names.SecretName, true)
}

func runSenderJob(run *zfsv1.ZFSReplicationRun, names runObjects, image, receiverPodIP string) *batchv1.Job {
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "sender"
	env := []corev1.EnvVar{
		{Name: "ZFSREP_ROLE", Value: "sender"},
		{Name: "SRC_DATASET", Value: run.Spec.Source.Dataset},
		{Name: "DST_HOST", Value: fmt.Sprintf("root@%s", receiverPodIP)},
		{Name: "SSH_KEY_FILE", Value: "/var/run/zfsrep/ssh/id_rsa"},
		{Name: "SSH_PORT", Value: "2222"},
		{Name: "DST_DATASET", Value: run.Spec.Target.Dataset},
	}
	env = append(env, syncoidEnv(run.Spec.Syncoid)...)
	return dataMoverJobForRun(run, names.SenderName, image, labels, run.Spec.Source.NodeName, "/usr/local/bin/zfsrep-sender", env, names.SecretName, false)
}

func syncoidEnv(spec zfsv1.SyncoidSpec) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: "SYNCOID_NO_SYNC_SNAP", Value: strconv.FormatBool(boolDefault(spec.NoSyncSnap, false))},
		{Name: "SYNCOID_NO_ROLLBACK", Value: strconv.FormatBool(boolDefault(spec.NoRollback, true))},
		{Name: "SYNCOID_FORCE_DELETE", Value: strconv.FormatBool(boolDefault(spec.ForceDelete, false))},
		{Name: "SYNCOID_COMPRESS", Value: spec.Compress},
		{Name: "RECEIVE_UNMOUNTED", Value: strconv.FormatBool(boolDefault(spec.ReceiveUnmounted, true))},
		{Name: "RECEIVE_RESUMABLE", Value: strconv.FormatBool(boolDefault(spec.ReceiveResumable, true))},
		{Name: "SYNCOID_INCLUDE_SNAPS", Value: strings.Join(spec.IncludeSnaps, "\n")},
		{Name: "SYNCOID_EXCLUDE_SNAPS", Value: strings.Join(spec.ExcludeSnaps, "\n")},
	}
}

func dataMoverJobForRun(run *zfsv1.ZFSReplicationRun, name, image string, labels map[string]string, nodeName, command string, env []corev1.EnvVar, secretName string, readiness bool) *batchv1.Job {
	return dataMoverJob(run.Namespace, name, image, labels, nodeName, command, env, secretName, readiness)
}
