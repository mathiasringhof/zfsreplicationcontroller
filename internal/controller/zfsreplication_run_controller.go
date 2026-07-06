package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/datamover"
	"github.com/mathias/zfsreplicationcontroller/internal/replication"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type ZFSReplicationRunReconciler struct {
	client.Client
	APIReader      client.Reader
	Scheme         *runtime.Scheme
	DataMoverImage string
	PodLogs        PodLogReader
}

type runReceiverStatus struct {
	podName string
	podIP   string
}

func (r *ZFSReplicationRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var run zfsv1.ZFSReplicationRun
	if err := r.Get(ctx, req.NamespacedName, &run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	names := objectNamesForRun(run.Name)
	logger := log.FromContext(ctx).WithValues(runLogValues(&run, names)...)
	ctx = log.IntoContext(ctx, logger)
	logger.Info("reconciling replication run")
	if run.Status.Phase.Terminal() {
		logger.WithValues("phase", run.Status.Phase).Info("cleaning up terminal replication run")
		return ctrl.Result{}, r.reconcileTerminalRun(ctx, &run, names)
	}
	if err := validateRunSpec(run.Spec); err != nil {
		return ctrl.Result{}, r.failRunValidation(ctx, &run, err.Error())
	}
	if locked, msg, err := r.destinationLocked(ctx, &run); err != nil || locked {
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, r.patchRunStatus(ctx, &run, func(st *zfsv1.ZFSReplicationRunStatus) {
			st.Phase = zfsv1.PhasePending
			st.LastError = msg
		})
	}

	if err := r.ensureRunSecret(ctx, &run, names); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureRunReceiveTask(ctx, &run, names); err != nil {
		return ctrl.Result{}, err
	}

	receiver := runReceiverStatus{
		podName: run.Status.ReceiverPodName,
		podIP:   run.Status.ReceiverPodIP,
	}
	senderExists, err := r.jobExists(ctx, run.Namespace, names.SenderName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !senderExists {
		nextReceiver, result, done, err := r.ensureSenderStarted(ctx, &run, names)
		if err != nil || done {
			return result, err
		}
		receiver = nextReceiver
	} else {
		logger.WithValues("receiverPod", receiver.podName, "receiverPodIP", receiver.podIP).Info("sender job already present")
	}

	if result, done, err := r.finishFromSenderJob(ctx, &run, names, receiver); err != nil || done {
		return result, err
	}

	return ctrl.Result{RequeueAfter: 5 * time.Second}, r.patchRunStatus(ctx, &run, func(st *zfsv1.ZFSReplicationRunStatus) {
		st.Phase = zfsv1.PhaseRunning
		st.ReceiverPodName = receiver.podName
		st.ReceiverPodIP = receiver.podIP
		fillRunStatusNames(st, names)
	})
}

func (r *ZFSReplicationRunReconciler) reconcileTerminalRun(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects) error {
	return errors.Join(
		r.markReceiveTaskTerminal(ctx, run, names, run.Status.Phase.ReceiveTaskTerminalPhase(), run.Status.LastError),
		r.cleanupRunEphemeralObjects(ctx, run, names),
	)
}

func (r *ZFSReplicationRunReconciler) ensureSenderStarted(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects) (runReceiverStatus, ctrl.Result, bool, error) {
	task, ready, msg, err := r.readyReceiveTask(ctx, run, names)
	if err != nil {
		return runReceiverStatus{}, ctrl.Result{}, false, err
	}
	if msg != "" {
		return runReceiverStatus{}, ctrl.Result{}, true, r.failRunObject(ctx, run, names, msg)
	}
	if !ready {
		log.FromContext(ctx).Info("waiting for replication receiver")
		return runReceiverStatus{}, ctrl.Result{RequeueAfter: 5 * time.Second}, true, r.patchRunStatus(ctx, run, func(st *zfsv1.ZFSReplicationRunStatus) {
			st.Phase = zfsv1.PhaseStartingReceiver
			fillRunStatusNames(st, names)
		})
	}

	receiver := runReceiverStatus{
		podName: task.Status.ReceiverPod.Name,
		podIP:   task.Status.Endpoint.Host,
	}
	log.FromContext(ctx).WithValues("receiverPod", receiver.podName, "receiverPodIP", receiver.podIP).Info("replication receiver is ready")
	if err := r.ensureRunKnownHosts(ctx, run, names, task); err != nil {
		return runReceiverStatus{}, ctrl.Result{}, false, err
	}
	if err := r.patchRunStatus(ctx, run, func(st *zfsv1.ZFSReplicationRunStatus) {
		st.Phase = zfsv1.PhaseReceiverReady
		st.ReceiverPodName = receiver.podName
		st.ReceiverPodIP = receiver.podIP
		fillRunStatusNames(st, names)
	}); err != nil {
		return runReceiverStatus{}, ctrl.Result{}, false, err
	}
	if err := r.ensureRunSenderJob(ctx, run, names, receiver.podIP); err != nil {
		return runReceiverStatus{}, ctrl.Result{}, false, err
	}
	log.FromContext(ctx).WithValues("receiverPod", receiver.podName, "receiverPodIP", receiver.podIP).Info("created sender job")
	return receiver, ctrl.Result{}, false, nil
}

func (r *ZFSReplicationRunReconciler) finishFromSenderJob(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects, receiver runReceiverStatus) (ctrl.Result, bool, error) {
	if failed, msg, err := r.jobFailed(ctx, run.Namespace, names.SenderName, "sender Job failed"); err != nil || failed {
		if err != nil {
			return ctrl.Result{}, false, err
		}
		log.FromContext(ctx).WithValues("receiverPod", receiver.podName, "receiverPodIP", receiver.podIP, "reason", msg).Info("sender job failed")
		return ctrl.Result{}, true, r.failRunObject(ctx, run, names, msg)
	}

	senderDone, err := r.jobSucceeded(ctx, run.Namespace, names.SenderName)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	if senderDone {
		log.FromContext(ctx).WithValues("receiverPod", receiver.podName, "receiverPodIP", receiver.podIP).Info("sender job succeeded")
		now := metav1.Now()
		if err := r.patchRunStatus(ctx, run, func(st *zfsv1.ZFSReplicationRunStatus) {
			st.Phase = zfsv1.PhaseSucceeded
			st.ReceiverPodName = receiver.podName
			st.ReceiverPodIP = receiver.podIP
			st.CompletedAt = &now
			st.LastError = ""
			fillRunStatusNames(st, names)
		}); err != nil {
			return ctrl.Result{}, false, err
		}
		return ctrl.Result{}, true, errors.Join(
			r.markReceiveTaskTerminal(ctx, run, names, zfsv1.ReceiveTaskPhaseCompleted, ""),
			r.cleanupRunEphemeralObjects(ctx, run, names),
		)
	}

	return ctrl.Result{}, false, nil
}

func (r *ZFSReplicationRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&zfsv1.ZFSReplicationRun{}).
		Owns(&batchv1.Job{}).
		Owns(&zfsv1.ZFSReceiveTask{}).
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

func (r *ZFSReplicationRunReconciler) ensureRunReceiveTask(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects) error {
	var existing zfsv1.ZFSReceiveTask
	err := r.Get(ctx, types.NamespacedName{Name: names.ReceiveTaskName, Namespace: run.Namespace}, &existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Name: names.SecretName, Namespace: run.Namespace}, &secret); err != nil {
		return err
	}
	publicKey := strings.TrimSpace(string(secret.Data["id_rsa.pub"]))
	if publicKey == "" {
		return fmt.Errorf("ssh secret %s/%s is missing id_rsa.pub", secret.Namespace, secret.Name)
	}
	expiresAt := metav1.NewTime(time.Now().Add(30 * time.Minute))
	task := runReceiveTask(run, names, publicKey, expiresAt)
	if err := ctrl.SetControllerReference(run, task, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, task)
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

func (r *ZFSReplicationRunReconciler) readyReceiveTask(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects) (*zfsv1.ZFSReceiveTask, bool, string, error) {
	var task zfsv1.ZFSReceiveTask
	if err := r.Get(ctx, types.NamespacedName{Name: names.ReceiveTaskName, Namespace: run.Namespace}, &task); err != nil {
		return nil, false, "", err
	}
	switch task.Status.Phase {
	case zfsv1.ReceiveTaskPhaseFailed:
		if task.Status.Error != "" {
			return &task, false, task.Status.Error, nil
		}
		return &task, false, "receive task failed", nil
	case zfsv1.ReceiveTaskPhaseReady:
		if task.Status.Endpoint.Host == "" {
			return &task, false, "receive task is Ready without endpoint host", nil
		}
		if task.Status.Endpoint.Port == 0 {
			return &task, false, "receive task is Ready without endpoint port", nil
		}
		if task.Status.SSH.HostKey == "" {
			return &task, false, "receive task is Ready without SSH host key", nil
		}
		return &task, true, "", nil
	default:
		return &task, false, "", nil
	}
}

func (r *ZFSReplicationRunReconciler) ensureRunKnownHosts(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects, task *zfsv1.ZFSReceiveTask) error {
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Name: names.SecretName, Namespace: run.Namespace}, &secret); err != nil {
		return err
	}
	line, err := knownHostsLine(task.Status.Endpoint.Host, task.Status.Endpoint.Port, task.Status.SSH.HostKey)
	if err != nil {
		return err
	}
	if string(secret.Data["known_hosts"]) == line {
		return nil
	}
	copy := secret.DeepCopy()
	if copy.Data == nil {
		copy.Data = map[string][]byte{}
	}
	copy.Data["known_hosts"] = []byte(line)
	return r.Update(ctx, copy)
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
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: names.SecretName, Namespace: run.Namespace}},
	} {
		if err := r.Delete(ctx, obj); client.IgnoreNotFound(err) != nil {
			errs = append(errs, fmt.Errorf("delete %s/%s: %w", obj.GetNamespace(), obj.GetName(), err))
		}
	}
	if err := r.deleteRunReceiverPods(ctx, run, names); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (r *ZFSReplicationRunReconciler) deleteRunReceiverPods(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects) error {
	var receiverPods corev1.PodList
	receiverLabels := cloneLabels(names.Labels)
	receiverLabels[labelPrefix+"/role"] = "receiver"
	if err := r.podReader().List(ctx, &receiverPods, client.InNamespace(run.Namespace), client.MatchingLabels(receiverLabels)); err != nil {
		return fmt.Errorf("list receiver Pods for %s/%s: %w", run.Namespace, run.Name, err)
	}
	var errs []error
	for i := range receiverPods.Items {
		pod := &receiverPods.Items[i]
		if err := r.Delete(ctx, pod); client.IgnoreNotFound(err) != nil {
			errs = append(errs, fmt.Errorf("delete receiver Pod %s/%s: %w", pod.Namespace, pod.Name, err))
		}
	}
	return errors.Join(errs...)
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
	return errors.Join(
		r.markReceiveTaskTerminal(ctx, run, names, zfsv1.ReceiveTaskPhaseFailed, msg),
		r.cleanupRunEphemeralObjects(ctx, run, names),
	)
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

func (r *ZFSReplicationRunReconciler) markReceiveTaskTerminal(ctx context.Context, run *zfsv1.ZFSReplicationRun, names runObjects, phase zfsv1.ReceiveTaskPhase, msg string) error {
	if phase == "" {
		return nil
	}
	var task zfsv1.ZFSReceiveTask
	if err := r.Get(ctx, types.NamespacedName{Name: names.ReceiveTaskName, Namespace: run.Namespace}, &task); err != nil {
		return client.IgnoreNotFound(err)
	}
	if task.Status.Phase == phase && task.Status.Error == msg {
		return nil
	}
	copy := task.DeepCopy()
	copy.Status.Phase = phase
	copy.Status.Error = msg
	return r.Status().Patch(ctx, copy, client.MergeFrom(&task))
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
	if !replication.ValidDatasetName(spec.Source.Dataset) {
		return fmt.Errorf("spec.source.dataset must be a valid zfs dataset name")
	}
	if spec.Target.NodeName == "" {
		return fmt.Errorf("spec.target.nodeName must not be empty")
	}
	if spec.Target.Dataset == "" {
		return fmt.Errorf("spec.target.dataset must not be empty")
	}
	if !replication.ValidDatasetName(spec.Target.Dataset) {
		return fmt.Errorf("spec.target.dataset must be a valid zfs dataset name")
	}
	if spec.Source.NodeName == spec.Target.NodeName && spec.Source.Dataset == spec.Target.Dataset {
		return fmt.Errorf("source and target must not reference the same dataset on the same node")
	}
	if !replication.CompressionSupported(spec.Syncoid.Compress) {
		return fmt.Errorf("spec.syncoid.compress has unsupported value %q", spec.Syncoid.Compress)
	}
	return nil
}

func (r *ZFSReplicationRunReconciler) destinationLocked(ctx context.Context, run *zfsv1.ZFSReplicationRun) (bool, string, error) {
	var runs zfsv1.ZFSReplicationRunList
	if err := r.List(ctx, &runs, client.InNamespace(run.Namespace)); err != nil {
		return false, "", err
	}
	for _, other := range runs.Items {
		if other.Name == run.Name || !other.Status.Phase.Active() {
			continue
		}
		if other.Spec.Target.NodeName != run.Spec.Target.NodeName || !targetDatasetsOverlap(run.Spec.Target.Dataset, other.Spec.Target.Dataset) {
			continue
		}
		if shouldWaitForDestinationRun(run, &other) {
			msg := fmt.Sprintf("waiting for active run %s to finish receiving into %s on %s", other.Name, run.Spec.Target.Dataset, run.Spec.Target.NodeName)
			return true, msg, nil
		}
	}
	return false, "", nil
}

func targetDatasetsOverlap(a, b string) bool {
	return replication.DatasetOrChild(a, b) || replication.DatasetOrChild(b, a)
}

func shouldWaitForDestinationRun(run, other *zfsv1.ZFSReplicationRun) bool {
	if other.Status.Phase != "" && other.Status.Phase != zfsv1.PhasePending {
		return true
	}
	runTime := run.CreationTimestamp.Time
	otherTime := other.CreationTimestamp.Time
	if runTime.IsZero() || otherTime.IsZero() || runTime.Equal(otherTime) {
		return other.Name < run.Name
	}
	return otherTime.Before(runTime)
}

func fillRunStatusNames(st *zfsv1.ZFSReplicationRunStatus, names runObjects) {
	if st.StartedAt == nil {
		now := metav1.Now()
		st.StartedAt = &now
	}
	st.SenderJobName = names.SenderName
	st.ReceiveTaskName = names.ReceiveTaskName
	st.SSHSecretName = names.SecretName
}

func runLogValues(run *zfsv1.ZFSReplicationRun, names runObjects) []any {
	return []any{
		"namespace", run.Namespace,
		"run", run.Name,
		"sourceNode", run.Spec.Source.NodeName,
		"sourceDataset", run.Spec.Source.Dataset,
		"targetNode", run.Spec.Target.NodeName,
		"targetDataset", run.Spec.Target.Dataset,
		"senderJob", names.SenderName,
		"receiveTask", names.ReceiveTaskName,
		"sshSecret", names.SecretName,
		"syncoidIdentifier", syncSnapshotIdentifierForRun(run),
	}
}

func objectNamesForRun(runName string) runObjects {
	labels := map[string]string{
		labelPrefix + "/run": runName,
	}
	return runObjects{
		SecretName:      sanitizeName("zfsrep", runName, "ssh"),
		ReceiveTaskName: sanitizeName("zfsrep", runName, "receiver"),
		SenderName:      sanitizeName("zfsrep", runName, "sender"),
		Labels:          labels,
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

func runReceiveTask(run *zfsv1.ZFSReplicationRun, names runObjects, publicKey string, expiresAt metav1.Time) *zfsv1.ZFSReceiveTask {
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "receiver"
	syncSnapshotIdentifier := syncSnapshotIdentifierForRun(run)
	options := normalizedSyncoidOptions(run.Spec.Syncoid)
	return &zfsv1.ZFSReceiveTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:      names.ReceiveTaskName,
			Namespace: run.Namespace,
			Labels:    labels,
		},
		Spec: zfsv1.ZFSReceiveTaskSpec{
			RunRef:      zfsv1.LocalObjectReference{Name: run.Name},
			NodeName:    run.Spec.Target.NodeName,
			Destination: zfsv1.ReceiveDestination{Dataset: run.Spec.Target.Dataset},
			SSH: zfsv1.ReceiveTaskSSHSpec{
				AuthorizedPublicKey: publicKey,
				ExpiresAt:           expiresAt,
			},
			Policy: zfsv1.ReceiveTaskPolicy{
				ReceiveUnmounted:         options.ReceiveUnmounted,
				ReceiveResumable:         options.ReceiveResumable,
				AllowRollback:            !options.NoRollback,
				AllowDestroy:             options.ForceDelete,
				AllowMount:               !options.ReceiveUnmounted,
				AllowSyncSnapshotDestroy: !options.NoSyncSnap,
				SyncSnapshotIdentifier:   syncSnapshotIdentifier,
				Compression:              options.Compress,
			},
		},
	}
}

func runSenderJob(run *zfsv1.ZFSReplicationRun, names runObjects, image, receiverPodIP string) *batchv1.Job {
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "sender"
	env := []corev1.EnvVar{
		{Name: datamover.EnvRole, Value: datamover.RoleSender},
		{Name: datamover.EnvSrcDataset, Value: run.Spec.Source.Dataset},
		{Name: datamover.EnvDstHost, Value: fmt.Sprintf("zfs-recv@%s", receiverPodIP)},
		{Name: datamover.EnvSSHKeyFile, Value: datamover.DefaultSSHKeyFile},
		{Name: datamover.EnvKnownHostsFile, Value: datamover.DefaultKnownHostsFile},
		{Name: datamover.EnvSSHPort, Value: datamover.DefaultSSHPort},
		{Name: datamover.EnvDstDataset, Value: run.Spec.Target.Dataset},
		{Name: datamover.EnvSyncoidIdentifier, Value: syncSnapshotIdentifierForRun(run)},
	}
	env = append(env, syncoidEnv(run.Spec.Syncoid)...)
	return dataMoverJobForRun(run, names.SenderName, image, labels, run.Spec.Source.NodeName, "/usr/local/bin/zfsrep-sender", env, names.SecretName, false)
}

func syncSnapshotIdentifierForRun(run *zfsv1.ZFSReplicationRun) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		run.Namespace,
		run.Spec.Source.NodeName,
		run.Spec.Source.Dataset,
		run.Spec.Target.NodeName,
		run.Spec.Target.Dataset,
	}, "\x00")))
	return "zrc-" + hex.EncodeToString(sum[:])[:24]
}

func syncoidEnv(spec zfsv1.SyncoidSpec) []corev1.EnvVar {
	options := normalizedSyncoidOptions(spec)
	return []corev1.EnvVar{
		{Name: datamover.EnvNoSyncSnap, Value: strconv.FormatBool(options.NoSyncSnap)},
		{Name: datamover.EnvNoRollback, Value: strconv.FormatBool(options.NoRollback)},
		{Name: datamover.EnvForceDelete, Value: strconv.FormatBool(options.ForceDelete)},
		{Name: datamover.EnvCompress, Value: options.Compress},
		{Name: datamover.EnvReceiveUnmounted, Value: strconv.FormatBool(options.ReceiveUnmounted)},
		{Name: datamover.EnvReceiveResumable, Value: strconv.FormatBool(options.ReceiveResumable)},
		{Name: datamover.EnvIncludeSnaps, Value: strings.Join(options.IncludeSnaps, "\n")},
		{Name: datamover.EnvExcludeSnaps, Value: strings.Join(options.ExcludeSnaps, "\n")},
	}
}

func normalizedSyncoidOptions(spec zfsv1.SyncoidSpec) replication.SyncoidOptions {
	return replication.NormalizeSyncoidOptions(replication.SyncoidOptionInput{
		NoSyncSnap:       spec.NoSyncSnap,
		NoRollback:       spec.NoRollback,
		ForceDelete:      spec.ForceDelete,
		Compress:         spec.Compress,
		ReceiveUnmounted: spec.ReceiveUnmounted,
		ReceiveResumable: spec.ReceiveResumable,
		IncludeSnaps:     spec.IncludeSnaps,
		ExcludeSnaps:     spec.ExcludeSnaps,
	})
}

func dataMoverJobForRun(run *zfsv1.ZFSReplicationRun, name, image string, labels map[string]string, nodeName, command string, env []corev1.EnvVar, secretName string, readiness bool) *batchv1.Job {
	return dataMoverJob(run.Namespace, name, image, labels, nodeName, command, env, secretName, readiness)
}

func knownHostsLine(host string, port int32, hostKey string) (string, error) {
	key, comment, options, rest, err := ssh.ParseAuthorizedKey([]byte(strings.TrimSpace(hostKey)))
	if err != nil {
		return "", fmt.Errorf("parse receiver host key: %w", err)
	}
	if len(options) > 0 {
		return "", fmt.Errorf("receiver host key must not include authorized_keys options")
	}
	if len(rest) > 0 {
		return "", fmt.Errorf("receiver host key contains trailing data")
	}
	line := knownhosts.Line([]string{net.JoinHostPort(host, strconv.FormatInt(int64(port), 10))}, key)
	if comment != "" {
		line += " " + comment
	}
	return line + "\n", nil
}
