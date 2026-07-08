package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ZFSReplicationScheduleReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Now    func() time.Time
}

const (
	defaultSuccessfulRunsHistoryLimit int32 = 3
	defaultFailedRunsHistoryLimit     int32 = 1
)

func (r *ZFSReplicationScheduleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var schedule zfsv1.ZFSReplicationSchedule
	if err := r.Get(ctx, req.NamespacedName, &schedule); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	parsed, err := parseCronSchedule(schedule.Spec.Schedule)
	if err != nil {
		return ctrl.Result{}, r.failSchedule(ctx, &schedule, err.Error())
	}
	if err := validateRunSpec(schedule.Spec.RunTemplate); err != nil {
		return ctrl.Result{}, r.failSchedule(ctx, &schedule, fmt.Sprintf("runTemplate: %v", err))
	}
	policy, err := validateConcurrencyPolicy(schedule.Spec.ConcurrencyPolicy)
	if err != nil {
		return ctrl.Result{}, r.failSchedule(ctx, &schedule, err.Error())
	}
	if err := validateScheduleHistoryLimits(&schedule); err != nil {
		return ctrl.Result{}, r.failSchedule(ctx, &schedule, err.Error())
	}
	if boolDefault(schedule.Spec.Suspend, false) {
		return r.patchScheduleStatusAndPruneHistory(ctx, &schedule, ctrl.Result{}, func(st *zfsv1.ZFSReplicationScheduleStatus) {
			st.LastError = ""
		})
	}

	now := r.now()
	last := schedule.CreationTimestamp.Time
	if schedule.Status.LastScheduleTime != nil {
		last = schedule.Status.LastScheduleTime.Time
	}
	if last.IsZero() {
		last = now
	}
	due, next := dueAndNext(parsed, last, now)
	if due.IsZero() {
		result := requeueAt(now, next)
		return r.patchScheduleStatusAndPruneHistory(ctx, &schedule, result, func(st *zfsv1.ZFSReplicationScheduleStatus) {
			st.LastError = ""
		})
	}

	if policy == zfsv1.ConcurrencyPolicyForbid {
		active, err := r.hasActiveRun(ctx, &schedule)
		if err != nil {
			return ctrl.Result{}, err
		}
		if active {
			result := requeueAt(now, next)
			return r.patchScheduleStatusAndPruneHistory(ctx, &schedule, result, func(st *zfsv1.ZFSReplicationScheduleStatus) {
				scheduled := metav1.NewTime(due)
				st.LastScheduleTime = &scheduled
				st.LastError = "skipped scheduled run because a previous run is still active"
			})
		}
	}

	runName := scheduledRunName(schedule.Name, due)
	if err := r.ensureScheduledRun(ctx, &schedule, runName, due); err != nil {
		return ctrl.Result{}, err
	}
	result := requeueAt(now, next)
	return r.patchScheduleStatusAndPruneHistory(ctx, &schedule, result, func(st *zfsv1.ZFSReplicationScheduleStatus) {
		scheduled := metav1.NewTime(due)
		st.LastScheduleTime = &scheduled
		st.LastRunName = runName
		st.LastError = ""
	})
}

func (r *ZFSReplicationScheduleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&zfsv1.ZFSReplicationSchedule{}).
		Owns(&zfsv1.ZFSReplicationRun{}).
		Complete(r)
}

func (r *ZFSReplicationScheduleReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *ZFSReplicationScheduleReconciler) ensureScheduledRun(ctx context.Context, schedule *zfsv1.ZFSReplicationSchedule, runName string, due time.Time) error {
	var existing zfsv1.ZFSReplicationRun
	key := types.NamespacedName{Name: runName, Namespace: schedule.Namespace}
	err := r.Get(ctx, key, &existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	run := &zfsv1.ZFSReplicationRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      runName,
			Namespace: schedule.Namespace,
			Labels: map[string]string{
				labelPrefix + "/schedule": schedule.Name,
			},
			Annotations: map[string]string{
				labelPrefix + "/scheduled-at": due.UTC().Format(time.RFC3339),
			},
		},
		Spec: *schedule.Spec.RunTemplate.DeepCopy(),
	}
	if err := ctrl.SetControllerReference(schedule, run, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, run)
}

func (r *ZFSReplicationScheduleReconciler) hasActiveRun(ctx context.Context, schedule *zfsv1.ZFSReplicationSchedule) (bool, error) {
	var runs zfsv1.ZFSReplicationRunList
	if err := r.List(ctx, &runs, client.InNamespace(schedule.Namespace), client.MatchingLabels{labelPrefix + "/schedule": schedule.Name}); err != nil {
		return false, err
	}
	for _, run := range runs.Items {
		if run.Status.Phase.Active() {
			return true, nil
		}
	}
	return false, nil
}

func (r *ZFSReplicationScheduleReconciler) pruneScheduleRunHistory(ctx context.Context, schedule *zfsv1.ZFSReplicationSchedule) error {
	var runs zfsv1.ZFSReplicationRunList
	if err := r.List(ctx, &runs, client.InNamespace(schedule.Namespace), client.MatchingLabels{labelPrefix + "/schedule": schedule.Name}); err != nil {
		return err
	}

	var successful []zfsv1.ZFSReplicationRun
	var failed []zfsv1.ZFSReplicationRun
	for _, run := range runs.Items {
		switch run.Status.Phase {
		case zfsv1.PhaseSucceeded:
			successful = append(successful, run)
		case zfsv1.PhaseFailed:
			failed = append(failed, run)
		}
	}

	return errors.Join(
		r.pruneTerminalRuns(ctx, successful, scheduleHistoryLimit(schedule.Spec.SuccessfulRunsHistoryLimit, defaultSuccessfulRunsHistoryLimit)),
		r.pruneTerminalRuns(ctx, failed, scheduleHistoryLimit(schedule.Spec.FailedRunsHistoryLimit, defaultFailedRunsHistoryLimit)),
	)
}

func (r *ZFSReplicationScheduleReconciler) pruneTerminalRuns(ctx context.Context, runs []zfsv1.ZFSReplicationRun, limit int) error {
	if len(runs) <= limit {
		return nil
	}
	sort.Slice(runs, func(i, j int) bool {
		return terminalRunOlder(runs[i], runs[j])
	})

	for i := 0; i < len(runs)-limit; i++ {
		run := runs[i].DeepCopy()
		if err := r.Delete(ctx, run); client.IgnoreNotFound(err) != nil {
			return fmt.Errorf("delete old scheduled run %s/%s: %w", run.Namespace, run.Name, err)
		}
	}
	return nil
}

func terminalRunOlder(a, b zfsv1.ZFSReplicationRun) bool {
	aTime := terminalRunHistoryTime(a)
	bTime := terminalRunHistoryTime(b)
	if !aTime.Equal(bTime) {
		if aTime.IsZero() {
			return true
		}
		if bTime.IsZero() {
			return false
		}
		return aTime.Before(bTime)
	}
	if !a.CreationTimestamp.Time.Equal(b.CreationTimestamp.Time) {
		return a.CreationTimestamp.Time.Before(b.CreationTimestamp.Time)
	}
	return a.Name < b.Name
}

func terminalRunHistoryTime(run zfsv1.ZFSReplicationRun) time.Time {
	if run.Status.CompletedAt != nil {
		return run.Status.CompletedAt.Time
	}
	return run.CreationTimestamp.Time
}

func (r *ZFSReplicationScheduleReconciler) failSchedule(ctx context.Context, schedule *zfsv1.ZFSReplicationSchedule, msg string) error {
	return r.patchScheduleStatus(ctx, schedule, func(st *zfsv1.ZFSReplicationScheduleStatus) {
		st.LastError = msg
	})
}

func (r *ZFSReplicationScheduleReconciler) patchScheduleStatus(ctx context.Context, schedule *zfsv1.ZFSReplicationSchedule, mutate func(*zfsv1.ZFSReplicationScheduleStatus)) error {
	copy := schedule.DeepCopy()
	mutate(&copy.Status)
	return r.Status().Patch(ctx, copy, client.MergeFrom(schedule))
}

func (r *ZFSReplicationScheduleReconciler) patchScheduleStatusAndPruneHistory(
	ctx context.Context,
	schedule *zfsv1.ZFSReplicationSchedule,
	result ctrl.Result,
	mutate func(*zfsv1.ZFSReplicationScheduleStatus),
) (ctrl.Result, error) {
	if err := r.patchScheduleStatus(ctx, schedule, mutate); err != nil {
		return result, err
	}
	return result, r.pruneScheduleRunHistory(ctx, schedule)
}

func scheduledRunName(scheduleName string, due time.Time) string {
	return sanitizeName("zfsrep", scheduleName, due.UTC().Format("20060102-1504"))
}

func validateConcurrencyPolicy(policy zfsv1.ConcurrencyPolicy) (zfsv1.ConcurrencyPolicy, error) {
	if policy == "" {
		return zfsv1.ConcurrencyPolicyForbid, nil
	}
	if policy == zfsv1.ConcurrencyPolicyAllow || policy == zfsv1.ConcurrencyPolicyForbid {
		return policy, nil
	}
	return "", fmt.Errorf("concurrencyPolicy must be Allow or Forbid")
}

func validateScheduleHistoryLimits(schedule *zfsv1.ZFSReplicationSchedule) error {
	if err := validateHistoryLimit("successfulRunsHistoryLimit", schedule.Spec.SuccessfulRunsHistoryLimit); err != nil {
		return err
	}
	return validateHistoryLimit("failedRunsHistoryLimit", schedule.Spec.FailedRunsHistoryLimit)
}

func validateHistoryLimit(field string, value *int32) error {
	if value != nil && *value < 0 {
		return fmt.Errorf("%s must be greater than or equal to 0", field)
	}
	return nil
}

func scheduleHistoryLimit(value *int32, defaultValue int32) int {
	if value == nil {
		return int(defaultValue)
	}
	return int(*value)
}

func requeueAt(now, next time.Time) ctrl.Result {
	if next.IsZero() {
		return ctrl.Result{RequeueAfter: time.Hour}
	}
	delay := next.Sub(now)
	if delay < time.Second {
		delay = time.Second
	}
	return ctrl.Result{RequeueAfter: delay}
}
