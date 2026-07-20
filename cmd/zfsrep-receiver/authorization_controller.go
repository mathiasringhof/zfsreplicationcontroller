package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/receiverauthorization"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var receiverAuthorizationRequest = reconcile.Request{}

type authorizationPublisher interface {
	Replace([]receiverauthorization.Candidate) (receiverauthorization.Activation, error)
}

type receiverAuthorizationReconciler struct {
	client         client.Client
	apiReader      client.Reader
	cfg            receiverConfig
	hostKey        string
	authorization  authorizationPublisher
	fatal          chan<- error
	now            func() time.Time
	initialTrigger chan event.GenericEvent
	startupGate    chan struct{}
	initialResult  chan error
	initialPending atomic.Bool
	startupOnce    sync.Once
}

func (r *receiverAuthorizationReconciler) SetupWithManager(mgr manager.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("receiver-authorization").
		Watches(&zfsv1.ZFSReceiveTask{}, handler.EnqueueRequestsFromMapFunc(receiverAuthorizationRequests)).
		WatchesRawSource(source.Channel(r.initialTrigger, handler.EnqueueRequestsFromMapFunc(receiverAuthorizationRequests))).
		Complete(r)
}

func receiverAuthorizationRequests(context.Context, client.Object) []reconcile.Request {
	return []reconcile.Request{receiverAuthorizationRequest}
}

func (r *receiverAuthorizationReconciler) StartInitial(ctx context.Context) error {
	if r.initialTrigger == nil || r.startupGate == nil || r.initialResult == nil {
		return fmt.Errorf("receiver authorization startup is not initialized")
	}
	r.initialPending.Store(true)
	select {
	case r.initialTrigger <- event.GenericEvent{Object: &zfsv1.ZFSReceiveTask{}}:
	case <-ctx.Done():
		return ctx.Err()
	}
	r.startupOnce.Do(func() { close(r.startupGate) })
	select {
	case err := <-r.initialResult:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *receiverAuthorizationReconciler) Reconcile(ctx context.Context, _ reconcile.Request) (ctrl.Result, error) {
	if r.startupGate == nil || r.apiReader == nil {
		return ctrl.Result{}, fmt.Errorf("receiver authorization reconciler is not initialized")
	}
	select {
	case <-r.startupGate:
	case <-ctx.Done():
		return ctrl.Result{}, ctx.Err()
	}
	candidates, candidateTasks, err := listReceiveTaskCandidates(ctx, r.client, r.cfg)
	if err != nil {
		err = fmt.Errorf("list complete node-local receive task view: %w", err)
		r.reportInitial(err)
		return ctrl.Result{}, err
	}
	activation, err := r.authorization.Replace(candidates)
	if err != nil {
		r.handlePublicationFailure(err)
		err = fmt.Errorf("publish complete receiver authorization snapshot: %w", err)
		r.reportInitial(err)
		return ctrl.Result{}, err
	}
	result := deadlineResult(r.now(), activation.NextDeadline)
	r.reportInitial(nil)

	var reconcileErrs []error
	for i, outcome := range activation.Outcomes() {
		task := candidateTasks[i]
		if outcome.Rejection != "" {
			if err := patchTaskFailed(ctx, r.client, r.apiReader, task, outcome.Rejection); err != nil {
				reconcileErrs = append(reconcileErrs, fmt.Errorf("report rejection for receive task %s/%s UID %s: %w", task.Namespace, task.Name, task.UID, err))
			}
			continue
		}
		if err := patchTaskReady(ctx, r.client, r.apiReader, task, r.cfg, r.hostKey); err != nil {
			reconcileErrs = append(reconcileErrs, fmt.Errorf("report Ready for receive task %s/%s UID %s: %w", task.Namespace, task.Name, task.UID, err))
		}
	}
	if activation.Warning != nil {
		reconcileErrs = append(reconcileErrs, fmt.Errorf("clean up retired receiver authorization generations: %w", activation.Warning))
	}
	return result, errors.Join(reconcileErrs...)
}

func (r *receiverAuthorizationReconciler) reportInitial(err error) {
	if !r.initialPending.CompareAndSwap(true, false) {
		return
	}
	r.initialResult <- err
}

func (r *receiverAuthorizationReconciler) handlePublicationFailure(publicationErr error) {
	var classified interface {
		ActiveAuthorityUsable() bool
	}
	if errors.As(publicationErr, &classified) && classified.ActiveAuthorityUsable() {
		log.Printf("receiver authorization is degraded; retaining last complete snapshot: %v", publicationErr)
		return
	}
	r.reportFatal(fmt.Errorf("active receiver authorization is unusable after reconciliation failure: %w", publicationErr))
}

func deadlineResult(now, next time.Time) ctrl.Result {
	if next.IsZero() {
		return ctrl.Result{}
	}
	if !next.After(now) {
		return ctrl.Result{Requeue: true}
	}
	return ctrl.Result{RequeueAfter: next.Sub(now)}
}

func (r *receiverAuthorizationReconciler) reportFatal(err error) {
	if r.fatal == nil {
		return
	}
	select {
	case r.fatal <- err:
	default:
	}
}
