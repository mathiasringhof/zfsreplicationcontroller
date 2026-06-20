package controller

import (
	"context"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const leaseStateAnnotation = labelPrefix + "/state"

func acquireLease(ctx context.Context, c client.Client, scheme *runtime.Scheme, rep *zfsv1.ZFSReplication, names runObjects) (bool, error) {
	var lease coordinationv1.Lease
	key := types.NamespacedName{Name: names.BaseName, Namespace: rep.Namespace}
	err := c.Get(ctx, key, &lease)
	now := metav1.MicroTime{Time: time.Now()}
	holder := rep.Spec.RunID
	if apierrors.IsNotFound(err) {
		lease = coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:        names.BaseName,
				Namespace:   rep.Namespace,
				Annotations: map[string]string{leaseStateAnnotation: "active"},
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity:       &holder,
				AcquireTime:          &now,
				RenewTime:            &now,
				LeaseDurationSeconds: ptr(int32(3600)),
			},
		}
		if err := ctrl.SetControllerReference(rep, &lease, scheme); err != nil {
			return false, err
		}
		return true, c.Create(ctx, &lease)
	}
	if err != nil {
		return false, err
	}
	if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != holder && lease.Annotations[leaseStateAnnotation] == "active" && !leaseExpired(lease, now.Time) {
		return false, nil
	}
	previousHolder := ""
	if lease.Spec.HolderIdentity != nil {
		previousHolder = *lease.Spec.HolderIdentity
	}
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[leaseStateAnnotation] = "active"
	lease.Spec.HolderIdentity = &holder
	if previousHolder != holder {
		lease.Spec.AcquireTime = &now
	}
	lease.Spec.RenewTime = &now
	if len(lease.OwnerReferences) == 0 {
		if err := ctrl.SetControllerReference(rep, &lease, scheme); err != nil {
			return false, err
		}
	}
	return true, c.Update(ctx, &lease)
}

func leaseExpired(lease coordinationv1.Lease, now time.Time) bool {
	if lease.Spec.LeaseDurationSeconds == nil || *lease.Spec.LeaseDurationSeconds <= 0 {
		return false
	}
	base := lease.Spec.RenewTime
	if base == nil {
		base = lease.Spec.AcquireTime
	}
	if base == nil || base.Time.IsZero() {
		return false
	}
	expiresAt := base.Add(time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second)
	return !expiresAt.After(now)
}

func finishLease(ctx context.Context, c client.Client, rep *zfsv1.ZFSReplication, names runObjects, state string) error {
	var lease coordinationv1.Lease
	err := c.Get(ctx, types.NamespacedName{Name: names.BaseName, Namespace: rep.Namespace}, &lease)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != rep.Spec.RunID {
		return nil
	}
	if lease.Annotations == nil {
		lease.Annotations = map[string]string{}
	}
	lease.Annotations[leaseStateAnnotation] = state
	return c.Update(ctx, &lease)
}

func ptr[T any](v T) *T { return &v }
