package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	destinationClaimsAnnotation = labelPrefix + "/destination-claims"
	destinationLockAttempts     = 5
)

type destinationClaim struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	UID       types.UID `json:"uid"`
	Dataset   string    `json:"dataset"`
}

type destinationLock struct {
	client    client.Client
	reader    client.Reader
	namespace string
}

func destinationLeaseName(node string) string {
	readable := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			return r
		}
		return '-'
	}, strings.ToLower(node))
	if len(readable) > 28 {
		readable = readable[:28]
	}
	readable = strings.Trim(readable, "-")
	if readable == "" {
		readable = "node"
	}
	digest := sha256.Sum256([]byte(node))
	return fmt.Sprintf("zfsrep-destination-%s-%x", readable, digest[:8])
}

func (r *ZFSReplicationRunReconciler) destinationLock() destinationLock {
	return destinationLock{client: r.Client, reader: r.APIReader, namespace: r.DestinationLockNamespace}
}

func (r *ZFSReplicationRunReconciler) claimDestination(ctx context.Context, run *zfsv1.ZFSReplicationRun) (bool, string, error) {
	lock := r.destinationLock()
	// An existing owner must not yield to a queued run, even if its cached
	// phase is still Pending. Reasserting ownership needs only this fresh Get.
	_, claims, err := lock.read(ctx, run.Spec.Target.NodeName)
	if err != nil {
		return false, "", err
	}
	if destinationClaimHeld(claims, run.UID) {
		return false, "", nil
	}
	if waiting, msg, err := r.destinationLocked(ctx, run); err != nil || waiting {
		return waiting, msg, err
	}
	blocker, err := lock.acquire(ctx, run)
	if err != nil {
		return false, "", err
	}
	if blocker != nil {
		return true, destinationWaitMessage(run, blocker.Namespace, blocker.Name), nil
	}
	return false, "", nil
}

func destinationWaitMessage(run *zfsv1.ZFSReplicationRun, namespace, name string) string {
	return fmt.Sprintf("waiting for active run %s/%s to finish receiving into %s on %s", namespace, name, run.Spec.Target.Dataset, run.Spec.Target.NodeName)
}

func (l destinationLock) acquire(ctx context.Context, run *zfsv1.ZFSReplicationRun) (*destinationClaim, error) {
	if run.UID == "" {
		return nil, fmt.Errorf("acquire destination lock: run UID must not be empty")
	}
	var lastErr error
	for range destinationLockAttempts {
		lease, claims, err := l.read(ctx, run.Spec.Target.NodeName)
		if err != nil {
			return nil, err
		}
		if destinationClaimHeld(claims, run.UID) {
			return nil, nil
		}
		active := make([]destinationClaim, 0, len(claims)+1)
		for _, claim := range claims {
			if l.stale(ctx, claim) {
				continue
			}
			if targetDatasetsOverlap(run.Spec.Target.Dataset, claim.Dataset) {
				return &claim, nil
			}
			active = append(active, claim)
		}
		active = append(active, destinationClaim{Namespace: run.Namespace, Name: run.Name, UID: run.UID, Dataset: run.Spec.Target.Dataset})
		lastErr = l.write(ctx, lease, active)
		if apierrors.IsConflict(lastErr) || apierrors.IsAlreadyExists(lastErr) {
			continue
		}
		return nil, lastErr
	}
	return nil, fmt.Errorf("acquire destination lock after %d attempts: %w", destinationLockAttempts, lastErr)
}

func (l destinationLock) release(ctx context.Context, run *zfsv1.ZFSReplicationRun) error {
	var lastErr error
	for range destinationLockAttempts {
		lease, claims, err := l.read(ctx, run.Spec.Target.NodeName)
		if err != nil {
			return err
		}
		if !destinationClaimHeld(claims, run.UID) {
			return nil
		}
		remaining := make([]destinationClaim, 0, len(claims)-1)
		for _, claim := range claims {
			if claim.UID != run.UID {
				remaining = append(remaining, claim)
			}
		}
		lastErr = l.write(ctx, lease, remaining)
		if apierrors.IsConflict(lastErr) || apierrors.IsAlreadyExists(lastErr) {
			continue
		}
		return lastErr
	}
	return fmt.Errorf("release destination lock after %d attempts: %w", destinationLockAttempts, lastErr)
}

func destinationClaimHeld(claims []destinationClaim, uid types.UID) bool {
	for _, claim := range claims {
		if claim.UID == uid {
			return true
		}
	}
	return false
}

func (l destinationLock) stale(ctx context.Context, claim destinationClaim) bool {
	var run zfsv1.ZFSReplicationRun
	err := l.reader.Get(ctx, types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name}, &run)
	// Errors other than NotFound retain the claim, including Forbidden in a
	// namespace this controller cannot verify. Time alone never expires a claim.
	return apierrors.IsNotFound(err) || err == nil && (run.UID != claim.UID || run.Status.Phase.Terminal())
}

func (l destinationLock) read(ctx context.Context, node string) (*coordinationv1.Lease, []destinationClaim, error) {
	if l.reader == nil {
		return nil, nil, fmt.Errorf("destination lock fresh API reader must not be nil")
	}
	if l.namespace == "" {
		return nil, nil, fmt.Errorf("destination lock namespace must not be empty")
	}
	key := types.NamespacedName{Namespace: l.namespace, Name: destinationLeaseName(node)}
	var lease coordinationv1.Lease
	// Never use the cached client here: Lease permissions deliberately exclude
	// list/watch, and coordination requires the current resourceVersion.
	if err := l.reader.Get(ctx, key, &lease); err != nil {
		if apierrors.IsNotFound(err) {
			return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}, nil, nil
		}
		return nil, nil, fmt.Errorf("read destination lock %s: %w", key, err)
	}
	var claims []destinationClaim
	if data := lease.Annotations[destinationClaimsAnnotation]; data != "" {
		if err := json.Unmarshal([]byte(data), &claims); err != nil {
			return nil, nil, fmt.Errorf("decode destination lock %s claims: %w", key, err)
		}
	}
	for _, claim := range claims {
		if claim.Namespace == "" || claim.Name == "" || claim.UID == "" || claim.Dataset == "" {
			return nil, nil, fmt.Errorf("destination lock %s contains an incomplete claim", key)
		}
	}
	return &lease, claims, nil
}

func (l destinationLock) write(ctx context.Context, lease *coordinationv1.Lease, claims []destinationClaim) error {
	data, err := json.Marshal(claims)
	if err != nil {
		return fmt.Errorf("encode destination lock claims: %w", err)
	}
	if lease.Annotations == nil {
		lease.Annotations = make(map[string]string)
	}
	lease.Annotations[destinationClaimsAnnotation] = string(data)
	if lease.ResourceVersion == "" {
		err = l.client.Create(ctx, lease)
	} else {
		err = l.client.Update(ctx, lease)
	}
	if err != nil {
		return fmt.Errorf("write destination lock %s: %w", client.ObjectKeyFromObject(lease), err)
	}
	return nil
}
