package main

import (
	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/receiverauthorization"
)

func receiveTaskCandidate(cfg receiverConfig, task *zfsv1.ZFSReceiveTask) receiverauthorization.Candidate {
	return receiverauthorization.Candidate{
		TaskUID:                    string(task.UID),
		AuthorizedPublicKey:        task.Spec.SSH.AuthorizedPublicKey,
		ExpiresAt:                  task.Spec.SSH.ExpiresAt.Time,
		TargetDataset:              task.Spec.Destination.Dataset,
		ReceiverDatasetPrefixes:    append([]string(nil), cfg.AllowedPrefixes...),
		ReceiveUnmounted:           task.Spec.Policy.ReceiveUnmounted,
		ReceiveResumable:           task.Spec.Policy.ReceiveResumable,
		AllowRollback:              task.Spec.Policy.AllowRollback,
		AllowDestroy:               task.Spec.Policy.AllowDestroy,
		AllowMount:                 task.Spec.Policy.AllowMount,
		AllowSyncSnapshotDestroy:   task.Spec.Policy.AllowSyncSnapshotDestroy,
		AllowTargetSnapshotDestroy: task.Spec.Policy.AllowTargetSnapshotDestroy,
		SyncSnapshotIdentifier:     task.Spec.Policy.SyncSnapshotIdentifier,
		Compression:                task.Spec.Policy.Compression,
	}
}
