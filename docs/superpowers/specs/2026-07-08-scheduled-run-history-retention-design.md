# Scheduled Run History Retention Design

## Problem

`ZFSReplicationSchedule` creates one `ZFSReplicationRun` for each due schedule
tick. Each run creates an owned `ZFSReceiveTask`. When a run reaches `Succeeded`
or `Failed`, the run controller marks the receive task terminal and deletes
ephemeral objects such as the per-run SSH Secret and receiver Pod. The sender Job
also has `ttlSecondsAfterFinished`.

The run and receive task custom resources are retained indefinitely. For long
running schedules, this leaves a growing set of terminal custom resources that
make API listings noisier and obscure active work.

## Goal

Mirror Kubernetes CronJob history behavior for scheduled replication runs:

- keep a configurable number of successful scheduled runs;
- keep a configurable number of failed scheduled runs;
- leave manually created `ZFSReplicationRun` objects alone by default.

## API

Add two optional fields to `ZFSReplicationScheduleSpec`:

```go
SuccessfulRunsHistoryLimit *int32 `json:"successfulRunsHistoryLimit,omitempty"`
FailedRunsHistoryLimit     *int32 `json:"failedRunsHistoryLimit,omitempty"`
```

Unset fields use Kubernetes CronJob defaults:

- `successfulRunsHistoryLimit`: `3`
- `failedRunsHistoryLimit`: `1`

An explicit value of `0` means no terminal runs of that phase are retained.
Negative values are invalid and should be rejected by the CRD schema.

The names intentionally mirror CronJob's `successfulJobsHistoryLimit` and
`failedJobsHistoryLimit`, replacing "Jobs" with "Runs".

## Behavior

The schedule controller owns retention because it already creates scheduled
`ZFSReplicationRun` objects and labels them with
`zfsreplication.ringhof.io/schedule`.

On each schedule reconcile, after the current scheduling and status update path,
the controller prunes terminal runs for that schedule:

1. List `ZFSReplicationRun` objects in the schedule namespace with the schedule
   label.
2. Ignore active or non-terminal runs.
3. Split terminal runs into `Succeeded` and `Failed` groups.
4. Sort each group by `status.completedAt`, then creation timestamp, then name.
5. Delete the oldest runs beyond the configured limit for each group.

The controller deletes only `ZFSReplicationRun` objects. Their owned
`ZFSReceiveTask` objects are removed by Kubernetes cascading garbage collection.
Existing run-controller cleanup for Secrets, receiver Pods, and receive-task
terminal status remains unchanged.

Manual runs are ignored because they are not part of a schedule's labeled run
set. This matches the Kubernetes distinction between Jobs created by a CronJob
and Jobs created directly by a user.

## Error Handling

Retention is follow-up cleanup, not scheduling state. The existing creation and
status update path should remain the source of truth for `LastScheduleTime` and
`LastRunName`; a transient pruning failure must not cause duplicate scheduled run
creation.

If listing or deleting old runs fails, the reconcile should return the error so
controller-runtime retries. This also makes RBAC or API failures visible instead
of silently accumulating objects.

## RBAC And Manifests

The controller currently has create/get/list/watch for `zfsreplicationruns` and
`zfsreceivetasks`, but not delete. Scheduled retention requires delete permission
for `zfsreplicationruns` in both cluster-wide and namespaced RBAC manifests.

No direct delete permission for `zfsreceivetasks` is required for this feature,
because receive tasks should be removed by owner-reference garbage collection
when their run is deleted.

## Testing

Unit tests should cover:

- default limits keep three successful runs and one failed run;
- explicit zero deletes all terminal runs for that phase;
- active runs are never pruned;
- manual runs without the schedule label are not pruned;
- successful and failed histories are counted independently;
- runs are deleted oldest first using completed time, creation time, then name;
- transient delete/list failures are returned for retry;
- RBAC manifests include delete on `zfsreplicationruns`.

An e2e test can extend the schedule path if needed, but focused controller unit
tests and RBAC manifest tests should cover the core behavior.

## Non-Goals

- Add a TTL field to `ZFSReplicationRun`.
- Delete manually created runs automatically.
- Change sender Job `ttlSecondsAfterFinished`.
- Change receive task schema or receiver authorization behavior.
- Prune snapshots or any ZFS data.
