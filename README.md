# ZFS Replication Controller

ZFS Replication Controller is a Kubernetes-native Syncoid orchestrator.

It creates the temporary connection between two Kubernetes nodes, then runs
`syncoid` in a privileged per-run sender Job. Snapshot policy stays outside the
controller: Syncoid can create its own sync snapshots, or external tools such as
SnapScheduler or Sanoid can create snapshots and the run can pass
`--no-sync-snap` with include/exclude filters.

## What It Does

- Watches `ZFSReplicationRun` objects for one-shot Syncoid invocations.
- Watches `ZFSReplicationSchedule` objects and creates `ZFSReplicationRun`
  objects on cron ticks.
- Creates a per-run SSH `Secret`.
- Starts a privileged receiver `Job` on the target node that accepts only the
  per-run key.
- Starts a privileged sender `Job` on the source node after the receiver pod is
  ready.
- Passes structured Syncoid options from YAML to the sender.
- Updates basic status after the sender Job succeeds or fails.

The controller does not create a Kubernetes `Service` or use long-lived node SSH
credentials. The per-run SSH Secret and receiver Job are removed after the run
finishes.

## Install

Build and push an image that includes `zfsutils-linux`, pinned upstream
`syncoid` 2.3.0, and the controller binaries:

```sh
docker build -t registry.example.com/zfsreplicationcontroller:latest .
docker push registry.example.com/zfsreplicationcontroller:latest
```

Set that image in `config/manager/deployment.yaml`, then install:

```sh
kubectl apply -k config
```

## One-Shot Run

```yaml
apiVersion: zfsreplication.example.com/v1alpha1
kind: ZFSReplicationRun
metadata:
  name: pg-a-to-b-manual-001
  namespace: storage
spec:
  source:
    nodeName: worker-a
    dataset: tank/pvc-source
  target:
    nodeName: worker-b
    dataset: tank/pvc-target
  syncoid:
    noSyncSnap: true
    noRollback: true
    compress: none
    receiveUnmounted: true
    receiveResumable: true
    includeSnaps:
      - "^snap-.*"
```

With `noSyncSnap: true`, the controller does not create snapshots. Syncoid lists
source and target snapshots, applies include/exclude filters, finds the newest
common GUID-compatible base, and replicates forward.

Omit `noSyncSnap` or set it to `false` when Syncoid should create its own sync
snapshot.

## Scheduled Runs

```yaml
apiVersion: zfsreplication.example.com/v1alpha1
kind: ZFSReplicationSchedule
metadata:
  name: pg-a-to-b-hourly
  namespace: storage
spec:
  schedule: "10 * * * *"
  concurrencyPolicy: Forbid
  runTemplate:
    source:
      nodeName: worker-a
      dataset: tank/pvc-source
    target:
      nodeName: worker-b
      dataset: tank/pvc-target
    syncoid:
      noSyncSnap: true
      includeSnaps:
        - "^snap-.*"
```

Schedules use numeric five-field cron expressions. The built-in parser supports
`*`, `*/n`, lists, ranges, and stepped ranges. `concurrencyPolicy: Forbid` is the
default and skips a tick while a previous scheduled run is still active.

## Syncoid Options

The controller owns connection-sensitive options: source dataset, target
dataset, SSH key, SSH port, and receiver address. The `syncoid` block exposes
the replication behavior:

- `noSyncSnap`: pass `--no-sync-snap`.
- `noRollback`: pass `--no-rollback` when true. Defaults to true.
- `forceDelete`: pass `--force-delete`.
- `compress`: pass `--compress=<value>`.
- `receiveUnmounted`: pass `--recvoptions=u` when true. Defaults to true.
- `receiveResumable`: pass `--no-resume` when false. Defaults to true.
- `includeSnaps`: one `--include-snaps=<regex>` per item.
- `excludeSnaps`: one `--exclude-snaps=<regex>` per item.

## Operational Warnings

The target dataset must be passive and suitable for `syncoid` to receive into.

`forceDelete` is destructive. When enabled, the sender passes `--force-delete`
to Syncoid.

When external snapshot tooling owns snapshots and retention, make sure retention
does not prune the common source/target base before a scheduled replication run
can complete.

Sender and receiver Jobs are pinned with `spec.template.spec.nodeName`, not only
a node selector. Each container verifies at startup that the actual node from
the downward API matches the expected node and exits before running ZFS or SSH
commands if it does not.

Jobs use `backoffLimit: 0` and `restartPolicy: Never`.

## Development

```sh
go fmt ./...
go test ./...
go vet ./...
```
