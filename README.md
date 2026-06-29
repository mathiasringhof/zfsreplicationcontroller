# ZFS Replication Controller

ZFS Replication Controller runs Syncoid replication between ZFS datasets on two
Kubernetes nodes.

For each `ZFSReplicationRun`, the controller creates a per-run SSH key, starts a
privileged receiver Job on the target node, waits for that receiver pod to become
ready, then starts a privileged sender Job on the source node. The sender invokes
`syncoid` against the receiver pod address.

Snapshot creation is controlled by Syncoid options. Syncoid can create its own
sync snapshots, or a run can set `noSyncSnap: true` and rely on snapshots created
by another tool.

The API is `v1alpha1`.

## Resources

- `ZFSReplicationRun`: one Syncoid invocation. The run spec is immutable after
  creation.
- `ZFSReplicationSchedule`: creates `ZFSReplicationRun` objects from a template
  on cron ticks.

The run controller rejects empty source or target fields. It also rejects a run
whose source and target refer to the same dataset on the same node.

## Requirements

- Kubernetes nodes with the required ZFS datasets and `/dev/zfs`.
- Permission to run privileged pods with a `/dev/zfs` hostPath mount.
- A controller image that contains `manager`, `zfsrep-sender`,
  `zfsrep-ssh-receiver`, `zfsutils-linux`, OpenSSH, and Syncoid.

## Install

Build and push an image that includes `zfsutils-linux`, pinned upstream
`syncoid` 2.3.0, and the controller binaries:

```sh
docker build -t registry.example.com/zfsreplicationcontroller:latest .
docker push registry.example.com/zfsreplicationcontroller:latest
```

Set that image in both places in `config/manager/deployment.yaml`:

- `spec.template.spec.containers[0].image`
- `DATA_MOVER_IMAGE`

Install the CRDs, RBAC, namespace, and controller deployment:

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
common base, and replicates forward.

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
`*`, `*/n`, lists, ranges, and stepped ranges. It does not support named months,
named weekdays, seconds, or macros such as `@hourly`.

`concurrencyPolicy: Forbid` is the default and skips a tick while a previous
scheduled run is still active. Set `suspend: true` to stop creating runs without
deleting the schedule.

## Syncoid Options

The controller owns connection-sensitive options: source dataset, target
dataset, SSH key, SSH port, and receiver address. The `syncoid` block exposes
the replication behavior:

- `noSyncSnap`: pass `--no-sync-snap`. Defaults to false.
- `noRollback`: pass `--no-rollback` when true. Defaults to true.
- `forceDelete`: pass `--force-delete`. Defaults to false.
- `compress`: pass `--compress=<value>`. Defaults to `none` in the sender.
- `receiveUnmounted`: pass `--recvoptions=u` when true. Defaults to true.
- `receiveResumable`: pass `--no-resume` when false. Defaults to true.
- `includeSnaps`: one `--include-snaps=<regex>` per item.
- `excludeSnaps`: one `--exclude-snaps=<regex>` per item.

## Object Lifecycle

The controller does not create a Kubernetes `Service` and does not use
long-lived node SSH credentials. Each run gets its own SSH `Secret`; the
receiver accepts only that key.

After a run succeeds or fails, the controller deletes the receiver Job and SSH
Secret. The sender Job has `ttlSecondsAfterFinished: 86400` so Kubernetes can
keep it briefly for inspection before TTL cleanup.

Sender and receiver Jobs use `spec.template.spec.nodeName`, not only a node
selector. Each container verifies at startup that the actual node from the
downward API matches the expected node and exits before running ZFS or SSH
commands if it does not.

Jobs use `backoffLimit: 0`, `restartPolicy: Never`, and
`automountServiceAccountToken: false`.

## Operational Notes

The target dataset must be passive and suitable for `syncoid` to receive into.

`forceDelete` is destructive. When enabled, the sender passes `--force-delete`
to Syncoid.

When external snapshot tooling owns snapshots and retention, make sure retention
does not prune the common source/target base before a scheduled replication run
can complete.

## Development

```sh
go fmt ./...
go test ./...
golangci-lint run
```

The VM e2e environment is documented in `test/e2e/README.md`.

## License

Apache-2.0. See `LICENSE`.
