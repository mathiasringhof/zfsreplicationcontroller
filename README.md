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

The controller is deployed as a container image, not as Go source. The Dockerfile
builds the `manager` and `zfsrep-sender` binaries, then packages them with
`zfsrep-ssh-receiver`, `zfsutils-linux`, OpenSSH, and pinned upstream `syncoid`
2.3.0.

GitHub Actions builds the image on pull requests and publishes it to GHCR on
pushes to `main` and version tags.

Published image:

```text
ghcr.io/mathiasringhof/zfsreplicationcontroller
```

Tags:

- `main`: latest image built from the default branch. Mutable; useful for quick
  testing, not preferred for GitOps pinning.
- `sha-<commit>`: commit-addressed tag, preferred over `main` when a digest is
  not available.
- `<version>` and `<major>.<minor>`: published from `v*` release tags.

The default manifests use `ghcr.io/mathiasringhof/zfsreplicationcontroller:main`
for both the controller and data mover image. The controller Deployment uses
`imagePullPolicy: Always`, and generated data mover Jobs also use `Always` for
mutable `main` and `latest` tags. For GitOps deployments, pin both values to the
same digest after the image has been published. A `sha-<commit>` tag is a useful
fallback, but a digest is the content-addressed immutable reference.

The deployment chain is:

```text
Go source -> container image -> CRDs/RBAC/Deployment -> manager pod -> custom resources -> Jobs
```

If you use a different registry or a pinned image, set it in both places in
`config/manager/deployment.yaml`:

- `spec.template.spec.containers[0].image`
- `DATA_MOVER_IMAGE`

Install the CRDs, RBAC, namespace, and controller Deployment:

```sh
kubectl apply -k config
```

Verify the manager pod is ready:

```sh
kubectl -n zfsreplication-system rollout status deploy/zfsreplication-controller
```

Create `ZFSReplicationRun` and `ZFSReplicationSchedule` objects only after
choosing real node names and disposable test datasets for your cluster. The
sender and receiver Jobs are created in the namespace of the run object, not in
`zfsreplication-system`.

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

With `noSyncSnap: true`, Syncoid uses the existing source and target snapshots,
applies include/exclude filters, finds the newest common base, and replicates
forward.

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

Schedules use the standard `github.com/robfig/cron/v3` parser, matching the
library used by Kubernetes CronJobs. It supports five-field cron expressions,
named months and weekdays, descriptors such as `@hourly`, and `@every`
durations. Seconds fields are not supported.

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

Each run gets its own SSH `Secret`. The sender connects to the receiver pod
address, and the receiver accepts only the per-run key.

After a run succeeds or fails, the controller deletes the receiver Job and SSH
Secret. The sender Job has `ttlSecondsAfterFinished: 86400` so Kubernetes can
keep it briefly for inspection before TTL cleanup.

Sender and receiver Jobs pin pods with `spec.template.spec.nodeName`. At
startup, each container compares the downward API node name with the expected
node and exits before running ZFS or SSH commands on a mismatch.

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
