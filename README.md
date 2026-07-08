# ZFS Replication Controller

> **Agentic engineering notice:** This project is intentionally developed with
> agentic engineering. AI coding agents help plan, implement, test, review, and
> verify changes; release decisions and project ownership remain human.

ZFS Replication Controller runs Syncoid replication between ZFS datasets on two
Kubernetes nodes.

For each `ZFSReplicationRun`, the controller creates a per-run SSH key and a
`ZFSReceiveTask` for the destination node. A privileged receiver DaemonSet on
that node authorizes the key, publishes its pod IP and SSH host key in task
status, then the controller starts a privileged sender Job on the source node.
The sender invokes `syncoid` against the receiver pod address with strict SSH
host-key checking.

Snapshot creation is controlled by Syncoid options. Syncoid can create its own
sync snapshots, or a run can set `noSyncSnap: true` and rely on snapshots created
by another tool.

The API is `v1alpha1`.

## Resources

- `ZFSReplicationRun`: one Syncoid invocation. The run spec is immutable after
  creation.
- `ZFSReceiveTask`: internal per-run receiver authorization and readiness state.
- `ZFSReplicationSchedule`: creates `ZFSReplicationRun` objects from a template
  on cron ticks.

The run controller rejects empty source or target fields. It also rejects a run
whose source and target refer to the same dataset on the same node.

## Requirements

- Kubernetes nodes with the required ZFS datasets and `/dev/zfs`.
- Permission to run privileged pods with a `/dev/zfs` hostPath mount.
- Destination nodes labelled `zfsreplication.ringhof.io/enabled=true` so the
  receiver DaemonSet is scheduled there.
- A controller image that contains `manager`, `zfsrep-sender`,
  `zfsrep-receiver`, `zfsutils-linux`, OpenSSH, and Syncoid.

## Install

The controller is deployed as a container image, not as Go source. The Dockerfile
builds the `manager`, `zfsrep-sender`, and `zfsrep-receiver` binaries, then
packages them with `zfsutils-linux`, OpenSSH, and pinned upstream `syncoid`
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
for the controller, receiver DaemonSet, and data mover image. The controller
Deployment, receiver DaemonSet, and generated data mover Jobs use
`imagePullPolicy: Always` for mutable `main` and `latest` tags. For GitOps
deployments, pin all image references to the same digest after the image has
been published. A `sha-<commit>` tag is a useful fallback, but a digest is the
content-addressed immutable reference.

The deployment chain is:

```text
Go source -> container image -> CRDs/RBAC/Deployment/DaemonSet -> manager pod -> custom resources -> ZFSReceiveTask + sender Job
```

If you use a different registry or a pinned image, set it in both places in
`config/manager/deployment.yaml`:

- `spec.template.spec.containers[0].image`
- `DATA_MOVER_IMAGE`

Install the CRDs, RBAC, namespace, and controller Deployment.

For an alpha release, prefer the rendered release manifest attached to the
GitHub release instead of the mutable `main` manifests in the repository:

```sh
curl -LO https://github.com/mathiasringhof/zfsreplicationcontroller/releases/download/v0.2.0/zfsreplicationcontroller-v0.2.0.yaml
kubectl apply -f zfsreplicationcontroller-v0.2.0.yaml
```

The `0.2.x` releases are alpha releases. The Kubernetes API remains
`zfsreplication.ringhof.io/v1alpha1`, and incompatible API changes may happen
before a stable `1.0.0`.

To install from the repository checkout:

```sh
kubectl apply -k config
```

By default, the manager watches all namespaces and the default install uses a
`ClusterRoleBinding` so runs and schedules can live in any namespace. To scope a
controller instance to one namespace, set `WATCH_NAMESPACE` or pass
`--watch-namespace`; an empty value keeps the all-namespaces behavior.

For a production smoke deployment with namespaced runtime permissions, use the
namespaced smoke profile from the repository root:

```sh
kubectl apply -k .
```

That profile still installs the CRDs cluster-wide, but it runs the manager with
`WATCH_NAMESPACE=zfsreplication-smoke` and grants `ZFSReplicationRun`,
`ZFSReplicationSchedule`, `ZFSReceiveTask`, Jobs, Secrets, Pods, Pods/log, and
Events access only in the `zfsreplication-smoke` namespace. Change
`zfsreplication-smoke`
consistently in
`config/namespaced/watched_namespace.yaml`,
`config/rbac/namespaced_role.yaml`,
`config/rbac/namespaced_role_binding.yaml`,
`config/rbac/receiver_namespaced_role.yaml`,
`config/rbac/receiver_namespaced_role_binding.yaml`,
`config/namespaced/manager_watch_namespace_patch.yaml`, and
`config/namespaced/receiver_watch_namespace_patch.yaml` for a different smoke
namespace.

Verify the manager pod is ready:

```sh
kubectl -n zfsreplication-system rollout status deploy/zfsreplication-controller
```

Create `ZFSReplicationRun` and `ZFSReplicationSchedule` objects only after
choosing real node names and disposable test datasets for your cluster. The
sender Job, per-run Secret, and `ZFSReceiveTask` are created in the namespace of
the run object. The receiver DaemonSet runs in `zfsreplication-system`.

## One-Shot Run

```yaml
apiVersion: zfsreplication.ringhof.io/v1alpha1
kind: ZFSReplicationRun
metadata:
  name: pg-a-to-b-manual-001
  namespace: zfsreplication-smoke
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
apiVersion: zfsreplication.ringhof.io/v1alpha1
kind: ZFSReplicationSchedule
metadata:
  name: pg-a-to-b-hourly
  namespace: zfsreplication-smoke
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
  Mounted receives are only authorized when this is false.
- `receiveResumable`: pass `--no-resume` when false. Defaults to true.
- `includeSnaps`: one `--include-snaps=<regex>` per item.
- `excludeSnaps`: one `--exclude-snaps=<regex>` per item.

The controller also passes a generated Syncoid `--identifier` derived from the
replication relationship so receiver-side sync snapshot pruning is scoped to
snapshots owned by that relationship.

Sender Jobs also set a stable pod hostname, `zfsrep-sender`, because upstream
Syncoid includes the local hostname in sync snapshot names and uses the
identifier-plus-hostname prefix when pruning obsolete sync snapshots. The
generated `--identifier` remains the relationship boundary; the stable hostname
only prevents ephemeral Kubernetes Job pod names from fragmenting Syncoid's own
pruning scope.

## Object Lifecycle

Each run gets its own SSH `Secret` and `ZFSReceiveTask`. The sender connects to
the receiver pod address from task status, verifies the receiver host key with a
controller-written `known_hosts` file, and the receiver accepts only active,
unexpired per-run keys.

After a run succeeds or fails, the controller marks the receive task Completed or
Failed and deletes the SSH Secret. The receiver DaemonSet stops authorizing
terminal or expired tasks. The sender Job has `ttlSecondsAfterFinished: 86400`
so Kubernetes can keep it briefly for inspection before TTL cleanup.

Sender Jobs pin pods with `spec.template.spec.nodeName` and use a stable pod
hostname so Syncoid-owned sync snapshots keep a stable pruning prefix across
runs. At startup, the sender compares the downward API node name with the
expected source node and exits before running ZFS commands on a mismatch.
Receiver DaemonSet pods publish their own node and pod IP through
`ZFSReceiveTask.status`.

Sender Jobs use `backoffLimit: 0`, `restartPolicy: Never`, and
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

Release tags require both CI workflows:

- `Test`: format, lint, unit/integration tests, and race tests.
- `E2E`: full Lima/k3s real-ZFS E2E on a self-hosted runner labelled
  `zfsreplication-e2e`.

For an alpha `0.2.x` release, the Kubernetes API remains
`zfsreplication.ringhof.io/v1alpha1`; compatibility-breaking API changes may
still happen before a stable `1.0.0`.

The VM e2e environment is documented in `test/e2e/README.md`.

## License

Apache-2.0. See `LICENSE`.
