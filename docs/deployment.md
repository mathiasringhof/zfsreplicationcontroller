# Deployment Checklist

Use this checklist when promoting a released controller image into a GitOps
repository or another long-lived Kubernetes install. The image is only one part
of a release; CRDs and RBAC are part of the runtime contract too.

## Before Deploying

1. Pick the release version and immutable image reference.

   Prefer a digest:

   ```sh
   ghcr.io/mathiasringhof/zfsreplicationcontroller@sha256:<digest>
   ```

   A matching release tag such as `v0.4.0` is acceptable for quick testing, but
   production GitOps should pin the digest.

2. Read the release diff for Kubernetes-facing changes.

   Check at least:

   ```sh
   git diff <old-tag>..<new-tag> -- config api README.md
   git diff <old-tag>..<new-tag> -- config/crd config/rbac config/manager config/receiver
   ```

3. Render the repository install manifest for the release image.

   ```sh
   ./hack/render-release-manifest.sh <new-tag> \
     ghcr.io/mathiasringhof/zfsreplicationcontroller@sha256:<digest> \
     > /tmp/zfsreplicationcontroller-<new-tag>.yaml
   ```

## GitOps Promotion

When the deployment is managed by another repository, sync all relevant release
artifacts, not just the container image.

1. Set the manager Deployment image to the immutable release image and preserve
   the manifest's image propagation:

   - controller Deployment container image
   - controller `RELEASE_IMAGE`
   - receiver DaemonSet container image

   The rendered values must be byte-for-byte identical. Manager, sender, and
   receiver are one release unit; mixed versions are unsupported.

2. Sync CRDs from `config/crd/`.

   Missing CRD schema fields can make new release options impossible to set even
   though the controller image supports them.

3. Sync RBAC from the matching profile in `config/rbac/`.

   For namespace-scoped installs, compare against:

   - `config/rbac/namespaced_role.yaml`
   - `config/rbac/namespaced_role_binding.yaml`
   - `config/rbac/receiver_namespaced_role.yaml`
   - `config/rbac/receiver_namespaced_role_binding.yaml`

4. Sync Deployment and DaemonSet manifest changes from `config/manager/` and
   `config/receiver/`.

   Keep local namespace, node selector, and image pinning adaptations
   intentional. Do not accidentally revert them while copying upstream changes.

5. Render and review the target GitOps overlay before pushing.

   Example:

   ```sh
   kubectl kustomize apps/production/zfsreplication
   kubectl kustomize apps/production --load-restrictor=LoadRestrictionsNone
   ```

6. After Flux or the GitOps controller applies the commit, verify the live
   objects:

   ```sh
   kubectl -n zfsreplication-system rollout status deploy/zfsreplication-controller
   kubectl -n zfsreplication-system rollout status ds/zfs-receiver
   kubectl -n zfsreplication get zfsreplicationschedules,zfsreplicationruns,zfsreceivetasks
   kubectl -n zfsreplication get jobs,pods
   kubectl -n zfsreplication-system logs deploy/zfsreplication-controller --since=30m
   ```

## Drain-First Upgrades

Before changing the release image, set `spec.suspend: true` on every
`ZFSReplicationSchedule` and wait for all active `ZFSReplicationRun` objects to
reach `Succeeded` or `Failed`. Then roll out the manager Deployment and receiver
DaemonSet together and verify both use the same image reference. Resume schedules
only after both rollouts are available.

Do not upgrade while a sender Job is active. Existing Jobs and Pods are not
replaced atomically with the manager and receiver, and cross-version operation is
not supported.

## Receiver Authorization Operations

Treat each Receiver's authorization state as an immutable, complete snapshot.
The Receiver atomically activates a new snapshot after observing the full
node-local `ZFSReceiveTask` view; it never edits an active grant in place. An
idle Receiver with the canonical empty snapshot is healthy and should remain
Kubernetes-ready. A terminal or expired task removes only its own grant, so an
unrelated replication must continue to authenticate normally.

Task Ready status is eventually consistent. It records that the exact task UID
was active on the exact reported Receiver Pod UID at the last successful status
update; it is not the authorization decision. OpenSSH key expiry and the
Receiver's forced-command admission are the live enforcement points. Before a
sender Job is created, the manager also performs fresh reads of the task and
reported Pod and requires that exact Pod to be currently ready.

The task's `spec.ssh.expiresAt` is a renewable lease. The manager extends it
monotonically while the run remains active, and the Receiver publishes the
renewed grant in a new snapshot. If control-plane access or renewal fails long
enough to cross the deadline, new SSH authentication fails closed even when a
Ready status update cannot be persisted. Already-admitted receive commands are
allowed to finish; later commands and lapsed or terminal tasks are denied.

When diagnosing a wait at `StartingReceiver`, inspect the task and the exact Pod
it reports together:

```sh
kubectl -n zfsreplication get zfsreceivetask <task> -o yaml
kubectl -n zfsreplication-system get pod <reported-pod> -o yaml
kubectl -n zfsreplication-system logs ds/zfs-receiver --since=15m
```

## v0.4.0 Notes

The `v0.4.0` release needs manifest updates in addition to image pins:

- CRDs expose `successfulRunsHistoryLimit` and `failedRunsHistoryLimit` on
  `ZFSReplicationSchedule`.
- CRDs expose `deleteTargetSnapshots` on run and schedule Syncoid settings.
- CRDs expose receiver policy `allowTargetSnapshotDestroy`.
- Namespace-scoped controller RBAC grants `delete` on `zfsreplicationruns` so
  scheduled run history pruning can delete old terminal runs.

If any of those are missing from the deployed manifests, the controller image is
newer than the Kubernetes API/RBAC it is running against. That is a bad half
upgrade: boring to create, annoying to debug.

## Rollback Notes

Rolling back the image is usually straightforward if the old image still works
with the current CRDs.

Do not blindly roll CRDs backward after users or controllers may have written
objects using new fields. Kubernetes will reject or prune data depending on the
schema and server behavior, and either version of that surprise is unpleasant.
