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

1. Update every runtime image reference to the same immutable image:

   - controller Deployment container image
   - controller `DATA_MOVER_IMAGE`
   - receiver DaemonSet container image

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
