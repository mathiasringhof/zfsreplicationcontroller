# Ship one release image

Manager, sender, and receiver are released and deployed from one immutable release image reference. Compatibility between independently versioned binaries is not supported, trading deployment flexibility for guaranteed compatibility across the manager-to-sender failure channel and the sender-to-receiver Syncoid command contract.

## Consequences

- A version tag such as `v0.4.0` is supported when the release process never rewrites it. A digest provides mechanical content pinning; mutable tags such as `main` and `latest` are for development rather than production.
- The manager Deployment image is the manifest source of truth. Kustomize copies that exact reference to the receiver DaemonSet image and to the manager's required `RELEASE_IMAGE` environment value, which the controller uses for sender Jobs.
- The manager fails at startup when `RELEASE_IMAGE` is empty instead of falling back to an implicit image. `DATA_MOVER_IMAGE`, `--datamover-image`, and the independent sender-image override are removed.
- Rendered-manifest tests enforce equality of the manager, sender, and receiver image references.
- Upgrades are drain-first: suspend schedules, wait for active Replication Runs to finish, then roll out manager and receiver. Existing Jobs and Pods cannot be replaced atomically, and cross-version operation is unsupported.
