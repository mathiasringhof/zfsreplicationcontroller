# Syncoid Stable Hostname Design

## Context

Issue #10 reports that Syncoid-owned sync snapshots can accumulate when the
sender hostname changes between runs.

The controller already generates a stable Syncoid `--identifier` from the
replication relationship and passes it to both the sender and receiver policy.
Syncoid 2.3.0 still appends the local hostname to sync snapshot names and uses
that combined identifier-plus-hostname prefix when pruning obsolete sync
snapshots:

```text
syncoid_<identifier><hostname>_<timestamp>
```

In this controller, each sender run is a Kubernetes Job pod. Without an
explicit pod hostname, each run gets a different pod-derived hostname. Later
runs can replicate incrementally from old sync snapshots, but Syncoid's own
pruning only matches snapshots created with the current run's hostname.

## Goals

- Let Syncoid's normal sync snapshot pruning work across repeated future runs.
- Keep the public CRD/API surface unchanged.
- Keep the sender binary and Syncoid argument contract unchanged.
- Preserve the existing relationship-scoped `--identifier` behavior.
- Avoid adding custom snapshot retention or broad cleanup logic.

## Non-Goals

- Do not clean up already-accumulated snapshots created under old per-pod
  hostnames.
- Do not patch or wrap the upstream Syncoid script.
- Do not add a user-facing hostname option unless a later requirement needs it.
- Do not change `noSyncSnap: true` behavior.

## Recommended Approach

Set a stable hostname on sender Job pod templates.

The controller should continue passing the generated relationship identifier as
`SYNCOID_IDENTIFIER` / `--identifier`. Separately, sender Jobs should set
`spec.template.spec.hostname` to a stable DNS label such as
`zfsrep-sender`. Syncoid will then see the same local hostname for each sender
Job, and future sync snapshots for the same relationship will share the same
pruning prefix:

```text
syncoid_<relationship-id>_zfsrep-sender_<timestamp>
```

This keeps ownership split cleanly:

- the existing Syncoid identifier scopes snapshots to one replication
  relationship
- the stable pod hostname makes Syncoid's built-in host-scoped pruning stable
  across ephemeral sender pods

Multiple sender pods may be active with the same Kubernetes pod hostname. That
is acceptable because Kubernetes pod identity still comes from pod name, UID,
IP, and Job ownership. The hostname only changes what processes inside the pod
see from `hostname(2)`. Snapshot isolation must continue to come from
`--identifier`, not from the pod hostname.

## Alternatives Considered

### Relationship-Derived Pod Hostname

The sender pod hostname could be set to the generated `zrc-...` identifier. This
would be stable and relationship-specific, but it would make snapshot names
repeat the relationship identifier in both the Syncoid identifier and hostname
positions. That does not improve pruning behavior over a constant sender
hostname because the `--identifier` already carries relationship scope.

### Custom Cleanup Across Old Hostnames

The controller or sender could list and destroy old
`syncoid_<identifier><old-hostname>_*` snapshots. This would address existing
accumulation, but it adds retention semantics outside Syncoid and increases the
risk of destroying snapshots that should be treated conservatively. This is out
of scope for the first fix.

### Syncoid Wrapper Or Patch

A wrapper could try to override `hostname(2)` behavior or patch Syncoid to
accept a hostname option. That would couple the controller to upstream Syncoid
internals and add runtime complexity when Kubernetes already provides a pod
hostname field.

## Controller Behavior

Sender Job construction should set pod hostname only for sender Jobs. Receiver
DaemonSet pods should keep their existing hostnames because receiver identity
and endpoint publication are separate from Syncoid's local sender hostname.

The hostname value must be a valid DNS label and short enough for Kubernetes
pod hostname constraints. A constant such as `zfsrep-sender` is sufficient.

No environment variables, CRD fields, receiver policies, or Syncoid options need
to change.

The design assumes the existing destination-overlap protection continues to
prevent concurrent sender Jobs for the same replication relationship or target
dataset. If two same-relationship Syncoid runs were active at the same time,
they would share both the same `--identifier` and the same hostname-derived
sync snapshot prefix. That case is unsafe and should remain blocked before the
second sender Job is created.

## Compatibility

Existing runs and old sender Jobs are unaffected. Future sender Jobs get the
stable hostname.

Snapshots created before this fix under old per-pod hostnames will not be
matched by Syncoid's future pruning pass. Operators can remove those manually if
needed after confirming they are stale. New snapshots created after the fix
should prune according to Syncoid's normal behavior.

## Testing Strategy

Unit tests should cover:

- `runSenderJob` sets the sender pod template hostname to the stable value
- existing sender environment, node pinning, and pod security settings remain
  unchanged
- the relationship `SYNCOID_IDENTIFIER` still round-trips through
  `SenderConfigFromLookup`
- overlapping target runs still do not create a second sender Job while the
  earlier run is active

E2E coverage should extend the existing two-run `noSyncSnap: false` path:

- run a full replication with Syncoid-owned sync snapshots
- mutate the source and run incremental replication against the same target
- verify replication still succeeds
- list source and target snapshots for the replicated datasets
- assert obsolete Syncoid sync snapshots do not accumulate after the second run

The e2e assertion should focus on future behavior after both runs are created
with the stable sender hostname. It should not attempt to validate cleanup of
snapshots created by older controller versions.

## Acceptance Criteria

- Future repeated runs with `noSyncSnap: false` use a stable sender hostname.
- Syncoid-owned sync snapshots for a relationship share a stable pruning prefix
  across sender Jobs.
- After a second successful run in e2e, stale Syncoid sync snapshots from the
  first run are pruned on source and target according to Syncoid's normal logic.
- No public API, wire format, dependency, or generated CRD file changes are
  required.
- `go test ./...` passes.
- The focused real-ZFS e2e path for repeated Syncoid-owned snapshots passes.
