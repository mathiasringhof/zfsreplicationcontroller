# Delete Target Snapshots Design

## Context

Issue #12 asks the controller to expose Syncoid's
`--delete-target-snapshots` option as an explicit opt-in.

Syncoid normally synchronizes source and target snapshots. With
`--delete-target-snapshots`, after a successful synchronization it also destroys
snapshots that exist only on the target. That is useful when the target dataset
is meant to be a strict mirror whose snapshot lifecycle is owned by the source.
It is destructive when operators keep target-local rollback, rescue, or
inspection snapshots.

The controller already exposes several Syncoid options through
`spec.syncoid`, normalizes them in `internal/replication`, passes them to the
sender Job as environment variables, and converts them to Syncoid CLI arguments
inside the sender binary.

The receiver currently has two destroy-related policies:

- `AllowDestroy`, derived from `forceDelete`, permits dataset destroys under the
  target dataset.
- `AllowSyncSnapshotDestroy`, derived from `!noSyncSnap`, permits destroying
  relationship-owned Syncoid sync snapshots that match the generated
  `syncoid_<identifier>_...` prefix.

`--delete-target-snapshots` is a different behavior. It may destroy arbitrary
snapshots on the exact target dataset, not only relationship-owned Syncoid sync
snapshots.

## Goals

- Add a direct user-facing mapping for Syncoid's
  `--delete-target-snapshots` option.
- Keep the default safe: target-only snapshots are preserved unless explicitly
  opted into.
- Authorize the receiver side narrowly enough for Syncoid to perform the new
  behavior without broadening dataset destroy permissions.
- Keep the implementation aligned with existing `spec.syncoid` option plumbing.
- Document the destructive behavior clearly.

## Non-Goals

- Do not enable target-only snapshot deletion through `forceDelete`.
- Do not change default replication behavior.
- Do not add custom controller-side snapshot reconciliation or retention logic.
- Do not allow recursive snapshot destroys or dataset destroys for this option.
- Do not change the existing relationship-scoped Syncoid sync snapshot pruning
  behavior.

## API

Add an optional field to `SyncoidSpec`:

```go
DeleteTargetSnapshots *bool `json:"deleteTargetSnapshots,omitempty"`
```

The field maps directly to Syncoid's `--delete-target-snapshots` flag.

Defaults:

- unset: `false`
- explicit `false`: `false`
- explicit `true`: pass `--delete-target-snapshots`

The CRD schemas for `ZFSReplicationRun.spec.syncoid` and
`ZFSReplicationSchedule.spec.runTemplate.syncoid` should expose the field as a
boolean with default `false`.

## Sender Behavior

Extend the existing Syncoid option pipeline:

1. Add `DeleteTargetSnapshots` to `replication.SyncoidOptionInput` and
   `replication.SyncoidOptions`.
2. Normalize it with the existing pointer-bool default helper.
3. Add a sender environment variable such as
   `SYNCOID_DELETE_TARGET_SNAPSHOTS`.
4. Include that env var in sender Job construction.
5. Parse it in `SenderConfigFromLookup`.
6. Append `--delete-target-snapshots` in `syncoidArgs` only when true.

The flag should be independent from `forceDelete`, `noSyncSnap`, and
`noRollback`. A run may choose any combination supported by Syncoid, but this
new flag only means target-only snapshot deletion.

Sender startup logs should include the normalized boolean alongside other
Syncoid options so operators can see whether the destructive mode was enabled
for a run.

## Receiver Authorization

Add a separate receive policy field, for example:

```go
AllowTargetSnapshotDestroy bool `json:"allowTargetSnapshotDestroy,omitempty"`
```

The run controller should set it from normalized
`options.DeleteTargetSnapshots`.

The receiver forced-command authorizer should allow `zfs destroy` for snapshots
only when all of these are true:

- `AllowTargetSnapshotDestroy` is true;
- the command is exactly `zfs destroy <target>@<snapshot>`;
- the dataset is exactly the configured target dataset;
- the snapshot name passes the existing safe snapshot-name parser;
- the command has no redirects and no recursive flags.

Existing batched destroy handling may continue to work, as long as each command
in the batch independently satisfies the same exact-target snapshot rules.

This keeps the new permission distinct from:

- `AllowSyncSnapshotDestroy`, which remains limited to
  `syncoid_<identifier>_...` snapshots;
- `AllowDestroy`, which remains the dataset-level permission used by
  `forceDelete`.

The authorizer should continue rejecting target snapshot destroys by default
and should continue rejecting child dataset snapshots for this new policy. The
issue is about Syncoid's target-only snapshots for the replicated target, not a
general recursive cleanup feature.

## Controller Behavior

The run controller should round-trip the new option the same way it handles
existing Syncoid booleans:

- include it in `normalizedSyncoidOptions`;
- write it into sender Job env;
- include it in the receive task policy;
- preserve current immutable-run behavior.

Scheduled runs inherit the option through the existing
`ZFSReplicationSchedule.spec.runTemplate` path. No schedule-specific logic is
needed beyond CRD schema exposure.

## Documentation

Update the README Syncoid options section with a warning-style description:

- `deleteTargetSnapshots`: pass `--delete-target-snapshots`. Defaults to false.
  When true, Syncoid may destroy snapshots that exist only on the target after a
  successful sync. Use it only for strict mirror targets whose snapshot
  lifecycle is owned by the source.

The operational notes should distinguish this from `forceDelete`:

- `forceDelete` allows Syncoid to destroy conflicting target datasets.
- `deleteTargetSnapshots` allows Syncoid to destroy target-only snapshots.

Sample manifests should not enable this destructive option by default. A future
strict-mirror example can show it explicitly if users need a full manifest.

## Testing Strategy

Unit tests should cover:

- `NormalizeSyncoidOptions` defaults the new field to false and preserves an
  explicit true value.
- Sender env parsing defaults false and accepts true.
- `syncoidArgs` includes `--delete-target-snapshots` only when configured.
- Sender startup logs include `deleteTargetSnapshots=<value>`.
- The run controller writes the sender env var and round-trips it through
  `SenderConfigFromLookup`.
- The receive task policy sets `AllowTargetSnapshotDestroy` only from
  `deleteTargetSnapshots`.
- CRD schema tests assert the new `syncoid.deleteTargetSnapshots` boolean.
- Receiver command tests reject arbitrary target snapshot destroy by default,
  allow exact target snapshot destroy with the new policy, and still reject
  child dataset snapshots, recursive snapshot destroys, commands with
  redirects, and dataset destroys unless `forceDelete` allows them.

Focused unit tests should be sufficient for the core behavior because this
feature is mostly API and command authorization plumbing. A real-ZFS e2e test
can be added later if there is uncertainty about the exact Syncoid command
shape, but it is not required for the first implementation.

## Alternatives Considered

### Sender-Only Flag

The controller could add the API field and pass the Syncoid argument without
changing receiver authorization. This is incomplete because the forced-command
receiver would reject Syncoid's target snapshot destroy commands at runtime.

### Reuse `forceDelete`

The controller could treat `forceDelete: true` as permission to pass
`--delete-target-snapshots`. This hides a distinct destructive behavior behind
an existing option and does not satisfy the issue's explicit opt-in requirement.

### Strict Mirror Naming

The API could use an intention-oriented name such as
`strictMirrorTargetSnapshots`. The project should instead use the direct
Syncoid mapping, `deleteTargetSnapshots`, so users can connect the field to the
upstream flag and documentation.

## Acceptance Criteria

- `spec.syncoid.deleteTargetSnapshots: true` passes
  `--delete-target-snapshots` to Syncoid.
- The option defaults to false for runs and schedules.
- Receiver authorization permits exact target snapshot destroys only when the
  new option is enabled.
- Existing Syncoid sync snapshot destroy authorization remains
  relationship-scoped.
- `forceDelete` behavior is unchanged.
- README and schema coverage make the destructive behavior visible.
- `go test ./...` passes.
