# Sender Job Logging Design

## Context

GitHub issue #8 reports that successful sender jobs do not leave enough useful
evidence in pod logs. The sender currently runs `syncoid`, mirrors child stderr
to the container log, captures child stdout in memory, and ignores stdout on
success. On failure, the returned error includes only stderr or the process
error.

The controller already records run phase and object names in status, and the
manager now installs a controller-runtime logger, but the run reconciler does
not emit lifecycle logs. Receiver pod logs prove that the SSH path was used,
but they mostly describe SSH envelope activity rather than replication
semantics.

## Goal

Make each replication run legible from normal Kubernetes logs without changing
the public API, CRDs, wire formats, or dependency set.

## Non-Goals

- Do not add status fields for snapshots, GUIDs, log excerpts, or durations.
- Do not change the `ZFSReplicationRun` or `ZFSReceiveTask` schemas.
- Do not add a logging dependency.
- Do not require post-run ZFS inspection to prove the final snapshot.
- Do not log private key material, host key material, or any other secret.

## Design

Add sender-side logging in `internal/datamover` around the `syncoid` execution.
The sender should emit consistent line-oriented logs to stderr because
Kubernetes pod logs naturally collect container stdout and stderr.

The sender will log:

- start of replication with source dataset, destination dataset, target host,
  SSH port, syncoid identifier, and safe syncoid options
- sanitized `syncoid` command shape with the private key path redacted
- captured `syncoid` stdout and stderr after the command exits
- completion with result, exit code when available, duration, and best-effort
  final snapshot evidence when safely found in `syncoid` output

The `syncoid` command should still receive the same arguments it receives
today. Logging must be observational.

On failure, return an error that preserves useful stdout and stderr context.
The final sender log line should contain the same summary that the controller
can extract into `status.lastError`.

Add controller-runtime logs in `internal/controller` for the major run lifecycle
steps:

- run accepted
- waiting for receiver readiness
- receiver selected and known hosts written
- sender job created or already present
- sender job succeeded or failed
- terminal cleanup attempted

Controller logs should include the run name, namespace, source dataset, target
dataset, source node, target node, sender job name, receive task name, receiver
pod name, receiver pod IP, and sync snapshot identifier when available.

## Snapshot And GUID Handling

Snapshot name and GUID logging is best-effort. The sender may scan `syncoid`
stdout and stderr for safe snapshot-looking tokens or GUID-looking fields and
log them when present. If `syncoid` output does not expose reliable snapshot
evidence, the sender must still complete successfully and must not perform an
extra ZFS query in this change.

This avoids broadening issue #8 into post-replication state inspection, where
`noSyncSnap`, include/exclude patterns, existing snapshots, and resumable
receives can make the meaning of "final snapshot" subtle.

## Error Handling

Sender validation errors should continue to return before running `syncoid`.
If `syncoid` exits non-zero, the sender should include useful stdout and stderr
in both logs and the returned error while trimming empty streams.

The controller's existing failed-job log extraction should keep working. Sender
logs should make the last non-empty line a concise failure summary so
`status.lastError` remains helpful.

## Security

Logging the SSH private key file path can leak implementation detail and looks
secret-adjacent in audits, so the sanitized command should replace any
`--sshkey=...` argument with `--sshkey=<redacted>`. Known hosts file paths,
target host, datasets, compression settings, include/exclude patterns, and the
sync snapshot identifier are considered safe operational metadata.

No private key contents, public key contents, host key contents, Kubernetes
Secret values, or authorized key lines should be logged.

## Testing

Use red/green TDD with focused Go tests:

- `internal/datamover` tests for sender start/completion logs, sanitized
  command logging, stdout/stderr preservation, failure summaries, exit code and
  duration presence, and best-effort snapshot extraction.
- `internal/controller` tests for lifecycle logging around receiver readiness,
  sender job creation, success, and failure.

Then run:

```sh
go fmt ./...
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover ./internal/controller ./cmd/zfsrep-sender
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./...
```

## Acceptance Criteria

- A successful sender job log identifies the source dataset, target dataset,
  target host, safe syncoid options, sanitized command shape, clean exit, and
  duration.
- A failed sender job log includes relevant `syncoid` stdout and stderr.
- The same concise failure summary appears in `status.lastError` through the
  existing failed-job log extraction path.
- Logs do not include private key material or unredacted `--sshkey` values.
- Controller logs show enough lifecycle events to follow a run from acceptance
  through receiver selection, sender job creation, terminal result, and cleanup.
- Receiver-side SSH logs can be correlated through run object names, receive
  task names, receiver pod names, and the sync snapshot identifier.
