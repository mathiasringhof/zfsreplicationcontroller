# Low-Noise Replication Logging Design

## Context

The Scotty branch `origin/scotty/fix-logging-followups` improves sender job
visibility by streaming `syncoid` output and bounding failure output. E2E
verification showed that this improves debugging, but it also exposed two
problems:

- Real `syncoid` failure output can include the SSH private key file path as an
  `ssh -i /var/run/zfsrep/ssh/id_rsa` argument. The current redactor only
  handles `--sshkey=...` forms.
- Controller logs still repeat some lifecycle messages around normal reconcile
  retries and create/get races.

The goal is low-noise operational logging that still lets an operator tell
whether a run succeeded, failed, or is waiting, and where to look next.

## Goals

- Keep normal controller logs lifecycle-oriented and sparse.
- Keep enough structured sender and controller output to diagnose run outcome.
- Preserve sanitized raw `syncoid` detail in sender pod logs for deeper
  debugging.
- Keep `ZFSReplicationRun.status.lastError` concise, actionable, bounded, and
  sanitized.
- Treat `syncoid` text as best-effort evidence, not as a stable API.

## Non-Goals

- Do not add a new logging dependency.
- Do not change replication semantics or `syncoid` arguments.
- Do not store full command transcripts in CR status.
- Do not try to perfectly parse every possible `syncoid` output line.

## Recommended Approach

Use low-noise controller lifecycle logs plus concise sender summaries. Raw
`syncoid` stream output remains available in sender pod logs, but every path
that can reach logs, returned errors, controller reasons, or CR status must pass
through a shared redactor.

## Controller Logging Policy

Controller logs should describe durable state transitions, not every reconcile
observation.

Log at normal info level:

- `accepted replication run` once, after the status patch that initializes
  `StartedAt` and object names succeeds.
- `waiting for replication destination` only when entering pending destination
  wait or when the stored wait reason changes.
- `waiting for replication receiver` only when entering `StartingReceiver`.
- `replication receiver is ready` only when moving into `ReceiverReady`.
- `created sender job` only after sender Job creation is known to have
  succeeded or the Job is already present as the intended object.
- `sender job succeeded` once before marking the run succeeded.
- `sender job failed` once before marking the run failed.
- `cleaning up terminal replication run` when reconciling a terminal run.

Keep repeated observations such as "sender job already present" and generic
"reconciling replication run" at verbose level.

Expected Kubernetes create/get races should be handled idempotently where
practical instead of producing error-level retry noise. Unexpected API failures
should remain errors.

## Sender Logging Policy

Sender logs should make the pod log useful by itself without turning controller
status into a transcript.

Log at normal info level:

- `sender starting` with source dataset, destination dataset, destination host,
  SSH port, syncoid identifier, and safe option values.
- `sender completed` with result, exit code, duration, and a concise failure
  cause when failed.

The sanitized `syncoid` command shape may remain in sender pod logs, but it
must be treated as diagnostic detail. If the project later adds logging
configuration, this line is a good candidate for verbose/debug level.

Stream sanitized `syncoid` stdout and stderr to sender pod logs. These stream
lines are useful for debugging and should remain bounded and redacted, but they
should not be copied wholesale into controller status.

Avoid duplicate failure summaries. `RunSender` already logs `sender completed
result=failure`; `cmd/zfsrep-sender` should not print the same long summary
again before exiting.

## Redaction Rules

Use one shared redactor for all text that can reach logs, returned errors,
controller failure reasons, or CR status.

Redact these forms:

- `--sshkey=...`
- quoted or escaped `--sshkey="..."`
- `ssh -i /path/to/key`
- standalone `-i /path/to/key` in real `syncoid` command dumps
- the known private key path `/var/run/zfsrep/ssh/id_rsa`
- likely `syncoid` SSH control socket paths such as `/tmp/syncoid-*`

The redaction target is the private key path and credential-location detail,
not public key material. The path itself is not equivalent to key contents, but
it has no operator value and should not be normalized as log content.

## Success Summary Rules

Treat success parsing as best effort. Emit only facts that are high confidence.

Allowed summary fields:

- `result=success`
- `exitCode=0`
- `duration`
- `mode=full` when output contains `Sending oldest full snapshot`
- `mode=incremental` when output contains `Sending incremental`
- a human size estimate when trivially extracted from a `(~ 13 KB)`-style token

Do not emit `finalSnapshot` unless the parser can identify the actual final
snapshot correctly. E2E output showed that the current scanner can select the
old fully-qualified snapshot during incremental replication because the new
snapshot appears as a bare snapshot name. Omitting this field is better than
logging a plausible but wrong value.

## Failure Summary Rules

Failure summaries should prefer the shortest actionable sanitized cause.

Preferred order:

1. A `CRITICAL ERROR: ...` line with pipeline details removed when possible.
2. A concise `cannot open ...` or `cannot receive ...` line.
3. A process exit error, such as `exit status 2`.
4. A generic bounded sanitized tail when no structured cause is recognized.

`ZFSReplicationRun.status.lastError` should contain this concise cause, not the
full `syncoid` pipeline command. Full sanitized stream detail remains in the
sender pod log.

## Testing Strategy

Unit tests should cover redaction for:

- unquoted, quoted, and escaped `--sshkey` values
- `-i /var/run/zfsrep/ssh/id_rsa`
- real-looking `syncoid` `CRITICAL ERROR` pipeline output
- bounded output with redaction boundaries near the retained tail

Unit tests should cover summary behavior:

- success mode parsing for full replication
- success mode parsing for incremental replication
- omission of `finalSnapshot` when the parser cannot identify it safely
- concise failure cause selection
- absence of full SSH pipeline text from `status.lastError`
- no duplicate sender failure summary from `cmd/zfsrep-sender`

Controller tests should cover:

- lifecycle logs emitted once for normal transition paths
- wait logs emitted only when entering or changing a wait reason
- idempotent create/get race handling where expected
- terminal cleanup logs preserved

E2E verification should include:

- successful full and incremental replication still pass
- failed syncoid run status mentions the actionable cause
- failed run status and controller reason do not contain
  `/var/run/zfsrep/ssh/id_rsa`
- destination wait logs once for the wait reason instead of every poll

## Acceptance Criteria

- Normal successful runs have enough logs to see accepted, receiver ready,
  sender created, and succeeded.
- Failed runs have a short actionable sanitized reason in CR status.
- Sender pod logs keep sanitized raw `syncoid` details for deeper debugging.
- No logs, returned errors, controller reasons, or CR status include the
  private key path `/var/run/zfsrep/ssh/id_rsa`.
- Controller logs do not repeat lifecycle messages during ordinary requeues.
- `go test ./...` and `golangci-lint run` pass.
- Relevant E2E tests pass, with a focused log inspection for success, failure,
  and destination-wait scenarios.
