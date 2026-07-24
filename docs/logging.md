# Logging

Logging is operational evidence for humans. It is structured where the
controller framework provides context, concise at normal verbosity, and never a
compatibility or automation contract.

## Component Boundaries

- The manager and Receiver daemon initialize controller-runtime logging once at
  process startup with the production Zap logger. Controller-managed code uses
  the logger from its `context.Context`.
- The Sender keeps concise `key=value` lifecycle lines alongside sanitized
  Syncoid output on stderr.
- SSH, forced-command, and child-process streams remain explicit stdout and
  stderr. Operational logging must not corrupt command or protocol output.
- Expected process startup or runtime failures produce one concise fatal
  message and a non-zero exit. Panic and stack traces are reserved for broken
  invariants.

The project does not wrap these APIs in a custom logging abstraction and does
not expose runtime log-format or verbosity configuration. Controller processes
use production JSON at `Info` level.

## Events and Levels

Normal-level logs describe process lifecycle and meaningful domain state
transitions. Repeated observations, unchanged reconciliations, expected
waiting, and routine requeues belong at `V(1)` or are omitted.

A Replication Run entering its defined `Failed` phase is a domain outcome, not
automatically a controller error. Log that transition at `Info` with a
sanitized reason. Error logs are for failures of the control mechanism, such as
API access, authorization publication, reconciliation, or process startup.

Important normal-level evidence includes:

- Replication Run accepted.
- Receiver became available for a Run.
- Sender Job created.
- Replication succeeded or failed.
- Sender started and completed.
- Receiver began serving.

`receiver serving` is emitted only after the cache has synchronized, the
initial Receiver Authorization Snapshot has activated, and SSHD has started.
It does not mean that a Receive Task or Kubernetes Pod has become Ready.

The Sender and manager may each record the same replication outcome once. They
are separate Pod log streams and report different observations.

## Error Ownership

Return errors with enough context for the controller-runtime or process
boundary to log them once. Log an error inside a function only when the error
is swallowed, downgraded, or converted into successful control flow.

Do not both log and return the same failure. In particular, a Receiver
authorization publication failure that retains usable authority should return
a classified error such as:

```text
receiver authorization degraded; retaining last complete snapshot: ...
```

Controller-runtime owns emitting that error and applying retry backoff. Logging
does not maintain separate degraded-state tracking solely to suppress retries.

## Context and Fields

Messages are short, static, and human-readable. Changing values belong in
lower-camel-case structured fields.

The shared Replication Run context contains only:

- `namespace`
- `run`
- `sourceNode`
- `sourceDataset`
- `targetNode`
- `targetDataset`

Attach child identifiers such as `senderJob`, `receiveTask`, or `receiverPod`
only to events involving that object. Do not attach SSH Secret names or derived
Syncoid identifiers to every record.

Use the public API's source/target vocabulary. Avoid abbreviated `src` and
`dst` field names. Log messages and field names may evolve and must not be
parsed by automation.

## Sensitive Output

[ADR-0001](adr/0001-use-termination-messages-for-replication-failures.md)
governs command-output capture and redaction. Syncoid output and returned
command errors must cross the replication diagnosis boundary before reaching
logs. Credentials, private-key material, credential paths, and unsafe raw
command errors must not be logged.

CR status, Kubernetes conditions, metrics, and termination messages are the
machine-readable surfaces. Controllers must not recover state by parsing Pod
logs.

## Testing

Test logging policy rather than logger implementation:

- Verify representative important events, redaction, and deliberate duplicate
  suppression.
- Assert exact counts only when once-per-transition behavior is an explicit
  requirement implemented by project code.
- Do not assert timestamps, complete field sets, exact JSON encoding, full log
  ordering, or every branch that can emit the same message.
- Do not unit-test controller-runtime's global logger state or Zap encoding.
- Keep one deployed Receiver smoke check: a known `receiver serving` event is
  visible and the controller-runtime missing-logger warning is absent after its
  fallback window.

Kubernetes Events are not part of this logging policy. CR status remains the
durable operator-facing record.
