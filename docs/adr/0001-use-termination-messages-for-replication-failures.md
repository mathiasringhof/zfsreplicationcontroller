# Use termination messages for replication failures

Sender Jobs publish one selected, concise, sanitized primary failure cause through the Kubernetes termination message instead of requiring the controller to recover machine-readable state from human-oriented pod logs. A Failure Diagnosis remains best-effort operator evidence rather than a stable classification, and detailed sanitized Syncoid output remains in pod logs for humans.

## Consequences

- `internal/replication/diagnosis` is a deep module that owns bounded stdout and stderr capture, live redaction across chunk boundaries, retained tails, and final primary-cause selection.
- Inside Go, a Failure Diagnosis is an opaque value constructible only through that module. It guarantees sanitized, single-line, valid UTF-8 text no longer than 4,096 bytes; sanitization precedes truncation and preserves useful dataset, host, and non-credential path details.
- Errors crossing the module seam render only safe diagnosis text and already-extracted exit metadata. They neither expose raw output nor unwrap an unsafe command error.
- The sender selects evidence in this order: `CRITICAL ERROR:` from stderr then stdout; `cannot receive` or `cannot open` from stderr then stdout; the returned sender or process error; the last non-empty stderr line; the last non-empty stdout line; then `sender failed`.
- The sender command adapter writes the diagnosis to the standard termination-message path with `terminationMessagePolicy: File`. A publication error is logged without replacing the original failure or exit code, and log fallback is forbidden.
- For an already-failed Job, the controller selects evidence in this order: sender termination message; container termination reason with exit code; sanitized Job failure-condition message; then `sender Job failed`.
- When multiple Pods exist, the controller considers only Pods owned by the exact Job UID and deterministically chooses the newest terminated `datamover` container by finish time, creation time, then name.
- Failures that prevent the sender process from running remain Kubernetes condition evidence. This decision does not add a startup timeout or change when a pending Job becomes a failed Replication Run.
- The controller neither reads sender pod logs nor holds `pods/log` permission. Manager, sender, and receiver versions are tied by ADR-0002 rather than supported through log-parsing compatibility.
