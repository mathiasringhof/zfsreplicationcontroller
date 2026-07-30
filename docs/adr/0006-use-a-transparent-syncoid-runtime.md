# Use a transparent Syncoid Runtime

The Sender is a transparent operator for one Syncoid process. The Syncoid
Replication Contract translates one immutable Replication Run into an opaque
value that exposes the Receiver Authorization policy immediately and produces
the final sender argument vector after Receiver connection details are known.
The controller places that vector directly in the Sender container
specification.

The Syncoid Runtime executes only the fixed `syncoid` executable. It forwards
standard output and standard error unchanged to the corresponding container
streams and returns one immutable Syncoid Runtime Outcome. The Outcome exposes
only the final exit code and a Sender Failure Message. A successful Outcome has
exit code zero and no message; a failed Outcome has a nonzero exit code and a
nonempty message.

The Runtime preserves an ordinary nonzero Syncoid exit code and reports it
generically, such as `syncoid exited with status 23`. When no usable process
exit code exists, it uses exit code 1. Sender Failure Messages are valid UTF-8,
single-line, and at most 4,096 bytes. They are derived only from process startup
or completion, never from Syncoid output.

The Sender command publishes a failed Outcome's message through the Kubernetes
termination-message file on a best-effort basis. Publication failure emits one
concise wrapper line on standard error without replacing the Runtime exit code.
The controller preserves a published message verbatim and otherwise falls back
to container termination reason and exit code, the Job failure condition, then
the generic Sender Job failure message. Kubernetes-generated fallback evidence
is also retained without the removed general-purpose sanitizer. The controller
never reads Sender Pod logs.

Consequently, output capture, redaction, retained tails, line bounding, primary
cause selection, lifecycle logging, transfer observation, the private
environment codec, and the intermediate invocation type are removed. Detailed
Syncoid evidence belongs only to normal container logs. Credential contents and
private-key material must never enter generated arguments; credential file
paths are ordinary process configuration.

This decision supersedes ADR-0001's output-derived Failure Diagnosis while
retaining its termination-message transport. ADR-0002's single immutable
release image and drain-first upgrades remain the compatibility strategy for
the private controller-to-Sender argument transport.
