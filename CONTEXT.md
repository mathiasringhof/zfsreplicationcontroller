# ZFS Replication

This context describes one-shot and scheduled replication of ZFS datasets between nodes.

## Language

**Receiver**:
A long-lived node-level replication endpoint that enforces the active Receiver Authorization Snapshot for one or more Replication Runs. Its lifecycle is independent of any individual run.

**Failure Diagnosis**:
A concise, sanitized, best-effort explanation of why a replication attempt failed. It is intended for operators, not as a stable classification or an input to automated behavior.

**Receiver Authorization Snapshot**:
The complete, immutable set of replication receive permissions available on one receiver at a reconciliation instant. Every accepted identity and its permitted operations belong to the same snapshot.

**Receiver Authorization Grant**:
The receive permission issued by one specific Receive Task incarnation. It binds one identity, unique within its snapshot, to a destination, a set of permitted operations, and an Authorization Lease; lease expiry or revocation prevents new operations but does not cancel an operation already admitted.

**Authorization Lease**:
A renewable deadline bounding how long a Receiver Authorization Grant remains available without confirmation from the run controller. Renewal may extend an unexpired deadline but cannot change the grant's identity, destination, or permitted operations.

**Ready Receive Task**:
A Receive Task for which the identified Receiver most recently reported the exact grant active. It is an eventually consistent readiness observation; live command admission remains authoritative.
