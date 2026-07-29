# Use controller ownership for Replication Run children

A Replication Run is the Kubernetes controller owner of its SSH Secret, Receive Task, and sender Job. Ownership is the lifecycle relationship: the child APIs do not duplicate it through a name-based Run reference or label. Ownership is not authorization; RBAC controls who may create Receive Tasks, and the receiver authorizes each exact Receive Task incarnation as described in ADR-0003.

## Consequences

- The Receive Task `runRef` field and the unused per-run labels on the Secret, Receive Task, and Job are removed outright.
- An existing child is usable only when its controller owner reference contains the exact Replication Run UID. A same-name object with any other ownership fails the Run; the controller does not adopt, overwrite, or delete it.
- Child names share the sender Job's 63-character envelope. A short Run name that needs no lossy normalization remains fully readable. Otherwise, names use `zfsrep-<30-character-readable-prefix>-<16-hex-character-hash>-<role>`, hashing the complete original Run name. The role is `ssh`, `receiver`, or `sender`.
- Before sender execution begins, a missing Receive Task may be recreated from its existing Secret, and a missing Secret and Receive Task may be recreated together with a fresh key. A Receive Task without its matching Secret fails the Run.
- Once sender execution begins, the sender Job is never recreated. Run status records the Job's name and exact UID; the UID is bound once and retained for the Run's lifetime even after Job TTL cleanup. While the Run is nonterminal, a missing Job or a different Job UID fails the Run; after the terminal outcome is recorded, TTL deletion is expected.
- Run status does not duplicate the deterministic SSH Secret or Receive Task names. Their relationship to the Run is expressed by ownership. The sender Job name remains alongside its UID because together they identify the exact historical execution.
- The controller does not add a finalizer solely to close the rare gap between Job creation and recording its UID.
- Deleting an active Replication Run is cancellation. The Run has no cleanup finalizer; Kubernetes garbage collection removes its owned Job, Receive Task, and Secret, and an in-progress Syncoid process may be interrupted.
- The SSH Secret is deleted as soon as the controller observes the sender Job succeed or fail. The Receive Task is made terminal so the receiver revokes its grant. The Job remains available for its configured TTL.
- Reconciliation validates exact ownership and the fields required for the next state transition. It does not continuously reread controller-owned children merely to detect hypothetical drift.
