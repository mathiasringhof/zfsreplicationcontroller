# Destination Contention E2E Design

## Purpose

Add a VM end-to-end test for the controller-owned safety contract that overlapping destination datasets on the same node are serialized.

The test should prove the deployed controller waits before starting conflicting receive work. It should not retest the full destination-overlap matrix, cron parsing, Syncoid argument construction, or successful ZFS replication semantics already covered by lower-level tests and existing e2e cases.

## Behavior Under Test

When one `ZFSReplicationRun` is active for a target dataset, a second run targeting the same dataset hierarchy on the same target node must remain pending. While blocked, the second run must not create its sender Job.

After the active run is made terminal or removed, the blocked run should reconcile forward past the destination-lock state.

## Test Shape

Use a seeded-active-run scenario:

1. Create run A with a valid target dataset on the target node. Its source node may be intentionally unschedulable so the test does not depend on a slow transfer.
2. Patch run A status to an active phase, such as `Running`, so the controller sees it as owning the target.
3. Create run B with a valid source dataset and an overlapping target dataset on the same target node.
4. Wait until run B reports `Pending` with a waiting message naming the active run or blocked destination.
5. Assert run B has no sender Job while it is blocked.
6. Mark run A terminal or delete run A.
7. Assert run B moves past the lock state, for example into `StartingReceiver` or a later non-`Pending` phase.

The final assertion should require progress past `Pending`, not full successful replication. Existing e2e coverage already proves real full and incremental replication.

## Non-Goals

- Do not make the test depend on a deliberately slow ZFS transfer.
- Do not cover every target-overlap variant in e2e; unit tests own same dataset, parent/child, child/parent, siblings, and different-node cases.
- Do not assert exact phase-by-phase progression beyond the destination-lock contract.
- Do not validate cron syntax or schedule timing.

## Robustness Notes

The test should assert externally visible behavior through Kubernetes objects: run status and absence of the blocked sender Job. It should avoid inspecting controller logs or generated implementation details unless needed for diagnostics.

Because run A is seeded into an active status, the test is intentionally a controller behavior contract, not a full user-flow replication scenario. This keeps the test stable while still exercising the live deployed controller, CRDs, status subresources, watches, RBAC, and Kubernetes reconciliation.
