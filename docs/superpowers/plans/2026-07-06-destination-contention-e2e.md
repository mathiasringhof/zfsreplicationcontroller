# Destination Contention E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a VM e2e test proving overlapping destination runs are serialized by the live controller.

**Architecture:** Add one seeded-active-run e2e case to `test/e2e/e2e_test.go`. The test creates one active run that occupies a destination, verifies a second overlapping run waits without creating a sender Job, then removes the active run and verifies the blocked run moves past the destination lock.

**Tech Stack:** Go `testing`, `kubectl`, Kubernetes custom resources, existing VM e2e harness helpers.

---

## File Structure

- Modify: `test/e2e/e2e_test.go`
  - Add `TestE2EDestinationContentionWaits` near the other `TestE2E...` run-controller tests.
  - Add focused kubectl helper methods near existing status/job assertion helpers.
- No product controller code should change for this plan.
- No generated files, public API files, dependency files, or manifests should change.
- Do not commit automatically. `AGENTS.md` says not to commit unless explicitly asked.

---

### Task 1: Add the Destination-Contention E2E Test

**Files:**
- Modify: `test/e2e/e2e_test.go`

- [ ] **Step 1: Insert the failing test**

Add this test after `TestE2ESyncoidFailure` in `test/e2e/e2e_test.go`:

```go
func TestE2EDestinationContentionWaits(t *testing.T) {
	k := newKubectlRunner(t)
	suffix := uniqueSuffix()
	pool := realZFSPool()
	blocker := replicationCase{
		Name:          "e2e-lock-a-" + suffix,
		SourceNode:    "missing-source-" + suffix,
		TargetNode:    e2eTargetNode,
		SourceDataset: pool + "/src-lock-a-" + suffix,
		TargetDataset: pool + "/dst-lock-" + suffix,
	}
	blocked := replicationCase{
		Name:          "e2e-lock-b-" + suffix,
		SourceNode:    e2eSourceNode,
		TargetNode:    e2eTargetNode,
		SourceDataset: pool + "/src-lock-b-" + suffix,
		TargetDataset: blocker.TargetDataset,
	}
	k.cleanupReplicationOnExit(blocker.Name)
	k.cleanupReplicationOnExit(blocked.Name)
	k.cleanupReplication(blocker.Name)
	k.cleanupReplication(blocked.Name)

	k.applyReplication(blocker)
	k.patchRunPhase(e2eNamespace, blocker.Name, "Running")
	k.applyReplication(blocked)

	k.waitForDestinationLock(blocked, blocker.Name, blocker.TargetDataset, 90*time.Second)
	k.assertNoSenderJob(blocked.Name)

	k.cleanupReplication(blocker.Name)
	k.waitForRunPastPending(blocked, 2*time.Minute)
}
```

This intentionally uses a missing source node for the blocker. The blocker remains active because its sender Job cannot schedule, so the test does not depend on a slow ZFS transfer.

- [ ] **Step 2: Run the focused test to verify it fails before helpers exist**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./test/e2e -run TestE2EDestinationContentionWaits -count=1 -v
```

Expected: FAIL at build time with errors like:

```text
k.waitForDestinationLock undefined
k.assertNoSenderJob undefined
k.waitForRunPastPending undefined
```

If the command skips because `KUBECONFIG` is missing after these compile errors are fixed later, that is acceptable for local compile verification. The live e2e verification happens in Task 3.

---

### Task 2: Add Focused E2E Helpers

**Files:**
- Modify: `test/e2e/e2e_test.go`

- [ ] **Step 1: Add destination-lock and progress helpers**

Add these methods after `waitForFailedInNamespace` and before `waitForStatusInNamespace`:

```go
func (k kubectlRunner) waitForDestinationLock(sc replicationCase, blockerName, targetDataset string, timeout time.Duration) replicationStatus {
	k.t.Helper()
	return k.waitForStatusInNamespace(e2eNamespace, sc, timeout, func(st replicationStatus) bool {
		return st.Phase == "Pending" &&
			strings.Contains(st.LastError, "waiting for active run "+blockerName) &&
			strings.Contains(st.LastError, targetDataset)
	}, "Pending destination lock")
}

func (k kubectlRunner) waitForRunPastPending(sc replicationCase, timeout time.Duration) replicationStatus {
	k.t.Helper()
	deadline := time.Now().Add(timeout)
	var last replicationStatus
	var lastErr error
	for time.Now().Before(deadline) {
		status, err := k.getStatusInNamespace(e2eNamespace, sc.Name)
		if err == nil {
			last = status
			if status.Phase != "" && status.Phase != "Pending" {
				return status
			}
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}
	k.collectDiagnosticsInNamespace(e2eNamespace, sc.Name)
	k.t.Fatalf("timed out waiting for %s to move past Pending; last status=%#v last error=%v", sc.Name, last, lastErr)
	return replicationStatus{}
}
```

These helpers assert the externally visible status contract. `waitForRunPastPending` accepts any non-empty phase other than `Pending` because this test only proves the destination lock released; existing e2e tests own successful replication.

- [ ] **Step 2: Add the sender-job absence assertion**

Add this method after `assertNoJobsOrSecretsInNamespace`:

```go
func (k kubectlRunner) assertNoSenderJob(name string) {
	k.t.Helper()
	selector := e2eLabelPrefix + "/run=" + name + "," + e2eLabelPrefix + "/role=sender"
	out, err := k.runOutput(20*time.Second, "get", "jobs", "-n", e2eNamespace, "-l", selector, "-o", "name")
	if err != nil && !strings.Contains(err.Error(), "No resources found") {
		k.t.Fatal(err)
	}
	if strings.TrimSpace(out) != "" {
		k.collectDiagnosticsInNamespace(e2eNamespace, name)
		k.t.Fatalf("sender job exists for blocked run %s:\n%s", name, out)
	}
}
```

This avoids over-constraining implementation details like whether a future controller might pre-create a Secret or receive task before acquiring the destination lock.

- [ ] **Step 3: Format the Go code**

Run:

```bash
go fmt ./...
```

Expected: command exits 0 and formats `test/e2e/e2e_test.go` if needed.

- [ ] **Step 4: Run the focused test for compile/skip behavior**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./test/e2e -run TestE2EDestinationContentionWaits -count=1 -v
```

Expected with no live VM kubeconfig: PASS with the test skipped due missing unusable `KUBECONFIG`.

Expected with a live e2e kubeconfig already available in `test/e2e/.artifacts/kubeconfig`: PASS and log a completed `TestE2EDestinationContentionWaits`.

---

### Task 3: Verify Against a Live E2E Cluster

**Files:**
- No file changes.

- [ ] **Step 1: Run the focused test against an existing live e2e cluster when available**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build KUBECONFIG=test/e2e/.artifacts/kubeconfig go test ./test/e2e -run TestE2EDestinationContentionWaits -count=1 -timeout=5m -v
```

Expected if the cluster is running: PASS.

Expected if no cluster is running or kubeconfig is missing: SKIP with a message explaining that `KUBECONFIG` is not usable. In that case, use Step 2 for full verification.

- [ ] **Step 2: Run the focused test through the full VM harness when full e2e verification is required**

Run:

```bash
E2E_TEST_RUN=TestE2EDestinationContentionWaits ./test/e2e/run.sh
```

Expected: the harness creates the Lima/k3s cluster, builds and imports the image, deploys the controller and receiver, runs only `TestE2EDestinationContentionWaits`, and exits 0.

- [ ] **Step 3: Run the normal Go test suite**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./...
```

Expected: PASS for all packages. The e2e package may skip live tests when `KUBECONFIG` is not configured, but it must compile.

---

### Task 4: Review the Diff

**Files:**
- Review: `test/e2e/e2e_test.go`
- Review: `docs/superpowers/specs/2026-07-06-destination-contention-e2e-design.md`
- Review: `docs/superpowers/plans/2026-07-06-destination-contention-e2e.md`

- [ ] **Step 1: Inspect the changed files**

Run:

```bash
git diff -- test/e2e/e2e_test.go docs/superpowers/specs/2026-07-06-destination-contention-e2e-design.md docs/superpowers/plans/2026-07-06-destination-contention-e2e.md
```

Expected:

- `test/e2e/e2e_test.go` contains one new e2e test and three small helper methods.
- The new test does not set up real ZFS datasets.
- The blocked run assertion checks status and absence of a sender Job.
- No public API, manifest, dependency, generated, or unrelated files changed.

- [ ] **Step 2: Check git status**

Run:

```bash
git status --short
```

Expected: changed files are limited to the new destination-contention spec, this plan, and `test/e2e/e2e_test.go`, plus any pre-existing unrelated untracked files that were already present before implementation.

Do not commit unless the user explicitly asks.

---

## Self-Review

Spec coverage:

- Seeded active run: Task 1 creates and patches `blocker` to `Running`.
- Overlapping destination on same node: Task 1 gives both runs the same `TargetNode` and `TargetDataset`.
- Blocked run remains pending: Task 2 `waitForDestinationLock` requires phase `Pending`.
- Waiting message names active run or destination: Task 2 requires `LastError` to include the blocker name and target dataset.
- No sender Job while blocked: Task 2 `assertNoSenderJob` checks only `role=sender`.
- Unblock and progress past lock: Task 1 deletes the blocker and waits for the blocked run to move past `Pending`.
- No slow ZFS dependency: Task 1 uses a missing source node for the blocker and does not prepare datasets.

Placeholder scan:

- The plan contains no forbidden placeholder markers or unspecified test steps.

Type consistency:

- Helper names used by the test match the helper definitions: `waitForDestinationLock`, `assertNoSenderJob`, and `waitForRunPastPending`.
- Existing e2e types and fields are used consistently: `replicationCase`, `replicationStatus`, `Phase`, `LastError`, `TargetDataset`, `TargetNode`.
