# Syncoid Stable Hostname Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make future Syncoid-owned sync snapshots prune correctly across repeated sender Jobs by giving sender pods a stable hostname.

**Architecture:** Keep relationship identity in the existing generated Syncoid `--identifier`. Set only the sender Job pod template hostname to a constant DNS label so upstream Syncoid sees the same local hostname across ephemeral Job pods. Verify the existing overlapping-destination guard continues to prevent unsafe same-relationship concurrency.

**Tech Stack:** Go, controller-runtime fake client tests, Kubernetes batch/v1 Jobs, real-ZFS e2e tests, upstream Syncoid 2.3.0.

## Global Constraints

- Keep the public CRD/API surface unchanged.
- Keep the sender binary and Syncoid argument contract unchanged.
- Preserve the existing relationship-scoped `--identifier` behavior.
- Do not clean up already-accumulated snapshots created under old per-pod hostnames.
- Do not patch or wrap the upstream Syncoid script.
- Do not add a user-facing hostname option.
- Do not change `noSyncSnap: true` behavior.
- Do not add dependencies or change dependency versions.
- Do not commit unless the user explicitly asks for a commit.
- Use red/green TDD for behavior changes.

---

## File Structure

- Modify `internal/controller/zfsreplication_run_controller.go`
  - Owns `runSenderJob`.
  - Add the stable sender hostname constant and assign it to sender Job pod templates.
- Modify `internal/controller/zfsreplication_run_controller_test.go`
  - Covers the sender Job contract and destination contention behavior.
  - Add the unit assertion for the stable hostname.
  - Add a Reconcile-level assertion that destination-locked runs do not create a sender Job.
- Modify `test/e2e/e2e_test.go`
  - Extends the existing full-plus-incremental replication test.
  - Adds a helper to list Syncoid-owned snapshots on a real ZFS dataset and assert only the current stable-hostname sync snapshot remains.
- Modify `README.md`
  - Documents why sender Jobs use a stable pod hostname and why `--identifier` remains the relationship boundary.

---

### Task 1: Set Stable Sender Pod Hostname

**Files:**
- Modify: `internal/controller/zfsreplication_run_controller_test.go`
- Modify: `internal/controller/zfsreplication_run_controller.go`

**Interfaces:**
- Consumes: `runSenderJob(run *zfsv1.ZFSReplicationRun, names runObjects, image, receiverPodIP string) *batchv1.Job`
- Produces: sender Job pod templates with `Spec.Template.Spec.Hostname == "zfsrep-sender"`

- [ ] **Step 1: Write the failing unit test**

In `internal/controller/zfsreplication_run_controller_test.go`, inside `TestRunReconcileSenderJobUsesSyncoidOptions`, immediately after:

```go
sender := getJob(t, r.Client, "zfsrep-manual-1-sender")
```

add:

```go
if got := sender.Spec.Template.Spec.Hostname; got != "zfsrep-sender" {
	t.Fatalf("sender pod hostname = %q, want zfsrep-sender", got)
}
```

- [ ] **Step 2: Run the focused test and confirm it fails**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run TestRunReconcileSenderJobUsesSyncoidOptions -count=1
```

Expected: FAIL with:

```text
sender pod hostname = "", want zfsrep-sender
```

- [ ] **Step 3: Implement the minimal controller change**

In `internal/controller/zfsreplication_run_controller.go`, extend the existing const block:

```go
const (
	failedJobLogMessageTailLimit         = 64 * 1024
	failedJobLogMessageRedactionLookback = 4 * 1024
	senderPodHostname                    = "zfsrep-sender"
)
```

Replace `runSenderJob` with:

```go
func runSenderJob(run *zfsv1.ZFSReplicationRun, names runObjects, image, receiverPodIP string) *batchv1.Job {
	labels := cloneLabels(names.Labels)
	labels[labelPrefix+"/role"] = "sender"
	env := []corev1.EnvVar{
		{Name: datamover.EnvRole, Value: datamover.RoleSender},
		{Name: datamover.EnvSrcDataset, Value: run.Spec.Source.Dataset},
		{Name: datamover.EnvDstHost, Value: fmt.Sprintf("zfs-recv@%s", receiverPodIP)},
		{Name: datamover.EnvSSHKeyFile, Value: datamover.DefaultSSHKeyFile},
		{Name: datamover.EnvKnownHostsFile, Value: datamover.DefaultKnownHostsFile},
		{Name: datamover.EnvSSHPort, Value: datamover.DefaultSSHPort},
		{Name: datamover.EnvDstDataset, Value: run.Spec.Target.Dataset},
		{Name: datamover.EnvSyncoidIdentifier, Value: syncSnapshotIdentifierForRun(run)},
	}
	env = append(env, syncoidEnv(run.Spec.Syncoid)...)
	job := dataMoverJobForRun(run, names.SenderName, image, labels, run.Spec.Source.NodeName, "/usr/local/bin/zfsrep-sender", env, names.SecretName, false)
	job.Spec.Template.Spec.Hostname = senderPodHostname
	return job
}
```

- [ ] **Step 4: Run the focused test and confirm it passes**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run TestRunReconcileSenderJobUsesSyncoidOptions -count=1
```

Expected: PASS.

---

### Task 2: Guard Same-Target Concurrency At Reconcile Level

**Files:**
- Modify: `internal/controller/zfsreplication_run_controller_test.go`

**Interfaces:**
- Consumes: `ZFSReplicationRunReconciler.Reconcile`
- Produces: explicit test coverage that a destination-locked run does not create `names.SenderName`

- [ ] **Step 1: Add the Reconcile-level sender Job absence assertion**

In `internal/controller/zfsreplication_run_controller_test.go`, inside `TestRunReconcileLogsDestinationWaitOnlyOnTransition`, after the first status assertion:

```go
if got.Status.Phase != zfsv1.PhasePending || got.Status.LastError != wantReason {
	t.Fatalf("status = phase %q lastError %q, want Pending/%q", got.Status.Phase, got.Status.LastError, wantReason)
}
```

add:

```go
assertObjectDeleted(t, r.Client, &batchv1.Job{}, names.SenderName)
```

- [ ] **Step 2: Run the focused test**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run TestRunReconcileLogsDestinationWaitOnlyOnTransition -count=1
```

Expected: PASS. This is a safety invariant that should already hold before and after the hostname change.

---

### Task 3: Verify Syncoid Snapshot Pruning In E2E

**Files:**
- Modify: `test/e2e/e2e_test.go`

**Interfaces:**
- Consumes: existing `kubectlRunner.runRealZFS`, `realZFSPreamble`, `shellQuote`, and the two-run `TestE2EFullAndIncrementalReplication`
- Produces:
  - `kubectlRunner.assertRealZFSSyncoidSnapshots(node, jobName, pool, dataset string, wantCount int)`
  - `realZFSSyncoidSnapshotsScript(pool, dataset string) string`

- [ ] **Step 1: Add e2e assertions after the second replication succeeds**

In `test/e2e/e2e_test.go`, inside `TestE2EFullAndIncrementalReplication`, after:

```go
k.assertRealZFSMarker(second.TargetNode, "zfs-dst-p2-"+suffix, pool, second.TargetDataset, "second-"+suffix)
```

add:

```go
k.assertRealZFSSyncoidSnapshots(second.SourceNode, "zfs-src-sync-snaps-"+suffix, pool, second.SourceDataset, 1)
k.assertRealZFSSyncoidSnapshots(second.TargetNode, "zfs-dst-sync-snaps-"+suffix, pool, second.TargetDataset, 1)
```

- [ ] **Step 2: Add the e2e helper methods**

In `test/e2e/e2e_test.go`, after `assertRealZFSSnapshotExists`, add:

```go
func (k kubectlRunner) assertRealZFSSyncoidSnapshots(node, jobName, pool, dataset string, wantCount int) {
	k.t.Helper()
	out := k.runRealZFS(node, jobName, realZFSSyncoidSnapshotsScript(pool, dataset))
	snapshots := nonEmptyOutputLines(out)
	if len(snapshots) != wantCount {
		k.t.Fatalf("syncoid snapshots for %s on %s = %v, want %d", dataset, node, snapshots, wantCount)
	}
	for _, snapshot := range snapshots {
		if !strings.Contains(snapshot, "_zfsrep-sender_") {
			k.t.Fatalf("syncoid snapshot %q does not include stable sender hostname", snapshot)
		}
	}
}

func nonEmptyOutputLines(out string) []string {
	var lines []string
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
```

In `test/e2e/e2e_test.go`, after `realZFSReadMarkerScript`, add:

```go
func realZFSSyncoidSnapshotsScript(pool, dataset string) string {
	return realZFSPreamble(pool) + "\n" + strings.Join([]string{
		"zfs list -H -t snapshot -o name -r " + shellQuote(dataset) + ` | awk -F@ '$2 ~ /^syncoid_/ { print $0 }'`,
	}, "\n")
}
```

- [ ] **Step 3: Run the focused e2e when a test cluster is available**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build KUBECONFIG=/Users/mathias/Developer/zfsreplicationcontroller/test/e2e/.artifacts/kubeconfig go test ./test/e2e -run TestE2EFullAndIncrementalReplication -count=1 -timeout=10m -v
```

Expected after Task 1 implementation: PASS. On the old implementation without the stable hostname, the new assertions should fail because source and target datasets retain more than one `syncoid_` snapshot after the second run.

---

### Task 4: Document Stable Sender Hostname Behavior

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: existing Syncoid Options and Object Lifecycle documentation
- Produces: README text that explains the stable hostname and why `--identifier` remains required

- [ ] **Step 1: Update the Syncoid Options section**

In `README.md`, after:

```markdown
The controller also passes a generated Syncoid `--identifier` derived from the
replication relationship so receiver-side sync snapshot pruning is scoped to
snapshots owned by that relationship.
```

add:

```markdown
Sender Jobs also set a stable pod hostname, `zfsrep-sender`, because upstream
Syncoid includes the local hostname in sync snapshot names and uses the
identifier-plus-hostname prefix when pruning obsolete sync snapshots. The
generated `--identifier` remains the relationship boundary; the stable hostname
only prevents ephemeral Kubernetes Job pod names from fragmenting Syncoid's own
pruning scope.
```

- [ ] **Step 2: Update the Object Lifecycle section**

In `README.md`, replace:

```markdown
Sender Jobs pin pods with `spec.template.spec.nodeName`. At startup, the sender
compares the downward API node name with the expected source node and exits
before running ZFS commands on a mismatch. Receiver DaemonSet pods publish their
own node and pod IP through `ZFSReceiveTask.status`.
```

with:

```markdown
Sender Jobs pin pods with `spec.template.spec.nodeName` and use a stable pod
hostname so Syncoid-owned sync snapshots keep a stable pruning prefix across
runs. At startup, the sender compares the downward API node name with the
expected source node and exits before running ZFS commands on a mismatch.
Receiver DaemonSet pods publish their own node and pod IP through
`ZFSReceiveTask.status`.
```

- [ ] **Step 3: Check the edited README excerpt**

Run:

```sh
sed -n '220,250p' README.md
```

Expected: output includes both the generated `--identifier` paragraph and the stable `zfsrep-sender` hostname paragraph.

---

### Task 5: Format And Verify

**Files:**
- Verify: all touched Go and Markdown files

**Interfaces:**
- Consumes: changes from Tasks 1-4
- Produces: formatted code and recorded verification results

- [ ] **Step 1: Format Go files**

Run:

```sh
go fmt ./...
```

Expected: command exits 0.

- [ ] **Step 2: Run all Go tests**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./...
```

Expected: PASS for all packages.

- [ ] **Step 3: Run lint**

Run:

```sh
golangci-lint run
```

Expected: command exits 0.

- [ ] **Step 4: Run focused real-ZFS e2e**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build KUBECONFIG=/Users/mathias/Developer/zfsreplicationcontroller/test/e2e/.artifacts/kubeconfig go test ./test/e2e -run TestE2EFullAndIncrementalReplication -count=1 -timeout=10m -v
```

Expected: PASS. If the local e2e cluster is unavailable, record the exact skip or connection error in the final summary.

- [ ] **Step 5: Inspect workspace changes**

Run:

```sh
git status --short
```

Expected: modified files are limited to `internal/controller/zfsreplication_run_controller.go`, `internal/controller/zfsreplication_run_controller_test.go`, `test/e2e/e2e_test.go`, `README.md`, and the new docs under `docs/superpowers/`.
