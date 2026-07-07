# Low-Noise Replication Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Scotty sender/controller logging changes quiet by default, strongly redacted, and still useful for diagnosing run outcomes.

**Architecture:** Keep the existing sender streaming design, but route every output path through stronger shared redaction and concise summary extraction. Keep raw sanitized `syncoid` detail in sender pod logs, while controller status and controller reasons use short actionable causes. Make controller lifecycle logs transition-aware to reduce ordinary reconcile noise.

**Tech Stack:** Go 1.26, standard library, controller-runtime fake client tests, existing Kubernetes E2E harness.

## Global Constraints

- Do not add a new logging dependency.
- Do not change replication semantics or `syncoid` arguments.
- Do not store full command transcripts in CR status.
- Treat `syncoid` text as best-effort evidence, not as a stable API.
- All logs, returned errors, controller reasons, and CR status must redact private key paths.
- Do not commit changes unless explicitly asked; this overrides generic Superpowers commit steps.

---

### Task 1: Shared Redaction And Concise Failure Summaries

**Files:**
- Modify: `internal/datamover/sender.go`
- Modify: `internal/datamover/datamover_test.go`
- Modify: `internal/controller/zfsreplication_run_controller.go`
- Modify: `internal/controller/zfsreplication_run_controller_test.go`

**Interfaces:**
- Consumes: `datamover.RedactSensitiveText(value string) string`
- Produces: stronger `datamover.RedactSensitiveText`, concise `syncoidFailureSummary`, and controller failure extraction that uses the same redactor.

- [ ] **Step 1: Write failing redaction tests**

Add table cases to `TestRedactSensitiveTextRedactsSSHKeyForms` in `internal/datamover/datamover_test.go`:

```go
{
	name: "ssh identity path",
	in:   `ssh -o StrictHostKeyChecking=yes -i /var/run/zfsrep/ssh/id_rsa zfs-recv@10.42.2.11 zfs receive`,
	want: `ssh -o StrictHostKeyChecking=yes -i <redacted> zfs-recv@10.42.2.11 zfs receive`,
},
{
	name: "standalone identity path",
	in:   `before -i /var/run/zfsrep/ssh/id_rsa after`,
	want: `before -i <redacted> after`,
},
{
	name: "known private key path",
	in:   `using /var/run/zfsrep/ssh/id_rsa directly`,
	want: `using <redacted> directly`,
},
{
	name: "syncoid control socket",
	in:   `-S /tmp/syncoid-zfs-recv1042211-1783418787-10-6113 zfs-recv@10.42.2.11`,
	want: `-S <redacted> zfs-recv@10.42.2.11`,
},
```

- [ ] **Step 2: Run redaction test and confirm failure**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover -run TestRedactSensitiveTextRedactsSSHKeyForms -count=1 -v
```

Expected: FAIL because `-i /var/run/zfsrep/ssh/id_rsa`, the bare key path, and `/tmp/syncoid-*` are not redacted.

- [ ] **Step 3: Implement redaction**

Update `RedactSensitiveText` in `internal/datamover/sender.go` so it repeatedly applies:

```go
value = redactOptionValue(value, "--sshkey=", "--sshkey=<redacted>")
value = redactSeparatedOptionValue(value, "-i", "-i <redacted>")
value = redactSeparatedOptionValue(value, "-S", "-S <redacted>")
value = strings.ReplaceAll(value, DefaultSSHKeyFile, "<redacted>")
```

Add helper functions near the existing quote parsing helpers:

```go
func redactOptionValue(value, option, replacement string) string
func redactSeparatedOptionValue(value, option, replacement string) string
func optionValueEnd(value string, start int) int
func isSpace(ch byte) bool
```

Reuse the existing quoted/unquoted value parsing rules instead of regexes.

- [ ] **Step 4: Run redaction test and confirm pass**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover -run TestRedactSensitiveTextRedactsSSHKeyForms -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Write failing concise failure summary tests**

Add a datamover test that passes E2E-shaped stdout/stderr to `syncoidFailureSummary` and asserts:

```go
summary := syncoidFailureSummary(stdout, stderr, fakeExitError{code: 2, msg: "exit status 2"})
if !strings.Contains(summary, "CRITICAL ERROR") {
	t.Fatalf("summary = %q, want CRITICAL ERROR", summary)
}
if strings.Contains(summary, "zfs send") || strings.Contains(summary, "/var/run/zfsrep/ssh/id_rsa") {
	t.Fatalf("summary contains raw pipeline or private key path: %q", summary)
}
```

Add a controller test near the existing failed-log tests that feeds `failureMessageFromLogs` a sender log with a full E2E-shaped `CRITICAL ERROR` pipeline and asserts the returned message is short, contains the actionable `CRITICAL ERROR`, and does not contain `zfs send` or `/var/run/zfsrep/ssh/id_rsa`.

- [ ] **Step 6: Run concise failure tests and confirm failure**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover ./internal/controller -run 'Test.*Concise.*Failure|Test.*Failure.*Summary|TestRunReconcileBoundsSenderPodLogLastError' -count=1 -v
```

Expected: FAIL because current summaries keep the full pipeline text.

- [ ] **Step 7: Implement concise failure extraction**

In `internal/datamover/sender.go`, add:

```go
func conciseSyncoidFailure(value string) string
func truncateSyncoidPipeline(value string) string
func firstUsefulFailureLine(value string) string
```

Use these from `syncoidFailureSummary` so stdout/stderr are redacted, single-lined, and shortened to the best actionable cause. Prefer `CRITICAL ERROR`, then `cannot open`, then `cannot receive`, then bounded tail.

In `internal/controller/zfsreplication_run_controller.go`, use the same concept in `boundedRedactedFailureMessage` by calling `datamover.RedactSensitiveText` first and then selecting a concise failure line before bounding.

- [ ] **Step 8: Run concise failure tests and confirm pass**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover ./internal/controller -run 'Test.*Concise.*Failure|Test.*Failure.*Summary|TestRunReconcileBoundsSenderPodLogLastError|TestRunReconcileRedactsQuotedJobFailedConditionWithoutPodLogs' -count=1 -v
```

Expected: PASS.

### Task 2: Success Summaries And Duplicate Sender Error

**Files:**
- Modify: `internal/datamover/sender.go`
- Modify: `internal/datamover/datamover_test.go`
- Modify: `cmd/zfsrep-sender/main.go`

**Interfaces:**
- Consumes: existing `runSender`
- Produces: final sender success logs with safe mode information and no misleading `finalSnapshot`; sender main exits nonzero without duplicating long summaries.

- [ ] **Step 1: Write failing success summary tests**

Update `TestSenderLogsSuccessfulSyncoidRun` to expect `mode=full` and stop expecting `finalSnapshot=...`.

Add an incremental output test:

```go
func TestSenderSuccessSummaryDoesNotReportMisleadingFinalSnapshotForIncremental(t *testing.T) {
	runner := &fakeRunner{
		stdout: "INFO: Sending incremental tank/src@old ... syncoid_new_2026 to zfs-recv@10.0.0.42:tank/dst (~ 7 KB):\n",
	}
	var logs strings.Builder
	err := runSender(context.Background(), validSenderConfig(), runner, &logs)
	if err != nil {
		t.Fatal(err)
	}
	out := logs.String()
	if !strings.Contains(out, "mode=incremental") {
		t.Fatalf("logs missing incremental mode:\n%s", out)
	}
	if strings.Contains(out, "finalSnapshot=") {
		t.Fatalf("logs contain misleading finalSnapshot:\n%s", out)
	}
}
```

Use an existing valid config literal if no helper exists.

- [ ] **Step 2: Run success summary tests and confirm failure**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover -run 'TestSenderLogsSuccessfulSyncoidRun|TestSenderSuccessSummaryDoesNotReportMisleadingFinalSnapshotForIncremental' -count=1 -v
```

Expected: FAIL because current code emits `finalSnapshot` and no `mode=`.

- [ ] **Step 3: Implement success summary**

Replace `finalSnapshotLogSuffix` usage with:

```go
successSummaryLogSuffix(stdout + "\n" + stderr)
```

Implement:

```go
func successSummaryLogSuffix(output string) string
func syncoidTransferMode(output string) string
func syncoidSizeEstimate(output string) string
```

Emit ` mode=full` or ` mode=incremental` only when confidently recognized. Emit a size estimate only when the `(~ ... )` token is trivially found. Do not emit `finalSnapshot`.

- [ ] **Step 4: Run success summary tests and confirm pass**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover -run 'TestSenderLogsSuccessfulSyncoidRun|TestSenderSuccessSummaryDoesNotReportMisleadingFinalSnapshotForIncremental' -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Write failing duplicate-error test for sender main**

Add a small testable helper in `cmd/zfsrep-sender/main.go` only after the test exists. First create `cmd/zfsrep-sender/main_test.go` with a test that calls the helper with a failing runner and asserts stderr does not contain `syncoid failed:` twice and ideally contains no long summary from `main`.

- [ ] **Step 6: Run sender main test and confirm failure**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./cmd/zfsrep-sender -count=1 -v
```

Expected: FAIL because no helper exists or `main` duplicates the returned error.

- [ ] **Step 7: Implement sender main helper**

Refactor `main` to call:

```go
func run(ctx context.Context, stderr io.Writer, runner datamover.CommandRunner) int
```

Production `main` calls `os.Exit(run(context.Background(), os.Stderr, datamover.ExecRunner{}))`. On `RunSender` error, return exit code `1` without printing the returned error, because `RunSender` already emitted the failure summary.

- [ ] **Step 8: Run sender main test and confirm pass**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./cmd/zfsrep-sender -count=1 -v
```

Expected: PASS.

### Task 3: Controller Transition Noise

**Files:**
- Modify: `internal/controller/zfsreplication_run_controller.go`
- Modify: `internal/controller/zfsreplication_run_controller_test.go`

**Interfaces:**
- Consumes: existing reconcile phases and fake logger tests
- Produces: transition-aware lifecycle logs and idempotent create handling for expected races.

- [ ] **Step 1: Write failing transition log tests**

Extend existing controller logging tests so repeated reconciles do not emit:

- duplicate `accepted replication run`
- duplicate `replication receiver is ready`
- duplicate `created sender job`

Add an interceptor test for a create/get race where sender job creation returns `AlreadyExists`, and assert it does not produce an error result.

- [ ] **Step 2: Run transition log tests and confirm failure**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run 'TestRunReconcileLogsReceiverAndSenderLifecycle|TestRunReconcileLogsReceiverWait|TestRunReconcileLogsDestinationWaitOnlyOnTransition|TestRunReconcile.*AlreadyExists' -count=1 -v
```

Expected: FAIL on duplicate logs or unhandled `AlreadyExists`, depending on exact test names.

- [ ] **Step 3: Implement transition-aware controller logging**

Adjust `Reconcile` and `ensureSenderStarted` so:

- `accepted replication run` is logged only after the status initializer is known to have stuck, or only from a branch that cannot repeat after status changes.
- `replication receiver is ready` and `created sender job` log only when entering `ReceiverReady` or when the sender job is actually newly created.
- `ensureRunJob` treats `apierrors.IsAlreadyExists(err)` from `Create` as success.
- existing verbose logs remain verbose.

- [ ] **Step 4: Run transition log tests and confirm pass**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run 'TestRunReconcileLogsReceiverAndSenderLifecycle|TestRunReconcileLogsReceiverWait|TestRunReconcileLogsDestinationWaitOnlyOnTransition|TestRunReconcile.*AlreadyExists' -count=1 -v
```

Expected: PASS.

### Task 4: Full Verification

**Files:**
- Verify all touched files.

- [ ] **Step 1: Format**

Run:

```bash
go fmt ./...
```

Expected: no error.

- [ ] **Step 2: Run package tests**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./...
```

Expected: PASS.

- [ ] **Step 3: Run lint**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build GOLANGCI_LINT_CACHE=/private/tmp/zfsreplicationcontroller-golangci-lint-cache golangci-lint run
```

Expected: PASS, including no `errcheck` issue for `outputTail.Write`.

- [ ] **Step 4: Run targeted race tests**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test -race ./internal/datamover ./internal/controller
```

Expected: PASS.

- [ ] **Step 5: E2E decision**

If time and environment allow, run the focused E2E subset with kept resources and inspect logs:

```bash
E2E_KEEP_CLUSTER=1 GOCACHE=/private/tmp/zfsreplicationcontroller-go-build ./test/e2e/run.sh
```

or against an existing cluster:

```bash
E2E_KEEP_RESOURCES=1 KUBECONFIG=test/e2e/.artifacts/kubeconfig GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./test/e2e -run 'TestE2EFullAndIncrementalReplication|TestE2ESyncoidFailure|TestE2EDestinationContentionWaits' -count=1 -v
```

Expected: tests pass, failure status/logs do not contain `/var/run/zfsrep/ssh/id_rsa`, and controller logs are not dominated by repeated transition messages.
