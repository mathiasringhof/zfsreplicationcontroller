# Sender Job Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make sender job and controller logs describe each replication run clearly enough to debug successful and failed `syncoid` executions from normal Kubernetes logs.

**Architecture:** Keep `RunSender` as the production entry point and add a package-private `runSender` helper that accepts a log writer for tests. Sender logs are line-oriented stderr output from the standard library only. Controller lifecycle logs use controller-runtime's context logger via `log.FromContext(ctx)`.

**Tech Stack:** Go standard library, controller-runtime/log, existing Kubernetes controller tests, existing fake command runner tests.

---

## File Structure

- Modify `internal/datamover/sender.go`: split sender execution into a testable helper, log safe run metadata, sanitize command arguments, preserve stdout/stderr in logs and failure errors, and derive best-effort snapshot/GUID evidence from child output.
- Modify `internal/datamover/datamover_test.go`: add focused tests for successful logging, failure logging, redaction, failure summary, and snapshot/GUID extraction.
- Modify `internal/controller/zfsreplication_run_controller.go`: add controller-runtime lifecycle logs at run acceptance, receiver wait/ready, sender job creation/existence, success/failure, and cleanup.
- Modify `internal/controller/zfsreplication_run_controller_test.go`: add a capture logger and tests that verify key lifecycle log messages.
- Verify `cmd/zfsrep-sender/main.go`: no production call-site change should be needed because `RunSender` keeps its signature.

## Task 1: Baseline Focused Tests

**Files:**
- Read: `internal/datamover/sender.go`
- Read: `internal/datamover/datamover_test.go`
- Read: `internal/controller/zfsreplication_run_controller.go`
- Read: `internal/controller/zfsreplication_run_controller_test.go`

- [ ] **Step 1: Run focused baseline tests**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover ./internal/controller ./cmd/zfsrep-sender
```

Expected: PASS. If this fails, stop and fix the baseline failure before changing logging behavior.

## Task 2: Sender Success Logging

**Files:**
- Modify: `internal/datamover/datamover_test.go`
- Modify: `internal/datamover/sender.go`

- [ ] **Step 1: Write the failing sender success log test**

Add this test near the existing sender tests in `internal/datamover/datamover_test.go`:

```go
func TestSenderLogsSuccessfulSyncoidRun(t *testing.T) {
	runner := &fakeRunner{
		stdout: "INFO: Sending oldest full snapshot tank/src@syncoid_zrc-123_2026-07-06:12:00:00-GUID-123456\n",
		stderr: "syncoid warning that should remain visible\n",
	}
	var logs strings.Builder

	err := runSender(context.Background(), SenderConfig{
		SrcDataset:        "tank/src",
		DstHost:           "root@10.0.0.42",
		DstDataset:        "tank/dst",
		SSHKeyFile:        "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:    "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:           "2222",
		NoRollback:        true,
		Compress:          "none",
		SyncoidIdentifier: "zrc-123",
		ReceiveUnmounted:  true,
		ReceiveResumable:  true,
	}, runner, &logs)
	if err != nil {
		t.Fatal(err)
	}

	out := logs.String()
	for _, want := range []string{
		"sender starting",
		"srcDataset=tank/src",
		"dstDataset=tank/dst",
		"dstHost=root@10.0.0.42",
		"syncoidIdentifier=zrc-123",
		"syncoid command",
		"--sshkey=<redacted>",
		"syncoid stdout",
		"INFO: Sending oldest full snapshot",
		"syncoid stderr",
		"syncoid warning that should remain visible",
		"sender completed",
		"result=success",
		"exitCode=0",
		"duration=",
		"finalSnapshot=tank/src@syncoid_zrc-123_2026-07-06:12:00:00-GUID-123456",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("logs missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "--sshkey=/var/run/zfsrep/ssh/id_rsa") {
		t.Fatalf("logs contain unredacted ssh key path:\n%s", out)
	}
}
```

Update `fakeRunner` at the top of `internal/datamover/datamover_test.go`:

```go
type fakeRunner struct {
	calls  []call
	stdout string
	stderr string
	err    error
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) (string, string, error) {
	f.calls = append(f.calls, call{name: name, args: args})
	return f.stdout, f.stderr, f.err
}
```

- [ ] **Step 2: Run the new test and confirm it fails**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover -run TestSenderLogsSuccessfulSyncoidRun -count=1 -v
```

Expected: FAIL with `undefined: runSender`.

- [ ] **Step 3: Implement minimal sender logging helper**

In `internal/datamover/sender.go`, add imports:

```go
	"io"
	"time"
```

Change `RunSender` to call a helper:

```go
func RunSender(ctx context.Context, cfg SenderConfig, r CommandRunner) error {
	return runSender(ctx, cfg, r, os.Stderr)
}
```

Move the existing function body into:

```go
func runSender(ctx context.Context, cfg SenderConfig, r CommandRunner, logw io.Writer) error {
	started := time.Now()
	if err := validateNode(cfg.ExpectedNode, cfg.ActualNode); err != nil {
		return err
	}
	compress, err := replication.SyncoidCompression(cfg.Compress)
	if err != nil {
		return err
	}
	if cfg.SyncoidIdentifier != "" && !replication.ValidSyncoidIdentifier(cfg.SyncoidIdentifier) {
		return fmt.Errorf("unsupported syncoid identifier %q", cfg.SyncoidIdentifier)
	}
	if cfg.DstHost != "" && cfg.KnownHostsFile == "" {
		return fmt.Errorf("known hosts file is required for SSH replication")
	}
	args := syncoidArgs(cfg, compress)
	logSenderStart(logw, cfg)
	logSenderLine(logw, "syncoid command command=%s", strings.Join(sanitizeSyncoidArgs(args), " "))
	stdout, stderr, err := r.Run(ctx, "syncoid", args...)
	logCommandOutput(logw, "stdout", stdout)
	logCommandOutput(logw, "stderr", stderr)
	duration := time.Since(started).Round(time.Millisecond)
	if err != nil {
		summary := clean(stderr, err)
		logSenderLine(logw, "sender completed result=failure exitCode=-1 duration=%s error=%q", duration, summary)
		return fmt.Errorf("syncoid failed: %s", summary)
	}
	logSenderLine(logw, "sender completed result=success exitCode=0 duration=%s%s", duration, finalSnapshotLogSuffix(stdout+"\n"+stderr))
	return nil
}
```

Extract the existing argument-building block into:

```go
func syncoidArgs(cfg SenderConfig, compress string) []string {
	var args []string
	if cfg.NoSyncSnap {
		args = append(args, "--no-sync-snap")
	}
	if cfg.NoRollback {
		args = append(args, "--no-rollback")
	}
	args = append(args, "--no-privilege-elevation")
	if compress != "" {
		args = append(args, "--compress="+compress)
	}
	if cfg.SyncoidIdentifier != "" {
		args = append(args, "--identifier="+cfg.SyncoidIdentifier)
	}
	if cfg.KnownHostsFile != "" {
		args = append(args,
			"--sshoption=UserKnownHostsFile="+cfg.KnownHostsFile,
			"--sshoption=StrictHostKeyChecking=yes",
			"--sshoption=IdentitiesOnly=yes",
		)
	}
	if cfg.SSHKeyFile != "" {
		args = append(args, "--sshkey="+cfg.SSHKeyFile)
	}
	if cfg.SSHPort != "" {
		args = append(args, "--sshport="+cfg.SSHPort)
	}
	if cfg.ReceiveUnmounted {
		args = append(args, "--recvoptions=u")
	}
	if !cfg.ReceiveResumable {
		args = append(args, "--no-resume")
	}
	for _, include := range cfg.IncludeSnaps {
		args = append(args, "--include-snaps="+include)
	}
	for _, exclude := range cfg.ExcludeSnaps {
		args = append(args, "--exclude-snaps="+exclude)
	}
	if cfg.ForceDelete {
		args = append(args, "--force-delete")
	}
	return append(args, cfg.SrcDataset, syncoidTarget(cfg.DstHost, cfg.DstDataset))
}
```

Add helpers:

```go
func logSenderStart(w io.Writer, cfg SenderConfig) {
	logSenderLine(w, "sender starting srcDataset=%s dstDataset=%s dstHost=%s sshPort=%s syncoidIdentifier=%s noSyncSnap=%t noRollback=%t forceDelete=%t compress=%s receiveUnmounted=%t receiveResumable=%t includeSnaps=%q excludeSnaps=%q",
		cfg.SrcDataset, cfg.DstDataset, cfg.DstHost, cfg.SSHPort, cfg.SyncoidIdentifier, cfg.NoSyncSnap, cfg.NoRollback, cfg.ForceDelete, cfg.Compress, cfg.ReceiveUnmounted, cfg.ReceiveResumable, strings.Join(cfg.IncludeSnaps, ","), strings.Join(cfg.ExcludeSnaps, ","))
}

func logSenderLine(w io.Writer, format string, args ...any) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, format+"\n", args...)
}

func logCommandOutput(w io.Writer, stream, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			logSenderLine(w, "syncoid %s %s", stream, line)
		}
	}
}

func sanitizeSyncoidArgs(args []string) []string {
	out := append([]string(nil), args...)
	for i, arg := range out {
		if strings.HasPrefix(arg, "--sshkey=") {
			out[i] = "--sshkey=<redacted>"
		}
	}
	return out
}
```

Add a first-pass best-effort extractor:

```go
func finalSnapshotLogSuffix(output string) string {
	if snapshot := lastSnapshotToken(output); snapshot != "" {
		return " finalSnapshot=" + snapshot
	}
	return ""
}

func lastSnapshotToken(output string) string {
	var last string
	for _, field := range strings.Fields(output) {
		field = strings.Trim(field, `"'.,;()[]{}<>`)
		if _, _, ok := replication.SplitSnapshotTarget(field); ok {
			last = field
		}
	}
	return last
}
```

Leave `commandExitCode` and `syncoidFailureSummary` for Task 3.

- [ ] **Step 4: Run the success logging test**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover -run TestSenderLogsSuccessfulSyncoidRun -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Run existing sender argument tests**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover -run 'TestSenderRunsSyncoidWithConfiguredSnapshotOptions|TestSenderPassesForceDelete|TestSenderRejectsUnsafeSyncoidIdentifier|TestSenderRejectsUnknownCompression|TestSenderNormalizesCompressionAliasesForSyncoid' -count=1 -v
```

Expected: PASS.

## Task 3: Sender Failure Logging And Status Summary

**Files:**
- Modify: `internal/datamover/datamover_test.go`
- Modify: `internal/datamover/sender.go`

- [ ] **Step 1: Write failing failure logging test**

Add this test:

```go
type fakeExitError struct {
	code int
	msg  string
}

func (e fakeExitError) Error() string {
	return e.msg
}

func (e fakeExitError) ExitCode() int {
	return e.code
}

func TestSenderLogsFailedSyncoidRunAndReturnsCombinedOutput(t *testing.T) {
	runner := &fakeRunner{
		stdout: "syncoid stdout detail\n",
		stderr: "syncoid stderr detail\n",
		err:    fakeExitError{code: 23, msg: "exit status 23"},
	}
	var logs strings.Builder

	err := runSender(context.Background(), SenderConfig{
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:   "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:          "2222",
		NoRollback:       true,
		Compress:         "none",
		ReceiveUnmounted: true,
		ReceiveResumable: true,
	}, runner, &logs)
	if err == nil {
		t.Fatal("runSender() error = nil, want syncoid failure")
	}
	for _, want := range []string{"syncoid stdout detail", "syncoid stderr detail", "exit status 23"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error missing %q: %v", want, err)
		}
	}
	out := logs.String()
	for _, want := range []string{
		"sender completed",
		"result=failure",
		"exitCode=23",
		"syncoid stdout syncoid stdout detail",
		"syncoid stderr syncoid stderr detail",
		`error="stdout: syncoid stdout detail; stderr: syncoid stderr detail; error: exit status 23"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("logs missing %q:\n%s", want, out)
		}
	}
	if last := failureMessageFromSenderLogs(out); last != "sender completed result=failure exitCode=23 duration=0s error=\"stdout: syncoid stdout detail; stderr: syncoid stderr detail; error: exit status 23\"" {
		t.Fatalf("last failure line = %q", last)
	}
}

func failureMessageFromSenderLogs(logs string) string {
	var last string
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			last = line
		}
	}
	last = strings.Replace(last, regexp.MustCompile(`duration=[^ ]+`).FindString(last), "duration=0s", 1)
	return last
}
```

Add `regexp` to the test imports.

- [ ] **Step 2: Run the failure test and confirm it fails**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover -run TestSenderLogsFailedSyncoidRunAndReturnsCombinedOutput -count=1 -v
```

Expected: FAIL because `syncoidFailureSummary` and `commandExitCode` are not defined or the error does not yet contain combined output.

- [ ] **Step 3: Implement failure summary helpers**

Add to `internal/datamover/sender.go`:

```go
type exitCoder interface {
	ExitCode() int
}

func commandExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr exitCoder
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func syncoidFailureSummary(stdout, stderr string, err error) string {
	var parts []string
	if stdout = strings.TrimSpace(stdout); stdout != "" {
		parts = append(parts, "stdout: "+singleLine(stdout))
	}
	if stderr = strings.TrimSpace(stderr); stderr != "" {
		parts = append(parts, "stderr: "+singleLine(stderr))
	}
	if err != nil {
		parts = append(parts, "error: "+err.Error())
	}
	if len(parts) == 0 {
		return "syncoid exited with an unknown error"
	}
	return strings.Join(parts, "; ")
}

func singleLine(value string) string {
	return strings.Join(strings.Fields(value), " ")
}
```

Add `errors` to the imports in `internal/datamover/sender.go`.

Then replace the failure branch in `runSender`:

```go
if err != nil {
	summary := syncoidFailureSummary(stdout, stderr, err)
	logSenderLine(logw, "sender completed result=failure exitCode=%d duration=%s error=%q", commandExitCode(err), duration, summary)
	return fmt.Errorf("syncoid failed: %s", summary)
}
```

- [ ] **Step 4: Run the failure logging test**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover -run TestSenderLogsFailedSyncoidRunAndReturnsCombinedOutput -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Run all datamover tests**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover -count=1 -v
```

Expected: PASS.

## Task 4: Controller Lifecycle Logs

**Files:**
- Modify: `internal/controller/zfsreplication_run_controller_test.go`
- Modify: `internal/controller/zfsreplication_run_controller.go`

- [ ] **Step 1: Add a capture logger helper and failing controller log tests**

Add imports to `internal/controller/zfsreplication_run_controller_test.go`:

```go
	"fmt"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
```

Add this helper near other test helpers:

```go
func captureLogContext() (context.Context, *strings.Builder) {
	var logs strings.Builder
	logger := funcr.New(func(prefix, args string) {
		fmt.Fprintf(&logs, "%s %s\n", prefix, args)
	}, funcr.Options{})
	return logr.NewContext(context.Background(), logger), &logs
}
```

Add this test:

```go
func TestRunReconcileLogsReceiverAndSenderLifecycle(t *testing.T) {
	run := replicationRun("manual-1")
	names := objectNamesForRun(run.Name)
	r := newRunReconciler(t, run, readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey))
	ctx, logs := captureLogContext()

	if _, err := r.Reconcile(ctx, request("manual-1")); err != nil {
		t.Fatal(err)
	}

	out := logs.String()
	for _, want := range []string{
		"reconciling replication run",
		"replication receiver is ready",
		"created sender job",
		"run=manual-1",
		"namespace=default",
		"sourceDataset=tank/src",
		"targetDataset=tank/dst",
		"senderJob=zfsrep-manual-1-sender",
		"receiveTask=zfsrep-manual-1-receiver",
		"receiverPod=zfsrep-manual-1-receiver",
		"receiverPodIP=10.0.0.42",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("logs missing %q:\n%s", want, out)
		}
	}
}
```

Add this test for sender success:

```go
func TestRunReconcileLogsSenderSuccess(t *testing.T) {
	run := replicationRun("manual-success")
	names := objectNamesForRun(run.Name)
	receiveTask := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	job := runSenderJob(run, names, "datamover:test", "10.0.0.42")
	job.Status.Succeeded = 1
	r := newRunReconciler(t, run, receiveTask, job, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: names.SecretName, Namespace: run.Namespace}})
	ctx, logs := captureLogContext()

	if _, err := r.Reconcile(ctx, request(run.Name)); err != nil {
		t.Fatal(err)
	}

	out := logs.String()
	for _, want := range []string{"sender job succeeded", "run=manual-success", "receiverPod=zfsrep-manual-success-receiver", "receiverPodIP=10.0.0.42"} {
		if !strings.Contains(out, want) {
			t.Fatalf("logs missing %q:\n%s", want, out)
		}
	}
}
```

Add this test for terminal cleanup:

```go
func TestRunReconcileLogsTerminalCleanup(t *testing.T) {
	run := replicationRun("manual-terminal")
	run.Status.Phase = zfsv1.PhaseSucceeded
	names := objectNamesForRun(run.Name)
	receiveTask := readyReceiveTask(run, names, "10.0.0.42", testReceiverHostKey)
	r := newRunReconciler(t, run, receiveTask, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: names.SecretName, Namespace: run.Namespace}})
	ctx, logs := captureLogContext()

	if _, err := r.Reconcile(ctx, request(run.Name)); err != nil {
		t.Fatal(err)
	}

	out := logs.String()
	for _, want := range []string{"cleaning up terminal replication run", "phase=Succeeded", "run=manual-terminal"} {
		if !strings.Contains(out, want) {
			t.Fatalf("logs missing %q:\n%s", want, out)
		}
	}
}
```

- [ ] **Step 2: Run the controller log tests and confirm they fail**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run 'TestRunReconcileLogsReceiverAndSenderLifecycle|TestRunReconcileLogsSenderSuccess|TestRunReconcileLogsTerminalCleanup' -count=1 -v
```

Expected: FAIL because the run reconciler does not emit these lifecycle messages yet.

- [ ] **Step 3: Add controller lifecycle logging**

Add an import to `internal/controller/zfsreplication_run_controller.go`:

```go
	"sigs.k8s.io/controller-runtime/pkg/log"
```

At the start of `Reconcile`, after calculating `names`, add:

```go
	logger := log.FromContext(ctx).WithValues(runLogValues(&run, names)...)
	ctx = log.IntoContext(ctx, logger)
	logger.Info("reconciling replication run")
```

Add this helper near `objectNamesForRun`:

```go
func runLogValues(run *zfsv1.ZFSReplicationRun, names runObjects) []any {
	return []any{
		"namespace", run.Namespace,
		"run", run.Name,
		"sourceNode", run.Spec.Source.NodeName,
		"sourceDataset", run.Spec.Source.Dataset,
		"targetNode", run.Spec.Target.NodeName,
		"targetDataset", run.Spec.Target.Dataset,
		"senderJob", names.SenderName,
		"receiveTask", names.ReceiveTaskName,
		"sshSecret", names.SecretName,
		"syncoidIdentifier", syncSnapshotIdentifierForRun(run),
	}
}
```

Add targeted `log.FromContext(ctx).Info(...)` calls:

```go
log.FromContext(ctx).Info("cleaning up terminal replication run", "phase", run.Status.Phase)
log.FromContext(ctx).Info("waiting for replication receiver")
log.FromContext(ctx).Info("replication receiver is ready", "receiverPod", receiver.podName, "receiverPodIP", receiver.podIP)
log.FromContext(ctx).Info("created sender job", "receiverPodIP", receiver.podIP)
log.FromContext(ctx).Info("sender job succeeded", "receiverPod", receiver.podName, "receiverPodIP", receiver.podIP)
log.FromContext(ctx).Info("sender job failed", "error", msg)
```

Place them at these points:

- terminal branch before `reconcileTerminalRun`
- `ensureSenderStarted` when `!ready`
- `ensureSenderStarted` after the receiver struct is built
- `ensureSenderStarted` after `ensureRunSenderJob` succeeds
- `finishFromSenderJob` before marking success
- `finishFromSenderJob` before `failRunObject`

- [ ] **Step 4: Run the controller log tests**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run 'TestRunReconcileLogsReceiverAndSenderLifecycle|TestRunReconcileLogsSenderSuccess|TestRunReconcileLogsTerminalCleanup' -count=1 -v
```

Expected: PASS.

- [ ] **Step 5: Run all controller tests**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -count=1 -v
```

Expected: PASS.

## Task 5: Format And Verify

**Files:**
- Verify: `internal/datamover/sender.go`
- Verify: `internal/datamover/datamover_test.go`
- Verify: `internal/controller/zfsreplication_run_controller.go`
- Verify: `internal/controller/zfsreplication_run_controller_test.go`
- Verify: `docs/superpowers/specs/2026-07-06-sender-job-logging-design.md`
- Verify: `docs/superpowers/plans/2026-07-06-sender-job-logging.md`

- [ ] **Step 1: Format Go files**

Run:

```sh
go fmt ./...
```

Expected: command exits 0.

- [ ] **Step 2: Run focused tests**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover ./internal/controller ./cmd/zfsrep-sender
```

Expected: PASS.

- [ ] **Step 3: Run the full Go test suite**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./...
```

Expected: PASS.

- [ ] **Step 4: Run lint if available**

Run:

```sh
golangci-lint run
```

Expected: PASS. If the command is not installed in the local environment, record that it could not be run.

- [ ] **Step 5: Inspect the diff**

Run:

```sh
git diff -- internal/datamover/sender.go internal/datamover/datamover_test.go internal/controller/zfsreplication_run_controller.go internal/controller/zfsreplication_run_controller_test.go docs/superpowers/specs/2026-07-06-sender-job-logging-design.md docs/superpowers/plans/2026-07-06-sender-job-logging.md
```

Expected: diff is limited to sender logging, controller lifecycle logs, tests, and the two planning documents.
