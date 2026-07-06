# Manager Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Initialize controller-runtime logging during manager startup so the fallback `log.SetLogger(...) was never called` warning is not emitted and manager logs remain visible in pod logs.

**Architecture:** Keep the change inside `cmd/manager/main.go`. Use controller-runtime's bundled zap logr adapter in production mode and install it before any controller-runtime config or manager setup can use the deferred global logger.

**Tech Stack:** Go, `sigs.k8s.io/controller-runtime`, `sigs.k8s.io/controller-runtime/pkg/log/zap`, Kubernetes manager startup.

---

## File Structure

- Modify `cmd/manager/main.go`: import the zap logger package and call `ctrl.SetLogger(zap.New())` immediately after `flag.Parse()`.
- No test file changes: avoid brittle unit tests around controller-runtime's global logger state.
- No manifest changes: this is a binary startup fix only.

### Task 1: Baseline Manager Package

**Files:**
- Read: `cmd/manager/main.go`
- Read: `cmd/manager/main_test.go`

- [ ] **Step 1: Run the current manager tests**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./cmd/manager
```

Expected: PASS. This confirms the existing manager package is healthy before the logging change.

### Task 2: Install Controller-Runtime Logger

**Files:**
- Modify: `cmd/manager/main.go`

- [ ] **Step 1: Add the zap logger import**

Update the import block in `cmd/manager/main.go` so it includes:

```go
	logzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
```

- [ ] **Step 2: Set the logger after parsing flags**

In `main()`, immediately after `flag.Parse()`, add:

```go
	ctrl.SetLogger(logzap.New())
```

The relevant startup sequence should become:

```go
	flag.StringVar(&watchNamespace, "watch-namespace", os.Getenv("WATCH_NAMESPACE"), "namespace to watch; empty watches all namespaces")
	flag.Parse()

	ctrl.SetLogger(logzap.New())

	scheme := runtime.NewScheme()
```

Expected: controller-runtime's deferred root logger is fulfilled before `ctrl.GetConfigOrDie()` and `ctrl.NewManager(...)` can use it.

### Task 3: Format And Verify

**Files:**
- Verify: `cmd/manager/main.go`

- [ ] **Step 1: Format Go code**

Run:

```sh
go fmt ./...
```

Expected: command exits 0.

- [ ] **Step 2: Run the manager package tests**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./cmd/manager
```

Expected: PASS.

- [ ] **Step 3: Run the full Go test suite**

Run:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./...
```

Expected: PASS.

- [ ] **Step 4: Inspect the diff**

Run:

```sh
git diff -- cmd/manager/main.go docs/superpowers/specs/2026-07-06-manager-logging-design.md docs/superpowers/plans/2026-07-06-manager-logging.md
```

Expected: diff contains only the logger import, `ctrl.SetLogger(logzap.New())`, and the two planning documents.
