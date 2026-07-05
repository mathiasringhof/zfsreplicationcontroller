# Supported Dependency Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the alpha `0.1.0` release baseline from unsupported Go/Kubernetes versions to a current supported build and E2E target.

**Architecture:** Keep the public Kubernetes API at `v1alpha1` for `0.1.0`; this plan only updates toolchain and dependency support. Add a first-party release baseline test that fails when Go, Kubernetes libraries, controller-runtime, Dockerfile, or E2E k3s defaults drift below the chosen release floor.

**Tech Stack:** Go modules, Go 1.26, Kubernetes client libraries `v0.35.x`, controller-runtime `v0.23.x`, k3s `v1.35.x+k3s1`, Dockerfile, Lima E2E harness.

---

## File Structure

- Modify: `go.mod` and `go.sum`
  - Own the Go language directive and Kubernetes/controller-runtime dependency versions.
- Modify: `Dockerfile`
  - Build binaries with a supported Go builder image.
- Modify: `test/e2e/env.sh`
  - Run the VM E2E cluster against a supported Kubernetes minor.
- Create: `internal/release/baseline_test.go`
  - Guard the release baseline with a cheap unit test.
- Modify as required by compiler errors: first-party Go files under `api/`, `cmd/`, `internal/`, and `test/e2e/`
  - Keep compatibility fixes local and mechanical; do not change CRD API shape in this plan.

## Target Baseline

- Go module directive: `go 1.26.0`
- Docker builder: `FROM docker.io/library/golang:1.26.4 AS build`
- Kubernetes libraries: `k8s.io/api`, `k8s.io/apimachinery`, and `k8s.io/client-go` at `v0.35.6`
- controller-runtime: `sigs.k8s.io/controller-runtime@v0.23.0`
- E2E k3s default: `v1.35.6+k3s1`

### Task 1: Add Release Baseline Guard

**Files:**
- Create: `internal/release/baseline_test.go`

- [ ] **Step 1: Write the failing test**

Create `internal/release/baseline_test.go`:

```go
package release_test

import (
	"os"
	"strings"
	"testing"
)

func TestSupportedDependencyBaseline(t *testing.T) {
	files := map[string]string{
		"go.mod":          readFile(t, "../../go.mod"),
		"Dockerfile":      readFile(t, "../../Dockerfile"),
		"test/e2e/env.sh": readFile(t, "../../test/e2e/env.sh"),
	}

	requireContains(t, "go.mod", files["go.mod"], "go 1.26.0")
	requireContains(t, "go.mod", files["go.mod"], "k8s.io/api v0.35.6")
	requireContains(t, "go.mod", files["go.mod"], "k8s.io/apimachinery v0.35.6")
	requireContains(t, "go.mod", files["go.mod"], "k8s.io/client-go v0.35.6")
	requireContains(t, "go.mod", files["go.mod"], "sigs.k8s.io/controller-runtime v0.23.0")
	requireContains(t, "Dockerfile", files["Dockerfile"], "FROM docker.io/library/golang:1.26.4 AS build")
	requireContains(t, "test/e2e/env.sh", files["test/e2e/env.sh"], `K3S_VERSION="${E2E_K3S_VERSION:-v1.35.6+k3s1}"`)
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func requireContains(t *testing.T, name, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Fatalf("%s does not contain %q", name, needle)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```sh
go test ./internal/release -run TestSupportedDependencyBaseline -count=1
```

Expected: FAIL because `go.mod` still says `go 1.22.0`, Kubernetes deps are still `v0.31.x`, the Dockerfile still uses `golang:1.22`, and E2E defaults to `v1.31.1+k3s1`.

- [ ] **Step 3: Commit the failing guard**

```sh
git add internal/release/baseline_test.go
git commit -m "test: guard release dependency baseline"
```

### Task 2: Update Go And Kubernetes Dependencies

**Files:**
- Modify: `go.mod`
- Modify: `go.sum`

- [ ] **Step 1: Update module directives and primary deps**

Run:

```sh
go mod edit -go=1.26.0
go get k8s.io/api@v0.35.6 k8s.io/apimachinery@v0.35.6 k8s.io/client-go@v0.35.6 sigs.k8s.io/controller-runtime@v0.23.0 golang.org/x/crypto@latest
go mod tidy
```

Expected: `go.mod` keeps the same module path, changes the Go directive to `go 1.26.0`, updates the Kubernetes libraries to `v0.35.6`, updates controller-runtime to `v0.23.0`, and refreshes indirect dependencies.

- [ ] **Step 2: Run package compilation**

Run:

```sh
go test ./... -run '^$' -count=1
```

Expected: Either PASS, or compile errors caused by upstream API drift. If compile errors appear, fix only the first-party call sites reported by the compiler.

- [ ] **Step 3: Commit dependency update once compilation succeeds**

```sh
git add go.mod go.sum
git commit -m "chore: update supported Go and Kubernetes dependencies"
```

### Task 3: Update Docker Builder Baseline

**Files:**
- Modify: `Dockerfile:1`
- Test: `internal/release/baseline_test.go`
- Test: `internal/datamover/runtime_image_test.go`

- [ ] **Step 1: Change the builder image**

Change the first line of `Dockerfile` to:

```Dockerfile
FROM docker.io/library/golang:1.26.4 AS build
```

Leave the runtime stage on `docker.io/library/ubuntu:24.04` for this plan.

- [ ] **Step 2: Run Dockerfile guard tests**

Run:

```sh
go test ./internal/release ./internal/datamover -run 'TestSupportedDependencyBaseline|TestRuntimeImagePinsSyncoid230' -count=1
```

Expected: PASS.

- [ ] **Step 3: Commit Dockerfile baseline**

```sh
git add Dockerfile internal/release/baseline_test.go
git commit -m "chore: build release image with supported Go"
```

### Task 4: Update E2E Kubernetes Baseline

**Files:**
- Modify: `test/e2e/env.sh:13`
- Test: `internal/release/baseline_test.go`

- [ ] **Step 1: Change the default k3s version**

Change the `K3S_VERSION` line in `test/e2e/env.sh` to:

```sh
K3S_VERSION="${E2E_K3S_VERSION:-v1.35.6+k3s1}"
```

- [ ] **Step 2: Run the baseline test**

Run:

```sh
go test ./internal/release -run TestSupportedDependencyBaseline -count=1
```

Expected: PASS.

- [ ] **Step 3: Run the E2E doctor**

Run:

```sh
./test/e2e/doctor.sh
```

Expected: PASS with `e2e VM environment prerequisites look good`.

- [ ] **Step 4: Commit E2E baseline**

```sh
git add test/e2e/env.sh internal/release/baseline_test.go
git commit -m "chore: test e2e against supported Kubernetes"
```

### Task 5: Fix Compatibility Breaks With TDD

**Files:**
- Modify: first-party Go files reported by failing tests
- Test: nearest existing package test

- [ ] **Step 1: Run all tests and capture the first failure**

Run:

```sh
go test ./... -count=1
```

Expected: PASS if upstream APIs are compatible. If this fails, pick the first failing package and continue in that package only.

- [ ] **Step 2: Add or update the narrowest regression test**

If the failure is in `cmd/manager`, add the failing behavior to `cmd/manager/main_test.go`.

If the failure is in `internal/controller`, add the failing behavior to the existing focused test file for that behavior, such as `internal/controller/zfsreplication_run_controller_test.go`, `internal/controller/zfsreplication_schedule_controller_test.go`, or `internal/controller/rbac_manifest_test.go`.

If the failure is in `test/e2e`, add the failing behavior to `test/e2e/e2e_test.go` only when the behavior is observable through the cluster.

Run the affected package test, for example:

```sh
go test ./internal/controller -run TestNameThatDocumentsTheFailure -count=1
```

Expected: FAIL for the same reason as the compile or behavior break.

- [ ] **Step 3: Apply the smallest compatibility fix**

Make the smallest source change needed to satisfy the failing test. Keep API types in `api/v1alpha1` unchanged unless the compiler requires generated deepcopy compatibility changes.

- [ ] **Step 4: Run the affected package**

Run:

```sh
go test ./internal/controller -count=1
```

Expected: PASS for the affected package. Use the actual package from Step 2 if it was not `internal/controller`.

- [ ] **Step 5: Commit each compatibility fix separately**

```sh
git add api cmd internal test go.mod go.sum
git commit -m "fix: restore compatibility with updated dependencies"
```

### Task 6: Full Verification

**Files:**
- No planned source changes

- [ ] **Step 1: Format**

Run:

```sh
go fmt ./...
git diff --exit-code
```

Expected: both commands exit 0.

- [ ] **Step 2: Unit and integration tests**

Run:

```sh
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 3: Lint**

Run:

```sh
golangci-lint run
```

Expected: `0 issues.`

- [ ] **Step 4: Race tests**

Run:

```sh
go test -race ./... -count=1
```

Expected: PASS.

- [ ] **Step 5: Full E2E**

Run:

```sh
./test/e2e/run.sh
```

Expected: PASS, including `ok github.com/mathias/zfsreplicationcontroller/test/e2e`.

- [ ] **Step 6: Commit final verification notes if a release checklist file exists**

If `docs/release.md` exists after the release artifact plan is implemented, add the supported baseline to it:

```markdown
## Supported Baseline For 0.1.x

- Go: 1.26.x
- Kubernetes libraries: 1.35.x / `v0.35.x`
- E2E Kubernetes: k3s 1.35.x
- Runtime base image: Ubuntu 24.04
- Public API: `zfsreplication.ringhof.io/v1alpha1`
```

Then commit:

```sh
git add docs/release.md
git commit -m "docs: record supported release baseline"
```

## Self-Review

- Spec coverage: Covers item 2 by updating Go, Kubernetes deps, controller-runtime, Dockerfile builder, and E2E Kubernetes default.
- Placeholder scan: No placeholders remain.
- Type consistency: The release guard test references only files and strings defined in this plan.
