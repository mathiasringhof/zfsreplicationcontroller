# CI Release Gates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make CI enforce the release checks we currently trust locally for an alpha `0.1.0` release.

**Architecture:** Keep a fast PR workflow for formatting, linting, unit tests, and race tests. Add a separate full E2E workflow for release candidates on a self-hosted runner because the VM/ZFS harness creates Lima VMs and needs capabilities that are not reliable on generic hosted runners.

**Tech Stack:** GitHub Actions, Go, golangci-lint v2, Lima/k3s/ZFS E2E harness, YAML workflow tests.

---

## File Structure

- Modify: `.github/workflows/test.yaml`
  - Enforce format, lint, unit tests, and race tests on PRs, `main`, and version tags.
- Create: `.github/workflows/e2e.yaml`
  - Run the full VM E2E harness manually and on version tags using a self-hosted release runner.
- Create: `internal/release/ci_workflow_test.go`
  - Parse workflow YAML and guard required release-gate commands.
- Modify: `README.md`
  - Document that 0.1.x release tags require both the regular CI workflow and the E2E workflow.

### Task 1: Add Workflow Guard Tests

**Files:**
- Create: `internal/release/ci_workflow_test.go`

- [ ] **Step 1: Write the failing workflow tests**

Create `internal/release/ci_workflow_test.go`:

```go
package release_test

import (
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type workflow struct {
	Name string `yaml:"name"`
	On   any    `yaml:"on"`
	Jobs map[string]struct {
		RunsOn any `yaml:"runs-on"`
		Steps  []struct {
			Name string `yaml:"name"`
			Run  string `yaml:"run"`
			Uses string `yaml:"uses"`
		} `yaml:"steps"`
	} `yaml:"jobs"`
}

func TestFastCIWorkflowHasReleaseGates(t *testing.T) {
	var wf workflow
	readWorkflow(t, "../../.github/workflows/test.yaml", &wf)

	job, ok := wf.Jobs["go-test"]
	if !ok {
		t.Fatalf("test workflow jobs = %#v, missing go-test", wf.Jobs)
	}
	combined := workflowCommands(job.Steps)
	for _, want := range []string{
		"go fmt ./...",
		"git diff --exit-code",
		"go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2",
		"golangci-lint run",
		"go test ./... -count=1",
		"go test -race ./... -count=1",
	} {
		if !strings.Contains(combined, want) {
			t.Fatalf("test workflow commands do not include %q; commands:\n%s", want, combined)
		}
	}
}

func TestE2EWorkflowRunsFullHarnessForReleaseTags(t *testing.T) {
	var wf workflow
	readWorkflow(t, "../../.github/workflows/e2e.yaml", &wf)

	job, ok := wf.Jobs["vm-e2e"]
	if !ok {
		t.Fatalf("e2e workflow jobs = %#v, missing vm-e2e", wf.Jobs)
	}
	runsOn, ok := job.RunsOn.([]any)
	if !ok {
		t.Fatalf("e2e workflow runs-on = %#v, want self-hosted label list", job.RunsOn)
	}
	for _, want := range []string{"self-hosted", "zfsreplication-e2e"} {
		if !containsAny(runsOn, want) {
			t.Fatalf("e2e workflow runs-on = %#v, missing %q", runsOn, want)
		}
	}
	combined := workflowCommands(job.Steps)
	for _, want := range []string{
		"./test/e2e/doctor.sh",
		"./test/e2e/run.sh",
	} {
		if !strings.Contains(combined, want) {
			t.Fatalf("e2e workflow commands do not include %q; commands:\n%s", want, combined)
		}
	}
}

func readWorkflow(t *testing.T, path string, out *workflow) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := yaml.Unmarshal(data, out); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
}

func workflowCommands(steps []struct {
	Name string `yaml:"name"`
	Run  string `yaml:"run"`
	Uses string `yaml:"uses"`
}) string {
	var b strings.Builder
	for _, step := range steps {
		b.WriteString(step.Name)
		b.WriteByte('\n')
		b.WriteString(step.Run)
		b.WriteByte('\n')
		b.WriteString(step.Uses)
		b.WriteByte('\n')
	}
	return b.String()
}

func containsAny(values []any, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:

```sh
go test ./internal/release -run 'TestFastCIWorkflowHasReleaseGates|TestE2EWorkflowRunsFullHarnessForReleaseTags' -count=1
```

Expected: FAIL because `.github/workflows/test.yaml` lacks format/lint/race steps and `.github/workflows/e2e.yaml` does not exist.

- [ ] **Step 3: Commit the failing workflow guard**

```sh
git add internal/release/ci_workflow_test.go
git commit -m "test: guard release CI gates"
```

### Task 2: Extend Fast CI Workflow

**Files:**
- Modify: `.github/workflows/test.yaml`

- [ ] **Step 1: Replace the workflow contents**

Replace `.github/workflows/test.yaml` with:

```yaml
name: Test

on:
  pull_request:
    branches:
      - main
  push:
    branches:
      - main
    tags:
      - "v*"

permissions:
  contents: read

jobs:
  go-test:
    name: Go checks
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
          cache: true

      - name: Format
        run: |
          go fmt ./...
          git diff --exit-code

      - name: Install golangci-lint
        run: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2

      - name: Lint
        run: golangci-lint run

      - name: Test
        run: go test ./... -count=1

      - name: Race test
        run: go test -race ./... -count=1
```

- [ ] **Step 2: Run workflow guard test**

Run:

```sh
go test ./internal/release -run TestFastCIWorkflowHasReleaseGates -count=1
```

Expected: PASS for the fast CI workflow guard.

- [ ] **Step 3: Commit the fast CI workflow**

```sh
git add .github/workflows/test.yaml internal/release/ci_workflow_test.go
git commit -m "ci: gate format lint test and race checks"
```

### Task 3: Add Release E2E Workflow

**Files:**
- Create: `.github/workflows/e2e.yaml`

- [ ] **Step 1: Create the E2E workflow**

Create `.github/workflows/e2e.yaml`:

```yaml
name: E2E

on:
  workflow_dispatch:
  push:
    tags:
      - "v*"

permissions:
  contents: read

jobs:
  vm-e2e:
    name: VM k3s ZFS E2E
    runs-on:
      - self-hosted
      - zfsreplication-e2e
    timeout-minutes: 60

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
          cache: true

      - name: Doctor
        run: ./test/e2e/doctor.sh

      - name: Run full E2E
        run: ./test/e2e/run.sh
```

- [ ] **Step 2: Run the E2E workflow guard**

Run:

```sh
go test ./internal/release -run TestE2EWorkflowRunsFullHarnessForReleaseTags -count=1
```

Expected: PASS.

- [ ] **Step 3: Commit the E2E workflow**

```sh
git add .github/workflows/e2e.yaml internal/release/ci_workflow_test.go
git commit -m "ci: add release e2e workflow"
```

### Task 4: Document Release Gate Expectations

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add release gate text**

In the `Development` section of `README.md`, after the command block, add:

```markdown
Release tags require both CI workflows:

- `Test`: format, lint, unit/integration tests, and race tests.
- `E2E`: full Lima/k3s real-ZFS E2E on a self-hosted runner labelled
  `zfsreplication-e2e`.

For an alpha `0.1.x` release, the Kubernetes API remains
`zfsreplication.ringhof.io/v1alpha1`; compatibility-breaking API changes may
still happen before a stable `1.0.0`.
```

- [ ] **Step 2: Run README-free checks**

Run:

```sh
go test ./internal/release -count=1
```

Expected: PASS.

- [ ] **Step 3: Commit documentation**

```sh
git add README.md
git commit -m "docs: document release CI gates"
```

### Task 5: Full Verification

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

- [ ] **Step 5: Trigger release E2E workflow**

Run this from a branch with the workflow committed:

```sh
gh workflow run E2E
```

Expected: workflow starts on the self-hosted `zfsreplication-e2e` runner and completes with the same E2E suite passing as local `./test/e2e/run.sh`.

## Self-Review

- Spec coverage: Covers item 3 by adding enforced format, lint, unit, race, and full E2E release gates.
- Placeholder scan: No placeholders remain.
- Type consistency: Workflow test structs match the YAML fields used by both workflows.
