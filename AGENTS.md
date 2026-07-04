# AGENTS.md

## Workflow

- First run the relevant tests to understand the current state.
- Use red/green TDD for behavior changes:
  1. Write or update a failing test.
  2. Run it and confirm it fails for the expected reason.
  3. Implement the smallest change that makes it pass.
  4. Run the relevant tests again.
- Prefer small, focused changes over broad rewrites.
- Do not claim success unless you ran the checks (test/lint), or state exactly why they could not be run.
- Treat lint failures like test failures: fix the root cause, then rerun the failing command.

## Commands

- Format: `go fmt ./...`
- Test: `go test ./...`
- Check: `golangci-lint run`
- Tidy modules after import or dependency changes: `go mod tidy`
- E2E: `./test/e2e/run.sh`
- E2E against an already-running test cluster: `GOCACHE=/private/tmp/zfsreplicationcontroller-go-build KUBECONFIG=/Users/mathias/Developer/zfsreplicationcontroller/test/e2e/.artifacts/kubeconfig go test ./test/e2e -run TestE2E -count=1 -timeout=10m -v`

## Go style

- Write idiomatic, simple Go.
- Prefer the standard library unless an added dependency is clearly justified.
- Keep exported APIs minimal.
- Keep `main` packages thin; put logic in testable packages.
- Put private application/library code under `internal/` when it should not be imported externally.
- Prefer table-driven tests for multiple related cases.
- Return errors with context; error strings should be lowercase and without trailing punctuation.
- Define interfaces at the consumer side when practical; avoid interfaces created only for mocking.
- Pass `context.Context` as the first parameter when needed; do not store it in structs.
- Do not add or broaden `//nolint` comments unless there is no reasonable code fix.
- Every `//nolint` must name the linter and explain why.

## Boundaries

- Do not change public APIs, wire formats, database schemas, generated files, or dependency versions unless the task asks for it.
- Do not commit, create branches, or rewrite git history unless explicitly asked.
- Leave unrelated code untouched.

## Finish

- Summarize changed files and behavior.
- List the commands run and their results.
- Note any checks not run.
