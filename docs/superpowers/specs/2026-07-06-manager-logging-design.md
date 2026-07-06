# Manager Logging Design

## Context

GitHub issue #7 reports that manager startup emits this controller-runtime
warning:

```text
[controller-runtime] log.SetLogger(...) was never called; logs will not be displayed.
```

The manager currently parses flags, builds the scheme, gets Kubernetes config,
and creates the controller-runtime manager without first installing a concrete
controller-runtime logger.

Controller-runtime's root logger starts as a deferred logger. If no logger is
installed with `SetLogger`, it warns after roughly 30 seconds and replaces the
deferred logger with a null sink. That can hide useful manager and reconcile
logs.

## Goal

Initialize controller-runtime logging during manager startup so normal
Kubernetes pod logs contain manager and controller-runtime logs, and the
`log.SetLogger(...) was never called` warning no longer appears.

## Non-Goals

- Do not add new logging configuration flags.
- Do not change data mover or receiver logging.
- Do not add brittle tests around controller-runtime's global logger state.
- Do not change public APIs, manifests, or wire formats.

## Design

Use controller-runtime's standard zap logger in production mode:

```go
ctrl.SetLogger(zap.New())
```

Call it in `cmd/manager/main.go` immediately after `flag.Parse()` and before
any controller-runtime setup that may use logging, including `ctrl.GetConfigOrDie`
and `ctrl.NewManager`.

The default zap logger writes structured JSON logs to stderr, which is suitable
for Kubernetes container logs. It also avoids expanding the manager CLI surface
with `--zap-*` flags before there is a concrete need for runtime log tuning.

## Alternatives Considered

1. Bind controller-runtime's `--zap-*` flags and create the logger with
   `zap.UseFlagOptions`. This is flexible, but it adds CLI surface area for a
   narrow startup fix.
2. Extract logger setup into a helper and unit test it. This creates a
   test seam around global controller-runtime state, but the test would be more
   brittle than the behavior it protects.
3. Use a custom standard-library or klog bridge. This is unnecessary because
   controller-runtime already provides the zap logr adapter.

## Testing

Run the existing Go test suite with the writable sandbox cache:

```sh
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./...
```

Optionally verify in a real or e2e cluster that manager pod logs no longer show
the fallback warning and that controller-runtime startup logs are visible.

## Acceptance Criteria

- `cmd/manager/main.go` installs a controller-runtime logger before manager
  startup.
- Manager startup no longer emits the `log.SetLogger(...) was never called`
  warning.
- Manager and controller-runtime logs remain visible through normal pod logs.
- Existing tests pass.
