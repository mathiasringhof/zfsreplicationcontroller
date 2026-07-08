# Scheduled Run History Retention Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add CronJob-style successful and failed run history limits to `ZFSReplicationSchedule`, pruning old scheduled `ZFSReplicationRun` objects while leaving manual runs alone.

**Architecture:** `ZFSReplicationScheduleSpec` gains optional `successfulRunsHistoryLimit` and `failedRunsHistoryLimit` fields. The schedule controller prunes terminal runs by schedule label after its normal scheduling/status path, deleting old `ZFSReplicationRun` objects and relying on owner-reference garbage collection for `ZFSReceiveTask` objects.

**Tech Stack:** Go 1.26, controller-runtime fake client tests, Kubernetes CRD YAML, RBAC YAML, `gopkg.in/yaml.v3`.

## Global Constraints

- Follow red/green TDD: write or update failing tests, run them and confirm expected failure, implement the smallest passing change, rerun tests.
- Do not commit, create branches, or rewrite git history unless explicitly asked.
- Keep public API changes limited to the approved `ZFSReplicationScheduleSpec` history limit fields.
- Do not add dependencies.
- Do not change sender Job `ttlSecondsAfterFinished`.
- Do not delete manually created `ZFSReplicationRun` objects automatically.
- Do not add a TTL field to `ZFSReplicationRun`.
- Do not change receive task schema or receiver authorization behavior.
- Use `GOCACHE=/private/tmp/zfsreplicationcontroller-go-build` for focused Go test commands.

---

## File Structure

- `api/v1alpha1/zfsreplication_types.go`: owns the Go API structs; add the two schedule history limit fields.
- `api/v1alpha1/zz_generated.deepcopy.go`: handwritten deepcopy helpers in this repo; copy the new pointer fields.
- `api/v1alpha1/zfsreplication_types_test.go`: new API test covering pointer deepcopy behavior.
- `config/crd/zfsreplication.ringhof.io_zfsreplicationschedules.yaml`: OpenAPI schema and defaults for the new fields.
- `internal/controller/rbac_manifest_test.go`: CRD schema assertion and RBAC permission assertions.
- `internal/controller/zfsreplication_schedule_controller.go`: schedule-side pruning implementation and history limit validation.
- `internal/controller/zfsreplication_schedule_controller_test.go`: controller behavior tests for default limits, zero limits, manual/active run retention, ordering, and retryable delete failures.
- `config/rbac/role.yaml`: add delete permission for scheduled run pruning in cluster-wide install.
- `config/rbac/namespaced_role.yaml`: add delete permission for scheduled run pruning in namespaced install.
- `config/samples/zfsreplication_v1alpha1_zfsreplicationschedule.yaml`: show the new fields in the schedule sample.
- `README.md`: document CronJob-like scheduled run history behavior.

---

### Task 1: API Fields, DeepCopy, And CRD Schema

**Files:**
- Create: `api/v1alpha1/zfsreplication_types_test.go`
- Modify: `api/v1alpha1/zfsreplication_types.go`
- Modify: `api/v1alpha1/zz_generated.deepcopy.go`
- Modify: `config/crd/zfsreplication.ringhof.io_zfsreplicationschedules.yaml`
- Modify: `internal/controller/rbac_manifest_test.go`

**Interfaces:**
- Produces: `ZFSReplicationScheduleSpec.SuccessfulRunsHistoryLimit *int32`
- Produces: `ZFSReplicationScheduleSpec.FailedRunsHistoryLimit *int32`
- Produces: CRD fields `.spec.successfulRunsHistoryLimit` and `.spec.failedRunsHistoryLimit`
- Consumed by: Task 2 controller pruning logic

- [ ] **Step 1: Write the failing API deepcopy test**

Create `api/v1alpha1/zfsreplication_types_test.go`:

```go
package v1alpha1

import "testing"

func TestScheduleDeepCopyCopiesHistoryLimitPointers(t *testing.T) {
	successful := int32(3)
	failed := int32(1)
	schedule := &ZFSReplicationSchedule{
		Spec: ZFSReplicationScheduleSpec{
			SuccessfulRunsHistoryLimit: &successful,
			FailedRunsHistoryLimit:     &failed,
		},
	}

	copy := schedule.DeepCopy()
	if copy.Spec.SuccessfulRunsHistoryLimit == schedule.Spec.SuccessfulRunsHistoryLimit {
		t.Fatalf("SuccessfulRunsHistoryLimit pointer was aliased")
	}
	if copy.Spec.FailedRunsHistoryLimit == schedule.Spec.FailedRunsHistoryLimit {
		t.Fatalf("FailedRunsHistoryLimit pointer was aliased")
	}

	*schedule.Spec.SuccessfulRunsHistoryLimit = 9
	*schedule.Spec.FailedRunsHistoryLimit = 8

	if got := *copy.Spec.SuccessfulRunsHistoryLimit; got != 3 {
		t.Fatalf("copied successful limit = %d, want 3", got)
	}
	if got := *copy.Spec.FailedRunsHistoryLimit; got != 1 {
		t.Fatalf("copied failed limit = %d, want 1", got)
	}
}
```

- [ ] **Step 2: Write the failing CRD schema test**

In `internal/controller/rbac_manifest_test.go`, add this helper type near `validationRule`:

```go
type crdSchemaProperty struct {
	Type       string                       `yaml:"type"`
	Format     string                       `yaml:"format"`
	Default    any                          `yaml:"default"`
	Minimum    *int64                       `yaml:"minimum"`
	Properties map[string]crdSchemaProperty `yaml:"properties"`
}
```

Add this test after `TestControllerClusterRoleHasRequiredPermissions`:

```go
func TestScheduleCRDHistoryLimitsHaveCronJobDefaults(t *testing.T) {
	t.Helper()

	crdPath := filepath.Join("..", "..", "config", "crd", "zfsreplication.ringhof.io_zfsreplicationschedules.yaml")
	data, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("read %s: %v", crdPath, err)
	}

	var crd struct {
		Spec struct {
			Versions []struct {
				Schema struct {
					OpenAPIV3Schema crdSchemaProperty `yaml:"openAPIV3Schema"`
				} `yaml:"schema"`
			} `yaml:"versions"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("parse %s: %v", crdPath, err)
	}
	if len(crd.Spec.Versions) != 1 {
		t.Fatalf("CRD versions = %d, want 1", len(crd.Spec.Versions))
	}

	specProps := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties
	for _, tt := range []struct {
		name       string
		wantDefault int64
	}{
		{name: "successfulRunsHistoryLimit", wantDefault: 3},
		{name: "failedRunsHistoryLimit", wantDefault: 1},
	} {
		prop, ok := specProps[tt.name]
		if !ok {
			t.Fatalf("schedule CRD spec properties missing %s", tt.name)
		}
		if prop.Type != "integer" {
			t.Fatalf("%s type = %q, want integer", tt.name, prop.Type)
		}
		if prop.Format != "int32" {
			t.Fatalf("%s format = %q, want int32", tt.name, prop.Format)
		}
		defaultValue, ok := prop.Default.(int)
		if !ok || int64(defaultValue) != tt.wantDefault {
			t.Fatalf("%s default = %#v, want %d", tt.name, prop.Default, tt.wantDefault)
		}
		if prop.Minimum == nil || *prop.Minimum != 0 {
			t.Fatalf("%s minimum = %v, want 0", tt.name, prop.Minimum)
		}
	}
}
```

- [ ] **Step 3: Run the focused tests and confirm they fail**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./api/v1alpha1 ./internal/controller -run 'TestScheduleDeepCopyCopiesHistoryLimitPointers|TestScheduleCRDHistoryLimitsHaveCronJobDefaults' -count=1
```

Expected:

- `./api/v1alpha1` fails to compile because `SuccessfulRunsHistoryLimit` and `FailedRunsHistoryLimit` do not exist.
- After adding the Go fields but before schema updates, the CRD test fails because the CRD properties are missing.

- [ ] **Step 4: Add schedule spec fields**

In `api/v1alpha1/zfsreplication_types.go`, change `ZFSReplicationScheduleSpec` to:

```go
type ZFSReplicationScheduleSpec struct {
	Schedule                   string                `json:"schedule"`
	Suspend                    *bool                 `json:"suspend,omitempty"`
	ConcurrencyPolicy          ConcurrencyPolicy     `json:"concurrencyPolicy,omitempty"`
	SuccessfulRunsHistoryLimit *int32                `json:"successfulRunsHistoryLimit,omitempty"`
	FailedRunsHistoryLimit     *int32                `json:"failedRunsHistoryLimit,omitempty"`
	RunTemplate                ZFSReplicationRunSpec `json:"runTemplate"`
}
```

- [ ] **Step 5: Copy the new pointer fields in DeepCopy**

In `api/v1alpha1/zz_generated.deepcopy.go`, update `func (in *ZFSReplicationSchedule) DeepCopy() *ZFSReplicationSchedule`:

```go
func (in *ZFSReplicationSchedule) DeepCopy() *ZFSReplicationSchedule {
	if in == nil {
		return nil
	}
	out := new(ZFSReplicationSchedule)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	if in.Spec.Suspend != nil {
		out.Spec.Suspend = new(bool)
		*out.Spec.Suspend = *in.Spec.Suspend
	}
	if in.Spec.SuccessfulRunsHistoryLimit != nil {
		out.Spec.SuccessfulRunsHistoryLimit = new(int32)
		*out.Spec.SuccessfulRunsHistoryLimit = *in.Spec.SuccessfulRunsHistoryLimit
	}
	if in.Spec.FailedRunsHistoryLimit != nil {
		out.Spec.FailedRunsHistoryLimit = new(int32)
		*out.Spec.FailedRunsHistoryLimit = *in.Spec.FailedRunsHistoryLimit
	}
	out.Spec.RunTemplate = *in.Spec.RunTemplate.DeepCopy()
	if in.Status.LastScheduleTime != nil {
		out.Status.LastScheduleTime = in.Status.LastScheduleTime.DeepCopy()
	}
	return out
}
```

- [ ] **Step 6: Add CRD schema properties**

In `config/crd/zfsreplication.ringhof.io_zfsreplicationschedules.yaml`, under `spec.properties.spec.properties`, place these fields after `concurrencyPolicy`:

```yaml
                successfulRunsHistoryLimit:
                  type: integer
                  format: int32
                  minimum: 0
                  default: 3
                failedRunsHistoryLimit:
                  type: integer
                  format: int32
                  minimum: 0
                  default: 1
```

- [ ] **Step 7: Run focused tests and gofmt**

Run:

```bash
gofmt -w api/v1alpha1/zfsreplication_types.go api/v1alpha1/zz_generated.deepcopy.go api/v1alpha1/zfsreplication_types_test.go internal/controller/rbac_manifest_test.go
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./api/v1alpha1 ./internal/controller -run 'TestScheduleDeepCopyCopiesHistoryLimitPointers|TestScheduleCRDHistoryLimitsHaveCronJobDefaults' -count=1
```

Expected: both focused tests pass.

- [ ] **Step 8: Review checkpoint**

Run:

```bash
git diff -- api/v1alpha1/zfsreplication_types.go api/v1alpha1/zz_generated.deepcopy.go api/v1alpha1/zfsreplication_types_test.go config/crd/zfsreplication.ringhof.io_zfsreplicationschedules.yaml internal/controller/rbac_manifest_test.go
```

Expected: diff contains only API fields, deepcopy handling, CRD schema, and schema test.

---

### Task 2: Schedule Controller History Pruning

**Files:**
- Modify: `internal/controller/zfsreplication_schedule_controller_test.go`
- Modify: `internal/controller/zfsreplication_schedule_controller.go`

**Interfaces:**
- Consumes: `ZFSReplicationScheduleSpec.SuccessfulRunsHistoryLimit *int32`
- Consumes: `ZFSReplicationScheduleSpec.FailedRunsHistoryLimit *int32`
- Produces: `pruneScheduleRunHistory(ctx context.Context, schedule *zfsv1.ZFSReplicationSchedule) error`
- Produces: `scheduleHistoryLimit(value *int32, defaultValue int32) int`

- [ ] **Step 1: Write failing controller tests**

In `internal/controller/zfsreplication_schedule_controller_test.go`, update the imports to:

```go
import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)
```

Add these tests after `TestScheduleReconcileCreatesRunForDueSchedule`:

```go
func TestScheduleReconcilePrunesTerminalRunHistoryWithDefaults(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 7, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.CreationTimestamp = metav1.NewTime(now)

	r := newScheduleReconciler(t, now,
		schedule,
		scheduledTerminalRun("success-1", "hourly", zfsv1.PhaseSucceeded, now.Add(-5*time.Hour)),
		scheduledTerminalRun("success-2", "hourly", zfsv1.PhaseSucceeded, now.Add(-4*time.Hour)),
		scheduledTerminalRun("success-3", "hourly", zfsv1.PhaseSucceeded, now.Add(-3*time.Hour)),
		scheduledTerminalRun("success-4", "hourly", zfsv1.PhaseSucceeded, now.Add(-2*time.Hour)),
		scheduledTerminalRun("failure-1", "hourly", zfsv1.PhaseFailed, now.Add(-5*time.Hour)),
		scheduledTerminalRun("failure-2", "hourly", zfsv1.PhaseFailed, now.Add(-4*time.Hour)),
	)

	if _, err := r.Reconcile(context.Background(), request("hourly")); err != nil {
		t.Fatal(err)
	}

	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "success-1")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "success-2")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "success-3")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "success-4")
	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "failure-1")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "failure-2")
}

func TestScheduleReconcilePrunesTerminalRunHistoryWithExplicitZero(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 7, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.CreationTimestamp = metav1.NewTime(now)
	schedule.Spec.SuccessfulRunsHistoryLimit = ptr(int32(0))
	schedule.Spec.FailedRunsHistoryLimit = ptr(int32(0))

	active := scheduledTerminalRun("active", "hourly", zfsv1.PhaseRunning, now.Add(-3*time.Hour))
	manual := replicationRun("manual-success")
	manual.Status.Phase = zfsv1.PhaseSucceeded
	completedAt := metav1.NewTime(now.Add(-3 * time.Hour))
	manual.Status.CompletedAt = &completedAt

	r := newScheduleReconciler(t, now,
		schedule,
		scheduledTerminalRun("success", "hourly", zfsv1.PhaseSucceeded, now.Add(-2*time.Hour)),
		scheduledTerminalRun("failure", "hourly", zfsv1.PhaseFailed, now.Add(-1*time.Hour)),
		active,
		manual,
	)

	if _, err := r.Reconcile(context.Background(), request("hourly")); err != nil {
		t.Fatal(err)
	}

	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "success")
	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "failure")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "active")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "manual-success")
}

func TestScheduleReconcilePrunesTerminalRunHistoryByCompletionCreationAndName(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 7, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.CreationTimestamp = metav1.NewTime(now)
	schedule.Spec.SuccessfulRunsHistoryLimit = ptr(int32(2))
	schedule.Spec.FailedRunsHistoryLimit = ptr(int32(10))

	withoutCompletedAt := scheduledTerminalRun("without-completed-at", "hourly", zfsv1.PhaseSucceeded, time.Time{})
	withoutCompletedAt.CreationTimestamp = metav1.NewTime(now.Add(-6 * time.Hour))

	sameCompletedAt := now.Add(-3 * time.Hour)
	nameTieLoser := scheduledTerminalRun("same-a", "hourly", zfsv1.PhaseSucceeded, sameCompletedAt)
	nameTieWinner := scheduledTerminalRun("same-b", "hourly", zfsv1.PhaseSucceeded, sameCompletedAt)
	sameCreationTime := metav1.NewTime(now.Add(-4 * time.Hour))
	nameTieLoser.CreationTimestamp = sameCreationTime
	nameTieWinner.CreationTimestamp = sameCreationTime

	newest := scheduledTerminalRun("newest", "hourly", zfsv1.PhaseSucceeded, now.Add(-1*time.Hour))

	r := newScheduleReconciler(t, now, schedule, withoutCompletedAt, nameTieLoser, nameTieWinner, newest)

	if _, err := r.Reconcile(context.Background(), request("hourly")); err != nil {
		t.Fatal(err)
	}

	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "without-completed-at")
	assertObjectDeleted(t, r.Client, &zfsv1.ZFSReplicationRun{}, "same-a")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "same-b")
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "newest")
}

func TestScheduleReconcilePruneFailureReturnsAfterStatusUpdate(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 7, 0, 0, time.UTC)
	schedule := replicationSchedule("hourly")
	schedule.CreationTimestamp = metav1.NewTime(now.Add(-2 * time.Hour))
	schedule.Spec.SuccessfulRunsHistoryLimit = ptr(int32(0))
	oldRun := scheduledTerminalRun("old-success", "hourly", zfsv1.PhaseSucceeded, now.Add(-2*time.Hour))

	r := newScheduleReconcilerWithInterceptors(t, now, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetName() == "old-success" {
				return errors.New("temporary run delete failure")
			}
			return c.Delete(ctx, obj, opts...)
		},
	}, schedule, oldRun)

	_, err := r.Reconcile(context.Background(), request("hourly"))
	if err == nil || !strings.Contains(err.Error(), "temporary run delete failure") {
		t.Fatalf("Reconcile() error = %v, want temporary run delete failure", err)
	}

	var got zfsv1.ZFSReplicationSchedule
	if err := r.Get(context.Background(), types.NamespacedName{Name: "hourly", Namespace: "storage"}, &got); err != nil {
		t.Fatal(err)
	}
	wantDue := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	if got.Status.LastScheduleTime == nil || !got.Status.LastScheduleTime.Time.Equal(wantDue) {
		t.Fatalf("LastScheduleTime = %v, want %v", got.Status.LastScheduleTime, wantDue)
	}
	if got.Status.LastRunName != scheduledRunName("hourly", wantDue) {
		t.Fatalf("LastRunName = %q, want %q", got.Status.LastRunName, scheduledRunName("hourly", wantDue))
	}
	assertObjectExists(t, r.Client, &zfsv1.ZFSReplicationRun{}, "old-success")
}
```

Add these helpers near `replicationSchedule`:

```go
func scheduledTerminalRun(name, scheduleName string, phase zfsv1.Phase, completedAt time.Time) *zfsv1.ZFSReplicationRun {
	run := replicationRun(name)
	run.CreationTimestamp = metav1.NewTime(completedAt.Add(-time.Minute))
	run.Labels = map[string]string{
		labelPrefix + "/schedule": scheduleName,
	}
	run.Status.Phase = phase
	if !completedAt.IsZero() {
		t := metav1.NewTime(completedAt)
		run.Status.CompletedAt = &t
	}
	return run
}

func newScheduleReconcilerWithInterceptors(t *testing.T, now time.Time, funcs interceptor.Funcs, objs ...client.Object) *ZFSReplicationScheduleReconciler {
	t.Helper()
	scheme := newTestScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&zfsv1.ZFSReplicationSchedule{}).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
	return &ZFSReplicationScheduleReconciler{Client: c, Scheme: scheme, Now: func() time.Time { return now }}
}
```

Also add this import because the helper uses the fake client:

```go
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
```

- [ ] **Step 2: Run the focused tests and confirm they fail**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run 'TestScheduleReconcilePrunesTerminalRunHistory|TestScheduleReconcilePruneFailureReturnsAfterStatusUpdate' -count=1
```

Expected: tests fail because no pruning exists; after adding the helper imports, the package compiles and the old terminal runs still exist.

- [ ] **Step 3: Implement history limit constants and validation**

In `internal/controller/zfsreplication_schedule_controller.go`, change imports to:

```go
import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)
```

Add constants after the reconciler type:

```go
const (
	defaultSuccessfulRunsHistoryLimit int32 = 3
	defaultFailedRunsHistoryLimit     int32 = 1
)
```

After concurrency policy validation in `Reconcile`, add:

```go
	if err := validateScheduleHistoryLimits(&schedule); err != nil {
		return ctrl.Result{}, r.failSchedule(ctx, &schedule, err.Error())
	}
```

Add helper functions near `validateConcurrencyPolicy`:

```go
func validateScheduleHistoryLimits(schedule *zfsv1.ZFSReplicationSchedule) error {
	if err := validateHistoryLimit("successfulRunsHistoryLimit", schedule.Spec.SuccessfulRunsHistoryLimit); err != nil {
		return err
	}
	return validateHistoryLimit("failedRunsHistoryLimit", schedule.Spec.FailedRunsHistoryLimit)
}

func validateHistoryLimit(field string, value *int32) error {
	if value != nil && *value < 0 {
		return fmt.Errorf("%s must be greater than or equal to 0", field)
	}
	return nil
}

func scheduleHistoryLimit(value *int32, defaultValue int32) int {
	if value == nil {
		return int(defaultValue)
	}
	return int(*value)
}
```

- [ ] **Step 4: Route successful status patches through pruning**

Replace the suspend branch:

```go
	if boolDefault(schedule.Spec.Suspend, false) {
		return r.patchScheduleStatusAndPruneHistory(ctx, &schedule, ctrl.Result{}, func(st *zfsv1.ZFSReplicationScheduleStatus) {
			st.LastError = ""
		})
	}
```

Replace the `due.IsZero()` branch:

```go
	if due.IsZero() {
		result := requeueAt(now, next)
		return r.patchScheduleStatusAndPruneHistory(ctx, &schedule, result, func(st *zfsv1.ZFSReplicationScheduleStatus) {
			st.LastError = ""
		})
	}
```

Replace the active-run skip branch:

```go
		if active {
			result := requeueAt(now, next)
			return r.patchScheduleStatusAndPruneHistory(ctx, &schedule, result, func(st *zfsv1.ZFSReplicationScheduleStatus) {
				scheduled := metav1.NewTime(due)
				st.LastScheduleTime = &scheduled
				st.LastError = "skipped scheduled run because a previous run is still active"
			})
		}
```

Replace the final status patch after `ensureScheduledRun`:

```go
	result := requeueAt(now, next)
	return r.patchScheduleStatusAndPruneHistory(ctx, &schedule, result, func(st *zfsv1.ZFSReplicationScheduleStatus) {
		scheduled := metav1.NewTime(due)
		st.LastScheduleTime = &scheduled
		st.LastRunName = runName
		st.LastError = ""
	})
```

Add the wrapper near `patchScheduleStatus`:

```go
func (r *ZFSReplicationScheduleReconciler) patchScheduleStatusAndPruneHistory(
	ctx context.Context,
	schedule *zfsv1.ZFSReplicationSchedule,
	result ctrl.Result,
	mutate func(*zfsv1.ZFSReplicationScheduleStatus),
) (ctrl.Result, error) {
	if err := r.patchScheduleStatus(ctx, schedule, mutate); err != nil {
		return result, err
	}
	return result, r.pruneScheduleRunHistory(ctx, schedule)
}
```

- [ ] **Step 5: Implement pruning helpers**

Add these helpers after `hasActiveRun`:

```go
func (r *ZFSReplicationScheduleReconciler) pruneScheduleRunHistory(ctx context.Context, schedule *zfsv1.ZFSReplicationSchedule) error {
	var runs zfsv1.ZFSReplicationRunList
	if err := r.List(ctx, &runs, client.InNamespace(schedule.Namespace), client.MatchingLabels{labelPrefix + "/schedule": schedule.Name}); err != nil {
		return err
	}

	var successful []zfsv1.ZFSReplicationRun
	var failed []zfsv1.ZFSReplicationRun
	for _, run := range runs.Items {
		switch run.Status.Phase {
		case zfsv1.PhaseSucceeded:
			successful = append(successful, run)
		case zfsv1.PhaseFailed:
			failed = append(failed, run)
		}
	}

	return errors.Join(
		r.pruneTerminalRuns(ctx, successful, scheduleHistoryLimit(schedule.Spec.SuccessfulRunsHistoryLimit, defaultSuccessfulRunsHistoryLimit)),
		r.pruneTerminalRuns(ctx, failed, scheduleHistoryLimit(schedule.Spec.FailedRunsHistoryLimit, defaultFailedRunsHistoryLimit)),
	)
}

func (r *ZFSReplicationScheduleReconciler) pruneTerminalRuns(ctx context.Context, runs []zfsv1.ZFSReplicationRun, limit int) error {
	if len(runs) <= limit {
		return nil
	}
	sort.Slice(runs, func(i, j int) bool {
		return terminalRunOlder(runs[i], runs[j])
	})

	var errs []error
	for i := 0; i < len(runs)-limit; i++ {
		run := runs[i].DeepCopy()
		if err := r.Delete(ctx, run); client.IgnoreNotFound(err) != nil {
			errs = append(errs, fmt.Errorf("delete old scheduled run %s/%s: %w", run.Namespace, run.Name, err))
		}
	}
	return errors.Join(errs...)
}

func terminalRunOlder(a, b zfsv1.ZFSReplicationRun) bool {
	aTime := terminalRunHistoryTime(a)
	bTime := terminalRunHistoryTime(b)
	if !aTime.Equal(bTime) {
		if aTime.IsZero() {
			return true
		}
		if bTime.IsZero() {
			return false
		}
		return aTime.Before(bTime)
	}
	if !a.CreationTimestamp.Time.Equal(b.CreationTimestamp.Time) {
		return a.CreationTimestamp.Time.Before(b.CreationTimestamp.Time)
	}
	return a.Name < b.Name
}

func terminalRunHistoryTime(run zfsv1.ZFSReplicationRun) time.Time {
	if run.Status.CompletedAt != nil {
		return run.Status.CompletedAt.Time
	}
	return run.CreationTimestamp.Time
}
```

- [ ] **Step 6: Run focused tests and gofmt**

Run:

```bash
gofmt -w internal/controller/zfsreplication_schedule_controller.go internal/controller/zfsreplication_schedule_controller_test.go
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run 'TestScheduleReconcilePrunesTerminalRunHistory|TestScheduleReconcilePruneFailureReturnsAfterStatusUpdate' -count=1
```

Expected: focused controller tests pass.

- [ ] **Step 7: Review checkpoint**

Run:

```bash
git diff -- internal/controller/zfsreplication_schedule_controller.go internal/controller/zfsreplication_schedule_controller_test.go
```

Expected: diff contains schedule-side pruning only; run-controller terminal cleanup remains unchanged.

---

### Task 3: RBAC, Samples, And README

**Files:**
- Modify: `internal/controller/rbac_manifest_test.go`
- Modify: `config/rbac/role.yaml`
- Modify: `config/rbac/namespaced_role.yaml`
- Modify: `config/samples/zfsreplication_v1alpha1_zfsreplicationschedule.yaml`
- Modify: `README.md`

**Interfaces:**
- Consumes: schedule controller deletes old `ZFSReplicationRun` objects.
- Produces: install manifests that grant `delete` on `zfsreplicationruns`.

- [ ] **Step 1: Update RBAC tests first**

In `internal/controller/rbac_manifest_test.go`, change the cluster role expectation for `zfsreplicationruns` to include delete:

```go
	verbs := verbsForResource(role.Rules, "zfsreplication.ringhof.io", "zfsreplicationruns")
	for _, verb := range []string{"create", "get", "list", "watch", "delete"} {
		if !contains(verbs, verb) {
			t.Fatalf("zfsreplicationruns RBAC verbs = %v, missing %q", verbs, verb)
		}
	}
```

In `TestNamespacedRBACRestrictsWorkloadPermissionsToWatchedNamespace`, change only the `zfsreplicationruns` case:

```go
		{apiGroup: "zfsreplication.ringhof.io", resource: "zfsreplicationruns", verbs: []string{"create", "get", "list", "watch", "delete"}},
```

- [ ] **Step 2: Run the RBAC tests and confirm they fail**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run 'TestControllerClusterRoleHasRequiredPermissions|TestNamespacedRBACRestrictsWorkloadPermissionsToWatchedNamespace' -count=1
```

Expected: tests fail because `delete` is not present for `zfsreplicationruns`.

- [ ] **Step 3: Add delete permission to RBAC manifests**

In `config/rbac/role.yaml`, split the custom resource rules so only runs get delete:

```yaml
  - apiGroups:
      - zfsreplication.ringhof.io
    resources:
      - zfsreplicationruns
    verbs:
      - create
      - get
      - list
      - watch
      - delete
  - apiGroups:
      - zfsreplication.ringhof.io
    resources:
      - zfsreceivetasks
    verbs:
      - create
      - get
      - list
      - watch
```

Apply the same split in `config/rbac/namespaced_role.yaml`.

- [ ] **Step 4: Update the schedule sample**

In `config/samples/zfsreplication_v1alpha1_zfsreplicationschedule.yaml`, add the fields after `concurrencyPolicy`:

```yaml
  successfulRunsHistoryLimit: 3
  failedRunsHistoryLimit: 1
```

- [ ] **Step 5: Update README scheduled run docs**

In `README.md`, update the scheduled run sample:

```yaml
  schedule: "10 * * * *"
  concurrencyPolicy: Forbid
  successfulRunsHistoryLimit: 3
  failedRunsHistoryLimit: 1
  runTemplate:
```

Replace the paragraph after `concurrencyPolicy: Forbid` with:

```markdown
`concurrencyPolicy: Forbid` is the default and skips a tick while a previous
scheduled run is still active. Set `suspend: true` to stop creating runs without
deleting the schedule.

Scheduled run history mirrors Kubernetes CronJob history limits. By default the
controller keeps the last three successful scheduled runs and the last failed
scheduled run. Set `successfulRunsHistoryLimit` or `failedRunsHistoryLimit` to
`0` to remove terminal scheduled runs of that phase after reconciliation.
Manually created `ZFSReplicationRun` objects are not pruned by schedule history
limits.
```

- [ ] **Step 6: Run RBAC tests**

Run:

```bash
gofmt -w internal/controller/rbac_manifest_test.go
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run 'TestControllerClusterRoleHasRequiredPermissions|TestNamespacedRBACRestrictsWorkloadPermissionsToWatchedNamespace|TestScheduleCRDHistoryLimitsHaveCronJobDefaults' -count=1
```

Expected: RBAC and CRD manifest tests pass.

- [ ] **Step 7: Review checkpoint**

Run:

```bash
git diff -- internal/controller/rbac_manifest_test.go config/rbac/role.yaml config/rbac/namespaced_role.yaml config/samples/zfsreplication_v1alpha1_zfsreplicationschedule.yaml README.md
```

Expected: diff contains delete permission for runs, sample history limits, and README documentation. It must not grant delete on `zfsreceivetasks`.

---

### Task 4: Full Verification

**Files:**
- Verify all changed Go, YAML, and docs files from Tasks 1-3.

**Interfaces:**
- Consumes: completed API, controller, RBAC, sample, and README changes.
- Produces: final confidence that repo checks pass.

- [ ] **Step 1: Run formatting**

Run:

```bash
go fmt ./...
```

Expected: command exits 0.

- [ ] **Step 2: Run the full unit/e2e package test suite**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./...
```

Expected: all packages pass.

- [ ] **Step 3: Run lint**

Run:

```bash
golangci-lint run
```

Expected: command exits 0. If `golangci-lint` is unavailable in the environment, report that explicitly and do not claim lint passed.

- [ ] **Step 4: Inspect final diff**

Run:

```bash
git status --short
git diff --stat
```

Expected: changed files match the file structure section; no unrelated files are modified.

- [ ] **Step 5: Final response checklist**

Report:

- changed files and behavior;
- tests and lint commands run with results;
- any checks that could not be run;
- note that no commit was made unless the user explicitly asked for one.
