# Delete Target Snapshots Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose Syncoid's `--delete-target-snapshots` option through `spec.syncoid.deleteTargetSnapshots` with a separate receiver-side authorization bit for exact target snapshot destroys.

**Architecture:** Extend the existing Syncoid option pipeline from API type to normalized options, sender Job env vars, sender config, and Syncoid arguments. Add `AllowTargetSnapshotDestroy` to receive task policy so the receiver can authorize exact target snapshot destroys only when the new option is enabled, while leaving `AllowDestroy` and `AllowSyncSnapshotDestroy` semantics unchanged.

**Tech Stack:** Go 1.26, controller-runtime fake client tests, Kubernetes CRD YAML, forced-command receiver parser tests, Dockerfile runtime guard tests.

## Global Constraints

- Follow red/green TDD: write or update failing tests, run them and confirm expected failure, implement the smallest passing change, rerun tests.
- Do not commit, create branches, or rewrite git history unless explicitly asked.
- Keep public API changes limited to `SyncoidSpec.DeleteTargetSnapshots` and `ReceiveTaskPolicy.AllowTargetSnapshotDestroy`.
- Do not add dependencies.
- Do not enable target-only snapshot deletion through `forceDelete`.
- Do not change default replication behavior.
- Do not add controller-side snapshot reconciliation or retention logic.
- Do not allow recursive snapshot destroys or dataset destroys through `deleteTargetSnapshots`.
- Do not change relationship-scoped Syncoid sync snapshot pruning behavior.
- Do not enable `deleteTargetSnapshots` in sample manifests by default.
- Use `GOCACHE=/private/tmp/zfsreplicationcontroller-go-build` for Go test commands.

---

## File Structure

- `api/v1alpha1/zfsreplication_types.go`: add `SyncoidSpec.DeleteTargetSnapshots` and `ReceiveTaskPolicy.AllowTargetSnapshotDestroy`.
- `api/v1alpha1/zz_generated.deepcopy.go`: handwritten deepcopy helper in this repo; copy the new `SyncoidSpec.DeleteTargetSnapshots` pointer field.
- `api/v1alpha1/zfsreplication_types_test.go`: add API deepcopy coverage for `DeleteTargetSnapshots`.
- `internal/replication/replication.go`: add `DeleteTargetSnapshots` to normalized Syncoid options.
- `internal/replication/replication_test.go`: cover explicit true and default false normalization.
- `internal/datamover/sender.go`: add sender env/config/log/argument plumbing for `--delete-target-snapshots`.
- `internal/datamover/datamover_test.go`: cover env defaults, explicit env values, controller env contract, command args, and startup logs.
- `internal/datamover/runtime_image_test.go`: assert the runtime image checks Syncoid help for `--delete-target-snapshots`.
- `Dockerfile`: add the runtime image help check for `--delete-target-snapshots`.
- `internal/controller/zfsreplication_run_controller.go`: write sender env and receive task policy fields.
- `internal/controller/zfsreplication_run_controller_test.go`: cover controller env round-trip and receive policy derivation.
- `cmd/zfsrep-receiver/receiver_command_authorize.go`: authorize exact target snapshot destroys when `AllowTargetSnapshotDestroy` is true.
- `cmd/zfsrep-receiver/forced_command_test.go`: cover default rejection, explicit allowance, dataset boundary checks, recursive rejection, and batch behavior.
- `config/crd/zfsreplication.ringhof.io_zfsreplicationruns.yaml`: expose `spec.syncoid.deleteTargetSnapshots`.
- `config/crd/zfsreplication.ringhof.io_zfsreplicationschedules.yaml`: expose `spec.runTemplate.syncoid.deleteTargetSnapshots`.
- `config/crd/zfsreplication.ringhof.io_zfsreceivetasks.yaml`: expose `spec.policy.allowTargetSnapshotDestroy`.
- `internal/controller/rbac_manifest_test.go`: assert all new CRD schema fields.
- `README.md`: document the destructive option and distinguish it from `forceDelete`.

---

### Task 1: API Types, Normalized Options, And CRD Schema

**Files:**
- Modify: `api/v1alpha1/zfsreplication_types.go`
- Modify: `api/v1alpha1/zz_generated.deepcopy.go`
- Modify: `api/v1alpha1/zfsreplication_types_test.go`
- Modify: `internal/replication/replication.go`
- Modify: `internal/replication/replication_test.go`
- Modify: `config/crd/zfsreplication.ringhof.io_zfsreplicationruns.yaml`
- Modify: `config/crd/zfsreplication.ringhof.io_zfsreplicationschedules.yaml`
- Modify: `internal/controller/rbac_manifest_test.go`

**Interfaces:**
- Produces: `SyncoidSpec.DeleteTargetSnapshots *bool`
- Produces: `replication.SyncoidOptionInput.DeleteTargetSnapshots *bool`
- Produces: `replication.SyncoidOptions.DeleteTargetSnapshots bool`
- Produces: CRD boolean field `.spec.syncoid.deleteTargetSnapshots` for runs
- Produces: CRD boolean field `.spec.runTemplate.syncoid.deleteTargetSnapshots` for schedules
- Consumed by: Task 2 sender plumbing and Task 3 controller policy plumbing

- [ ] **Step 1: Write the failing API deepcopy test**

Add this test to `api/v1alpha1/zfsreplication_types_test.go`:

```go
func TestSyncoidSpecDeepCopyCopiesDeleteTargetSnapshotsPointer(t *testing.T) {
	deleteTargetSnapshots := true
	spec := &SyncoidSpec{
		DeleteTargetSnapshots: &deleteTargetSnapshots,
	}

	copy := spec.DeepCopy()
	if copy.DeleteTargetSnapshots == spec.DeleteTargetSnapshots {
		t.Fatalf("DeleteTargetSnapshots pointer was aliased")
	}

	*spec.DeleteTargetSnapshots = false

	if got := *copy.DeleteTargetSnapshots; !got {
		t.Fatalf("copied DeleteTargetSnapshots = %t, want true", got)
	}
}
```

- [ ] **Step 2: Update the normalization tests before implementation**

In `internal/replication/replication_test.go`, add `DeleteTargetSnapshots: ptr(true),` to the `SyncoidOptionInput` in `TestNormalizeSyncoidOptions`, and change the first assertion to:

```go
if !opts.NoSyncSnap || opts.NoRollback || !opts.ForceDelete || !opts.DeleteTargetSnapshots || opts.Compress != "zstd" {
	t.Fatalf("normalized syncoid behavior = %#v", opts)
}
```

In `TestDefaultSyncoidOptions`, change the first assertion to:

```go
if opts.NoSyncSnap || !opts.NoRollback || opts.ForceDelete || opts.DeleteTargetSnapshots {
	t.Fatalf("default syncoid behavior = %#v", opts)
}
```

- [ ] **Step 3: Add failing CRD schema assertions**

In `internal/controller/rbac_manifest_test.go`, add this assertion in `TestCRDSchemaExposesSyncoidOptions` immediately after the existing `noSyncSnap` assertion:

```go
if syncoidProps["deleteTargetSnapshots"].Type != "boolean" || syncoidProps["deleteTargetSnapshots"].Default != false {
	t.Fatalf("deleteTargetSnapshots schema = %#v", syncoidProps["deleteTargetSnapshots"])
}
```

Add this new test near `TestCRDSchemaExposesSyncoidOptions`:

```go
func TestScheduleCRDSchemaExposesSyncoidDeleteTargetSnapshots(t *testing.T) {
	t.Helper()

	crdPath := filepath.Join("..", "..", "config", "crd", "zfsreplication.ringhof.io_zfsreplicationschedules.yaml")
	data, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("read %s: %v", crdPath, err)
	}

	type schemaNode struct {
		Default    any                   `yaml:"default"`
		Properties map[string]schemaNode `yaml:"properties"`
		Type       string                `yaml:"type"`
	}
	var crd struct {
		Spec struct {
			Versions []struct {
				Schema struct {
					OpenAPIV3Schema schemaNode `yaml:"openAPIV3Schema"`
				} `yaml:"schema"`
			} `yaml:"versions"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("parse %s: %v", crdPath, err)
	}
	if len(crd.Spec.Versions) == 0 {
		t.Fatalf("%s has no versions", crdPath)
	}
	runTemplate := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties["runTemplate"]
	syncoidProps := runTemplate.Properties["syncoid"].Properties
	if syncoidProps["deleteTargetSnapshots"].Type != "boolean" || syncoidProps["deleteTargetSnapshots"].Default != false {
		t.Fatalf("schedule deleteTargetSnapshots schema = %#v", syncoidProps["deleteTargetSnapshots"])
	}
}
```

- [ ] **Step 4: Run focused tests and confirm expected failures**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./api/v1alpha1 -run TestSyncoidSpecDeepCopyCopiesDeleteTargetSnapshotsPointer -count=1
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/replication -run 'TestNormalizeSyncoidOptions|TestDefaultSyncoidOptions' -count=1
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run 'TestCRDSchemaExposesSyncoidOptions|TestScheduleCRDSchemaExposesSyncoidDeleteTargetSnapshots' -count=1
```

Expected:

- API test fails to compile because `SyncoidSpec.DeleteTargetSnapshots` does not exist.
- Replication tests fail to compile because normalized option structs do not have `DeleteTargetSnapshots`.
- CRD tests fail because `deleteTargetSnapshots` is absent from the run and schedule CRDs.

- [ ] **Step 5: Implement the API and normalized option fields**

In `api/v1alpha1/zfsreplication_types.go`, add the field after `ForceDelete` in `SyncoidSpec`:

```go
DeleteTargetSnapshots *bool `json:"deleteTargetSnapshots,omitempty"`
```

In `api/v1alpha1/zz_generated.deepcopy.go`, add this block after the `ForceDelete` pointer copy:

```go
if in.DeleteTargetSnapshots != nil {
	out.DeleteTargetSnapshots = new(bool)
	*out.DeleteTargetSnapshots = *in.DeleteTargetSnapshots
}
```

In `internal/replication/replication.go`, add `DeleteTargetSnapshots bool` to `SyncoidOptions`, add `DeleteTargetSnapshots *bool` to `SyncoidOptionInput`, and add this line in `NormalizeSyncoidOptions` after `ForceDelete`:

```go
out.DeleteTargetSnapshots = boolDefault(in.DeleteTargetSnapshots, out.DeleteTargetSnapshots)
```

- [ ] **Step 6: Implement the run and schedule CRD schema fields**

In both `config/crd/zfsreplication.ringhof.io_zfsreplicationruns.yaml` and `config/crd/zfsreplication.ringhof.io_zfsreplicationschedules.yaml`, add this property under each `syncoid.properties`, immediately after `forceDelete`:

```yaml
                    deleteTargetSnapshots:
                      type: boolean
                      default: false
```

For the schedule CRD, keep the same relative indentation under `spec.runTemplate.syncoid.properties`.

- [ ] **Step 7: Format and rerun focused tests**

Run:

```bash
gofmt -w api/v1alpha1/zfsreplication_types.go api/v1alpha1/zz_generated.deepcopy.go api/v1alpha1/zfsreplication_types_test.go internal/replication/replication.go internal/replication/replication_test.go internal/controller/rbac_manifest_test.go
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./api/v1alpha1 -run TestSyncoidSpecDeepCopyCopiesDeleteTargetSnapshotsPointer -count=1
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/replication -run 'TestNormalizeSyncoidOptions|TestDefaultSyncoidOptions' -count=1
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run 'TestCRDSchemaExposesSyncoidOptions|TestScheduleCRDSchemaExposesSyncoidDeleteTargetSnapshots' -count=1
```

Expected: all focused tests pass.

- [ ] **Step 8: Review the task diff without committing**

Run:

```bash
git diff -- api/v1alpha1/zfsreplication_types.go api/v1alpha1/zz_generated.deepcopy.go api/v1alpha1/zfsreplication_types_test.go internal/replication/replication.go internal/replication/replication_test.go config/crd/zfsreplication.ringhof.io_zfsreplicationruns.yaml config/crd/zfsreplication.ringhof.io_zfsreplicationschedules.yaml internal/controller/rbac_manifest_test.go
```

Expected: diff contains only the new Syncoid option field, deepcopy handling, normalization, CRD schema, and tests.

---

### Task 2: Sender Environment, Logging, And Syncoid Argument Plumbing

**Files:**
- Modify: `internal/datamover/sender.go`
- Modify: `internal/datamover/datamover_test.go`
- Modify: `internal/datamover/runtime_image_test.go`
- Modify: `Dockerfile`

**Interfaces:**
- Consumes: `replication.SyncoidOptions.DeleteTargetSnapshots bool`
- Produces: `datamover.EnvDeleteTargetSnapshots = "SYNCOID_DELETE_TARGET_SNAPSHOTS"`
- Produces: `datamover.SenderConfig.DeleteTargetSnapshots bool`
- Produces: Syncoid argument `--delete-target-snapshots`
- Consumed by: Task 3 controller env wiring

- [ ] **Step 1: Update sender command and env tests first**

In `internal/datamover/datamover_test.go`, update `TestSenderRunsSyncoidWithConfiguredSnapshotOptions`:

```go
DeleteTargetSnapshots: true,
```

Add `--delete-target-snapshots` to the expected command after `--identifier=zrc-123`:

```go
want := "--no-sync-snap --no-rollback --no-privilege-elevation --compress=zstd-fast --identifier=zrc-123 --delete-target-snapshots --sshoption=UserKnownHostsFile=/var/run/zfsrep/ssh/known_hosts --sshoption=StrictHostKeyChecking=yes --sshoption=IdentitiesOnly=yes --sshkey=/var/run/zfsrep/ssh/id_rsa --sshport=2222 --no-resume --include-snaps=^snap-.* --include-snaps=^manual$ --exclude-snaps=.*-tmp$ tank/src root@10.0.0.42:tank/dst"
```

In `TestSenderConfigFromEnvDefaults`, add:

```go
t.Setenv("SYNCOID_DELETE_TARGET_SNAPSHOTS", "")
```

and assert:

```go
if cfg.DeleteTargetSnapshots {
	t.Fatalf("DeleteTargetSnapshots = true, want false")
}
```

In `TestSenderConfigFromEnvExplicitValuesOverrideDefaults`, add:

```go
t.Setenv("SYNCOID_DELETE_TARGET_SNAPSHOTS", "true")
```

and assert:

```go
if !sender.DeleteTargetSnapshots {
	t.Fatalf("sender DeleteTargetSnapshots = false, want true")
}
```

In `TestSenderConfigFromLookupParsesControllerEnvContract`, add to the `values` map:

```go
EnvDeleteTargetSnapshots: "true",
```

and change the Syncoid config assertion to:

```go
if !cfg.NoSyncSnap || cfg.NoRollback || !cfg.ForceDelete || !cfg.DeleteTargetSnapshots || cfg.Compress != "zstd" || cfg.SyncoidIdentifier != "zrc-123" {
	t.Fatalf("syncoid config = %#v", cfg)
}
```

- [ ] **Step 2: Update sender startup log and runtime image tests first**

In `TestSenderLogsSuccessfulSyncoidRun`, add this expected log substring:

```go
"deleteTargetSnapshots=false",
```

In `internal/datamover/runtime_image_test.go`, add this expected Dockerfile substring after the `--identifier` help check:

```go
"syncoid --help 2>&1 | grep -F -- \"--delete-target-snapshots\"",
```

- [ ] **Step 3: Run focused tests and confirm expected failures**

Run:

```bash
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover -run 'TestSenderRunsSyncoidWithConfiguredSnapshotOptions|TestSenderConfigFromEnvDefaults|TestSenderConfigFromEnvExplicitValuesOverrideDefaults|TestSenderConfigFromLookupParsesControllerEnvContract|TestSenderLogsSuccessfulSyncoidRun|TestRuntimeImagePinsSyncoid230' -count=1
```

Expected:

- Tests fail to compile because `SenderConfig.DeleteTargetSnapshots` and `EnvDeleteTargetSnapshots` do not exist.
- `TestRuntimeImagePinsSyncoid230` fails because the Dockerfile does not check the new Syncoid help flag.

- [ ] **Step 4: Implement sender env/config parsing**

In `internal/datamover/sender.go`, add the env const after `EnvForceDelete`:

```go
EnvDeleteTargetSnapshots = "SYNCOID_DELETE_TARGET_SNAPSHOTS"
```

Add the field to `SenderConfig` after `ForceDelete`:

```go
DeleteTargetSnapshots bool
```

Add this assignment in `SenderConfigFromLookup` after `ForceDelete`:

```go
DeleteTargetSnapshots: boolLookupDefault(lookup, EnvDeleteTargetSnapshots, defaults.DeleteTargetSnapshots),
```

- [ ] **Step 5: Implement sender argument and logging behavior**

In `syncoidArgs`, append the Syncoid flag after the identifier block:

```go
if cfg.DeleteTargetSnapshots {
	args = append(args, "--delete-target-snapshots")
}
```

Update `logSenderStart` to include the new boolean. Replace the existing format string with:

```go
"sender starting srcDataset=%s dstDataset=%s dstHost=%s sshPort=%s syncoidIdentifier=%s noSyncSnap=%t noRollback=%t forceDelete=%t deleteTargetSnapshots=%t compress=%s receiveUnmounted=%t receiveResumable=%t includeSnaps=%q excludeSnaps=%q"
```

and add `cfg.DeleteTargetSnapshots` between `cfg.ForceDelete` and `cfg.Compress` in the argument list.

- [ ] **Step 6: Add the Dockerfile runtime support check**

In `Dockerfile`, add this line next to the existing Syncoid help checks:

```dockerfile
    syncoid --help 2>&1 | grep -F -- "--delete-target-snapshots" && \
```

- [ ] **Step 7: Format and rerun focused tests**

Run:

```bash
gofmt -w internal/datamover/sender.go internal/datamover/datamover_test.go internal/datamover/runtime_image_test.go
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/datamover -run 'TestSenderRunsSyncoidWithConfiguredSnapshotOptions|TestSenderConfigFromEnvDefaults|TestSenderConfigFromEnvExplicitValuesOverrideDefaults|TestSenderConfigFromLookupParsesControllerEnvContract|TestSenderLogsSuccessfulSyncoidRun|TestRuntimeImagePinsSyncoid230' -count=1
```

Expected: all focused datamover tests pass.

- [ ] **Step 8: Review the task diff without committing**

Run:

```bash
git diff -- internal/datamover/sender.go internal/datamover/datamover_test.go internal/datamover/runtime_image_test.go Dockerfile
```

Expected: diff contains only env/config/log/argument plumbing and the Dockerfile Syncoid help check.

---

### Task 3: Controller Receive Policy And Receiver Authorization

**Files:**
- Modify: `api/v1alpha1/zfsreplication_types.go`
- Modify: `internal/controller/zfsreplication_run_controller.go`
- Modify: `internal/controller/zfsreplication_run_controller_test.go`
- Modify: `cmd/zfsrep-receiver/receiver_command_authorize.go`
- Modify: `cmd/zfsrep-receiver/forced_command_test.go`
- Modify: `config/crd/zfsreplication.ringhof.io_zfsreceivetasks.yaml`
- Modify: `internal/controller/rbac_manifest_test.go`

**Interfaces:**
- Consumes: `SyncoidSpec.DeleteTargetSnapshots *bool`
- Consumes: `replication.SyncoidOptions.DeleteTargetSnapshots bool`
- Consumes: `datamover.EnvDeleteTargetSnapshots`
- Produces: `ReceiveTaskPolicy.AllowTargetSnapshotDestroy bool`
- Produces: CRD boolean field `.spec.policy.allowTargetSnapshotDestroy`

- [ ] **Step 1: Update controller tests before implementation**

In `TestRunReconcileSenderJobUsesSyncoidOptions`, set the option immediately after creating the run:

```go
run.Spec.Syncoid.DeleteTargetSnapshots = ptr(true)
```

Add this sender env assertion after the `SYNCOID_NO_SYNC_SNAP` assertion:

```go
if got := envValue(sender, "SYNCOID_DELETE_TARGET_SNAPSHOTS"); got != "true" {
	t.Fatalf("SYNCOID_DELETE_TARGET_SNAPSHOTS = %q", got)
}
```

Change the round-trip Syncoid config assertion to:

```go
if !cfg.NoSyncSnap || !cfg.NoRollback || cfg.ForceDelete || !cfg.DeleteTargetSnapshots || cfg.Compress != "zstd" {
	t.Fatalf("round-tripped Syncoid config = %#v", cfg)
}
```

In `TestRunReconcileCreatesReceiveTaskBeforeSenderJob`, set the option immediately after creating the run:

```go
run.Spec.Syncoid.DeleteTargetSnapshots = ptr(true)
```

Add this assertion after the existing `AllowSyncSnapshotDestroy` assertion:

```go
if !task.Spec.Policy.AllowTargetSnapshotDestroy {
	t.Fatal("task does not allow target snapshot destroy when deleteTargetSnapshots is true")
}
```

- [ ] **Step 2: Update receiver authorization tests before implementation**

In `TestAuthorizeReceiverCommandRejectsCommandsOutsidePolicy`, add this rejected command:

```go
"zfs destroy tank/dst@manual-rescue",
```

Add this new test after `TestAuthorizeReceiverCommandEnforcesDatasetAndSnapshotBoundaries`:

```go
func TestAuthorizeReceiverCommandAllowsTargetSnapshotDestroyPolicy(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted:            true,
		ReceiveResumable:            true,
		AllowTargetSnapshotDestroy:  true,
		SyncSnapshotIdentifier:      "rel123",
		Compression:                 "none",
	})

	for _, cmd := range []string{
		"zfs destroy tank/dst@manual-rescue",
		"zfs destroy tank/dst@snap-2026-07-09",
		"zfs destroy tank/dst@manual-a; zfs destroy tank/dst@manual-b",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := authorizeReceiverCommand(cmd, policy); err != nil {
				t.Fatalf("authorizeReceiverCommand() error = %v, want nil", err)
			}
		})
	}

	for _, cmd := range []string{
		"zfs destroy tank/dst/child@manual-rescue",
		"zfs destroy tank/other@manual-rescue",
		"zfs destroy tank/dst@manual-rescue,hold",
		"zfs destroy -r tank/dst@manual-rescue",
		"zfs destroy tank/dst@manual-rescue 2>&1",
		"zfs destroy tank/dst",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := authorizeReceiverCommand(cmd, policy); err == nil {
				t.Fatal("authorizeReceiverCommand() error = nil, want rejection")
			}
		})
	}
}
```

After adding the test, run `gofmt` once. If `gofmt` aligns the struct fields differently, keep the formatted result.

- [ ] **Step 3: Add failing receive task CRD schema assertion**

In `TestReceiveTaskCRDSchemaExposesPolicyAndStatus`, add this assertion after `allowSyncSnapshotDestroy`:

```go
if spec.Properties["policy"].Properties["allowTargetSnapshotDestroy"].Type != "boolean" {
	t.Fatalf("allowTargetSnapshotDestroy schema = %#v", spec.Properties["policy"].Properties["allowTargetSnapshotDestroy"])
}
```

- [ ] **Step 4: Run focused tests and confirm expected failures**

Run:

```bash
gofmt -w internal/controller/zfsreplication_run_controller_test.go cmd/zfsrep-receiver/forced_command_test.go internal/controller/rbac_manifest_test.go
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run 'TestRunReconcileSenderJobUsesSyncoidOptions|TestRunReconcileCreatesReceiveTaskBeforeSenderJob|TestReceiveTaskCRDSchemaExposesPolicyAndStatus' -count=1
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./cmd/zfsrep-receiver -run 'TestAuthorizeReceiverCommandRejectsCommandsOutsidePolicy|TestAuthorizeReceiverCommandAllowsTargetSnapshotDestroyPolicy' -count=1
```

Expected:

- Controller tests fail to compile until `AllowTargetSnapshotDestroy` and sender env wiring exist.
- Receiver tests fail to compile until `AllowTargetSnapshotDestroy` exists.
- Receive task CRD schema assertion fails until the schema is updated.

- [ ] **Step 5: Implement controller policy and env wiring**

In `api/v1alpha1/zfsreplication_types.go`, add this field to `ReceiveTaskPolicy` after `AllowSyncSnapshotDestroy`:

```go
AllowTargetSnapshotDestroy bool `json:"allowTargetSnapshotDestroy,omitempty"`
```

In `internal/controller/zfsreplication_run_controller.go`, add this field in `runReceiveTask` after `AllowSyncSnapshotDestroy`:

```go
AllowTargetSnapshotDestroy: options.DeleteTargetSnapshots,
```

In `syncoidEnv`, add this env var after `EnvForceDelete`:

```go
{Name: datamover.EnvDeleteTargetSnapshots, Value: strconv.FormatBool(options.DeleteTargetSnapshots)},
```

In `normalizedSyncoidOptions`, add this field after `ForceDelete`:

```go
DeleteTargetSnapshots: spec.DeleteTargetSnapshots,
```

- [ ] **Step 6: Implement receiver authorization**

In `cmd/zfsrep-receiver/receiver_command_authorize.go`, replace the snapshot branch inside `zfsDestroyAllowed` with:

```go
if dataset, snapshot, ok := replication.SplitSnapshotTarget(args[1]); ok {
	if dataset != policy.TargetDataset {
		return false
	}
	return policy.AllowTargetSnapshotDestroy ||
		policy.AllowSyncSnapshotDestroy &&
			replication.SyncoidSnapshotTarget(snapshot, policy.SyncSnapshotIdentifier)
}
```

Keep the dataset-destroy branch below it unchanged:

```go
return policy.AllowDestroy && replication.DatasetOrChild(args[1], policy.TargetDataset) && !strings.Contains(args[1], "@")
```

- [ ] **Step 7: Implement receive task CRD schema**

In `config/crd/zfsreplication.ringhof.io_zfsreceivetasks.yaml`, add this field under `spec.policy.properties`, immediately after `allowSyncSnapshotDestroy`:

```yaml
                    allowTargetSnapshotDestroy:
                      type: boolean
```

- [ ] **Step 8: Format and rerun focused tests**

Run:

```bash
gofmt -w api/v1alpha1/zfsreplication_types.go internal/controller/zfsreplication_run_controller.go internal/controller/zfsreplication_run_controller_test.go cmd/zfsrep-receiver/receiver_command_authorize.go cmd/zfsrep-receiver/forced_command_test.go internal/controller/rbac_manifest_test.go
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./internal/controller -run 'TestRunReconcileSenderJobUsesSyncoidOptions|TestRunReconcileCreatesReceiveTaskBeforeSenderJob|TestReceiveTaskCRDSchemaExposesPolicyAndStatus' -count=1
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./cmd/zfsrep-receiver -run 'TestAuthorizeReceiverCommandRejectsCommandsOutsidePolicy|TestAuthorizeReceiverCommandAllowsTargetSnapshotDestroyPolicy|TestAuthorizeReceiverCommandAllowsSyncoidTargetCommands|TestAuthorizeReceiverCommandEnforcesDatasetAndSnapshotBoundaries|TestAuthorizeReceiverCommandAllowsForceDeletePolicy' -count=1
```

Expected: all focused controller and receiver tests pass.

- [ ] **Step 9: Review the task diff without committing**

Run:

```bash
git diff -- api/v1alpha1/zfsreplication_types.go internal/controller/zfsreplication_run_controller.go internal/controller/zfsreplication_run_controller_test.go cmd/zfsrep-receiver/receiver_command_authorize.go cmd/zfsrep-receiver/forced_command_test.go config/crd/zfsreplication.ringhof.io_zfsreceivetasks.yaml internal/controller/rbac_manifest_test.go
```

Expected: diff shows a separate target snapshot destroy policy and does not broaden `AllowDestroy`.

---

### Task 4: Documentation And Full Verification

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: `spec.syncoid.deleteTargetSnapshots`
- Consumes: `--delete-target-snapshots`
- Produces: user-facing documentation that distinguishes `forceDelete`, Syncoid sync snapshot pruning, and target-only snapshot deletion

- [ ] **Step 1: Update README documentation**

In `README.md`, add this bullet after `forceDelete` in the Syncoid options section:

```markdown
- `deleteTargetSnapshots`: pass `--delete-target-snapshots`. Defaults to
  false. When true, Syncoid may destroy snapshots that exist only on the target
  after a successful sync. Use it only for strict mirror targets whose snapshot
  lifecycle is owned by the source.
```

In the operational notes, replace the current `forceDelete` note with:

```markdown
`forceDelete` is destructive. When enabled, the sender passes `--force-delete`
to Syncoid so it may destroy conflicting target datasets.

`deleteTargetSnapshots` is also destructive. When enabled, the sender passes
`--delete-target-snapshots` to Syncoid so it may destroy snapshots that exist
only on the target. Do not enable it when target-local rollback, rescue, or
inspection snapshots must be preserved.
```

- [ ] **Step 2: Run formatting and full test suite**

Run:

```bash
gofmt -w api/v1alpha1/zfsreplication_types.go api/v1alpha1/zz_generated.deepcopy.go api/v1alpha1/zfsreplication_types_test.go internal/replication/replication.go internal/replication/replication_test.go internal/datamover/sender.go internal/datamover/datamover_test.go internal/datamover/runtime_image_test.go internal/controller/zfsreplication_run_controller.go internal/controller/zfsreplication_run_controller_test.go internal/controller/rbac_manifest_test.go cmd/zfsrep-receiver/receiver_command_authorize.go cmd/zfsrep-receiver/forced_command_test.go
GOCACHE=/private/tmp/zfsreplicationcontroller-go-build go test ./...
```

Expected: `go test ./...` passes.

- [ ] **Step 3: Run lint if available**

Run:

```bash
golangci-lint run
```

Expected: lint passes. If the command is unavailable in the environment, record the exact shell error in the final summary and do not claim lint passed.

- [ ] **Step 4: Review all changes without committing**

Run:

```bash
git diff --stat
git diff -- api/v1alpha1/zfsreplication_types.go api/v1alpha1/zz_generated.deepcopy.go api/v1alpha1/zfsreplication_types_test.go internal/replication/replication.go internal/replication/replication_test.go internal/datamover/sender.go internal/datamover/datamover_test.go internal/datamover/runtime_image_test.go Dockerfile internal/controller/zfsreplication_run_controller.go internal/controller/zfsreplication_run_controller_test.go cmd/zfsrep-receiver/receiver_command_authorize.go cmd/zfsrep-receiver/forced_command_test.go config/crd/zfsreplication.ringhof.io_zfsreplicationruns.yaml config/crd/zfsreplication.ringhof.io_zfsreplicationschedules.yaml config/crd/zfsreplication.ringhof.io_zfsreceivetasks.yaml internal/controller/rbac_manifest_test.go README.md docs/superpowers/specs/2026-07-09-delete-target-snapshots-design.md docs/superpowers/plans/2026-07-09-delete-target-snapshots.md
```

Expected:

- No sample manifests enable `deleteTargetSnapshots`.
- `AllowDestroy` still rejects snapshot targets.
- `AllowSyncSnapshotDestroy` still requires the Syncoid relationship snapshot prefix.
- `AllowTargetSnapshotDestroy` allows exact target snapshots only.
- Documentation calls out destructive behavior.

---

## Self-Review

- Spec coverage: Task 1 covers API normalization and run/schedule CRD schema. Task 2 covers sender env, logs, runtime support, and Syncoid args. Task 3 covers controller receive policy and receiver authorization. Task 4 covers documentation and full verification.
- Placeholder scan: this plan contains no deferred implementation markers.
- Type consistency: the plan uses `DeleteTargetSnapshots` for API/option/sender config fields, `EnvDeleteTargetSnapshots` for the env constant, `SYNCOID_DELETE_TARGET_SNAPSHOTS` for the env var, and `AllowTargetSnapshotDestroy` for receiver policy.
