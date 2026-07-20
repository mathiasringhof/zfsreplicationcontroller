package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/receiverauthorization"
)

const (
	testReceiverTaskUID  = "11111111-2222-3333-4444-555555555555"
	testReceiverPolicyID = "grant-666ff6ccaa5b3c07feaa3a95d3a4bd2c46ac9e9abdb09ca9133528d3dc1e8952"
)

type receiverCommandPolicy struct {
	TargetDataset string
	zfsv1.ReceiveTaskPolicy
}

func TestModuleAdmissionAllowsSyncoidTargetCommands(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted:         true,
		ReceiveResumable:         true,
		AllowSyncSnapshotDestroy: true,
		SyncSnapshotIdentifier:   "rel123",
		Compression:              "none",
	})

	for _, cmd := range []string{
		"exit",
		"echo -n hello",
		"command -v mbuffer",
		"command -v mbuffer 2>/dev/null",
		"zpool get -o value -H feature@extensible_dataset tank 2>/dev/null",
		"zpool get -o value -H feature@extensible_dataset tank | grep '(active|enabled)' >/dev/null 2>&1",
		"zpool get -o value -H feature@extensible_dataset tank 2>/dev/null | grep '\\(active\\|enabled\\)' >/dev/null 2>&1",
		"ps -Ao args=",
		"zfs get -H name tank/dst",
		"zfs get -H receive_resume_token tank/dst",
		"zfs get -Hpd 1 -t snapshot guid,creation tank/dst",
		"zfs get -Hpd 1 type,guid,creation tank/dst",
		"zfs get -H -p used tank/dst",
		"mbuffer -q -s 128k -m 16M | zfs receive -u -s tank/dst 2>&1",
		"zfs receive -A tank/dst",
		"zfs destroy tank/dst@syncoid_rel123_worker_2026-07-04:10:00:00-GMT00:00",
		"zfs destroy tank/dst@syncoid_rel123_worker_old; zfs destroy tank/dst@syncoid_rel123_worker_older",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := admitReceiverCommand(t, cmd, policy); err != nil {
				t.Fatalf("Admit() error = %v, want nil", err)
			}
		})
	}
}

func TestModuleAdmissionRejectsCommandsOutsidePolicy(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted: true,
		ReceiveResumable: true,
		Compression:      "none",
	})

	for _, cmd := range []string{
		"id",
		"sh -c id",
		"zfs list",
		"zfs get -H name tank/other",
		"zfs receive -u -s tank/other 2>&1",
		"zfs receive -u -s tank/dst; id",
		"zfs receive -u -s tank/dst $(id)",
		"zfs destroy tank/dst@manual-rescue",
		"zfs destroy tank/dst@syncoid_2026-07-04",
		"zfs destroy tank/dst@syncoid_rel123_worker_old",
		"zfs destroy tank/dst@syncoid_old; id",
		"zfs destroy -r tank/dst",
		"command -v busybox",
		"command -v zstd",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := admitReceiverCommand(t, cmd, policy); err == nil {
				t.Fatal("Admit() error = nil, want rejection")
			}
		})
	}
}

func TestModuleAdmissionRejectsUnsafeReceiveFlags(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted: true,
		ReceiveResumable: true,
		Compression:      "none",
	})

	for _, cmd := range []string{
		"zfs receive -F -u -s tank/dst",
		"zfs receive -d -u -s tank/dst",
		"zfs receive -e -u -s tank/dst",
		"zfs receive -o readonly=on -u -s tank/dst",
		"zfs receive -x mountpoint -u -s tank/dst",
		"zfs receive -M -u -s tank/dst",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := admitReceiverCommand(t, cmd, policy); err == nil {
				t.Fatal("Admit() error = nil, want rejection")
			}
		})
	}
}

func TestModuleAdmissionAllowsMountedReceiveOnlyWhenPolicyAllowsMount(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted: true,
		ReceiveResumable: true,
		Compression:      "none",
	})
	cmd := "zfs receive -s tank/dst"

	if _, err := admitReceiverCommand(t, cmd, policy); err == nil {
		t.Fatal("Admit() error = nil, want mounted receive rejection")
	}

	policy.AllowMount = true
	if _, err := admitReceiverCommand(t, cmd, policy); err != nil {
		t.Fatalf("Admit() error = %v, want mounted receive allowed", err)
	}

	policy.ReceiveUnmounted = false
	policy.AllowMount = false
	if _, err := admitReceiverCommand(t, cmd, policy); err == nil {
		t.Fatal("Admit() error = nil, want mounted receive rejection without allowMount")
	}
}

func TestModuleAdmissionEnforcesDatasetAndSnapshotBoundaries(t *testing.T) {
	policy := testReceiverPolicy("tank/app", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted:         true,
		ReceiveResumable:         true,
		AllowDestroy:             true,
		AllowSyncSnapshotDestroy: true,
		SyncSnapshotIdentifier:   "rel123",
		Compression:              "none",
	})

	for _, cmd := range []string{
		"zfs destroy tank/app2",
		"zfs destroy tank/app-evil",
		"zfs destroy tank/app@syncoid_other_worker_2026-07-04",
		"zfs destroy tank/app@syncoid_rel123bad_worker_2026-07-04",
		"zfs destroy tank/app@syncoid_rel123_worker_2026-07-04,hold",
		"zfs destroy -R tank/app",
		"zfs destroy -r tank/app@syncoid_rel123_worker_2026-07-04",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := admitReceiverCommand(t, cmd, policy); err == nil {
				t.Fatal("Admit() error = nil, want rejection")
			}
		})
	}

	for _, cmd := range []string{
		"zfs destroy tank/app/child",
		"zfs destroy -r tank/app/child",
		"zfs destroy tank/app@syncoid_rel123_worker_2026-07-04",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := admitReceiverCommand(t, cmd, policy); err != nil {
				t.Fatalf("Admit() error = %v, want nil", err)
			}
		})
	}
}

func TestModuleAdmissionAllowsTargetSnapshotDestroyPolicy(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted:           true,
		ReceiveResumable:           true,
		AllowSyncSnapshotDestroy:   false,
		AllowTargetSnapshotDestroy: true,
		SyncSnapshotIdentifier:     "rel123",
		Compression:                "none",
	})

	for _, cmd := range []string{
		"zfs destroy tank/dst@manual-rescue",
		"zfs destroy tank/dst@snap-2026-07-09",
		"zfs destroy tank/dst@syncoid_other_worker_2026-07-09",
		"zfs destroy 'tank/dst@csi-snapshot-a,csi-snapshot-b,csi-snapshot-c'",
		"zfs destroy tank/dst@manual-a; zfs destroy tank/dst@manual-b",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := admitReceiverCommand(t, cmd, policy); err != nil {
				t.Fatalf("Admit() error = %v, want nil", err)
			}
		})
	}

	for _, cmd := range []string{
		"zfs destroy tank/dst/child@manual-rescue",
		"zfs destroy 'tank/dst/child@manual-rescue,hold'",
		"zfs destroy tank/other@manual-rescue",
		"zfs destroy tank/dst@manual-rescue,",
		"zfs destroy tank/dst@manual-rescue,../hold",
		"zfs destroy tank/dst@manual-rescue,tank/other@hold",
		"zfs destroy -r tank/dst@manual-rescue",
		"zfs destroy -r 'tank/dst@manual-rescue,hold'",
		"zfs destroy tank/dst@manual-rescue 2>&1",
		"zfs destroy 'tank/dst@manual-rescue,hold' 2>&1",
		"zfs destroy tank/dst",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := admitReceiverCommand(t, cmd, policy); err == nil {
				t.Fatal("Admit() error = nil, want rejection")
			}
		})
	}
}

func TestModuleAdmissionAllowsBatchedSnapshotSyntaxForSyncPolicy(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted:         true,
		ReceiveResumable:         true,
		AllowSyncSnapshotDestroy: true,
		SyncSnapshotIdentifier:   "rel123",
		Compression:              "none",
	})

	allowed := "zfs destroy 'tank/dst@syncoid_rel123_worker_old,syncoid_rel123_worker_older'"
	if _, err := admitReceiverCommand(t, allowed, policy); err != nil {
		t.Fatalf("Admit() error = %v, want nil", err)
	}

	for _, cmd := range []string{
		"zfs destroy 'tank/dst@syncoid_rel123_worker_old,manual-rescue'",
		"zfs destroy 'tank/dst@syncoid_rel123_worker_old,syncoid_other_worker_old'",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := admitReceiverCommand(t, cmd, policy); err == nil {
				t.Fatal("Admit() error = nil, want rejection")
			}
		})
	}
}

func TestModuleAdmissionLimitsDestroyBatches(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted:         true,
		ReceiveResumable:         true,
		AllowSyncSnapshotDestroy: true,
		SyncSnapshotIdentifier:   "rel123",
		Compression:              "none",
	})
	var parts []string
	for i := 0; i < 33; i++ {
		parts = append(parts, "zfs destroy tank/dst@syncoid_rel123_worker_"+strings.Repeat("x", i+1))
	}

	if _, err := admitReceiverCommand(t, strings.Join(parts, "; "), policy); err == nil {
		t.Fatal("Admit() error = nil, want batch size rejection")
	}
}

func TestModuleAdmissionLimitsCommandLength(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted: true,
		ReceiveResumable: true,
		Compression:      "none",
	})

	if _, err := admitReceiverCommand(t, strings.Repeat("x", 8193), policy); err == nil {
		t.Fatal("Admit() error = nil, want command length rejection")
	}
}

func TestModuleAdmissionAllowsForceDeletePolicy(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted: true,
		ReceiveResumable: true,
		AllowDestroy:     true,
		Compression:      "none",
	})

	for _, cmd := range []string{
		"zfs destroy -r tank/dst",
		"zfs destroy tank/dst",
		"zfs destroy -r tank/dst;",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := admitReceiverCommand(t, cmd, policy); err != nil {
				t.Fatalf("Admit() error = %v, want nil", err)
			}
		})
	}
}

func TestModuleAdmissionUsesCompressionPolicy(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted: true,
		ReceiveResumable: true,
		Compression:      "zstd",
	})

	for _, cmd := range []string{
		"command -v zstd",
		"mbuffer -q | zstd -dc | zfs receive -u -s tank/dst 2>&1",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := admitReceiverCommand(t, cmd, policy); err != nil {
				t.Fatalf("Admit() error = %v, want nil", err)
			}
		})
	}
	if _, err := admitReceiverCommand(t, "mbuffer -q | gzip -dc | zfs receive -u -s tank/dst 2>&1", policy); err == nil {
		t.Fatal("Admit() error = nil, want compressor mismatch rejection")
	}
}

func TestModuleAdmissionAllowsSyncoidDecompressorForms(t *testing.T) {
	for _, tt := range []struct {
		name        string
		compression string
		command     string
	}{
		{name: "gzip", compression: "gzip", command: "mbuffer -q | zcat | zfs receive -u -s tank/dst 2>&1"},
		{name: "pigz", compression: "pigz", command: "mbuffer -q | pigz -dc | zfs receive -u -s tank/dst 2>&1"},
		{name: "xz", compression: "xz", command: "mbuffer -q | xz -d | zfs receive -u -s tank/dst 2>&1"},
		{name: "lzop", compression: "lzop", command: "mbuffer -q | lzop -dfc | zfs receive -u -s tank/dst 2>&1"},
		{name: "lz4", compression: "lz4", command: "mbuffer -q | lz4 -dc | zfs receive -u -s tank/dst 2>&1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
				ReceiveUnmounted: true,
				ReceiveResumable: true,
				Compression:      tt.compression,
			})
			if _, err := admitReceiverCommand(t, tt.command, policy); err != nil {
				t.Fatalf("Admit() error = %v, want nil", err)
			}
		})
	}
}

func TestPlanExecuteEmulatesBuiltins(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{Compression: "none"})
	plan, err := admitReceiverCommand(t, "echo -n hello world", policy)
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	if err := plan.Execute(context.Background(), nil, &stdout, nil); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "hello world" {
		t.Fatalf("stdout = %q, want echo output", stdout.String())
	}
}

func TestPlanExecuteEmulatesProcessList(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted: true,
		ReceiveResumable: true,
		Compression:      "none",
	})
	plan, err := admitReceiverCommand(t, "ps -Ao args=", policy)
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	if err := plan.Execute(context.Background(), nil, &stdout, nil); err != nil {
		t.Fatal(err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("emulated ps stdout = %q, want empty process list", stdout.String())
	}
}

func TestRunForcedCommandRejectsMissingOriginalCommand(t *testing.T) {
	dir := t.TempDir()
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{Compression: "none"})
	if err := activateReceiverPolicyFixture(dir, policy); err != nil {
		t.Fatal(err)
	}
	err := runForcedCommand(context.Background(), forcedCommandConfig{
		Authorization:   receiverauthorization.New(testManifestPath(dir)),
		Reference:       testAuthorizationReference(t, dir),
		OriginalCommand: "",
	})
	if err == nil {
		t.Fatal("runForcedCommand() error = nil, want missing command rejection")
	}
}

func TestRunForcedCommandUsesOpaqueAuthorizationReference(t *testing.T) {
	dir := t.TempDir()
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{Compression: "none"})
	if err := activateReceiverPolicyFixture(dir, policy); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer

	err := runForcedCommand(context.Background(), forcedCommandConfig{
		Authorization:   receiverauthorization.New(testManifestPath(dir)),
		Reference:       testAuthorizationReference(t, dir),
		OriginalCommand: "echo -n hello",
		Stdout:          &stdout,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "hello" {
		t.Fatalf("stdout = %q, want %q", stdout.String(), "hello")
	}
}

func testReceiverPolicy(targetDataset string, policy zfsv1.ReceiveTaskPolicy) receiverCommandPolicy {
	return receiverCommandPolicy{
		TargetDataset:     targetDataset,
		ReceiveTaskPolicy: policy,
	}
}

func admitReceiverCommand(t *testing.T, raw string, policy receiverCommandPolicy) (receiverauthorization.Plan, error) {
	t.Helper()
	dir := t.TempDir()
	if err := activateReceiverPolicyFixture(dir, policy); err != nil {
		t.Fatal(err)
	}
	return receiverauthorization.New(testManifestPath(dir)).Admit(testAuthorizationReference(t, dir), raw)
}

func testAuthorizationReference(t *testing.T, dir string) receiverauthorization.Reference {
	t.Helper()
	data, err := os.ReadFile(testManifestPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	header := strings.Fields(strings.SplitN(string(data), "\n", 2)[0])
	if len(header) != 3 {
		t.Fatalf("manifest header = %q, want snapshot identity", data)
	}
	reference, err := receiverauthorization.ReferenceFromArgs([]string{"--snapshot-id", header[2], "--grant-id", testReceiverPolicyID})
	if err != nil {
		t.Fatal(err)
	}
	return reference
}

func activateReceiverPolicyFixture(dir string, policy receiverCommandPolicy) error {
	candidate := receiverauthorization.Candidate{
		TaskUID:                    testReceiverTaskUID,
		AuthorizedPublicKey:        validReceiverPublicKey,
		ExpiresAt:                  time.Now().Add(10 * time.Minute),
		TargetDataset:              policy.TargetDataset,
		ReceiveUnmounted:           policy.ReceiveUnmounted,
		ReceiveResumable:           policy.ReceiveResumable,
		AllowRollback:              policy.AllowRollback,
		AllowDestroy:               policy.AllowDestroy,
		AllowMount:                 policy.AllowMount,
		AllowSyncSnapshotDestroy:   policy.AllowSyncSnapshotDestroy,
		AllowTargetSnapshotDestroy: policy.AllowTargetSnapshotDestroy,
		SyncSnapshotIdentifier:     policy.SyncSnapshotIdentifier,
		Compression:                policy.Compression,
	}
	module := receiverauthorization.New(testManifestPath(dir))
	activation, err := module.Replace([]receiverauthorization.Candidate{candidate})
	if err != nil {
		return fmt.Errorf("activate receiver policy fixture: %w", err)
	}
	for _, outcome := range activation.Outcomes() {
		if outcome.Rejection != "" {
			return fmt.Errorf("compile receiver grant: %s", outcome.Rejection)
		}
	}
	return nil
}

func testManifestPath(dir string) string {
	return filepath.Join(dir, "authorized_keys")
}
