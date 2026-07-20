package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/receiverauthorization"
)

const testReceiverPolicyID = "policy-0123456789abcdef0123456789abcdef"

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

func TestWriteReceiverPoliciesUsesPolicyIDsAndDoesNotFollowSymlinks(t *testing.T) {
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte("outside\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	id := "policy-0123456789abcdef0123456789abcdef"
	if err := os.Symlink(outside, filepath.Join(dir, id+".json")); err != nil {
		t.Fatal(err)
	}

	err := writeReceiverPolicies(dir, map[string]receiverCommandPolicy{
		id: testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
			ReceiveUnmounted: true,
			Compression:      "none",
		}),
	})
	if err != nil {
		t.Fatalf("writeReceiverPolicies() error = %v", err)
	}
	data, err := os.ReadFile(outside)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "outside\n" {
		t.Fatalf("symlink target was overwritten: %q", data)
	}
	info, err := os.Lstat(filepath.Join(dir, id+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("policy path is still a symlink: %s", info.Mode())
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("policy mode = %o, want 0600", got)
	}

	if err := writeReceiverPolicies(dir, map[string]receiverCommandPolicy{"../escape": {TargetDataset: "tank/dst"}}); err == nil {
		t.Fatal("writeReceiverPolicies() error = nil, want unsafe policy ID rejection")
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
	if err := writeReceiverPolicies(dir, map[string]receiverCommandPolicy{testReceiverPolicyID: policy}); err != nil {
		t.Fatal(err)
	}
	err := runForcedCommand(context.Background(), forcedCommandConfig{
		Authorization:   receiverauthorization.New(dir),
		Reference:       testAuthorizationReference(t),
		OriginalCommand: "",
	})
	if err == nil {
		t.Fatal("runForcedCommand() error = nil, want missing command rejection")
	}
}

func TestRunForcedCommandUsesOpaqueAuthorizationReference(t *testing.T) {
	dir := t.TempDir()
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{Compression: "none"})
	if err := writeReceiverPolicies(dir, map[string]receiverCommandPolicy{testReceiverPolicyID: policy}); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer

	err := runForcedCommand(context.Background(), forcedCommandConfig{
		Authorization:   receiverauthorization.New(dir),
		Reference:       testAuthorizationReference(t),
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
	data, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	fields["allowMount"] = json.RawMessage("false")
	if policy.AllowMount {
		fields["allowMount"] = json.RawMessage("true")
	}
	data, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, testReceiverPolicyID+".json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	return receiverauthorization.New(dir).Admit(testAuthorizationReference(t), raw)
}

func testAuthorizationReference(t *testing.T) receiverauthorization.Reference {
	t.Helper()
	reference, err := receiverauthorization.ReferenceFromArgs([]string{"--policy-id", testReceiverPolicyID})
	if err != nil {
		t.Fatal(err)
	}
	return reference
}
