package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
)

func TestAuthorizeReceiverCommandAllowsSyncoidTargetCommands(t *testing.T) {
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
			if _, err := authorizeReceiverCommand(cmd, policy); err != nil {
				t.Fatalf("authorizeReceiverCommand() error = %v, want nil", err)
			}
		})
	}
}

func TestAuthorizeReceiverCommandRejectsCommandsOutsidePolicy(t *testing.T) {
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
		"zfs destroy tank/dst@syncoid_2026-07-04",
		"zfs destroy tank/dst@syncoid_rel123_worker_old",
		"zfs destroy tank/dst@syncoid_old; id",
		"zfs destroy -r tank/dst",
		"command -v busybox",
		"command -v zstd",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := authorizeReceiverCommand(cmd, policy); err == nil {
				t.Fatal("authorizeReceiverCommand() error = nil, want rejection")
			}
		})
	}
}

func TestAuthorizeReceiverCommandRejectsUnsafeReceiveFlags(t *testing.T) {
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
			if _, err := authorizeReceiverCommand(cmd, policy); err == nil {
				t.Fatal("authorizeReceiverCommand() error = nil, want rejection")
			}
		})
	}
}

func TestAuthorizeReceiverCommandAllowsMountedReceiveOnlyWhenPolicyAllowsMount(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted: true,
		ReceiveResumable: true,
		Compression:      "none",
	})
	cmd := "zfs receive -s tank/dst"

	if _, err := authorizeReceiverCommand(cmd, policy); err == nil {
		t.Fatal("authorizeReceiverCommand() error = nil, want mounted receive rejection")
	}

	policy.AllowMount = true
	if _, err := authorizeReceiverCommand(cmd, policy); err != nil {
		t.Fatalf("authorizeReceiverCommand() error = %v, want mounted receive allowed", err)
	}

	policy.ReceiveUnmounted = false
	policy.AllowMount = false
	if _, err := authorizeReceiverCommand(cmd, policy); err == nil {
		t.Fatal("authorizeReceiverCommand() error = nil, want mounted receive rejection without allowMount")
	}
}

func TestAuthorizeReceiverCommandEnforcesDatasetAndSnapshotBoundaries(t *testing.T) {
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
			if _, err := authorizeReceiverCommand(cmd, policy); err == nil {
				t.Fatal("authorizeReceiverCommand() error = nil, want rejection")
			}
		})
	}

	for _, cmd := range []string{
		"zfs destroy tank/app/child",
		"zfs destroy -r tank/app/child",
		"zfs destroy tank/app@syncoid_rel123_worker_2026-07-04",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := authorizeReceiverCommand(cmd, policy); err != nil {
				t.Fatalf("authorizeReceiverCommand() error = %v, want nil", err)
			}
		})
	}
}

func TestAuthorizeReceiverCommandLimitsDestroyBatches(t *testing.T) {
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

	if _, err := authorizeReceiverCommand(strings.Join(parts, "; "), policy); err == nil {
		t.Fatal("authorizeReceiverCommand() error = nil, want batch size rejection")
	}
}

func TestAuthorizeReceiverCommandLimitsCommandLength(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted: true,
		ReceiveResumable: true,
		Compression:      "none",
	})

	if _, err := authorizeReceiverCommand(strings.Repeat("x", maxReceiverCommandLength+1), policy); err == nil {
		t.Fatal("authorizeReceiverCommand() error = nil, want command length rejection")
	}
}

func TestAuthorizeReceiverCommandAllowsForceDeletePolicy(t *testing.T) {
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
			if _, err := authorizeReceiverCommand(cmd, policy); err != nil {
				t.Fatalf("authorizeReceiverCommand() error = %v, want nil", err)
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

func TestReadReceiverPolicyRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.json")
	if err := os.WriteFile(target, []byte(`{"targetDataset":"tank/dst","receiveUnmounted":true,"compression":"none"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "policy-0123456789abcdef0123456789abcdef.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	if _, err := readReceiverPolicy(link); err == nil {
		t.Fatal("readReceiverPolicy() error = nil, want symlink rejection")
	}
}

func TestReadReceiverPolicyNormalizesLegacyMountedReceivePolicy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy-0123456789abcdef0123456789abcdef.json")
	if err := os.WriteFile(path, []byte(`{"targetDataset":"tank/dst","receiveUnmounted":false,"receiveResumable":true,"compression":"none"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	policy, err := readReceiverPolicy(path)
	if err != nil {
		t.Fatal(err)
	}
	if !policy.AllowMount {
		t.Fatal("AllowMount = false, want legacy receiveUnmounted=false policy to allow mounted receive")
	}
	if _, err := authorizeReceiverCommand("zfs receive -s tank/dst", policy); err != nil {
		t.Fatalf("authorizeReceiverCommand() error = %v, want mounted receive allowed", err)
	}
}

func TestReadReceiverPolicyPreservesExplicitMountedReceiveDenial(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy-0123456789abcdef0123456789abcdef.json")
	if err := os.WriteFile(path, []byte(`{"targetDataset":"tank/dst","receiveUnmounted":false,"allowMount":false,"receiveResumable":true,"compression":"none"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	policy, err := readReceiverPolicy(path)
	if err != nil {
		t.Fatal(err)
	}
	if policy.AllowMount {
		t.Fatal("AllowMount = true, want explicit allowMount=false preserved")
	}
	if _, err := authorizeReceiverCommand("zfs receive -s tank/dst", policy); err == nil {
		t.Fatal("authorizeReceiverCommand() error = nil, want mounted receive rejected")
	}
}

func TestAuthorizeReceiverCommandUsesCompressionPolicy(t *testing.T) {
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
			if _, err := authorizeReceiverCommand(cmd, policy); err != nil {
				t.Fatalf("authorizeReceiverCommand() error = %v, want nil", err)
			}
		})
	}
	if _, err := authorizeReceiverCommand("mbuffer -q | gzip -dc | zfs receive -u -s tank/dst 2>&1", policy); err == nil {
		t.Fatal("authorizeReceiverCommand() error = nil, want compressor mismatch rejection")
	}
}

func TestAuthorizeReceiverCommandAllowsSyncoidDecompressorForms(t *testing.T) {
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
			if _, err := authorizeReceiverCommand(tt.command, policy); err != nil {
				t.Fatalf("authorizeReceiverCommand() error = %v, want nil", err)
			}
		})
	}
}

func TestExecuteReceiverCommandPlanEmulatesBuiltins(t *testing.T) {
	var stdout bytes.Buffer
	err := executeReceiverCommandPlan(context.Background(), forcedCommandConfig{Stdout: &stdout}, receiverCommandPlan{
		kind:     receiverCommandEcho,
		echoArgs: []string{"hello", "world"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "hello world" {
		t.Fatalf("stdout = %q, want echo output", stdout.String())
	}
}

func TestExecuteReceiverCommandPlanEmulatesProcessList(t *testing.T) {
	policy := testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{
		ReceiveUnmounted: true,
		ReceiveResumable: true,
		Compression:      "none",
	})
	plan, err := authorizeReceiverCommand("ps -Ao args=", policy)
	if err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	if err := executeReceiverCommandPlan(context.Background(), forcedCommandConfig{Stdout: &stdout}, plan); err != nil {
		t.Fatal(err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("emulated ps stdout = %q, want empty process list", stdout.String())
	}
}

func TestExecuteReceiverPipelineUsesMinimalEnvironment(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, "env.txt")
	script := filepath.Join(dir, "zfs")
	writeScript(t, script, "#!/bin/sh\nprintf '%s|%s|%s|%s\\n' \"$SSH_ORIGINAL_COMMAND\" \"$LD_PRELOAD\" \"$LC_ALL\" \"$LANG\" > \"$1\"\n")
	restore := replaceAllowedCommandResolver(t, map[string]string{"zfs": script})
	defer restore()
	t.Setenv("SSH_ORIGINAL_COMMAND", "attacker-controlled")
	t.Setenv("LD_PRELOAD", "/tmp/injected.dylib")

	err := executeReceiverPipeline(context.Background(), strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{}, []receiverCommandStep{
		{Name: "zfs", Args: []string{envPath}},
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(envPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(data)); got != "||C|C" {
		t.Fatalf("child environment = %q, want only explicit locale values", got)
	}
}

func TestExecuteReceiverPipelineCancelsAndWaitsForRemainingProcesses(t *testing.T) {
	dir := t.TempDir()
	fail := filepath.Join(dir, "mbuffer")
	pidFile := filepath.Join(dir, "zfs.pid")
	block := filepath.Join(dir, "zfs")
	writeScript(t, fail, "#!/bin/sh\nsleep 0.5\nexit 42\n")
	writeScript(t, block, "#!/bin/sh\nprintf '%s' \"$$\" > \"$1\"\nwhile :; do sleep 1; done\n")
	restore := replaceAllowedCommandResolver(t, map[string]string{
		"mbuffer": fail,
		"zfs":     block,
	})
	defer restore()

	err := executeReceiverPipeline(context.Background(), strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{}, []receiverCommandStep{
		{Name: "mbuffer"},
		{Name: "zfs", Args: []string{pidFile}},
	})
	if err == nil {
		t.Fatal("executeReceiverPipeline() error = nil, want pipeline failure")
	}
	waitForFile(t, pidFile)
	pidText, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(pidText))
	if err != nil {
		t.Fatal(err)
	}
	if processExists(pid) {
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("kill downstream process %d: %v", pid, err)
		}
		t.Fatalf("downstream process %d is still running after pipeline returned", pid)
	}
}

func TestRunForcedCommandRejectsMissingOriginalCommand(t *testing.T) {
	err := runForcedCommand(context.Background(), forcedCommandConfig{
		OriginalCommand: "",
		Policy:          testReceiverPolicy("tank/dst", zfsv1.ReceiveTaskPolicy{Compression: "none"}),
	})
	if err == nil {
		t.Fatal("runForcedCommand() error = nil, want missing command rejection")
	}
}

func writeScript(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
}

func replaceAllowedCommandResolver(t *testing.T, paths map[string]string) func() {
	t.Helper()
	previous := resolveAllowedCommand
	resolveAllowedCommand = func(name string) (string, error) {
		if path, ok := paths[name]; ok {
			return path, nil
		}
		return "", errors.New("unexpected command: " + name)
	}
	return func() {
		resolveAllowedCommand = previous
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func processExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func testReceiverPolicy(targetDataset string, policy zfsv1.ReceiveTaskPolicy) receiverCommandPolicy {
	return receiverCommandPolicy{
		TargetDataset:     targetDataset,
		ReceiveTaskPolicy: policy,
	}
}
