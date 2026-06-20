package datamover

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

type call struct {
	name string
	args []string
}

type fakeRunner struct {
	calls        []call
	snapshots    map[string]bool
	guids        map[string]string
	failSnapshot bool
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) (string, string, error) {
	f.calls = append(f.calls, call{name: name, args: args})
	if name == "syncoid" {
		f.replicateSyncoid(args)
		return "", "", nil
	}
	if len(args) >= 4 && args[0] == "list" && args[3] != "" {
		if f.snapshots[args[len(args)-1]] {
			return args[len(args)-1], "", nil
		}
		return "", "not found", errFake
	}
	if args[0] == "snapshot" {
		if f.failSnapshot {
			return "", "snapshot failed", errFake
		}
		f.snapshots[args[1]] = true
		return "", "", nil
	}
	if args[0] == "get" && args[4] == "guid" {
		snap := args[5]
		if !f.snapshots[snap] {
			return "", "not found", errFake
		}
		if guid := f.guids[snap]; guid != "" {
			return guid + "\n", "", nil
		}
		return "123\n", "", nil
	}
	return "", "", nil
}

func (f *fakeRunner) replicateSyncoid(args []string) {
	if len(args) < 2 {
		return
	}
	srcDataset := args[len(args)-2]
	dstDataset := args[len(args)-1]
	if _, dataset, ok := strings.Cut(dstDataset, ":"); ok {
		dstDataset = dataset
	}
	snapshotName := ""
	for _, arg := range args {
		if strings.HasPrefix(arg, "--include-snaps=^") && strings.HasSuffix(arg, "$") {
			snapshotName = strings.TrimSuffix(strings.TrimPrefix(arg, "--include-snaps=^"), "$")
			break
		}
	}
	if snapshotName == "" {
		return
	}
	srcSnap := srcDataset + "@" + snapshotName
	if !f.snapshots[srcSnap] {
		return
	}
	dstSnap := dstDataset + "@" + snapshotName
	f.snapshots[dstSnap] = true
	if f.guids == nil {
		f.guids = map[string]string{}
	}
	f.guids[dstSnap] = f.guids[srcSnap]
}

type fakeErr struct{}

func (fakeErr) Error() string { return "fake error" }

var errFake error = fakeErr{}

func TestSenderRunsSyncoidForRunSnapshot(t *testing.T) {
	runner := &fakeRunner{snapshots: map[string]bool{
		"tank/src@zsync-run-1": true,
		"tank/src@zsync-run-0": true,
	}}
	guid, err := RunSender(context.Background(), SenderConfig{
		RunID:            "run-1",
		SnapshotPrefix:   "zsync",
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		SSHPort:          "2222",
		BaseSnapshot:     "zsync-run-0",
		BootstrapMode:    "FailIfNoBase",
		ReceiveUnmounted: true,
		ReceiveResumable: true,
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	if guid != "123" {
		t.Fatalf("guid = %q", guid)
	}
	want := "--no-sync-snap --no-rollback --compress=none --sshoption=StrictHostKeyChecking=no --sshoption=UserKnownHostsFile=/dev/null --sshkey=/var/run/zfsrep/ssh/id_rsa --sshport=2222 --recvoptions=u --include-snaps=^zsync-run-1$ --include-snaps=^zsync-run-0$ tank/src root@10.0.0.42:tank/dst"
	if !hasNamedCall(runner.calls, "syncoid", want) {
		t.Fatalf("syncoid was not called with %q: %#v", want, runner.calls)
	}
	if hasCall(runner.calls, "send -i tank/src@zsync-run-0 tank/src@zsync-run-1") {
		t.Fatalf("zfs send should not be called directly when using syncoid: %#v", runner.calls)
	}
}

func TestSenderConfigFromEnvDefaults(t *testing.T) {
	t.Setenv("SNAPSHOT_PREFIX", "")
	t.Setenv("RECEIVE_UNMOUNTED", "")
	t.Setenv("RECEIVE_RESUMABLE", "")
	cfg := SenderConfigFromEnv()
	if cfg.SnapshotPrefix != "zsync" {
		t.Fatalf("SnapshotPrefix = %q, want zsync", cfg.SnapshotPrefix)
	}
	if !cfg.ReceiveUnmounted {
		t.Fatalf("ReceiveUnmounted = false, want true")
	}
	if !cfg.ReceiveResumable {
		t.Fatalf("ReceiveResumable = false, want true")
	}
}

func TestExecRunnerMirrorsSuccessfulStderr(t *testing.T) {
	oldStderr := os.Stderr
	readStderr, writeStderr, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.Stderr = oldStderr
		if err := readStderr.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Errorf("close stderr pipe reader: %v", err)
		}
	})
	os.Stderr = writeStderr

	stdout, stderr, err := ExecRunner{}.Run(context.Background(), "sh", "-c", "printf stdout; printf stderr >&2")
	if err != nil {
		t.Fatal(err)
	}
	if err := writeStderr.Close(); err != nil {
		t.Fatal(err)
	}
	mirrored, err := io.ReadAll(readStderr)
	if err != nil {
		t.Fatal(err)
	}

	if stdout != "stdout" {
		t.Fatalf("stdout = %q", stdout)
	}
	if stderr != "stderr" {
		t.Fatalf("stderr = %q", stderr)
	}
	if string(mirrored) != "stderr" {
		t.Fatalf("mirrored stderr = %q", string(mirrored))
	}
}

func TestSenderConfigFromEnvExplicitValuesOverrideDefaults(t *testing.T) {
	t.Setenv("SNAPSHOT_PREFIX", "nightly")
	t.Setenv("BOOTSTRAP_MODE", BootstrapDestroyTargetAndReceiveFull)
	t.Setenv("RECEIVE_UNMOUNTED", "false")
	t.Setenv("RECEIVE_RESUMABLE", "false")

	sender := SenderConfigFromEnv()
	if sender.SnapshotPrefix != "nightly" {
		t.Fatalf("sender SnapshotPrefix = %q, want nightly", sender.SnapshotPrefix)
	}
	if sender.BootstrapMode != BootstrapDestroyTargetAndReceiveFull {
		t.Fatalf("sender BootstrapMode = %q, want %s", sender.BootstrapMode, BootstrapDestroyTargetAndReceiveFull)
	}
	if sender.ReceiveUnmounted {
		t.Fatalf("sender ReceiveUnmounted = true, want false")
	}
	if sender.ReceiveResumable {
		t.Fatalf("sender ReceiveResumable = true, want false")
	}
}

func TestSenderExitsBeforeWorkWhenNodeMismatch(t *testing.T) {
	runner := &fakeRunner{snapshots: map[string]bool{}}
	_, err := RunSender(context.Background(), SenderConfig{
		ExpectedNode: "worker-a",
		ActualNode:   "worker-b",
	}, runner)
	if err == nil || !strings.Contains(err.Error(), "node verification failed") {
		t.Fatalf("error = %v", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("zfs calls = %#v", runner.calls)
	}
}

func TestSenderRefusesFullWhenBootstrapDisabled(t *testing.T) {
	runner := &fakeRunner{snapshots: map[string]bool{}}
	_, err := RunSender(context.Background(), SenderConfig{
		RunID:          "run-1",
		SnapshotPrefix: "zsync",
		SrcDataset:     "tank/src",
		BootstrapMode:  "FailIfNoBase",
	}, runner)
	if err == nil || !strings.Contains(err.Error(), "no base snapshot") {
		t.Fatalf("error = %v", err)
	}
}

func TestSenderPassesForceDeleteForDestructiveBootstrap(t *testing.T) {
	runner := &fakeRunner{snapshots: map[string]bool{}}
	_, err := RunSender(context.Background(), SenderConfig{
		RunID:            "run-1",
		SnapshotPrefix:   "zsync",
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		SSHPort:          "2222",
		BootstrapMode:    BootstrapDestroyTargetAndReceiveFull,
		ReceiveUnmounted: true,
		ReceiveResumable: true,
	}, runner)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	want := "--no-sync-snap --no-rollback --compress=none --sshoption=StrictHostKeyChecking=no --sshoption=UserKnownHostsFile=/dev/null --sshkey=/var/run/zfsrep/ssh/id_rsa --sshport=2222 --recvoptions=u --include-snaps=^zsync-run-1$ --force-delete tank/src root@10.0.0.42:tank/dst"
	if !hasNamedCall(runner.calls, "syncoid", want) {
		t.Fatalf("destructive bootstrap syncoid call missing %q: %#v", want, runner.calls)
	}
}

func hasCall(calls []call, args string) bool {
	return callIndexNamed(calls, "zfs", args) != -1
}

func hasNamedCall(calls []call, name, args string) bool {
	return callIndexNamed(calls, name, args) != -1
}

func callIndexNamed(calls []call, name, args string) int {
	for i, c := range calls {
		if c.name == name && strings.Join(c.args, " ") == args {
			return i
		}
	}
	return -1
}
