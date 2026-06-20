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

func TestSenderRunsSyncoidWithConfiguredSnapshotOptions(t *testing.T) {
	runner := &fakeRunner{snapshots: map[string]bool{}}
	guid, err := RunSender(context.Background(), SenderConfig{
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		SSHPort:          "2222",
		NoSyncSnap:       true,
		NoRollback:       true,
		Compress:         "zstd",
		ReceiveUnmounted: false,
		ReceiveResumable: false,
		IncludeSnaps:     []string{"^snap-.*", "^manual$"},
		ExcludeSnaps:     []string{".*-tmp$"},
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	if guid != "" {
		t.Fatalf("guid = %q, want empty when syncoid owns snapshot selection", guid)
	}
	want := "--no-sync-snap --no-rollback --compress=zstd --sshoption=StrictHostKeyChecking=no --sshoption=UserKnownHostsFile=/dev/null --sshkey=/var/run/zfsrep/ssh/id_rsa --sshport=2222 --no-resume --include-snaps=^snap-.* --include-snaps=^manual$ --exclude-snaps=.*-tmp$ tank/src root@10.0.0.42:tank/dst"
	if !hasNamedCall(runner.calls, "syncoid", want) {
		t.Fatalf("syncoid was not called with %q: %#v", want, runner.calls)
	}
	if hasNamedCall(runner.calls, "zfs", "snapshot tank/src@") {
		t.Fatalf("zfs snapshot should not be called when syncoid owns snapshot selection: %#v", runner.calls)
	}
}

func TestSenderConfigFromEnvDefaults(t *testing.T) {
	t.Setenv("SYNCOID_NO_SYNC_SNAP", "")
	t.Setenv("SYNCOID_NO_ROLLBACK", "")
	t.Setenv("SYNCOID_FORCE_DELETE", "")
	t.Setenv("SYNCOID_COMPRESS", "")
	t.Setenv("RECEIVE_UNMOUNTED", "")
	t.Setenv("RECEIVE_RESUMABLE", "")
	cfg := SenderConfigFromEnv()
	if cfg.NoSyncSnap {
		t.Fatalf("NoSyncSnap = true, want false")
	}
	if !cfg.NoRollback {
		t.Fatalf("NoRollback = false, want true")
	}
	if cfg.ForceDelete {
		t.Fatalf("ForceDelete = true, want false")
	}
	if cfg.Compress != "none" {
		t.Fatalf("Compress = %q, want none", cfg.Compress)
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
	t.Setenv("SYNCOID_NO_SYNC_SNAP", "true")
	t.Setenv("SYNCOID_NO_ROLLBACK", "false")
	t.Setenv("SYNCOID_FORCE_DELETE", "true")
	t.Setenv("SYNCOID_COMPRESS", "zstd")
	t.Setenv("RECEIVE_UNMOUNTED", "false")
	t.Setenv("RECEIVE_RESUMABLE", "false")
	t.Setenv("SYNCOID_INCLUDE_SNAPS", "^snap-.*\n^manual$")
	t.Setenv("SYNCOID_EXCLUDE_SNAPS", ".*-tmp$")

	sender := SenderConfigFromEnv()
	if !sender.NoSyncSnap {
		t.Fatalf("sender NoSyncSnap = false, want true")
	}
	if sender.NoRollback {
		t.Fatalf("sender NoRollback = true, want false")
	}
	if !sender.ForceDelete {
		t.Fatalf("sender ForceDelete = false, want true")
	}
	if sender.Compress != "zstd" {
		t.Fatalf("sender Compress = %q, want zstd", sender.Compress)
	}
	if sender.ReceiveUnmounted {
		t.Fatalf("sender ReceiveUnmounted = true, want false")
	}
	if sender.ReceiveResumable {
		t.Fatalf("sender ReceiveResumable = true, want false")
	}
	if strings.Join(sender.IncludeSnaps, " ") != "^snap-.* ^manual$" {
		t.Fatalf("sender IncludeSnaps = %#v", sender.IncludeSnaps)
	}
	if strings.Join(sender.ExcludeSnaps, " ") != ".*-tmp$" {
		t.Fatalf("sender ExcludeSnaps = %#v", sender.ExcludeSnaps)
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

func TestSenderPassesForceDelete(t *testing.T) {
	runner := &fakeRunner{snapshots: map[string]bool{}}
	_, err := RunSender(context.Background(), SenderConfig{
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		SSHPort:          "2222",
		NoRollback:       true,
		Compress:         "none",
		ForceDelete:      true,
		ReceiveUnmounted: true,
		ReceiveResumable: true,
	}, runner)
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	want := "--no-rollback --compress=none --sshoption=StrictHostKeyChecking=no --sshoption=UserKnownHostsFile=/dev/null --sshkey=/var/run/zfsrep/ssh/id_rsa --sshport=2222 --recvoptions=u --force-delete tank/src root@10.0.0.42:tank/dst"
	if !hasNamedCall(runner.calls, "syncoid", want) {
		t.Fatalf("force-delete syncoid call missing %q: %#v", want, runner.calls)
	}
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
