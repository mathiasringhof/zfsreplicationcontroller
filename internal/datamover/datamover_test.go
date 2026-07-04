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
	calls []call
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) (string, string, error) {
	f.calls = append(f.calls, call{name: name, args: args})
	return "", "", nil
}

func TestSenderRunsSyncoidWithConfiguredSnapshotOptions(t *testing.T) {
	runner := &fakeRunner{}
	err := RunSender(context.Background(), SenderConfig{
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:   "/var/run/zfsrep/ssh/known_hosts",
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
	want := "--no-sync-snap --no-rollback --no-privilege-elevation --compress=zstd-fast --sshoption=UserKnownHostsFile=/var/run/zfsrep/ssh/known_hosts --sshoption=StrictHostKeyChecking=yes --sshoption=IdentitiesOnly=yes --sshkey=/var/run/zfsrep/ssh/id_rsa --sshport=2222 --no-resume --include-snaps=^snap-.* --include-snaps=^manual$ --exclude-snaps=.*-tmp$ tank/src root@10.0.0.42:tank/dst"
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
	runner := &fakeRunner{}
	err := RunSender(context.Background(), SenderConfig{
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
	runner := &fakeRunner{}
	err := RunSender(context.Background(), SenderConfig{
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:   "/var/run/zfsrep/ssh/known_hosts",
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
	want := "--no-rollback --no-privilege-elevation --compress=none --sshoption=UserKnownHostsFile=/var/run/zfsrep/ssh/known_hosts --sshoption=StrictHostKeyChecking=yes --sshoption=IdentitiesOnly=yes --sshkey=/var/run/zfsrep/ssh/id_rsa --sshport=2222 --recvoptions=u --force-delete tank/src root@10.0.0.42:tank/dst"
	if !hasNamedCall(runner.calls, "syncoid", want) {
		t.Fatalf("force-delete syncoid call missing %q: %#v", want, runner.calls)
	}
}

func TestSenderRejectsUnknownCompression(t *testing.T) {
	runner := &fakeRunner{}
	err := RunSender(context.Background(), SenderConfig{
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:   "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:          "2222",
		NoRollback:       true,
		Compress:         "sh",
		ReceiveUnmounted: true,
		ReceiveResumable: true,
	}, runner)
	if err == nil || !strings.Contains(err.Error(), "unsupported compression") {
		t.Fatalf("RunSender() error = %v, want unsupported compression", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("syncoid calls = %#v, want none", runner.calls)
	}
}

func TestSenderNormalizesCompressionAliasesForSyncoid(t *testing.T) {
	for _, tt := range []struct {
		name     string
		compress string
		want     string
	}{
		{name: "none", compress: "none", want: "--compress=none"},
		{name: "pigz", compress: "pigz", want: "--compress=pigz-fast"},
		{name: "zstd", compress: "zstd", want: "--compress=zstd-fast"},
		{name: "zstdmt", compress: "zstdmt", want: "--compress=zstdmt-fast"},
		{name: "lzop", compress: "lzop", want: "--compress=lzo"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			runner := &fakeRunner{}
			err := RunSender(context.Background(), SenderConfig{
				SrcDataset:       "tank/src",
				DstHost:          "root@10.0.0.42",
				DstDataset:       "tank/dst",
				SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
				KnownHostsFile:   "/var/run/zfsrep/ssh/known_hosts",
				SSHPort:          "2222",
				NoRollback:       true,
				Compress:         tt.compress,
				ReceiveUnmounted: true,
				ReceiveResumable: true,
			}, runner)
			if err != nil {
				t.Fatal(err)
			}
			if len(runner.calls) != 1 {
				t.Fatalf("calls = %#v, want one syncoid call", runner.calls)
			}
			if !strings.Contains(strings.Join(runner.calls[0].args, " "), tt.want) {
				t.Fatalf("syncoid args = %q, want %q", strings.Join(runner.calls[0].args, " "), tt.want)
			}
		})
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
