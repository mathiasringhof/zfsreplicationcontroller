package datamover

import (
	"context"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/mathias/zfsreplicationcontroller/internal/replication/diagnosis"
)

type call struct {
	name string
	args []string
}

type fakeRunner struct {
	calls  []call
	stdout string
	stderr string
	err    error
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) (string, string, error) {
	f.calls = append(f.calls, call{name: name, args: args})
	return f.stdout, f.stderr, f.err
}

type streamingFakeRunner struct {
	calls       []call
	stream      func(stdout, stderr io.Writer)
	beforeDone  func()
	err         error
	runFallback bool
}

func (f *streamingFakeRunner) Run(_ context.Context, name string, args ...string) (string, string, error) {
	f.runFallback = true
	f.calls = append(f.calls, call{name: name, args: args})
	return "", "", f.err
}

func (f *streamingFakeRunner) RunStreaming(_ context.Context, name string, stdout, stderr io.Writer, args ...string) error {
	f.calls = append(f.calls, call{name: name, args: args})
	if f.stream != nil {
		f.stream(stdout, stderr)
	}
	if f.beforeDone != nil {
		f.beforeDone()
	}
	return f.err
}

func TestSenderStreamsSyncoidOutputBeforeCommandReturns(t *testing.T) {
	var logs strings.Builder
	runner := &streamingFakeRunner{
		stream: func(stdout, stderr io.Writer) {
			if _, err := io.WriteString(stdout, "live stdout --sshkey=/var/run/zfsrep/ssh/id_rsa\n"); err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(stderr, "live stderr --sshkey=/var/run/zfsrep/ssh/id_rsa\n"); err != nil {
				t.Fatal(err)
			}
		},
		beforeDone: func() {
			out := logs.String()
			for _, want := range []string{
				"syncoid stdout live stdout --sshkey=<redacted>",
				"syncoid stderr live stderr --sshkey=<redacted>",
			} {
				if !strings.Contains(out, want) {
					t.Fatalf("logs before command returned missing %q:\n%s", want, out)
				}
			}
			if strings.Contains(out, "--sshkey=/var/run/zfsrep/ssh/id_rsa") {
				t.Fatalf("logs before command returned contain unredacted ssh key path:\n%s", out)
			}
		},
	}

	err := runSender(context.Background(), SenderConfig{
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:   "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:          "2222",
		NoRollback:       true,
		Compress:         "none",
		ReceiveUnmounted: true,
		ReceiveResumable: true,
	}, runner, &logs)
	if err != nil {
		t.Fatal(err)
	}
	if runner.runFallback {
		t.Fatal("runSender used non-streaming Run fallback")
	}
}

func TestExecRunnerProvidesStreamingCommandExecution(t *testing.T) {
	streaming, ok := any(ExecRunner{}).(interface {
		RunStreaming(context.Context, string, io.Writer, io.Writer, ...string) error
	})
	if !ok {
		t.Fatal("ExecRunner does not implement streaming execution")
	}
	var stdout, stderr strings.Builder
	if err := streaming.RunStreaming(context.Background(), "sh", &stdout, &stderr, "-c", "printf stdout; printf stderr >&2"); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "stdout" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if stderr.String() != "stderr" {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestSenderLogsSuccessfulSyncoidRun(t *testing.T) {
	runner := &fakeRunner{
		stdout: "INFO: Sending oldest full snapshot tank/src@syncoid_zrc-123_2026-07-06:12:00:00-GUID-123456 --sshkey=/var/run/zfsrep/ssh/id_rsa\n",
		stderr: "syncoid warning that should remain visible --sshkey=/var/run/zfsrep/ssh/id_rsa\n",
	}
	var logs strings.Builder

	err := runSender(context.Background(), SenderConfig{
		SrcDataset:        "tank/src",
		DstHost:           "root@10.0.0.42",
		DstDataset:        "tank/dst",
		SSHKeyFile:        "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:    "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:           "2222",
		NoRollback:        true,
		Compress:          "none",
		SyncoidIdentifier: "zrc-123",
		ReceiveUnmounted:  true,
		ReceiveResumable:  true,
	}, runner, &logs)
	if err != nil {
		t.Fatal(err)
	}

	out := logs.String()
	for _, want := range []string{
		"sender starting",
		"srcDataset=tank/src",
		"dstDataset=tank/dst",
		"dstHost=root@10.0.0.42",
		"syncoidIdentifier=zrc-123",
		"deleteTargetSnapshots=false",
		"syncoid command",
		"--sshkey=<redacted>",
		"syncoid stdout",
		"INFO: Sending oldest full snapshot",
		"syncoid stderr",
		"syncoid warning that should remain visible",
		"--sshkey=<redacted>",
		"sender completed",
		"result=success",
		"exitCode=0",
		"duration=",
		"mode=full",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("logs missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "finalSnapshot=") {
		t.Fatalf("logs contain misleading finalSnapshot:\n%s", out)
	}
	if strings.Contains(out, "--sshkey=/var/run/zfsrep/ssh/id_rsa") {
		t.Fatalf("logs contain unredacted ssh key path:\n%s", out)
	}
}

func TestSenderSuccessSummaryDoesNotReportMisleadingFinalSnapshotForIncremental(t *testing.T) {
	runner := &fakeRunner{
		stdout: "INFO: Sending incremental tank/src@syncoid_old_2026 ... syncoid_new_2026 to zfs-recv@10.0.0.42:tank/dst (~ 7 KB):\n",
	}
	var logs strings.Builder

	err := runSender(context.Background(), SenderConfig{
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:   "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:          "2222",
		NoRollback:       true,
		Compress:         "none",
		ReceiveUnmounted: true,
		ReceiveResumable: true,
	}, runner, &logs)
	if err != nil {
		t.Fatal(err)
	}

	out := logs.String()
	if !strings.Contains(out, "mode=incremental") {
		t.Fatalf("logs missing incremental mode:\n%s", out)
	}
	if strings.Contains(out, "finalSnapshot=") {
		t.Fatalf("logs contain misleading finalSnapshot:\n%s", out)
	}
}

type fakeExitError struct {
	code int
	msg  string
}

func (e fakeExitError) Error() string {
	return e.msg
}

func (e fakeExitError) ExitCode() int {
	return e.code
}

func TestSenderLogsFailedSyncoidRunAndReturnsFailureDiagnosis(t *testing.T) {
	runner := &fakeRunner{
		stdout: "syncoid stdout detail --sshkey=/var/run/zfsrep/ssh/id_rsa\n",
		stderr: "syncoid stderr detail --sshkey=/var/run/zfsrep/ssh/id_rsa\n",
		err:    fakeExitError{code: 23, msg: "exit status 23\nretry marker --sshkey=/var/run/zfsrep/ssh/id_rsa"},
	}
	var logs strings.Builder

	err := runSender(context.Background(), SenderConfig{
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:   "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:          "2222",
		NoRollback:       true,
		Compress:         "none",
		ReceiveUnmounted: true,
		ReceiveResumable: true,
	}, runner, &logs)
	if err == nil {
		t.Fatal("runSender() error = nil, want syncoid failure")
	}
	var failure diagnosis.Failure
	if !errors.As(err, &failure) {
		t.Fatalf("error type = %T, want diagnosis.Failure", err)
	}
	if got, want := failure.Diagnosis().String(), "exit status 23 retry marker --sshkey=<redacted>"; got != want {
		t.Fatalf("diagnosis = %q, want %q", got, want)
	}
	if failure.ExitCode() != 23 {
		t.Fatalf("exit code = %d, want 23", failure.ExitCode())
	}
	for _, forbidden := range []string{"syncoid stdout detail", "syncoid stderr detail", "/var/run/zfsrep/ssh/id_rsa"} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("error contains unsafe captured evidence %q: %v", forbidden, err)
		}
	}
	out := logs.String()
	for _, want := range []string{
		"sender completed",
		"result=failure",
		"exitCode=23",
		"syncoid stdout syncoid stdout detail --sshkey=<redacted>",
		"syncoid stderr syncoid stderr detail --sshkey=<redacted>",
		`error="exit status 23 retry marker --sshkey=<redacted>"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("logs missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "--sshkey=/var/run/zfsrep/ssh/id_rsa") {
		t.Fatalf("logs contain unredacted ssh key path:\n%s", out)
	}
	if last := failureMessageFromSenderLogs(out); last != "sender completed result=failure exitCode=23 duration=0s error=\"exit status 23 retry marker --sshkey=<redacted>\"" {
		t.Fatalf("last failure line = %q", last)
	}
}

func TestSenderKeepsDetailedSanitizedLogsSeparateFromFailureDiagnosis(t *testing.T) {
	oldStdout := "stdout-old-marker"
	oldStderr := "stderr-old-marker"
	stdoutTail := "stdout-tail-marker --sshkey=/var/run/zfsrep/ssh/id_rsa"
	stderrTail := "stderr-tail-marker --sshkey=/var/run/zfsrep/ssh/id_rsa"
	runner := &fakeRunner{
		stdout: oldStdout + "\n" + strings.Repeat("o", 70*1024) + "\n" + stdoutTail + "\n",
		stderr: oldStderr + "\n" + strings.Repeat("e", 70*1024) + "\n" + stderrTail + "\n",
		err:    fakeExitError{code: 23, msg: "exit status 23"},
	}
	var logs strings.Builder

	err := runSender(context.Background(), SenderConfig{
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:   "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:          "2222",
		NoRollback:       true,
		Compress:         "none",
		ReceiveUnmounted: true,
		ReceiveResumable: true,
	}, runner, &logs)
	if err == nil {
		t.Fatal("runSender() error = nil, want syncoid failure")
	}
	if err.Error() != "exit status 23" {
		t.Fatalf("failure diagnosis = %q, want safe process error", err)
	}
	value := logs.String()
	for _, want := range []string{oldStdout, oldStderr} {
		if !strings.Contains(value, want) {
			t.Fatalf("detailed logs missing %q:\n%s", want, value)
		}
	}
	for _, want := range []string{
		"stdout-tail-marker --sshkey=<redacted>",
		"stderr-tail-marker --sshkey=<redacted>",
	} {
		if !strings.Contains(value, want) {
			t.Fatalf("logs missing %q:\n%s", want, value)
		}
	}
	if strings.Contains(value, "--sshkey=/var/run/zfsrep/ssh/id_rsa") {
		t.Fatalf("logs contain unredacted ssh key path:\n%s", value)
	}
}

func TestSenderStreamingHugeLineLogsBoundedOmission(t *testing.T) {
	oldOutput := "stdout-old-marker"
	tailOutput := `stdout-tail-marker --sshkey="/var/run/zfsrep/ssh/id_rsa"`
	var logs strings.Builder
	runner := &streamingFakeRunner{
		stream: func(stdout, _ io.Writer) {
			if _, err := io.WriteString(stdout, oldOutput+strings.Repeat("o", 70*1024)+tailOutput); err != nil {
				t.Fatal(err)
			}
		},
		beforeDone: func() {
			out := logs.String()
			if strings.Contains(out, oldOutput) {
				t.Fatalf("streaming log contains beginning of huge line before command returned:\n%s", out)
			}
			if !strings.Contains(out, "syncoid stdout <output line omitted: exceeded 65536 bytes>") {
				t.Fatalf("streaming log missing bounded omission before command returned:\n%s", out)
			}
			if strings.Contains(out, "--sshkey=/var/run/zfsrep/ssh/id_rsa") {
				t.Fatalf("streaming log contains unredacted ssh key path before command returned:\n%s", out)
			}
		},
	}

	err := runSender(context.Background(), SenderConfig{
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:   "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:          "2222",
		NoRollback:       true,
		Compress:         "none",
		ReceiveUnmounted: true,
		ReceiveResumable: true,
	}, runner, &logs)
	if err != nil {
		t.Fatal(err)
	}
	out := logs.String()
	if strings.Contains(out, oldOutput) {
		t.Fatalf("streaming log contains beginning of huge line:\n%s", out)
	}
	if !strings.Contains(out, "syncoid stdout <output line omitted: exceeded 65536 bytes>") {
		t.Fatalf("streaming log missing bounded omission:\n%s", out)
	}
	if strings.Contains(out, "--sshkey=/var/run/zfsrep/ssh/id_rsa") {
		t.Fatalf("streaming log contains unredacted ssh key path:\n%s", out)
	}
}

func failureMessageFromSenderLogs(logs string) string {
	var last string
	for _, line := range strings.Split(logs, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			last = line
		}
	}
	last = strings.Replace(last, regexp.MustCompile(`duration=[^ ]+`).FindString(last), "duration=0s", 1)
	return last
}

func TestSenderRunsSyncoidWithConfiguredSnapshotOptions(t *testing.T) {
	runner := &fakeRunner{}
	err := RunSender(context.Background(), SenderConfig{
		SrcDataset:            "tank/src",
		DstHost:               "root@10.0.0.42",
		DstDataset:            "tank/dst",
		SSHKeyFile:            "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:        "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:               "2222",
		NoSyncSnap:            true,
		NoRollback:            true,
		Compress:              "zstd",
		SyncoidIdentifier:     "zrc-123",
		DeleteTargetSnapshots: true,
		ReceiveUnmounted:      false,
		ReceiveResumable:      false,
		IncludeSnaps:          []string{"^snap-.*", "^manual$"},
		ExcludeSnaps:          []string{".*-tmp$"},
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	want := "--no-sync-snap --no-rollback --no-privilege-elevation --compress=zstd-fast --identifier=zrc-123 --delete-target-snapshots --sshoption=UserKnownHostsFile=/var/run/zfsrep/ssh/known_hosts --sshoption=StrictHostKeyChecking=yes --sshoption=IdentitiesOnly=yes --sshkey=/var/run/zfsrep/ssh/id_rsa --sshport=2222 --no-resume --include-snaps=^snap-.* --include-snaps=^manual$ --exclude-snaps=.*-tmp$ tank/src root@10.0.0.42:tank/dst"
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
	t.Setenv("SYNCOID_DELETE_TARGET_SNAPSHOTS", "")
	t.Setenv("SYNCOID_COMPRESS", "")
	t.Setenv("SYNCOID_IDENTIFIER", "")
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
	if cfg.DeleteTargetSnapshots {
		t.Fatalf("DeleteTargetSnapshots = true, want false")
	}
	if cfg.Compress != "none" {
		t.Fatalf("Compress = %q, want none", cfg.Compress)
	}
	if cfg.SyncoidIdentifier != "" {
		t.Fatalf("SyncoidIdentifier = %q, want empty default", cfg.SyncoidIdentifier)
	}
	if !cfg.ReceiveUnmounted {
		t.Fatalf("ReceiveUnmounted = false, want true")
	}
	if !cfg.ReceiveResumable {
		t.Fatalf("ReceiveResumable = false, want true")
	}
}

func TestExecRunnerCapturesStderrWithoutMirroringRawOutput(t *testing.T) {
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

	rawStderr := "--sshkey=/var/run/zfsrep/ssh/id_rsa"
	stdout, stderr, err := ExecRunner{}.Run(context.Background(), "sh", "-c", "printf stdout; printf '%s' '"+rawStderr+"' >&2")
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
	if stderr != rawStderr {
		t.Fatalf("stderr = %q", stderr)
	}
	if string(mirrored) != "" {
		t.Fatalf("mirrored stderr = %q, want no raw mirror", string(mirrored))
	}
}

func TestSenderConfigFromEnvExplicitValuesOverrideDefaults(t *testing.T) {
	t.Setenv("SYNCOID_NO_SYNC_SNAP", "true")
	t.Setenv("SYNCOID_NO_ROLLBACK", "false")
	t.Setenv("SYNCOID_FORCE_DELETE", "true")
	t.Setenv("SYNCOID_DELETE_TARGET_SNAPSHOTS", "true")
	t.Setenv("SYNCOID_COMPRESS", "zstd")
	t.Setenv("SYNCOID_IDENTIFIER", "zrc-123")
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
	if !sender.DeleteTargetSnapshots {
		t.Fatalf("sender DeleteTargetSnapshots = false, want true")
	}
	if sender.Compress != "zstd" {
		t.Fatalf("sender Compress = %q, want zstd", sender.Compress)
	}
	if sender.SyncoidIdentifier != "zrc-123" {
		t.Fatalf("sender SyncoidIdentifier = %q, want zrc-123", sender.SyncoidIdentifier)
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

func TestSenderConfigFromLookupParsesControllerEnvContract(t *testing.T) {
	values := map[string]string{
		EnvSrcDataset:            "tank/src",
		EnvDstHost:               "zfs-recv@10.0.0.42",
		EnvDstDataset:            "tank/dst",
		EnvSSHKeyFile:            DefaultSSHKeyFile,
		EnvKnownHostsFile:        DefaultKnownHostsFile,
		EnvSSHPort:               DefaultSSHPort,
		EnvNoSyncSnap:            "true",
		EnvNoRollback:            "false",
		EnvForceDelete:           "true",
		EnvDeleteTargetSnapshots: "true",
		EnvCompress:              "zstd",
		EnvSyncoidIdentifier:     "zrc-123",
		EnvReceiveUnmounted:      "false",
		EnvReceiveResumable:      "false",
		EnvIncludeSnaps:          "^snap-.*\n^manual$",
		EnvExcludeSnaps:          ".*-tmp$",
		EnvExpectedNodeName:      "worker-a",
		EnvActualNodeName:        "worker-a",
	}

	cfg := SenderConfigFromLookup(func(key string) string {
		return values[key]
	})

	if cfg.SrcDataset != "tank/src" || cfg.DstHost != "zfs-recv@10.0.0.42" || cfg.DstDataset != "tank/dst" {
		t.Fatalf("dataset/host config = %#v", cfg)
	}
	if cfg.SSHKeyFile != DefaultSSHKeyFile || cfg.KnownHostsFile != DefaultKnownHostsFile || cfg.SSHPort != DefaultSSHPort {
		t.Fatalf("ssh config = %#v", cfg)
	}
	if !cfg.NoSyncSnap || cfg.NoRollback || !cfg.ForceDelete || !cfg.DeleteTargetSnapshots || cfg.Compress != "zstd" || cfg.SyncoidIdentifier != "zrc-123" {
		t.Fatalf("syncoid config = %#v", cfg)
	}
	if cfg.ReceiveUnmounted || cfg.ReceiveResumable {
		t.Fatalf("receive flags = %#v, want both false", cfg)
	}
	if strings.Join(cfg.IncludeSnaps, " ") != "^snap-.* ^manual$" {
		t.Fatalf("IncludeSnaps = %#v", cfg.IncludeSnaps)
	}
	if strings.Join(cfg.ExcludeSnaps, " ") != ".*-tmp$" {
		t.Fatalf("ExcludeSnaps = %#v", cfg.ExcludeSnaps)
	}
	if cfg.ExpectedNode != "worker-a" || cfg.ActualNode != "worker-a" {
		t.Fatalf("node config = %#v", cfg)
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

func TestSenderRejectsUnsafeSyncoidIdentifier(t *testing.T) {
	runner := &fakeRunner{}
	err := RunSender(context.Background(), SenderConfig{
		SrcDataset:        "tank/src",
		DstHost:           "root@10.0.0.42",
		DstDataset:        "tank/dst",
		SSHKeyFile:        "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:    "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:           "2222",
		NoRollback:        true,
		Compress:          "none",
		SyncoidIdentifier: "bad;id",
		ReceiveUnmounted:  true,
		ReceiveResumable:  true,
	}, runner)
	if err == nil || !strings.Contains(err.Error(), "unsupported syncoid identifier") {
		t.Fatalf("RunSender() error = %v, want unsupported identifier", err)
	}
	if len(runner.calls) != 0 {
		t.Fatalf("syncoid calls = %#v, want none", runner.calls)
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
