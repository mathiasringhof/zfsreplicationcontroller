package sender

import (
	"context"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"
	"testing"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/replication/diagnosis"
	"github.com/mathias/zfsreplicationcontroller/internal/syncoid"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	err := runSender(context.Background(), defaultSenderConfig(t), runner, &logs)
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

	err := runSender(context.Background(), defaultSenderConfig(t), runner, &logs)
	if err != nil {
		t.Fatal(err)
	}

	out := logs.String()
	for _, want := range []string{
		"sender starting",
		"sourceDataset=tank/src",
		"targetDataset=tank/dst",
		"targetHost=root@10.0.0.42",
		"syncoidIdentifier=zrc-",
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
	for _, field := range []string{"srcDataset=", "dstDataset=", "dstHost="} {
		if strings.Contains(out, field) {
			t.Fatalf("logs contain abbreviated field %q:\n%s", field, out)
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

	err := runSender(context.Background(), defaultSenderConfig(t), runner, &logs)
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

	err := runSender(context.Background(), defaultSenderConfig(t), runner, &logs)
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

	err := runSender(context.Background(), defaultSenderConfig(t), runner, &logs)
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

	err := runSender(context.Background(), defaultSenderConfig(t), runner, &logs)
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
	err := RunSender(context.Background(), senderConfig(t, zfsv1.SyncoidSpec{
		NoSyncSnap:            pointer(true),
		NoRollback:            pointer(true),
		ForceDelete:           pointer(true),
		Compress:              "zstd",
		DeleteTargetSnapshots: pointer(true),
		ReceiveUnmounted:      pointer(false),
		ReceiveResumable:      pointer(false),
		IncludeSnaps:          []string{"^snap-.*", "^manual$"},
		ExcludeSnaps:          []string{".*-tmp$"},
	}), runner)
	if err != nil {
		t.Fatal(err)
	}
	identifierArgument := argumentWithPrefix(t, runner.calls[0].args, "--identifier=")
	want := "--no-sync-snap --no-rollback --no-privilege-elevation --compress=zstd-fast " + identifierArgument + " --delete-target-snapshots --sshoption=UserKnownHostsFile=/var/run/zfsrep/ssh/known_hosts --sshoption=StrictHostKeyChecking=yes --sshoption=IdentitiesOnly=yes --sshkey=/var/run/zfsrep/ssh/id_rsa --sshport=2222 --no-resume --include-snaps=^snap-.* --include-snaps=^manual$ --exclude-snaps=.*-tmp$ --force-delete tank/src root@10.0.0.42:tank/dst"
	if !hasNamedCall(runner.calls, "syncoid", want) {
		t.Fatalf("syncoid was not called with %q: %#v", want, runner.calls)
	}
	if hasNamedCall(runner.calls, "zfs", "snapshot tank/src@") {
		t.Fatalf("zfs snapshot should not be called when syncoid owns snapshot selection: %#v", runner.calls)
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

func defaultSenderConfig(t *testing.T) SenderConfig {
	t.Helper()
	return senderConfig(t, zfsv1.SyncoidSpec{})
}

func senderConfig(t *testing.T, spec zfsv1.SyncoidSpec) SenderConfig {
	t.Helper()
	run := &zfsv1.ZFSReplicationRun{
		ObjectMeta: metav1.ObjectMeta{Namespace: "storage"},
		Spec: zfsv1.ZFSReplicationRunSpec{
			Source:  zfsv1.DatasetRef{NodeName: "worker-a", Dataset: "tank/src"},
			Target:  zfsv1.DatasetRef{NodeName: "worker-b", Dataset: "tank/dst"},
			Syncoid: spec,
		},
	}
	contract, err := syncoid.Translate(run)
	if err != nil {
		t.Fatal(err)
	}
	values := make(map[string]string)
	for _, entry := range contract.SenderEnvironment {
		values[entry.Name] = entry.Value
	}
	for _, entry := range syncoid.ConnectionEnvironment(syncoid.Connection{
		TargetHost:     "root@10.0.0.42",
		SSHKeyFile:     "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile: "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:        "2222",
	}) {
		values[entry.Name] = entry.Value
	}
	invocation, err := syncoid.DecodeSenderEnvironment(func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	return SenderConfig{Invocation: invocation}
}

func argumentWithPrefix(t *testing.T, arguments []string, prefix string) string {
	t.Helper()
	for _, argument := range arguments {
		if strings.HasPrefix(argument, prefix) {
			return argument
		}
	}
	t.Fatalf("arguments %#v do not contain prefix %q", arguments, prefix)
	return ""
}

func pointer[T any](value T) *T {
	return &value
}
