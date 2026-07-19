package diagnosis_test

import (
	"errors"
	"io"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/mathias/zfsreplicationcontroller/internal/replication/diagnosis"
)

type exitError struct {
	code int
	text string
}

func TestFailureDoesNotExposeUnsafeCommandError(t *testing.T) {
	raw := exitError{code: 9, text: "process failed --sshkey=top-secret"}
	capture := diagnosis.NewCapture(nil)

	failure := capture.Failure(raw)

	if got := failure.Error(); got != "process failed --sshkey=<redacted>" {
		t.Fatalf("failure = %q", got)
	}
	if errors.Unwrap(failure) != nil {
		t.Fatal("failure unwraps an unsafe command error")
	}
	var recovered exitError
	if errors.As(failure, &recovered) {
		t.Fatalf("recovered unsafe command error: %#v", recovered)
	}
}

func TestSanitizeProducesIdempotentBoundedSingleLineUTF8(t *testing.T) {
	raw := "cannot receive tank/archive\x00\n" + strings.Repeat("界", 1400) +
		" --sshkey=super-secret-value\xff trailing"

	first := diagnosis.Sanitize(raw).String()
	second := diagnosis.Sanitize(first).String()

	if first != second {
		t.Fatalf("sanitize is not idempotent:\nfirst:  %q\nsecond: %q", first, second)
	}
	if len(first) > 4096 {
		t.Fatalf("diagnosis is %d bytes, want at most 4096", len(first))
	}
	if !utf8.ValidString(first) {
		t.Fatalf("diagnosis is not valid UTF-8: %q", first)
	}
	if strings.ContainsAny(first, "\r\n\x00") {
		t.Fatalf("diagnosis is not one normalized line: %q", first)
	}
	if strings.Contains(first, "super-secret") {
		t.Fatalf("diagnosis contains secret: %q", first)
	}
}

func TestCaptureRedactsSecretAcrossChunksFromLogsAndFailure(t *testing.T) {
	var live []string
	capture := diagnosis.NewCapture(func(stream diagnosis.Stream, line string) {
		live = append(live, string(stream)+": "+line)
	})
	write(t, capture.Stderr(), "CRITICAL ERROR: ssh -")
	write(t, capture.Stderr(), "i /var/run/zfsrep/ssh/id_rsa host cannot receive tank/archive\n")

	failure := capture.Failure(exitError{code: 1, text: "unsafe -i /var/run/zfsrep/ssh/id_rsa"})

	want := "CRITICAL ERROR: ssh -i <redacted> host cannot receive tank/archive"
	if got := failure.Diagnosis().String(); got != want {
		t.Fatalf("diagnosis = %q, want %q", got, want)
	}
	if got := strings.Join(live, "\n"); got != "stderr: "+want {
		t.Fatalf("live lines = %q", got)
	}
	if strings.Contains(failure.Error()+strings.Join(live, "\n"), "/var/run/zfsrep/ssh/id_rsa") {
		t.Fatal("secret escaped diagnosis module")
	}
}

func TestCaptureRedactsSecretAtEveryChunkBoundary(t *testing.T) {
	const (
		raw  = `CRITICAL ERROR: ssh --sshkey=\"/secret key\" host cannot receive tank/archive` + "\n"
		want = "CRITICAL ERROR: ssh --sshkey=<redacted> host cannot receive tank/archive"
	)
	for split := 0; split <= len(raw); split++ {
		var live []string
		capture := diagnosis.NewCapture(func(_ diagnosis.Stream, line string) {
			live = append(live, line)
		})
		write(t, capture.Stderr(), raw[:split])
		write(t, capture.Stderr(), raw[split:])

		failure := capture.Failure(nil)
		if got := failure.Diagnosis().String(); got != want {
			t.Fatalf("split %d: diagnosis = %q, want %q", split, got, want)
		}
		if got := strings.Join(live, "\n"); got != want {
			t.Fatalf("split %d: live output = %q, want %q", split, got, want)
		}
	}
}

func TestCaptureSelectsCauseFromBoundedOutputTail(t *testing.T) {
	capture := diagnosis.NewCapture(nil)
	write(t, capture.Stderr(), "CRITICAL ERROR: stale failure\n")
	write(t, capture.Stderr(), strings.Repeat("ordinary diagnostic output\n", 4000))
	write(t, capture.Stderr(), "current failure detail\n")

	failure := capture.Failure(nil)

	if got := failure.Diagnosis().String(); got != "current failure detail" {
		t.Fatalf("diagnosis = %q, want current bounded-tail evidence", got)
	}
}

func TestCaptureCondensesCriticalPipelineToReceiveTarget(t *testing.T) {
	capture := diagnosis.NewCapture(nil)
	write(t, capture.Stderr(), "CRITICAL ERROR: zfs send -w tank/src@snap | ssh -i /var/run/zfsrep/ssh/id_rsa zfs-recv@10.42.2.11 zfs receive -s -F -u missingpool/dst 2>&1 failed: 256\n")

	failure := capture.Failure(exitError{code: 2, text: "exit status 2"})

	if got, want := failure.Diagnosis().String(), "CRITICAL ERROR: syncoid command failed target=missingpool/dst"; got != want {
		t.Fatalf("diagnosis = %q, want %q", got, want)
	}
}

func TestCapturePrefersStderrSpecificCauseBeforeStdout(t *testing.T) {
	capture := diagnosis.NewCapture(nil)
	write(t, capture.Stderr(), "cannot open 'tank/source': dataset does not exist\n")
	write(t, capture.Stdout(), "cannot receive 'tank/target': dataset is busy\n")

	failure := capture.Failure(exitError{code: 1, text: "exit status 1"})

	if got, want := failure.Diagnosis().String(), "cannot open 'tank/source': dataset does not exist"; got != want {
		t.Fatalf("diagnosis = %q, want %q", got, want)
	}
}

func TestCaptureCausePriority(t *testing.T) {
	tests := []struct {
		name   string
		stdout string
		stderr string
		err    error
		want   string
	}{
		{
			name:   "stdout critical before stderr specific",
			stdout: "CRITICAL ERROR: remote refused replication\n",
			stderr: "cannot open 'tank/source': dataset does not exist\n",
			err:    exitError{code: 1, text: "exit status 1"},
			want:   "CRITICAL ERROR: remote refused replication",
		},
		{
			name:   "process error before ordinary tails",
			stdout: "stdout detail\n",
			stderr: "stderr detail\n",
			err:    exitError{code: 1, text: "process launch failed"},
			want:   "process launch failed",
		},
		{
			name:   "stderr tail before stdout tail",
			stdout: "stdout detail\n",
			stderr: "stderr detail\n",
			want:   "stderr detail",
		},
		{
			name:   "stdout tail before generic",
			stdout: "stdout detail\n",
			want:   "stdout detail",
		},
		{
			name: "generic without evidence",
			want: "sender failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			capture := diagnosis.NewCapture(nil)
			write(t, capture.Stdout(), tt.stdout)
			write(t, capture.Stderr(), tt.stderr)

			if got := capture.Failure(tt.err).Diagnosis().String(); got != tt.want {
				t.Fatalf("diagnosis = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSanitizeRedactsRecognizedSecrets(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "sshkey", in: `before --sshkey="/secret/key" after`, want: `before --sshkey=<redacted> after`},
		{name: "escaped quoted sshkey", in: `before --sshkey=\"/secret key\" after`, want: `before --sshkey=<redacted> after`},
		{name: "identity", in: `ssh -i '/secret key' host`, want: `ssh -i <redacted> host`},
		{name: "control socket", in: `ssh -S /tmp/control.sock host`, want: `ssh -S <redacted> host`},
		{name: "known default path", in: `using /var/run/zfsrep/ssh/id_rsa directly`, want: `using <redacted> directly`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := diagnosis.Sanitize(tt.in).String(); got != tt.want {
				t.Fatalf("sanitize = %q, want %q", got, tt.want)
			}
		})
	}
}

func (e exitError) Error() string { return e.text }

func (e exitError) ExitCode() int { return e.code }

func TestCaptureSelectsCriticalStderrAsFailureDiagnosis(t *testing.T) {
	capture := diagnosis.NewCapture(nil)
	write(t, capture.Stdout(), "cannot open 'tank/archive': dataset does not exist\n")
	write(t, capture.Stderr(), "warning: retrying\nCRITICAL ERROR: cannot receive 'tank/archive': dataset is busy\n")

	failure := capture.Failure(exitError{code: 2, text: "exit status 2"})

	if got := failure.Diagnosis().String(); got != "CRITICAL ERROR: cannot receive 'tank/archive': dataset is busy" {
		t.Fatalf("diagnosis = %q", got)
	}
	if got := failure.ExitCode(); got != 2 {
		t.Fatalf("exit code = %d, want 2", got)
	}
}

func write(t *testing.T, dst io.Writer, value string) {
	t.Helper()
	if _, err := io.WriteString(dst, value); err != nil {
		t.Fatal(err)
	}
}
