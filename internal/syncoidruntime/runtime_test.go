package syncoidruntime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"
)

type fakeProcess struct {
	name         string
	arguments    []string
	stdout       []byte
	stderr       []byte
	result       processResult
	beforeReturn func()
}

func (p *fakeProcess) run(
	_ context.Context,
	name string,
	arguments []string,
	stdout io.Writer,
	stderr io.Writer,
) processResult {
	p.name = name
	p.arguments = slices.Clone(arguments)
	if _, err := stdout.Write(p.stdout); err != nil {
		panic(err)
	}
	if _, err := stderr.Write(p.stderr); err != nil {
		panic(err)
	}
	if p.beforeReturn != nil {
		p.beforeReturn()
	}
	return p.result
}

func TestRuntimeForwardsSyncoidStreamsUnchangedBeforeCompletion(t *testing.T) {
	stdoutBytes := []byte{'o', 'u', 't', 0xff, '\n', 'x'}
	stderrBytes := []byte{'e', 'r', 'r', 0xfe, '\r', 'y'}
	var stdout, stderr bytes.Buffer
	process := &fakeProcess{
		stdout: stdoutBytes,
		stderr: stderrBytes,
		result: processResult{started: true},
		beforeReturn: func() {
			if !bytes.Equal(stdout.Bytes(), stdoutBytes) {
				t.Fatalf("stdout before return = %v, want %v", stdout.Bytes(), stdoutBytes)
			}
			if !bytes.Equal(stderr.Bytes(), stderrBytes) {
				t.Fatalf("stderr before return = %v, want %v", stderr.Bytes(), stderrBytes)
			}
		},
	}
	arguments := []string{"--no-rollback", "tank/src", "host:tank/dst"}

	outcome := run(context.Background(), arguments, &stdout, &stderr, process)

	if process.name != "syncoid" {
		t.Fatalf("executable = %q, want syncoid", process.name)
	}
	if !slices.Equal(process.arguments, arguments) {
		t.Fatalf("arguments = %#v, want %#v", process.arguments, arguments)
	}
	if outcome.ExitCode() != 0 || outcome.FailureMessage() != "" {
		t.Fatalf("outcome = (%d, %q), want success", outcome.ExitCode(), outcome.FailureMessage())
	}
}

func TestRuntimePreservesOrdinarySyncoidExitCodeWithoutInspectingOutput(t *testing.T) {
	var stdout, stderr bytes.Buffer
	process := &fakeProcess{
		stdout: []byte("detailed stdout that is not a status"),
		stderr: []byte("CRITICAL ERROR: detailed stderr that is not a status"),
		result: processResult{started: true, exitCode: 23, err: fakeExitError{code: 23}},
	}

	outcome := run(context.Background(), []string{"tank/src", "host:tank/dst"}, &stdout, &stderr, process)

	if outcome.ExitCode() != 23 {
		t.Fatalf("exit code = %d, want 23", outcome.ExitCode())
	}
	if got := outcome.FailureMessage(); got != "syncoid exited with status 23" {
		t.Fatalf("failure message = %q", got)
	}
	for _, output := range []string{stdout.String(), stderr.String()} {
		if strings.Contains(outcome.FailureMessage(), output) {
			t.Fatalf("failure message contains Syncoid output %q", output)
		}
	}
}

func TestRuntimeUsesGenericFailureForMissingExitCode(t *testing.T) {
	var stderr bytes.Buffer
	process := &fakeProcess{
		result: processResult{started: true, exitCode: -1, err: errors.New("signal: killed")},
	}

	outcome := run(context.Background(), nil, io.Discard, &stderr, process)

	if outcome.ExitCode() != 1 {
		t.Fatalf("exit code = %d, want fallback 1", outcome.ExitCode())
	}
	if got := outcome.FailureMessage(); got != "syncoid failed: signal: killed" {
		t.Fatalf("failure message = %q", got)
	}
	if got := stderr.String(); got != "syncoid failed: signal: killed\n" {
		t.Fatalf("wrapper stderr = %q", got)
	}
}

func TestRuntimeReportsAndNormalizesProcessStartupFailure(t *testing.T) {
	var stderr bytes.Buffer
	process := &fakeProcess{
		result: processResult{
			err: errors.New("not found\n" + strings.Repeat("ø", 5000) + string([]byte{0xff})),
		},
	}

	outcome := run(context.Background(), nil, io.Discard, &stderr, process)

	if outcome.ExitCode() != 1 {
		t.Fatalf("exit code = %d, want fallback 1", outcome.ExitCode())
	}
	message := outcome.FailureMessage()
	if !strings.HasPrefix(message, "start syncoid: not found ") {
		t.Fatalf("failure message = %q", message)
	}
	if len(message) > 4096 || !utf8.ValidString(message) || strings.ContainsAny(message, "\r\n") {
		t.Fatalf("failure message is not bounded single-line UTF-8: %q", message)
	}
	if got := stderr.String(); got != message+"\n" {
		t.Fatalf("wrapper stderr = %q, want failure message line %q", got, message)
	}
}

func TestExecProcessKeepsStreamsDistinctAndReportsRealExitCode(t *testing.T) {
	var stdout, stderr bytes.Buffer

	result := execProcess{}.run(
		context.Background(),
		"sh",
		[]string{"-c", "printf 'real stdout'; printf 'real stderr' >&2; exit 17"},
		&stdout,
		&stderr,
	)

	if !result.started || result.exitCode != 17 || result.err == nil {
		t.Fatalf("process result = %#v, want started exit 17", result)
	}
	if stdout.String() != "real stdout" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if stderr.String() != "real stderr" {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

type fakeExitError struct {
	code int
}

func (e fakeExitError) Error() string {
	return "unsafe process prose that must not become the Sender Failure Message"
}

func (e fakeExitError) ExitCode() int {
	return e.code
}
