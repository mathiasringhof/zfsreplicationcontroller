package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

type fakeOutcome struct {
	exitCode int
	message  string
}

func (o fakeOutcome) ExitCode() int {
	return o.exitCode
}

func (o fakeOutcome) FailureMessage() string {
	return o.message
}

type recordingPublisher struct {
	messages []string
	err      error
}

func (p *recordingPublisher) Publish(message string) error {
	p.messages = append(p.messages, message)
	return p.err
}

func TestRunPassesArgumentsAndStreamsToRuntimeWithoutPublishingOnSuccess(t *testing.T) {
	arguments := []string{"--no-rollback", "tank/src", "host:tank/dst"}
	var stdout, stderr bytes.Buffer
	publisher := &recordingPublisher{}
	var gotArguments []string
	var gotStdout, gotStderr io.Writer
	operation := func(
		_ context.Context,
		arguments []string,
		stdout io.Writer,
		stderr io.Writer,
	) runtimeOutcome {
		gotArguments = slices.Clone(arguments)
		gotStdout = stdout
		gotStderr = stderr
		return fakeOutcome{}
	}

	code := run(context.Background(), arguments, &stdout, &stderr, operation, publisher)

	if code != 0 {
		t.Fatalf("run() code = %d, want 0", code)
	}
	if !slices.Equal(gotArguments, arguments) || gotStdout != &stdout || gotStderr != &stderr {
		t.Fatalf("runtime call = (%#v, %T, %T)", gotArguments, gotStdout, gotStderr)
	}
	if len(publisher.messages) != 0 {
		t.Fatalf("published messages = %#v, want none", publisher.messages)
	}
}

func TestRunPublishesSenderFailureMessageAndReturnsRuntimeExitCode(t *testing.T) {
	publisher := &recordingPublisher{}
	operation := func(context.Context, []string, io.Writer, io.Writer) runtimeOutcome {
		return fakeOutcome{exitCode: 23, message: "syncoid exited with status 23"}
	}

	code := run(context.Background(), nil, io.Discard, io.Discard, operation, publisher)

	if code != 23 {
		t.Fatalf("run() code = %d, want 23", code)
	}
	if !slices.Equal(publisher.messages, []string{"syncoid exited with status 23"}) {
		t.Fatalf("published messages = %#v", publisher.messages)
	}
}

func TestRunPreservesRuntimeExitCodeWhenTerminationPublicationFails(t *testing.T) {
	var stderr bytes.Buffer
	publisher := &recordingPublisher{err: errors.New("disk\nis read-only")}
	operation := func(context.Context, []string, io.Writer, io.Writer) runtimeOutcome {
		return fakeOutcome{exitCode: 17, message: "syncoid exited with status 17"}
	}

	code := run(context.Background(), nil, io.Discard, &stderr, operation, publisher)

	if code != 17 {
		t.Fatalf("run() code = %d, want original Runtime exit code 17", code)
	}
	if got := stderr.String(); !strings.Contains(got, "publish Sender Failure Message") || strings.Count(got, "\n") != 1 {
		t.Fatalf("wrapper stderr = %q, want one concise publication error line", got)
	}
}
