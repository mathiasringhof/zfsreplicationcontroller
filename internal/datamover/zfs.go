package datamover

import (
	"bytes"
	"context"
	"io"
	"os/exec"
)

type CommandRunner interface {
	Run(ctx context.Context, name string, args ...string) (string, string, error)
}

type StreamingCommandRunner interface {
	RunStreaming(ctx context.Context, name string, stdout, stderr io.Writer, args ...string) error
}

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, name string, args ...string) (string, string, error) {
	var stdout, stderr bytes.Buffer
	err := ExecRunner{}.RunStreaming(ctx, name, &stdout, &stderr, args...)
	return stdout.String(), stderr.String(), err
}

func (ExecRunner) RunStreaming(ctx context.Context, name string, stdout, stderr io.Writer, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	return cmd.Run()
}
