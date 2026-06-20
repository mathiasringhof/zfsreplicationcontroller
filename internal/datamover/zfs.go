package datamover

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
)

type CommandRunner interface {
	Run(ctx context.Context, name string, args ...string) (string, string, error)
}

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, name string, args ...string) (string, string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = io.MultiWriter(&stderr, os.Stderr)
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}
