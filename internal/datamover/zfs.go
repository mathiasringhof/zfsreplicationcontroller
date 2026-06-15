package datamover

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

type CommandRunner interface {
	Run(ctx context.Context, name string, args ...string) (string, string, error)
	StartPipe(ctx context.Context, name string, args ...string) (io.ReadCloser, <-chan error, error)
	RunWithStdin(ctx context.Context, stdin io.Reader, name string, args ...string) (string, string, error)
}

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, name string, args ...string) (string, string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

func (ExecRunner) StartPipe(ctx context.Context, name string, args ...string) (io.ReadCloser, <-chan error, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var stderr bytes.Buffer
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}
	cmd.Stderr = &stderr
	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}
	go func() {
		err := cmd.Wait()
		if err != nil && stderr.Len() > 0 {
			err = fmt.Errorf("%w: %s", err, strings.TrimSpace(stderr.String()))
		}
		done <- err
		close(done)
	}()
	return stdout, done, nil
}

func (ExecRunner) RunWithStdin(ctx context.Context, stdin io.Reader, name string, args ...string) (string, string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdin = stdin
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

func snapshotExists(ctx context.Context, r CommandRunner, snap string) bool {
	_, _, err := r.Run(ctx, "zfs", "list", "-H", "-t", "snapshot", snap)
	return err == nil
}

func snapshotGUID(ctx context.Context, r CommandRunner, snap string) string {
	out, _, err := r.Run(ctx, "zfs", "get", "-H", "-o", "value", "guid", snap)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}
