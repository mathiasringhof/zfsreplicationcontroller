package datamover

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
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

func snapshotExists(ctx context.Context, r CommandRunner, snap string) bool {
	_, _, err := r.Run(ctx, "zfs", "list", "-H", "-t", "snapshot", snap)
	return err == nil
}

func snapshotGUID(ctx context.Context, r CommandRunner, snap string) string {
	out, _, err := snapshotGUIDValue(ctx, r, snap)
	if err != nil {
		return ""
	}
	return out
}

func snapshotGUIDValue(ctx context.Context, r CommandRunner, snap string) (string, string, error) {
	out, stderr, err := r.Run(ctx, "zfs", "get", "-H", "-o", "value", "guid", snap)
	if err != nil {
		return "", stderr, err
	}
	return strings.TrimSpace(out), stderr, nil
}
