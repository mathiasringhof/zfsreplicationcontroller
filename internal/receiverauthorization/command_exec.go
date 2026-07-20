package receiverauthorization

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
)

func executeReceiverCommandPlan(ctx context.Context, streams commandStreams, plan receiverCommandPlan) error {
	stdin := streams.stdin
	if stdin == nil {
		stdin = os.Stdin
	}
	stdout := streams.stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	stderr := streams.stderr
	if stderr == nil {
		stderr = os.Stderr
	}
	switch plan.kind {
	case receiverCommandExit:
		return nil
	case receiverCommandEcho:
		_, err := fmt.Fprint(stdout, strings.Join(plan.echoArgs, " "))
		return err
	case receiverCommandLookup:
		path, err := resolveAllowedCommand(plan.lookupCommand)
		if err != nil {
			return exitError{code: 1}
		}
		_, err = fmt.Fprintln(stdout, path)
		return err
	case receiverCommandPS:
		return nil
	case receiverCommandPipeline:
		return executeReceiverPipeline(ctx, stdin, stdout, stderr, plan.pipeline)
	case receiverCommandBatch:
		for _, item := range plan.batch {
			if err := executeReceiverCommandPlan(ctx, commandStreams{
				stdin:  stdin,
				stdout: stdout,
				stderr: stderr,
			}, item); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported receiver command plan %q", plan.kind)
	}
}

func executeReceiverPipeline(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer, steps []receiverCommandStep) error {
	cmds := make([]*exec.Cmd, 0, len(steps))
	previousStdout := stdin
	var outputMu sync.Mutex
	stdout = lockedWriter{mu: &outputMu, w: stdout}
	stderr = lockedWriter{mu: &outputMu, w: stderr}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for i, step := range steps {
		path, err := resolveAllowedCommand(step.Name)
		if err != nil {
			return err
		}
		cmd := exec.CommandContext(ctx, path, step.Args...)
		cmd.Env = receiverChildEnvironment()
		cmd.Stdin = previousStdout
		if i == len(steps)-1 {
			if step.StdoutNull {
				cmd.Stdout = io.Discard
			} else {
				cmd.Stdout = stdout
			}
		} else {
			pipe, err := cmd.StdoutPipe()
			if err != nil {
				return fmt.Errorf("create command pipe: %w", err)
			}
			previousStdout = pipe
		}
		if step.StderrToStdout {
			cmd.Stderr = cmd.Stdout
		} else if step.StderrNull {
			cmd.Stderr = io.Discard
		} else {
			cmd.Stderr = stderr
		}
		cmds = append(cmds, cmd)
	}
	started := make([]*exec.Cmd, 0, len(cmds))
	for _, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			cancel()
			waitReceiverCommands(started)
			return fmt.Errorf("start %s: %w", cmd.Path, err)
		}
		started = append(started, cmd)
	}
	results := make(chan error, len(started))
	for _, cmd := range started {
		go func(cmd *exec.Cmd) {
			results <- commandExitError(cmd.Wait())
		}(cmd)
	}
	var firstErr error
	for range started {
		if err := <-results; err != nil {
			if firstErr == nil {
				firstErr = err
				cancel()
			}
		}
	}
	return firstErr
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(p)
}

func receiverChildEnvironment() []string {
	return []string{"LC_ALL=C", "LANG=C"}
}

func waitReceiverCommands(cmds []*exec.Cmd) {
	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			continue
		}
	}
}

func commandExitError(err error) error {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitError{code: exitErr.ExitCode()}
	}
	return err
}

var resolveAllowedCommand = defaultResolveAllowedCommand

func defaultResolveAllowedCommand(name string) (string, error) {
	for _, path := range allowedCommandPaths(name) {
		info, err := os.Stat(path)
		if err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			return path, nil
		}
	}
	return "", fmt.Errorf("allowed command %q was not found", name)
}

func allowedCommandPaths(name string) []string {
	switch name {
	case "zfs":
		return []string{"/usr/sbin/zfs", "/sbin/zfs"}
	case "zpool":
		return []string{"/usr/sbin/zpool", "/sbin/zpool"}
	case "grep":
		return []string{"/usr/bin/grep", "/bin/grep"}
	case "ps":
		return []string{"/usr/bin/ps", "/bin/ps"}
	case "mbuffer":
		return []string{"/usr/bin/mbuffer", "/usr/local/bin/mbuffer"}
	case "gzip":
		return []string{"/usr/bin/gzip", "/bin/gzip"}
	case "zcat":
		return []string{"/usr/bin/zcat", "/bin/zcat"}
	case "pigz":
		return []string{"/usr/bin/pigz", "/usr/local/bin/pigz"}
	case "zstd":
		return []string{"/usr/bin/zstd", "/usr/local/bin/zstd"}
	case "zstdmt":
		return []string{"/usr/bin/zstdmt", "/usr/local/bin/zstdmt"}
	case "xz":
		return []string{"/usr/bin/xz", "/bin/xz"}
	case "lzop":
		return []string{"/usr/bin/lzop", "/usr/local/bin/lzop"}
	case "lz4":
		return []string{"/usr/bin/lz4", "/usr/local/bin/lz4"}
	default:
		return nil
	}
}
