package datamover

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mathias/zfsreplicationcontroller/internal/replication/diagnosis"
	"github.com/mathias/zfsreplicationcontroller/internal/syncoid"
)

const (
	EnvRole             = "ZFSREP_ROLE"
	EnvExpectedNodeName = "EXPECTED_NODE_NAME"
	EnvActualNodeName   = "ACTUAL_NODE_NAME"

	RoleSender            = "sender"
	DefaultSSHKeyFile     = "/var/run/zfsrep/ssh/id_rsa"
	DefaultKnownHostsFile = "/var/run/zfsrep/ssh/known_hosts"
	DefaultSSHPort        = "2222"
)

type SenderConfig struct {
	Invocation   syncoid.Invocation
	ExpectedNode string
	ActualNode   string
}

func SenderConfigFromEnv() (SenderConfig, error) {
	return SenderConfigFromLookup(os.LookupEnv)
}

func SenderConfigFromLookup(lookup func(string) (string, bool)) (SenderConfig, error) {
	invocation, err := syncoid.DecodeSenderEnvironment(lookup)
	if err != nil {
		return SenderConfig{}, fmt.Errorf("decode Syncoid sender environment: %w", err)
	}
	expectedNode, _ := lookup(EnvExpectedNodeName)
	actualNode, _ := lookup(EnvActualNodeName)
	return SenderConfig{Invocation: invocation, ExpectedNode: expectedNode, ActualNode: actualNode}, nil
}

func RunSender(ctx context.Context, cfg SenderConfig, r CommandRunner) error {
	return RunSenderWithLog(ctx, cfg, r, os.Stderr)
}

func RunSenderWithLog(ctx context.Context, cfg SenderConfig, r CommandRunner, logw io.Writer) error {
	return runSender(ctx, cfg, r, logw)
}

func runSender(ctx context.Context, cfg SenderConfig, r CommandRunner, logw io.Writer) error {
	started := time.Now()
	if err := validateNode(cfg.ExpectedNode, cfg.ActualNode); err != nil {
		return err
	}
	args := cfg.Invocation.Arguments()
	logSenderStart(logw, cfg)
	logSenderLine(logw, "syncoid command command=%s", strings.Join(sanitizeSyncoidArgs(args), " "))
	summarySuffix, err := runSyncoidCommand(ctx, r, logw, args...)
	duration := time.Since(started).Round(time.Millisecond)
	if err != nil {
		logSenderLine(logw, "sender completed result=failure exitCode=%d duration=%s error=%q", commandExitCode(err), duration, err.Error())
		return err
	}
	logSenderLine(logw, "sender completed result=success exitCode=0 duration=%s%s", duration, summarySuffix)
	return nil
}

func runSyncoidCommand(ctx context.Context, r CommandRunner, logw io.Writer, args ...string) (string, error) {
	var logMu sync.Mutex
	var summary syncoidSuccessSummary
	capture := diagnosis.NewCapture(func(stream diagnosis.Stream, line string) {
		logMu.Lock()
		defer logMu.Unlock()
		logSenderLine(logw, "syncoid %s %s", stream, line)
		summary.observe(line)
	})
	var err error
	if streaming, ok := r.(StreamingCommandRunner); ok {
		err = streaming.RunStreaming(ctx, "syncoid", capture.Stdout(), capture.Stderr(), args...)
	} else {
		stdout, stderr, runErr := r.Run(ctx, "syncoid", args...)
		if _, writeErr := io.WriteString(capture.Stdout(), stdout); writeErr != nil {
			return "", capture.Failure(fmt.Errorf("capture syncoid stdout: %w", writeErr))
		}
		if _, writeErr := io.WriteString(capture.Stderr(), stderr); writeErr != nil {
			return "", capture.Failure(fmt.Errorf("capture syncoid stderr: %w", writeErr))
		}
		err = runErr
	}
	if err != nil {
		return "", capture.Failure(err)
	}
	capture.Flush()
	return summary.suffix(), nil
}

func logSenderStart(w io.Writer, cfg SenderConfig) {
	logSenderLine(w, "sender starting %s", cfg.Invocation.Summary())
}

func logSenderLine(w io.Writer, format string, args ...any) {
	if w == nil {
		return
	}
	if _, err := fmt.Fprintf(w, format+"\n", args...); err != nil {
		return
	}
}

func sanitizeSyncoidArgs(args []string) []string {
	out := append([]string(nil), args...)
	for i, arg := range out {
		if strings.HasPrefix(arg, "--sshkey=") {
			out[i] = "--sshkey=<redacted>"
		}
	}
	return out
}

type syncoidSuccessSummary struct {
	mode string
	size string
}

func (s *syncoidSuccessSummary) observe(output string) {
	if mode := syncoidTransferMode(output); mode != "" {
		s.mode = mode
	}
	if size := syncoidSizeEstimate(output); size != "" {
		s.size = size
	}
}

func (s syncoidSuccessSummary) suffix() string {
	var parts []string
	if s.mode != "" {
		parts = append(parts, "mode="+s.mode)
	}
	if s.size != "" {
		parts = append(parts, "sizeEstimate="+strings.ReplaceAll(s.size, " ", ""))
	}
	if len(parts) == 0 {
		return ""
	}
	return " " + strings.Join(parts, " ")
}

func syncoidTransferMode(output string) string {
	switch {
	case strings.Contains(output, "Sending oldest full snapshot"):
		return "full"
	case strings.Contains(output, "Sending incremental"):
		return "incremental"
	default:
		return ""
	}
}

func syncoidSizeEstimate(output string) string {
	searchStart := 0
	for searchStart < len(output) {
		idx := strings.Index(output[searchStart:], "(~ ")
		if idx < 0 {
			return ""
		}
		start := searchStart + idx + len("(~ ")
		end := strings.IndexByte(output[start:], ')')
		if end < 0 {
			return ""
		}
		size := strings.TrimSpace(output[start : start+end])
		if size != "" {
			return size
		}
		searchStart = start + end + 1
	}
	return ""
}

type exitCoder interface {
	ExitCode() int
}

func commandExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr exitCoder
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func validateNode(expected, actual string) error {
	if expected == "" {
		return nil
	}
	if actual != expected {
		return fmt.Errorf("node verification failed: expected %q, got %q", expected, actual)
	}
	return nil
}
