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

	"github.com/mathias/zfsreplicationcontroller/internal/replication"
	"github.com/mathias/zfsreplicationcontroller/internal/replication/diagnosis"
)

const (
	EnvRole                  = "ZFSREP_ROLE"
	EnvSrcDataset            = "SRC_DATASET"
	EnvDstHost               = "DST_HOST"
	EnvDstDataset            = "DST_DATASET"
	EnvSSHKeyFile            = "SSH_KEY_FILE"
	EnvKnownHostsFile        = "KNOWN_HOSTS_FILE"
	EnvSSHPort               = "SSH_PORT"
	EnvNoSyncSnap            = "SYNCOID_NO_SYNC_SNAP"
	EnvNoRollback            = "SYNCOID_NO_ROLLBACK"
	EnvForceDelete           = "SYNCOID_FORCE_DELETE"
	EnvDeleteTargetSnapshots = "SYNCOID_DELETE_TARGET_SNAPSHOTS"
	EnvCompress              = "SYNCOID_COMPRESS"
	EnvSyncoidIdentifier     = "SYNCOID_IDENTIFIER"
	EnvReceiveUnmounted      = "RECEIVE_UNMOUNTED"
	EnvReceiveResumable      = "RECEIVE_RESUMABLE"
	EnvIncludeSnaps          = "SYNCOID_INCLUDE_SNAPS"
	EnvExcludeSnaps          = "SYNCOID_EXCLUDE_SNAPS"
	EnvExpectedNodeName      = "EXPECTED_NODE_NAME"
	EnvActualNodeName        = "ACTUAL_NODE_NAME"

	RoleSender            = "sender"
	DefaultSSHKeyFile     = "/var/run/zfsrep/ssh/id_rsa"
	DefaultKnownHostsFile = "/var/run/zfsrep/ssh/known_hosts"
	DefaultSSHPort        = "2222"
)

type SenderConfig struct {
	SrcDataset            string
	DstHost               string
	DstDataset            string
	SSHKeyFile            string
	KnownHostsFile        string
	SSHPort               string
	NoSyncSnap            bool
	NoRollback            bool
	ForceDelete           bool
	DeleteTargetSnapshots bool
	Compress              string
	SyncoidIdentifier     string
	ReceiveUnmounted      bool
	ReceiveResumable      bool
	IncludeSnaps          []string
	ExcludeSnaps          []string
	ExpectedNode          string
	ActualNode            string
}

func SenderConfigFromEnv() SenderConfig {
	return SenderConfigFromLookup(os.Getenv)
}

func SenderConfigFromLookup(lookup func(string) string) SenderConfig {
	defaults := replication.DefaultSyncoidOptions()
	return SenderConfig{
		SrcDataset:            lookup(EnvSrcDataset),
		DstHost:               lookup(EnvDstHost),
		DstDataset:            lookup(EnvDstDataset),
		SSHKeyFile:            lookup(EnvSSHKeyFile),
		KnownHostsFile:        lookup(EnvKnownHostsFile),
		SSHPort:               lookup(EnvSSHPort),
		NoSyncSnap:            boolLookupDefault(lookup, EnvNoSyncSnap, defaults.NoSyncSnap),
		NoRollback:            boolLookupDefault(lookup, EnvNoRollback, defaults.NoRollback),
		ForceDelete:           boolLookupDefault(lookup, EnvForceDelete, defaults.ForceDelete),
		DeleteTargetSnapshots: boolLookupDefault(lookup, EnvDeleteTargetSnapshots, defaults.DeleteTargetSnapshots),
		Compress:              lookupDefault(lookup, EnvCompress, defaults.Compress),
		SyncoidIdentifier:     lookup(EnvSyncoidIdentifier),
		ReceiveUnmounted:      boolLookupDefault(lookup, EnvReceiveUnmounted, defaults.ReceiveUnmounted),
		ReceiveResumable:      boolLookupDefault(lookup, EnvReceiveResumable, defaults.ReceiveResumable),
		IncludeSnaps:          listLookup(lookup, EnvIncludeSnaps),
		ExcludeSnaps:          listLookup(lookup, EnvExcludeSnaps),
		ExpectedNode:          lookup(EnvExpectedNodeName),
		ActualNode:            lookup(EnvActualNodeName),
	}
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
	compress, err := replication.SyncoidCompression(cfg.Compress)
	if err != nil {
		return err
	}
	if cfg.SyncoidIdentifier != "" && !replication.ValidSyncoidIdentifier(cfg.SyncoidIdentifier) {
		return fmt.Errorf("unsupported syncoid identifier %q", cfg.SyncoidIdentifier)
	}
	if cfg.DstHost != "" && cfg.KnownHostsFile == "" {
		return fmt.Errorf("known hosts file is required for SSH replication")
	}
	args := syncoidArgs(cfg, compress)
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

func syncoidArgs(cfg SenderConfig, compress string) []string {
	var args []string
	if cfg.NoSyncSnap {
		args = append(args, "--no-sync-snap")
	}
	if cfg.NoRollback {
		args = append(args, "--no-rollback")
	}
	args = append(args, "--no-privilege-elevation")
	if compress != "" {
		args = append(args, "--compress="+compress)
	}
	if cfg.SyncoidIdentifier != "" {
		args = append(args, "--identifier="+cfg.SyncoidIdentifier)
	}
	if cfg.DeleteTargetSnapshots {
		args = append(args, "--delete-target-snapshots")
	}
	if cfg.KnownHostsFile != "" {
		args = append(args,
			"--sshoption=UserKnownHostsFile="+cfg.KnownHostsFile,
			"--sshoption=StrictHostKeyChecking=yes",
			"--sshoption=IdentitiesOnly=yes",
		)
	}
	if cfg.SSHKeyFile != "" {
		args = append(args, "--sshkey="+cfg.SSHKeyFile)
	}
	if cfg.SSHPort != "" {
		args = append(args, "--sshport="+cfg.SSHPort)
	}
	if cfg.ReceiveUnmounted {
		args = append(args, "--recvoptions=u")
	}
	if !cfg.ReceiveResumable {
		args = append(args, "--no-resume")
	}
	for _, include := range cfg.IncludeSnaps {
		args = append(args, "--include-snaps="+include)
	}
	for _, exclude := range cfg.ExcludeSnaps {
		args = append(args, "--exclude-snaps="+exclude)
	}
	if cfg.ForceDelete {
		args = append(args, "--force-delete")
	}
	return append(args, cfg.SrcDataset, syncoidTarget(cfg.DstHost, cfg.DstDataset))
}

func logSenderStart(w io.Writer, cfg SenderConfig) {
	logSenderLine(w, "sender starting srcDataset=%s dstDataset=%s dstHost=%s sshPort=%s syncoidIdentifier=%s noSyncSnap=%t noRollback=%t forceDelete=%t deleteTargetSnapshots=%t compress=%s receiveUnmounted=%t receiveResumable=%t includeSnaps=%q excludeSnaps=%q",
		cfg.SrcDataset, cfg.DstDataset, cfg.DstHost, cfg.SSHPort, cfg.SyncoidIdentifier, cfg.NoSyncSnap, cfg.NoRollback, cfg.ForceDelete, cfg.DeleteTargetSnapshots, cfg.Compress, cfg.ReceiveUnmounted, cfg.ReceiveResumable, strings.Join(cfg.IncludeSnaps, ","), strings.Join(cfg.ExcludeSnaps, ","))
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

func syncoidTarget(host, dataset string) string {
	if host == "" {
		return dataset
	}
	return host + ":" + dataset
}

func lookupDefault(lookup func(string) string, key, def string) string {
	if v := lookup(key); v != "" {
		return v
	}
	return def
}

func boolLookupDefault(lookup func(string) string, key string, def bool) bool {
	v := lookup(key)
	if v == "" {
		return def
	}
	return v == "true"
}

func listLookup(lookup func(string) string, key string) []string {
	var out []string
	for _, line := range strings.Split(lookup(key), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return out
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
