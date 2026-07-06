package datamover

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/mathias/zfsreplicationcontroller/internal/replication"
)

const (
	EnvRole              = "ZFSREP_ROLE"
	EnvSrcDataset        = "SRC_DATASET"
	EnvDstHost           = "DST_HOST"
	EnvDstDataset        = "DST_DATASET"
	EnvSSHKeyFile        = "SSH_KEY_FILE"
	EnvKnownHostsFile    = "KNOWN_HOSTS_FILE"
	EnvSSHPort           = "SSH_PORT"
	EnvNoSyncSnap        = "SYNCOID_NO_SYNC_SNAP"
	EnvNoRollback        = "SYNCOID_NO_ROLLBACK"
	EnvForceDelete       = "SYNCOID_FORCE_DELETE"
	EnvCompress          = "SYNCOID_COMPRESS"
	EnvSyncoidIdentifier = "SYNCOID_IDENTIFIER"
	EnvReceiveUnmounted  = "RECEIVE_UNMOUNTED"
	EnvReceiveResumable  = "RECEIVE_RESUMABLE"
	EnvIncludeSnaps      = "SYNCOID_INCLUDE_SNAPS"
	EnvExcludeSnaps      = "SYNCOID_EXCLUDE_SNAPS"
	EnvExpectedNodeName  = "EXPECTED_NODE_NAME"
	EnvActualNodeName    = "ACTUAL_NODE_NAME"

	RoleSender            = "sender"
	DefaultSSHKeyFile     = "/var/run/zfsrep/ssh/id_rsa"
	DefaultKnownHostsFile = "/var/run/zfsrep/ssh/known_hosts"
	DefaultSSHPort        = "2222"
)

var sshKeyArgPattern = regexp.MustCompile(`--sshkey=\S+`)

type SenderConfig struct {
	SrcDataset        string
	DstHost           string
	DstDataset        string
	SSHKeyFile        string
	KnownHostsFile    string
	SSHPort           string
	NoSyncSnap        bool
	NoRollback        bool
	ForceDelete       bool
	Compress          string
	SyncoidIdentifier string
	ReceiveUnmounted  bool
	ReceiveResumable  bool
	IncludeSnaps      []string
	ExcludeSnaps      []string
	ExpectedNode      string
	ActualNode        string
}

func SenderConfigFromEnv() SenderConfig {
	return SenderConfigFromLookup(os.Getenv)
}

func SenderConfigFromLookup(lookup func(string) string) SenderConfig {
	defaults := replication.DefaultSyncoidOptions()
	return SenderConfig{
		SrcDataset:        lookup(EnvSrcDataset),
		DstHost:           lookup(EnvDstHost),
		DstDataset:        lookup(EnvDstDataset),
		SSHKeyFile:        lookup(EnvSSHKeyFile),
		KnownHostsFile:    lookup(EnvKnownHostsFile),
		SSHPort:           lookup(EnvSSHPort),
		NoSyncSnap:        boolLookupDefault(lookup, EnvNoSyncSnap, defaults.NoSyncSnap),
		NoRollback:        boolLookupDefault(lookup, EnvNoRollback, defaults.NoRollback),
		ForceDelete:       boolLookupDefault(lookup, EnvForceDelete, defaults.ForceDelete),
		Compress:          lookupDefault(lookup, EnvCompress, defaults.Compress),
		SyncoidIdentifier: lookup(EnvSyncoidIdentifier),
		ReceiveUnmounted:  boolLookupDefault(lookup, EnvReceiveUnmounted, defaults.ReceiveUnmounted),
		ReceiveResumable:  boolLookupDefault(lookup, EnvReceiveResumable, defaults.ReceiveResumable),
		IncludeSnaps:      listLookup(lookup, EnvIncludeSnaps),
		ExcludeSnaps:      listLookup(lookup, EnvExcludeSnaps),
		ExpectedNode:      lookup(EnvExpectedNodeName),
		ActualNode:        lookup(EnvActualNodeName),
	}
}

func RunSender(ctx context.Context, cfg SenderConfig, r CommandRunner) error {
	return runSender(ctx, cfg, r, os.Stderr)
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
	stdout, stderr, err := r.Run(ctx, "syncoid", args...)
	logCommandOutput(logw, "stdout", stdout)
	logCommandOutput(logw, "stderr", stderr)
	duration := time.Since(started).Round(time.Millisecond)
	if err != nil {
		summary := syncoidFailureSummary(stdout, stderr, err)
		logSenderLine(logw, "sender completed result=failure exitCode=%d duration=%s error=%q", commandExitCode(err), duration, summary)
		return fmt.Errorf("syncoid failed: %s", summary)
	}
	logSenderLine(logw, "sender completed result=success exitCode=0 duration=%s%s", duration, finalSnapshotLogSuffix(stdout+"\n"+stderr))
	return nil
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
	logSenderLine(w, "sender starting srcDataset=%s dstDataset=%s dstHost=%s sshPort=%s syncoidIdentifier=%s noSyncSnap=%t noRollback=%t forceDelete=%t compress=%s receiveUnmounted=%t receiveResumable=%t includeSnaps=%q excludeSnaps=%q",
		cfg.SrcDataset, cfg.DstDataset, cfg.DstHost, cfg.SSHPort, cfg.SyncoidIdentifier, cfg.NoSyncSnap, cfg.NoRollback, cfg.ForceDelete, cfg.Compress, cfg.ReceiveUnmounted, cfg.ReceiveResumable, strings.Join(cfg.IncludeSnaps, ","), strings.Join(cfg.ExcludeSnaps, ","))
}

func logSenderLine(w io.Writer, format string, args ...any) {
	if w == nil {
		return
	}
	if _, err := fmt.Fprintf(w, format+"\n", args...); err != nil {
		return
	}
}

func logCommandOutput(w io.Writer, stream, value string) {
	value = strings.TrimSpace(redactSensitiveText(value))
	if value == "" {
		return
	}
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			logSenderLine(w, "syncoid %s %s", stream, line)
		}
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

func redactSensitiveText(value string) string {
	return sshKeyArgPattern.ReplaceAllString(value, "--sshkey=<redacted>")
}

func finalSnapshotLogSuffix(output string) string {
	if snapshot := lastSnapshotToken(output); snapshot != "" {
		return " finalSnapshot=" + snapshot
	}
	return ""
}

func lastSnapshotToken(output string) string {
	var last string
	for _, field := range strings.Fields(output) {
		field = strings.Trim(field, `"'.,;()[]{}<>`)
		if _, _, ok := replication.SplitSnapshotTarget(field); ok {
			last = field
		}
	}
	return last
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

func syncoidFailureSummary(stdout, stderr string, err error) string {
	var parts []string
	if stdout = strings.TrimSpace(redactSensitiveText(stdout)); stdout != "" {
		parts = append(parts, "stdout: "+singleLine(stdout))
	}
	if stderr = strings.TrimSpace(redactSensitiveText(stderr)); stderr != "" {
		parts = append(parts, "stderr: "+singleLine(stderr))
	}
	if err != nil {
		parts = append(parts, "error: "+singleLine(redactSensitiveText(err.Error())))
	}
	if len(parts) == 0 {
		return "syncoid exited with an unknown error"
	}
	return strings.Join(parts, "; ")
}

func singleLine(value string) string {
	return strings.Join(strings.Fields(value), " ")
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
