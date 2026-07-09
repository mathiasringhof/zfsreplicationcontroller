package datamover

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mathias/zfsreplicationcontroller/internal/replication"
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

	commandOutputTailLimit         = 64 * 1024
	commandOutputRedactionLookback = 4 * 1024
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
	stdout, stderr, err := runSyncoidCommand(ctx, r, logw, args...)
	duration := time.Since(started).Round(time.Millisecond)
	if err != nil {
		summary := syncoidFailureSummary(stdout, stderr, err)
		logSenderLine(logw, "sender completed result=failure exitCode=%d duration=%s error=%q", commandExitCode(err), duration, summary)
		return fmt.Errorf("syncoid failed: %s", summary)
	}
	logSenderLine(logw, "sender completed result=success exitCode=0 duration=%s%s", duration, successSummaryLogSuffix(stdout+"\n"+stderr))
	return nil
}

func runSyncoidCommand(ctx context.Context, r CommandRunner, logw io.Writer, args ...string) (string, string, error) {
	if streaming, ok := r.(StreamingCommandRunner); ok {
		var logMu sync.Mutex
		stdout := newCommandOutputStreamer(logw, "stdout", &logMu)
		stderr := newCommandOutputStreamer(logw, "stderr", &logMu)
		err := streaming.RunStreaming(ctx, "syncoid", stdout, stderr, args...)
		stdout.Flush()
		stderr.Flush()
		return stdout.Tail(), stderr.Tail(), err
	}
	stdout, stderr, err := r.Run(ctx, "syncoid", args...)
	stdout = boundedRedactedOutputTail(stdout)
	stderr = boundedRedactedOutputTail(stderr)
	logCommandOutput(logw, "stdout", stdout)
	logCommandOutput(logw, "stderr", stderr)
	return stdout, stderr, err
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

type commandOutputStreamer struct {
	logw    io.Writer
	stream  string
	logMu   *sync.Mutex
	tail    outputTail
	pending []byte
}

func newCommandOutputStreamer(logw io.Writer, stream string, logMu *sync.Mutex) *commandOutputStreamer {
	return &commandOutputStreamer{
		logw:   logw,
		stream: stream,
		logMu:  logMu,
		tail:   outputTail{limit: commandOutputTailLimit + commandOutputRedactionLookback},
	}
}

func (s *commandOutputStreamer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if _, err := s.tail.Write(p); err != nil {
		return 0, err
	}
	remaining := p
	for len(remaining) > 0 {
		if i := bytes.IndexAny(remaining, "\r\n"); i >= 0 {
			s.appendPending(remaining[:i])
			s.flushLine()
			remaining = remaining[i+1:]
			continue
		}
		s.appendPending(remaining)
		s.flushPartial()
		break
	}
	return len(p), nil
}

func (s *commandOutputStreamer) Flush() {
	s.flushLine()
}

func (s *commandOutputStreamer) Tail() string {
	return boundedRedactedOutputTail(s.tail.String())
}

func (s *commandOutputStreamer) appendPending(p []byte) {
	s.pending = append(s.pending, p...)
}

func (s *commandOutputStreamer) flushLine() {
	if len(s.pending) == 0 {
		return
	}
	s.logPending(boundedRedactedOutputTail(string(s.pending)))
	s.pending = s.pending[:0]
}

func (s *commandOutputStreamer) flushPartial() {
	if len(s.pending) <= commandOutputTailLimit+commandOutputRedactionLookback {
		return
	}
	s.logPending(boundedRedactedOutputTail(string(s.pending)))
	keep := commandOutputRedactionLookback
	if keep > len(s.pending) {
		keep = len(s.pending)
	}
	s.pending = append(s.pending[:0], s.pending[len(s.pending)-keep:]...)
}

func (s *commandOutputStreamer) logPending(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	if s.logMu != nil {
		s.logMu.Lock()
		defer s.logMu.Unlock()
	}
	logSenderLine(s.logw, "syncoid %s %s", s.stream, line)
}

type outputTail struct {
	limit int
	data  []byte
}

func (t *outputTail) Write(p []byte) (int, error) {
	if t.limit <= 0 {
		return len(p), nil
	}
	if len(p) >= t.limit {
		t.data = append(t.data[:0], p[len(p)-t.limit:]...)
		return len(p), nil
	}
	overflow := len(t.data) + len(p) - t.limit
	if overflow > 0 {
		copy(t.data, t.data[overflow:])
		t.data = t.data[:len(t.data)-overflow]
	}
	t.data = append(t.data, p...)
	return len(p), nil
}

func (t *outputTail) String() string {
	return string(t.data)
}

func boundedOutputTail(value string) string {
	if len(value) <= commandOutputTailLimit {
		return value
	}
	return value[len(value)-commandOutputTailLimit:]
}

func boundedRedactedOutputTail(value string) string {
	if limit := commandOutputTailLimit + commandOutputRedactionLookback; len(value) > limit {
		value = value[len(value)-limit:]
	}
	return boundedOutputTail(redactSensitiveText(value))
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
	return RedactSensitiveText(value)
}

func RedactSensitiveText(value string) string {
	value = redactOptionValue(value, "--sshkey=", "--sshkey=<redacted>")
	value = redactSeparatedOptionValue(value, "-i", "-i <redacted>")
	value = redactSeparatedOptionValue(value, "-S", "-S <redacted>")
	value = strings.ReplaceAll(value, DefaultSSHKeyFile, "<redacted>")
	return value
}

func redactOptionValue(value, option, replacement string) string {
	if !strings.Contains(value, option) {
		return value
	}
	var out strings.Builder
	out.Grow(len(value))
	remaining := value
	for {
		idx := strings.Index(remaining, option)
		if idx < 0 {
			out.WriteString(remaining)
			return out.String()
		}
		out.WriteString(remaining[:idx])
		out.WriteString(replacement)
		remaining = remaining[optionValueEnd(remaining, idx+len(option)):]
	}
}

func redactSeparatedOptionValue(value, option, replacement string) string {
	var out strings.Builder
	out.Grow(len(value))
	remaining := value
	for {
		idx := separatedOptionIndex(remaining, option)
		if idx < 0 {
			out.WriteString(remaining)
			return out.String()
		}
		out.WriteString(remaining[:idx])
		valueStart := idx + len(option)
		for valueStart < len(remaining) && isSpace(remaining[valueStart]) {
			valueStart++
		}
		if valueStart >= len(remaining) {
			out.WriteString(remaining[idx:])
			return out.String()
		}
		out.WriteString(replacement)
		remaining = remaining[optionValueEnd(remaining, valueStart):]
	}
}

func separatedOptionIndex(value, option string) int {
	for searchStart := 0; searchStart < len(value); {
		idx := strings.Index(value[searchStart:], option)
		if idx < 0 {
			return -1
		}
		idx += searchStart
		beforeOK := idx == 0 || isSpace(value[idx-1])
		after := idx + len(option)
		afterOK := after < len(value) && isSpace(value[after])
		if beforeOK && afterOK {
			return idx
		}
		searchStart = idx + len(option)
	}
	return -1
}

func optionValueEnd(value string, start int) int {
	if start >= len(value) {
		return start
	}
	if quote, consumed, ok := escapedQuoteAt(value, start); ok {
		return quotedValueEnd(value, start+consumed, quote, true, start)
	}
	if isQuote(value[start]) {
		return quotedValueEnd(value, start+1, value[start], false, start)
	}
	return unquotedValueEnd(value, start)
}

func quotedValueEnd(value string, start int, quote byte, escaped bool, fallbackStart int) int {
	for i := start; i < len(value); i++ {
		if escaped {
			if foundQuote, consumed, ok := escapedQuoteAt(value, i); ok && foundQuote == quote {
				return i + consumed
			}
		} else if value[i] == '\\' && i+1 < len(value) {
			i++
			continue
		}
		if value[i] == quote {
			return i + 1
		}
	}
	return unquotedValueEnd(value, fallbackStart)
}

func unquotedValueEnd(value string, start int) int {
	for i := start; i < len(value); i++ {
		if isSpace(value[i]) {
			return i
		}
	}
	return len(value)
}

func isSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func escapedQuoteAt(value string, pos int) (byte, int, bool) {
	backslashes := 0
	for pos+backslashes < len(value) && value[pos+backslashes] == '\\' {
		backslashes++
	}
	if backslashes == 0 || pos+backslashes >= len(value) || !isQuote(value[pos+backslashes]) {
		return 0, 0, false
	}
	return value[pos+backslashes], backslashes + 1, true
}

func isQuote(ch byte) bool {
	return ch == '"' || ch == '\''
}

func successSummaryLogSuffix(output string) string {
	var parts []string
	if mode := syncoidTransferMode(output); mode != "" {
		parts = append(parts, "mode="+mode)
	}
	if size := syncoidSizeEstimate(output); size != "" {
		parts = append(parts, "sizeEstimate="+strings.ReplaceAll(size, " ", ""))
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

func syncoidFailureSummary(stdout, stderr string, err error) string {
	var parts []string
	if stdout = conciseSyncoidFailure(stdout); stdout != "" {
		parts = append(parts, "stdout: "+stdout)
	}
	if stderr = conciseSyncoidFailure(stderr); stderr != "" {
		parts = append(parts, "stderr: "+stderr)
	}
	if err != nil {
		parts = append(parts, "error: "+conciseSyncoidFailure(err.Error()))
	}
	if len(parts) == 0 {
		return "syncoid exited with an unknown error"
	}
	return strings.Join(parts, "; ")
}

func conciseSyncoidFailure(value string) string {
	value = strings.TrimSpace(redactSensitiveText(value))
	if value == "" {
		return ""
	}
	if line := firstUsefulFailureLine(value); line != "" {
		return singleLine(truncateSyncoidPipeline(line))
	}
	return singleLine(value)
}

func truncateSyncoidPipeline(value string) string {
	value = strings.TrimSpace(value)
	if idx := strings.Index(value, "CRITICAL ERROR:"); idx >= 0 {
		critical := strings.TrimSpace(value[idx:])
		if strings.Contains(critical, "zfs send") || strings.Contains(critical, "zfs receive") || strings.Contains(critical, "|") {
			if target := criticalPipelineReceiveTarget(critical); target != "" {
				return "CRITICAL ERROR: syncoid command failed target=" + target
			}
			return "CRITICAL ERROR: syncoid command failed"
		}
		return critical
	}
	return value
}

func criticalPipelineReceiveTarget(value string) string {
	idx := strings.LastIndex(value, " zfs receive")
	if idx < 0 {
		return ""
	}
	fields := strings.Fields(value[idx+len(" zfs receive"):])
	for i := 0; i < len(fields); i++ {
		field := cleanFailureTargetField(fields[i])
		if field == "" || isShellRedirection(field) {
			continue
		}
		if field == "failed" || field == "failed:" {
			return ""
		}
		if strings.HasPrefix(field, "-") {
			if receiveOptionTakesValue(field) {
				i++
			}
			continue
		}
		if field != "sh" {
			return field
		}
	}
	return ""
}

func cleanFailureTargetField(field string) string {
	field = strings.Trim(field, `"'.,;()[]{}<>`)
	if field == "" || strings.HasPrefix(field, "-") {
		return ""
	}
	return field
}

func receiveOptionTakesValue(field string) bool {
	return field == "-o" || field == "-x"
}

func isShellRedirection(field string) bool {
	return strings.Contains(field, ">&") || strings.HasPrefix(field, ">") || strings.HasPrefix(field, "2>")
}

func firstUsefulFailureLine(value string) string {
	lines := strings.Split(value, "\n")
	for _, needle := range []string{"CRITICAL ERROR:", "cannot open", "cannot receive"} {
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if strings.Contains(line, needle) {
				return line
			}
		}
	}
	return ""
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
