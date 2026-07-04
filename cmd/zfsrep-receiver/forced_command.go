package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"unicode"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
)

const receiverCommandPath = "/usr/local/bin/zfsrep-receiver"

type receiverTaskAuthorization struct {
	AuthorizedKey string
	PolicyPath    string
	Policy        receiverCommandPolicy
}

type receiverCommandPolicy struct {
	TargetDataset            string `json:"targetDataset"`
	ReceiveUnmounted         bool   `json:"receiveUnmounted"`
	ReceiveResumable         bool   `json:"receiveResumable"`
	AllowRollback            bool   `json:"allowRollback,omitempty"`
	AllowDestroy             bool   `json:"allowDestroy,omitempty"`
	AllowMount               bool   `json:"allowMount,omitempty"`
	AllowSyncSnapshotDestroy bool   `json:"allowSyncSnapshotDestroy,omitempty"`
	Compression              string `json:"compression"`
}

type forcedCommandConfig struct {
	OriginalCommand string
	Policy          receiverCommandPolicy
	Stdin           io.Reader
	Stdout          io.Writer
	Stderr          io.Writer
}

type receiverCommandPlan struct {
	kind          receiverCommandKind
	echoArgs      []string
	lookupCommand string
	pipeline      []receiverCommandStep
	batch         []receiverCommandPlan
}

type receiverCommandKind string

const (
	receiverCommandExit     receiverCommandKind = "exit"
	receiverCommandEcho     receiverCommandKind = "echo"
	receiverCommandLookup   receiverCommandKind = "lookup"
	receiverCommandPipeline receiverCommandKind = "pipeline"
	receiverCommandBatch    receiverCommandKind = "batch"
)

type receiverCommandStep struct {
	Name           string
	Args           []string
	StdoutNull     bool
	StderrNull     bool
	StderrToStdout bool
}

type forcedCommandExitError struct {
	code int
	msg  string
}

func (e forcedCommandExitError) Error() string {
	return e.msg
}

func (e forcedCommandExitError) ExitCode() int {
	return e.code
}

func runForcedCommandFromArgs(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("exec", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var policyPath string
	fs.StringVar(&policyPath, "policy", "", "receiver policy file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if policyPath == "" {
		return fmt.Errorf("receiver policy file is required")
	}
	policy, err := readReceiverPolicy(policyPath)
	if err != nil {
		return err
	}
	return runForcedCommand(ctx, forcedCommandConfig{
		OriginalCommand: os.Getenv("SSH_ORIGINAL_COMMAND"),
		Policy:          policy,
		Stdin:           os.Stdin,
		Stdout:          os.Stdout,
		Stderr:          os.Stderr,
	})
}

func runForcedCommand(ctx context.Context, cfg forcedCommandConfig) error {
	if strings.TrimSpace(cfg.OriginalCommand) == "" {
		return fmt.Errorf("missing SSH_ORIGINAL_COMMAND")
	}
	plan, err := authorizeReceiverCommand(cfg.OriginalCommand, cfg.Policy)
	if err != nil {
		return fmt.Errorf("receiver command denied: %w", err)
	}
	return executeReceiverCommandPlan(ctx, cfg, plan)
}

func readReceiverPolicy(path string) (receiverCommandPolicy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return receiverCommandPolicy{}, fmt.Errorf("read receiver policy: %w", err)
	}
	var policy receiverCommandPolicy
	if err := json.Unmarshal(data, &policy); err != nil {
		return receiverCommandPolicy{}, fmt.Errorf("parse receiver policy: %w", err)
	}
	policy.Compression = compressionDefault(policy.Compression)
	return policy, nil
}

func receiveTaskAuthorization(cfg receiverConfig, task *zfsv1.ZFSReceiveTask) receiverTaskAuthorization {
	policyPath := filepath.Join(receiverPolicyDir(cfg), receiverPolicyFileName(task))
	policy := receiverCommandPolicy{
		TargetDataset:            task.Spec.Destination.Dataset,
		ReceiveUnmounted:         task.Spec.Policy.ReceiveUnmounted,
		ReceiveResumable:         task.Spec.Policy.ReceiveResumable,
		AllowRollback:            task.Spec.Policy.AllowRollback,
		AllowDestroy:             task.Spec.Policy.AllowDestroy,
		AllowMount:               task.Spec.Policy.AllowMount,
		AllowSyncSnapshotDestroy: task.Spec.Policy.AllowSyncSnapshotDestroy,
		Compression:              compressionDefault(task.Spec.Policy.Compression),
	}
	forcedCommand := receiverCommandPath + " exec --policy " + policyPath
	key := "restrict,command=\"" + escapeAuthorizedKeysOption(forcedCommand) + "\" " + strings.TrimSpace(task.Spec.SSH.AuthorizedPublicKey)
	return receiverTaskAuthorization{
		AuthorizedKey: key,
		PolicyPath:    policyPath,
		Policy:        policy,
	}
}

func receiverPolicyDir(cfg receiverConfig) string {
	return filepath.Join(filepath.Dir(cfg.AuthorizedKeysFile), "policies")
}

func receiverPolicyFileName(task *zfsv1.ZFSReceiveTask) string {
	return sanitizePolicyPathSegment(task.Namespace) + "_" + sanitizePolicyPathSegment(task.Name) + ".json"
}

func sanitizePolicyPathSegment(value string) string {
	var out strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '.' {
			out.WriteRune(r)
		} else {
			out.WriteByte('_')
		}
	}
	if out.Len() == 0 {
		return "default"
	}
	return out.String()
}

func escapeAuthorizedKeysOption(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `"`, `\"`)
}

func writeReceiverPolicies(dir string, policies map[string]receiverCommandPolicy) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create receiver policy directory: %w", err)
	}
	active := map[string]struct{}{}
	paths := make([]string, 0, len(policies))
	for path := range policies {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		if filepath.Dir(path) != dir {
			return fmt.Errorf("receiver policy path %q is outside policy directory", path)
		}
		data, err := json.Marshal(policies[path])
		if err != nil {
			return fmt.Errorf("marshal receiver policy: %w", err)
		}
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, append(data, '\n'), 0o600); err != nil {
			return fmt.Errorf("write receiver policy: %w", err)
		}
		if err := os.Rename(tmp, path); err != nil {
			return fmt.Errorf("replace receiver policy: %w", err)
		}
		active[filepath.Base(path)] = struct{}{}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("list receiver policy directory: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if _, ok := active[entry.Name()]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return fmt.Errorf("remove stale receiver policy: %w", err)
		}
	}
	return nil
}

func authorizeReceiverCommand(raw string, policy receiverCommandPolicy) (receiverCommandPlan, error) {
	if strings.Contains(raw, ";") {
		return authorizeReceiverCommandBatch(raw, policy)
	}
	policy.Compression = compressionDefault(policy.Compression)
	if policy.TargetDataset == "" {
		return receiverCommandPlan{}, fmt.Errorf("target dataset is empty")
	}
	tokens, err := tokenizeReceiverCommand(raw)
	if err != nil {
		return receiverCommandPlan{}, err
	}
	steps, err := parseReceiverCommandSteps(tokens)
	if err != nil {
		return receiverCommandPlan{}, err
	}
	if len(steps) == 1 {
		step := steps[0]
		switch step.Name {
		case "exit":
			if receiverStepHasNoRedirects(step) && (len(step.Args) == 0 || len(step.Args) == 1 && step.Args[0] == "0") {
				return receiverCommandPlan{kind: receiverCommandExit}, nil
			}
		case "echo":
			if receiverStepHasNoRedirects(step) && len(step.Args) >= 1 && step.Args[0] == "-n" {
				return receiverCommandPlan{kind: receiverCommandEcho, echoArgs: step.Args[1:]}, nil
			}
		case "command":
			if receiverStepHasNoStdoutRedirects(step) && len(step.Args) == 2 && step.Args[0] == "-v" && commandLookupAllowed(step.Args[1], policy) {
				return receiverCommandPlan{kind: receiverCommandLookup, lookupCommand: step.Args[1]}, nil
			}
		}
	}
	if validateZpoolFeatureCheck(steps, policy) == nil {
		return receiverCommandPlan{kind: receiverCommandPipeline, pipeline: steps}, nil
	}
	if validateReceivePipeline(steps, policy) == nil {
		return receiverCommandPlan{kind: receiverCommandPipeline, pipeline: steps}, nil
	}
	if len(steps) != 1 {
		return receiverCommandPlan{}, fmt.Errorf("unsupported command pipeline")
	}
	if err := validateSingleReceiverStep(steps[0], policy); err != nil {
		return receiverCommandPlan{}, err
	}
	return receiverCommandPlan{kind: receiverCommandPipeline, pipeline: steps}, nil
}

func authorizeReceiverCommandBatch(raw string, policy receiverCommandPolicy) (receiverCommandPlan, error) {
	parts := strings.Split(raw, ";")
	batch := make([]receiverCommandPlan, 0, len(parts))
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			if i == len(parts)-1 {
				continue
			}
			return receiverCommandPlan{}, fmt.Errorf("empty command in batch")
		}
		plan, err := authorizeReceiverCommand(part, policy)
		if err != nil {
			return receiverCommandPlan{}, err
		}
		if !receiverPlanIsDestroy(plan) {
			return receiverCommandPlan{}, fmt.Errorf("only zfs destroy commands may be batched")
		}
		batch = append(batch, plan)
	}
	if len(batch) == 0 {
		return receiverCommandPlan{}, fmt.Errorf("empty command batch")
	}
	return receiverCommandPlan{kind: receiverCommandBatch, batch: batch}, nil
}

func receiverPlanIsDestroy(plan receiverCommandPlan) bool {
	return plan.kind == receiverCommandPipeline &&
		len(plan.pipeline) == 1 &&
		plan.pipeline[0].Name == "zfs" &&
		len(plan.pipeline[0].Args) > 0 &&
		plan.pipeline[0].Args[0] == "destroy"
}

func tokenizeReceiverCommand(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("command is empty")
	}
	var tokens []string
	var current strings.Builder
	var quote rune
	flush := func() {
		if current.Len() == 0 {
			return
		}
		tokens = append(tokens, current.String())
		current.Reset()
	}
	for _, r := range raw {
		if quote != 0 {
			if r == quote {
				quote = 0
				continue
			}
			if r == '\n' || r == '\r' || r == '`' || r == '$' {
				return nil, fmt.Errorf("unsupported quoted character %q", r)
			}
			current.WriteRune(r)
			continue
		}
		switch {
		case unicode.IsSpace(r):
			flush()
		case r == '\'' || r == '"':
			quote = r
		case r == '|':
			flush()
			tokens = append(tokens, "|")
		case r == '\n' || r == '\r' || r == ';' || r == '`' || r == '$' || r == '(' || r == ')' || r == '<':
			return nil, fmt.Errorf("unsupported shell character %q", r)
		default:
			current.WriteRune(r)
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote")
	}
	flush()
	for _, token := range tokens {
		if token == "||" || token == "&&" {
			return nil, fmt.Errorf("unsupported shell operator %q", token)
		}
		if strings.Contains(token, "&") && token != "2>&1" {
			return nil, fmt.Errorf("unsupported shell operator in %q", token)
		}
		if strings.Contains(token, ">") && token != ">/dev/null" && token != "2>/dev/null" && token != "2>&1" {
			return nil, fmt.Errorf("unsupported redirection %q", token)
		}
	}
	return tokens, nil
}

func parseReceiverCommandSteps(tokens []string) ([]receiverCommandStep, error) {
	var steps []receiverCommandStep
	var part []string
	for _, token := range tokens {
		if token != "|" {
			part = append(part, token)
			continue
		}
		step, err := parseReceiverCommandStep(part)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
		part = nil
	}
	step, err := parseReceiverCommandStep(part)
	if err != nil {
		return nil, err
	}
	steps = append(steps, step)
	return steps, nil
}

func parseReceiverCommandStep(tokens []string) (receiverCommandStep, error) {
	if len(tokens) == 0 {
		return receiverCommandStep{}, fmt.Errorf("empty command in pipeline")
	}
	var args []string
	var step receiverCommandStep
	seenRedirect := false
	for _, token := range tokens {
		switch token {
		case ">/dev/null":
			seenRedirect = true
			step.StdoutNull = true
		case "2>/dev/null":
			seenRedirect = true
			step.StderrNull = true
		case "2>&1":
			seenRedirect = true
			step.StderrToStdout = true
		default:
			if seenRedirect {
				return receiverCommandStep{}, fmt.Errorf("command arguments after redirection are not supported")
			}
			args = append(args, token)
		}
	}
	if len(args) == 0 {
		return receiverCommandStep{}, fmt.Errorf("empty command")
	}
	step.Name = args[0]
	step.Args = args[1:]
	return step, nil
}

func commandLookupAllowed(name string, policy receiverCommandPolicy) bool {
	if name == "mbuffer" {
		return true
	}
	return policy.Compression != "none" && name == policy.Compression && compressorAllowed(name)
}

func validateSingleReceiverStep(step receiverCommandStep, policy receiverCommandPolicy) error {
	switch step.Name {
	case "ps":
		if !receiverStepHasNoRedirects(step) || !stringSlicesEqual(step.Args, []string{"-Ao", "args="}) {
			return fmt.Errorf("unsupported ps command")
		}
	case "zpool":
		if !zpoolFeatureGetAllowed(step, policy) {
			return fmt.Errorf("unsupported zpool command")
		}
	case "zfs":
		if err := validateZFSStep(step, policy); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported command %q", step.Name)
	}
	return nil
}

func validateZFSStep(step receiverCommandStep, policy receiverCommandPolicy) error {
	if len(step.Args) == 0 {
		return fmt.Errorf("missing zfs subcommand")
	}
	switch step.Args[0] {
	case "get":
		if !receiverStepHasNoRedirects(step) || !zfsGetAllowed(step.Args, policy.TargetDataset) {
			return fmt.Errorf("unsupported zfs get command")
		}
	case "receive":
		if err := validateZFSReceiveStep(step, policy); err != nil {
			return err
		}
	case "destroy":
		if !receiverStepHasNoRedirects(step) || !zfsDestroyAllowed(step.Args, policy) {
			return fmt.Errorf("unsupported zfs destroy command")
		}
	default:
		return fmt.Errorf("unsupported zfs subcommand %q", step.Args[0])
	}
	return nil
}

func zfsGetAllowed(args []string, target string) bool {
	allowed := [][]string{
		{"get", "-H", "name", target},
		{"get", "-H", "receive_resume_token", target},
		{"get", "-Hpd", "1", "-t", "snapshot", "guid,creation", target},
		{"get", "-Hpd", "1", "type,guid,creation", target},
		{"get", "-H", "-p", "used", target},
	}
	for _, pattern := range allowed {
		if stringSlicesEqual(args, pattern) {
			return true
		}
	}
	return false
}

func validateZpoolFeatureCheck(steps []receiverCommandStep, policy receiverCommandPolicy) error {
	if len(steps) != 2 {
		return fmt.Errorf("not a zpool feature check")
	}
	first := steps[0]
	if first.Name != "zpool" || !receiverStepHasNoStdoutRedirects(first) {
		return fmt.Errorf("unsupported zpool command")
	}
	if !zpoolFeatureGetAllowed(first, policy) {
		return fmt.Errorf("unsupported zpool get command")
	}
	second := steps[1]
	if second.Name != "grep" || !second.StdoutNull || !second.StderrToStdout {
		return fmt.Errorf("unsupported grep command")
	}
	if len(second.Args) != 1 || second.Args[0] != "(active|enabled)" && second.Args[0] != `\(active\|enabled\)` {
		return fmt.Errorf("unsupported grep pattern")
	}
	return nil
}

func zpoolFeatureGetAllowed(step receiverCommandStep, policy receiverCommandPolicy) bool {
	return step.Name == "zpool" &&
		receiverStepHasNoStdoutRedirects(step) &&
		stringSlicesEqual(step.Args, []string{"get", "-o", "value", "-H", "feature@extensible_dataset", targetPool(policy.TargetDataset)})
}

func validateReceivePipeline(steps []receiverCommandStep, policy receiverCommandPolicy) error {
	if len(steps) < 1 || len(steps) > 3 {
		return fmt.Errorf("unsupported receive pipeline length")
	}
	last := steps[len(steps)-1]
	if last.Name != "zfs" || len(last.Args) == 0 || last.Args[0] != "receive" {
		return fmt.Errorf("receive pipeline must end with zfs receive")
	}
	if len(steps) == 1 {
		return validateZFSReceiveStep(last, policy)
	}
	previous := steps[:len(steps)-1]
	if len(previous) == 1 {
		if previous[0].Name == "mbuffer" {
			if err := validateMbufferStep(previous[0]); err != nil {
				return err
			}
			if policy.Compression != "none" {
				return fmt.Errorf("receive pipeline is missing decompressor")
			}
			return validateZFSReceiveStep(last, policy)
		}
		if err := validateDecompressorStep(previous[0], policy); err != nil {
			return err
		}
		return validateZFSReceiveStep(last, policy)
	}
	if previous[0].Name != "mbuffer" {
		return fmt.Errorf("receive pipeline must start with mbuffer")
	}
	if err := validateMbufferStep(previous[0]); err != nil {
		return err
	}
	if err := validateDecompressorStep(previous[1], policy); err != nil {
		return err
	}
	return validateZFSReceiveStep(last, policy)
}

func validateMbufferStep(step receiverCommandStep) error {
	if !receiverStepHasNoRedirects(step) {
		return fmt.Errorf("mbuffer redirection is not supported")
	}
	for i := 0; i < len(step.Args); i++ {
		arg := step.Args[i]
		switch arg {
		case "-q", "-Q":
			continue
		case "-s", "-m", "-P":
			i++
			if i >= len(step.Args) || !safeMbufferValue(step.Args[i]) {
				return fmt.Errorf("unsupported mbuffer value")
			}
		default:
			if strings.HasPrefix(arg, "-s") || strings.HasPrefix(arg, "-m") || strings.HasPrefix(arg, "-P") {
				if safeMbufferValue(arg[2:]) {
					continue
				}
			}
			return fmt.Errorf("unsupported mbuffer argument %q", arg)
		}
	}
	return nil
}

func safeMbufferValue(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r >= '0' && r <= '9' || r == 'k' || r == 'K' || r == 'm' || r == 'M' || r == 'g' || r == 'G' || r == '%' {
			continue
		}
		return false
	}
	return true
}

func validateDecompressorStep(step receiverCommandStep, policy receiverCommandPolicy) error {
	if policy.Compression == "none" {
		return fmt.Errorf("decompressor is not allowed when compression is none")
	}
	if !receiverStepHasNoRedirects(step) {
		return fmt.Errorf("unsupported decompressor command")
	}
	switch policy.Compression {
	case "gzip":
		if step.Name == "zcat" && len(step.Args) == 0 {
			return nil
		}
		if step.Name == "gzip" && stringSlicesEqual(step.Args, []string{"-dc"}) {
			return nil
		}
	case "pigz", "zstd", "zstdmt", "lz4":
		if step.Name == policy.Compression && stringSlicesEqual(step.Args, []string{"-dc"}) {
			return nil
		}
	case "xz":
		if step.Name == "xz" && (stringSlicesEqual(step.Args, []string{"-d"}) ||
			stringSlicesEqual(step.Args, []string{"-dc"}) ||
			stringSlicesEqual(step.Args, []string{"-d", "-c"})) {
			return nil
		}
	case "lzop":
		if step.Name == "lzop" && (stringSlicesEqual(step.Args, []string{"-dfc"}) ||
			stringSlicesEqual(step.Args, []string{"-dc"})) {
			return nil
		}
	}
	return fmt.Errorf("unsupported decompressor arguments")
}

func validateZFSReceiveStep(step receiverCommandStep, policy receiverCommandPolicy) error {
	if len(step.Args) == 3 && step.Args[0] == "receive" && step.Args[1] == "-A" {
		if step.Args[2] != policy.TargetDataset {
			return fmt.Errorf("zfs receive abort target is outside policy")
		}
		if !policy.ReceiveResumable {
			return fmt.Errorf("zfs receive abort requires resumable receive policy")
		}
		return nil
	}
	if len(step.Args) < 2 || step.Args[0] != "receive" {
		return fmt.Errorf("unsupported zfs receive command")
	}
	target := step.Args[len(step.Args)-1]
	if target != policy.TargetDataset {
		return fmt.Errorf("zfs receive target is outside policy")
	}
	seenUnmounted := false
	for _, arg := range step.Args[1 : len(step.Args)-1] {
		switch arg {
		case "-u":
			if !policy.ReceiveUnmounted {
				return fmt.Errorf("zfs receive -u is not allowed by policy")
			}
			seenUnmounted = true
		case "-s":
			if !policy.ReceiveResumable {
				return fmt.Errorf("zfs receive -s is not allowed by policy")
			}
		case "-F":
			if !policy.AllowRollback {
				return fmt.Errorf("zfs receive rollback is not allowed by policy")
			}
		default:
			return fmt.Errorf("unsupported zfs receive argument %q", arg)
		}
	}
	if policy.ReceiveUnmounted && !seenUnmounted {
		return fmt.Errorf("zfs receive must include -u")
	}
	if len(step.Args) > 1 && step.Args[1] == "-A" && step.StderrToStdout {
		return fmt.Errorf("zfs receive -A must not redirect stderr")
	}
	return nil
}

func zfsDestroyAllowed(args []string, policy receiverCommandPolicy) bool {
	if len(args) == 2 && args[0] == "destroy" {
		if syncoidSnapshotTarget(args[1], policy.TargetDataset) {
			return policy.AllowSyncSnapshotDestroy
		}
		return policy.AllowDestroy && datasetOrChild(args[1], policy.TargetDataset) && !strings.Contains(args[1], "@")
	}
	if len(args) == 3 && args[0] == "destroy" && args[1] == "-r" {
		return policy.AllowDestroy && datasetOrChild(args[2], policy.TargetDataset) && !strings.Contains(args[2], "@")
	}
	return false
}

func syncoidSnapshotTarget(value, target string) bool {
	return strings.HasPrefix(value, target+"@syncoid_") && !strings.Contains(value, ",")
}

func datasetOrChild(value, target string) bool {
	return value == target || strings.HasPrefix(value, target+"/")
}

func targetPool(dataset string) string {
	if i := strings.IndexByte(dataset, '/'); i >= 0 {
		return dataset[:i]
	}
	return dataset
}

func receiverStepHasNoRedirects(step receiverCommandStep) bool {
	return !step.StdoutNull && !step.StderrNull && !step.StderrToStdout
}

func receiverStepHasNoStdoutRedirects(step receiverCommandStep) bool {
	return !step.StdoutNull && !step.StderrToStdout
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func compressorAllowed(name string) bool {
	switch name {
	case "gzip", "pigz", "zstd", "zstdmt", "xz", "lzop", "lz4":
		return true
	default:
		return false
	}
}

func compressionDefault(compression string) string {
	if compression == "" {
		return "none"
	}
	return compression
}

func executeReceiverCommandPlan(ctx context.Context, cfg forcedCommandConfig, plan receiverCommandPlan) error {
	stdin := cfg.Stdin
	if stdin == nil {
		stdin = os.Stdin
	}
	stdout := cfg.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	stderr := cfg.Stderr
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
			return forcedCommandExitError{code: 1}
		}
		_, err = fmt.Fprintln(stdout, path)
		return err
	case receiverCommandPipeline:
		return executeReceiverPipeline(ctx, stdin, stdout, stderr, plan.pipeline)
	case receiverCommandBatch:
		for _, item := range plan.batch {
			if err := executeReceiverCommandPlan(ctx, forcedCommandConfig{
				Policy: cfg.Policy,
				Stdin:  stdin,
				Stdout: stdout,
				Stderr: stderr,
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
	for i, step := range steps {
		path, err := resolveAllowedCommand(step.Name)
		if err != nil {
			return err
		}
		cmd := exec.CommandContext(ctx, path, step.Args...)
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
	for _, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			return fmt.Errorf("start %s: %w", cmd.Path, err)
		}
	}
	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			return commandExitError(err)
		}
	}
	return nil
}

func commandExitError(err error) error {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return forcedCommandExitError{code: exitErr.ExitCode()}
	}
	return err
}

func resolveAllowedCommand(name string) (string, error) {
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
