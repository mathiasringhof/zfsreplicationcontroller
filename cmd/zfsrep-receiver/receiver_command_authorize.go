package main

import (
	"fmt"
	"slices"
	"strings"

	"github.com/mathias/zfsreplicationcontroller/internal/replication"
)

func authorizeReceiverCommand(raw string, policy receiverCommandPolicy) (receiverCommandPlan, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) > maxReceiverCommandLength {
		return receiverCommandPlan{}, fmt.Errorf("command is too long")
	}
	if strings.Contains(raw, ";") {
		return authorizeReceiverCommandBatch(raw, policy)
	}
	policy.Compression = replication.CompressionDefault(policy.Compression)
	if policy.TargetDataset == "" {
		return receiverCommandPlan{}, fmt.Errorf("target dataset is empty")
	}
	if !replication.ValidDatasetName(policy.TargetDataset) {
		return receiverCommandPlan{}, fmt.Errorf("target dataset is invalid")
	}
	if policy.SyncSnapshotIdentifier != "" && !replication.ValidSyncoidIdentifier(policy.SyncSnapshotIdentifier) {
		return receiverCommandPlan{}, fmt.Errorf("sync snapshot identifier is invalid")
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
		case "ps":
			if receiverStepHasNoRedirects(step) && slices.Equal(step.Args, []string{"-Ao", "args="}) {
				return receiverCommandPlan{kind: receiverCommandPS}, nil
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
		if len(batch) > maxReceiverDestroyBatchCommand {
			return receiverCommandPlan{}, fmt.Errorf("too many commands in batch")
		}
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

func commandLookupAllowed(name string, policy receiverCommandPolicy) bool {
	if name == "mbuffer" {
		return true
	}
	return policy.Compression != replication.CompressionNone && name == policy.Compression && replication.CompressorAllowed(name)
}

func validateSingleReceiverStep(step receiverCommandStep, policy receiverCommandPolicy) error {
	switch step.Name {
	case "ps":
		if !receiverStepHasNoRedirects(step) || !slices.Equal(step.Args, []string{"-Ao", "args="}) {
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
		if slices.Equal(args, pattern) {
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
		slices.Equal(step.Args, []string{"get", "-o", "value", "-H", "feature@extensible_dataset", replication.TargetPool(policy.TargetDataset)})
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
			if policy.Compression != replication.CompressionNone {
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
	if policy.Compression == replication.CompressionNone {
		return fmt.Errorf("decompressor is not allowed when compression is none")
	}
	if !receiverStepHasNoRedirects(step) {
		return fmt.Errorf("unsupported decompressor command")
	}
	if replication.DecompressorAllowed(step.Name, step.Args, policy.Compression) {
		return nil
	}
	return fmt.Errorf("unsupported decompressor arguments")
}

func validateZFSReceiveStep(step receiverCommandStep, policy receiverCommandPolicy) error {
	if len(step.Args) == 3 && step.Args[0] == "receive" && step.Args[1] == "-A" {
		if !replication.ValidDatasetName(step.Args[2]) || step.Args[2] != policy.TargetDataset {
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
	if !replication.ValidDatasetName(target) || target != policy.TargetDataset {
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
	if !seenUnmounted && !policy.AllowMount {
		return fmt.Errorf("zfs receive mounted receive is not allowed by policy")
	}
	if len(step.Args) > 1 && step.Args[1] == "-A" && step.StderrToStdout {
		return fmt.Errorf("zfs receive -A must not redirect stderr")
	}
	return nil
}

func zfsDestroyAllowed(args []string, policy receiverCommandPolicy) bool {
	if len(args) == 2 && args[0] == "destroy" {
		if dataset, snapshot, ok := replication.SplitSnapshotTarget(args[1]); ok {
			if dataset != policy.TargetDataset {
				return false
			}
			return policy.AllowTargetSnapshotDestroy ||
				policy.AllowSyncSnapshotDestroy &&
					replication.SyncoidSnapshotTarget(snapshot, policy.SyncSnapshotIdentifier)
		}
		return policy.AllowDestroy && replication.DatasetOrChild(args[1], policy.TargetDataset) && !strings.Contains(args[1], "@")
	}
	if len(args) == 3 && args[0] == "destroy" && args[1] == "-r" {
		return policy.AllowDestroy && replication.DatasetOrChild(args[2], policy.TargetDataset) && !strings.Contains(args[2], "@")
	}
	return false
}

func receiverStepHasNoRedirects(step receiverCommandStep) bool {
	return !step.StdoutNull && !step.StderrNull && !step.StderrToStdout
}

func receiverStepHasNoStdoutRedirects(step receiverCommandStep) bool {
	return !step.StdoutNull && !step.StderrToStdout
}
