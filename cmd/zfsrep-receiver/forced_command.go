package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
)

const receiverCommandPath = "/usr/local/bin/zfsrep-receiver"

const (
	maxReceiverCommandLength       = 8192
	maxReceiverDestroyBatchCommand = 32
)

type receiverTaskAuthorization struct {
	AuthorizedKey string
	PolicyID      string
	PolicyPath    string
	Policy        receiverCommandPolicy
}

type receiverCommandPolicy struct {
	TargetDataset string `json:"targetDataset"`
	zfsv1.ReceiveTaskPolicy
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
	receiverCommandPS       receiverCommandKind = "ps"
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
	var policyID string
	fs.StringVar(&policyID, "policy-id", "", "receiver policy ID")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if policyID == "" {
		return fmt.Errorf("receiver policy ID is required")
	}
	policyPath, err := receiverPolicyPathForID(receiverPolicyDir(configFromEnv()), policyID)
	if err != nil {
		return err
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
