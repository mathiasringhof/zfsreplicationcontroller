package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/receiverauthorization"
)

const receiverCommandPath = "/usr/local/bin/zfsrep-receiver"

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
	Authorization   receiverauthorization.Module
	PolicyID        string
	OriginalCommand string
	Stdin           io.Reader
	Stdout          io.Writer
	Stderr          io.Writer
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
	policyDir := receiverPolicyDir(configFromEnv())
	return runForcedCommand(ctx, forcedCommandConfig{
		Authorization:   receiverauthorization.New(policyDir),
		PolicyID:        policyID,
		OriginalCommand: os.Getenv("SSH_ORIGINAL_COMMAND"),
		Stdin:           os.Stdin,
		Stdout:          os.Stdout,
		Stderr:          os.Stderr,
	})
}

func runForcedCommand(ctx context.Context, cfg forcedCommandConfig) error {
	plan, err := cfg.Authorization.Admit(cfg.PolicyID, cfg.OriginalCommand)
	if err != nil {
		return err
	}
	return plan.Execute(ctx, cfg.Stdin, cfg.Stdout, cfg.Stderr)
}
