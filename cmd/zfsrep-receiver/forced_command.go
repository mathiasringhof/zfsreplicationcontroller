package main

import (
	"context"
	"io"
	"os"

	"github.com/mathias/zfsreplicationcontroller/internal/receiverauthorization"
)

const receiverCommandPath = "/usr/local/bin/zfsrep-receiver"

type forcedCommandConfig struct {
	Authorization   receiverauthorization.Module
	Reference       receiverauthorization.Reference
	OriginalCommand string
	Stdin           io.Reader
	Stdout          io.Writer
	Stderr          io.Writer
}

func runForcedCommandFromArgs(ctx context.Context, args []string) error {
	reference, err := receiverauthorization.ReferenceFromArgs(args)
	if err != nil {
		return err
	}
	policyDir := receiverPolicyDir(configFromEnv())
	return runForcedCommand(ctx, forcedCommandConfig{
		Authorization:   receiverauthorization.New(policyDir),
		Reference:       reference,
		OriginalCommand: os.Getenv("SSH_ORIGINAL_COMMAND"),
		Stdin:           os.Stdin,
		Stdout:          os.Stdout,
		Stderr:          os.Stderr,
	})
}

func runForcedCommand(ctx context.Context, cfg forcedCommandConfig) error {
	plan, err := cfg.Authorization.Admit(cfg.Reference, cfg.OriginalCommand)
	if err != nil {
		return err
	}
	return plan.Execute(ctx, cfg.Stdin, cfg.Stdout, cfg.Stderr)
}
