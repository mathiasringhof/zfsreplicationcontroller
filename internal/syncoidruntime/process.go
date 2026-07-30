package syncoidruntime

import (
	"context"
	"io"
	"os/exec"
)

type processAdapter interface {
	run(context.Context, string, []string, io.Writer, io.Writer) processResult
}

type processResult struct {
	started  bool
	exitCode int
	err      error
}

type execProcess struct{}

func (execProcess) run(
	ctx context.Context,
	name string,
	arguments []string,
	stdout io.Writer,
	stderr io.Writer,
) processResult {
	command := exec.CommandContext(ctx, name, arguments...)
	command.Stdout = stdout
	command.Stderr = stderr
	err := command.Run()
	if command.ProcessState == nil {
		return processResult{exitCode: -1, err: err}
	}
	return processResult{
		started:  true,
		exitCode: command.ProcessState.ExitCode(),
		err:      err,
	}
}
