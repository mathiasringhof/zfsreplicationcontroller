package main

import (
	"context"
	"io"
	"os"

	"github.com/mathias/zfsreplicationcontroller/internal/datamover"
)

func main() {
	os.Exit(run(context.Background(), datamover.SenderConfigFromEnv(), os.Stderr, datamover.ExecRunner{}))
}

func run(ctx context.Context, cfg datamover.SenderConfig, stderr io.Writer, runner datamover.CommandRunner) int {
	if err := datamover.RunSenderWithLog(ctx, cfg, runner, stderr); err != nil {
		return 1
	}
	return 0
}
