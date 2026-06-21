package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mathias/zfsreplicationcontroller/internal/datamover"
)

func main() {
	if err := datamover.RunSender(context.Background(), datamover.SenderConfigFromEnv(), datamover.ExecRunner{}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
