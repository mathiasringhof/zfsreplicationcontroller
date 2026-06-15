package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mathias/zfsreplicationcontroller/internal/datamover"
)

func main() {
	guid, err := datamover.RunSender(context.Background(), datamover.SenderConfigFromEnv(), datamover.ExecRunner{}, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if guid != "" {
		fmt.Println("snapshot_guid=" + guid)
	}
}
