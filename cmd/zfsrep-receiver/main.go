package main

import (
	"context"
	"fmt"
	"os"

	"github.com/mathias/zfsreplicationcontroller/internal/datamover"
)

func main() {
	receiver, err := datamover.NewReceiver(datamover.ReceiverConfigFromEnv(), datamover.ExecRunner{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := receiver.Serve(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
