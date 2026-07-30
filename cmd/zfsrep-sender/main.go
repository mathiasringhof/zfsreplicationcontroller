package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/mathias/zfsreplicationcontroller/internal/syncoidruntime"
)

const terminationMessagePath = "/dev/termination-log"

func main() {
	os.Exit(run(
		context.Background(),
		os.Args[1:],
		os.Stdout,
		os.Stderr,
		func(ctx context.Context, arguments []string, stdout, stderr io.Writer) runtimeOutcome {
			return syncoidruntime.Run(ctx, arguments, stdout, stderr)
		},
		filePublisher{path: terminationMessagePath},
	))
}

type runtimeOutcome interface {
	ExitCode() int
	FailureMessage() string
}

type runtimeOperation func(context.Context, []string, io.Writer, io.Writer) runtimeOutcome

type failureMessagePublisher interface {
	Publish(string) error
}

type filePublisher struct {
	path string
}

func (p filePublisher) Publish(message string) error {
	return os.WriteFile(p.path, []byte(message), 0o600)
}

func run(
	ctx context.Context,
	arguments []string,
	stdout io.Writer,
	stderr io.Writer,
	operation runtimeOperation,
	publisher failureMessagePublisher,
) int {
	outcome := operation(ctx, arguments, stdout, stderr)
	if outcome.ExitCode() == 0 {
		return 0
	}
	if publisher != nil {
		if err := publisher.Publish(outcome.FailureMessage()); err != nil && stderr != nil {
			if _, writeErr := fmt.Fprintf(stderr, "publish Sender Failure Message: %q\n", err); writeErr != nil {
				return outcome.ExitCode()
			}
		}
	}
	return outcome.ExitCode()
}
