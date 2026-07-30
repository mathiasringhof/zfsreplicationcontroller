package syncoidruntime

import (
	"context"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const (
	syncoidExecutable      = "syncoid"
	maxFailureMessageBytes = 4096
	fallbackExitCode       = 1
)

// Outcome is the immutable result of one Syncoid Runtime operation.
type Outcome struct {
	exitCode       int
	failureMessage string
}

// ExitCode returns the final process exit code.
func (o Outcome) ExitCode() int {
	return o.exitCode
}

// FailureMessage returns the Sender Failure Message. It is empty on success.
func (o Outcome) FailureMessage() string {
	return o.failureMessage
}

// Run executes the fixed Syncoid executable with the supplied ready argument
// vector and forwards its output streams unchanged.
func Run(
	ctx context.Context,
	arguments []string,
	stdout io.Writer,
	stderr io.Writer,
) Outcome {
	return run(ctx, arguments, stdout, stderr, execProcess{})
}

func run(
	ctx context.Context,
	arguments []string,
	stdout io.Writer,
	stderr io.Writer,
	process processAdapter,
) Outcome {
	result := process.run(ctx, syncoidExecutable, arguments, stdout, stderr)
	if result.started && result.exitCode == 0 && result.err == nil {
		return Outcome{}
	}
	if result.started && result.exitCode > 0 {
		return failedOutcome(result.exitCode, fmt.Sprintf("%s exited with status %d", syncoidExecutable, result.exitCode))
	}

	var message string
	if !result.started {
		message = "start syncoid: process did not start"
		if result.err != nil {
			message = "start syncoid: " + result.err.Error()
		}
	} else {
		message = "syncoid failed without a usable exit status"
		if result.err != nil {
			message = "syncoid failed: " + result.err.Error()
		}
	}
	outcome := failedOutcome(fallbackExitCode, message)
	if stderr != nil {
		if _, err := fmt.Fprintln(stderr, outcome.FailureMessage()); err != nil {
			return outcome
		}
	}
	return outcome
}

func failedOutcome(exitCode int, message string) Outcome {
	if exitCode <= 0 {
		exitCode = fallbackExitCode
	}
	message = normalizeFailureMessage(message)
	if message == "" {
		message = "syncoid failed"
	}
	return Outcome{exitCode: exitCode, failureMessage: message}
}

func normalizeFailureMessage(message string) string {
	message = strings.ToValidUTF8(message, "\uFFFD")
	message = strings.Join(strings.Fields(message), " ")
	if len(message) <= maxFailureMessageBytes {
		return message
	}
	message = message[:maxFailureMessageBytes]
	for !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	return message
}
