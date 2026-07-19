package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/mathias/zfsreplicationcontroller/internal/datamover"
	"github.com/mathias/zfsreplicationcontroller/internal/replication/diagnosis"
)

const terminationMessagePath = "/dev/termination-log"

func main() {
	os.Exit(run(
		context.Background(),
		datamover.SenderConfigFromEnv(),
		os.Stderr,
		datamover.ExecRunner{},
		filePublisher{path: terminationMessagePath},
	))
}

type diagnosisPublisher interface {
	Publish(diagnosis.Diagnosis) error
}

type filePublisher struct {
	path string
}

func (p filePublisher) Publish(value diagnosis.Diagnosis) error {
	return os.WriteFile(p.path, []byte(value.String()), 0o600)
}

func run(ctx context.Context, cfg datamover.SenderConfig, stderr io.Writer, runner datamover.CommandRunner, publisher diagnosisPublisher) int {
	if err := datamover.RunSenderWithLog(ctx, cfg, runner, stderr); err != nil {
		value := diagnosisFromError(err)
		if publisher != nil {
			if publishErr := publisher.Publish(value); publishErr != nil {
				if _, logErr := fmt.Fprintf(stderr, "termination message publication failed error=%q\n", diagnosis.Sanitize(publishErr.Error()).String()); logErr != nil {
					return 1
				}
			}
		}
		return 1
	}
	return 0
}

func diagnosisFromError(err error) diagnosis.Diagnosis {
	type diagnosed interface {
		Diagnosis() diagnosis.Diagnosis
	}
	var value diagnosed
	if errors.As(err, &value) {
		return value.Diagnosis()
	}
	return diagnosis.Sanitize(err.Error())
}
