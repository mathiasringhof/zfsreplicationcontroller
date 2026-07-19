package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mathias/zfsreplicationcontroller/internal/datamover"
	"github.com/mathias/zfsreplicationcontroller/internal/replication/diagnosis"
)

type failingRunner struct{}

func (failingRunner) Run(_ context.Context, _ string, _ ...string) (string, string, error) {
	return "cannot open 'missingpool/dst': dataset does not exist\n",
		"CRITICAL ERROR: zfs send -w tank/src@syncoid_new_2026 | ssh -i /var/run/zfsrep/ssh/id_rsa zfs-recv@10.42.2.11 zfs receive -F missingpool/dst failed: 256\n",
		mainExitError{code: 2, msg: "exit status 2"}
}

type mainExitError struct {
	code int
	msg  string
}

func (e mainExitError) Error() string {
	return e.msg
}

func (e mainExitError) ExitCode() int {
	return e.code
}

func TestRunDoesNotPrintReturnedSyncoidFailure(t *testing.T) {
	var stderr bytes.Buffer
	publisher := &recordingPublisher{}

	code := run(context.Background(), datamover.SenderConfig{
		SrcDataset:       "tank/src",
		DstHost:          "root@10.0.0.42",
		DstDataset:       "tank/dst",
		SSHKeyFile:       "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile:   "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:          "2222",
		NoRollback:       true,
		Compress:         "none",
		ReceiveUnmounted: true,
		ReceiveResumable: true,
	}, &stderr, failingRunner{}, publisher)
	if code != 1 {
		t.Fatalf("run() code = %d, want 1", code)
	}

	out := stderr.String()
	if strings.Contains(out, "syncoid failed:") {
		t.Fatalf("stderr contains duplicated returned error summary:\n%s", out)
	}
	if count := strings.Count(out, "sender completed result=failure"); count != 1 {
		t.Fatalf("stderr contains %d sender failure summaries, want 1:\n%s", count, out)
	}
	if strings.Contains(out, "/var/run/zfsrep/ssh/id_rsa") {
		t.Fatalf("stderr contains unredacted key path:\n%s", out)
	}
	if publisher.diagnosis.String() == "" {
		t.Fatal("termination message was not published")
	}
}

func TestRunPreservesFailureWhenTerminationPublicationFails(t *testing.T) {
	var stderr bytes.Buffer
	publisher := &recordingPublisher{err: errors.New("write failed --sshkey=unsafe-secret")}

	code := run(context.Background(), datamover.SenderConfig{
		SrcDataset:       "tank/src",
		DstDataset:       "tank/dst",
		Compress:         "none",
		ReceiveResumable: true,
	}, &stderr, failingRunner{}, publisher)

	if code != 1 {
		t.Fatalf("run() code = %d, want original failure code 1", code)
	}
	if got := publisher.diagnosis.String(); !strings.Contains(got, "CRITICAL ERROR") || !strings.Contains(got, "missingpool/dst") {
		t.Fatalf("published diagnosis = %q", got)
	}
	if got := stderr.String(); !strings.Contains(got, "termination message publication failed") || strings.Contains(got, "unsafe-secret") {
		t.Fatalf("stderr does not contain a safe publication failure:\n%s", got)
	}
}

type recordingPublisher struct {
	diagnosis diagnosis.Diagnosis
	err       error
}

func (p *recordingPublisher) Publish(value diagnosis.Diagnosis) error {
	p.diagnosis = value
	return p.err
}
