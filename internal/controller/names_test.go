package controller

import (
	"strings"
	"testing"
)

func TestObjectNamesForRun(t *testing.T) {
	for _, tt := range []struct {
		name     string
		runName  string
		wantSSH  string
		wantRecv string
		wantSend string
	}{
		{
			name:     "short readable name",
			runName:  "manual-1",
			wantSSH:  "zfsrep-manual-1-ssh",
			wantRecv: "zfsrep-manual-1-receiver",
			wantSend: "zfsrep-manual-1-sender",
		},
		{
			name:     "lossy dot normalization includes hash",
			runName:  "manual.run",
			wantSSH:  "zfsrep-manual-run-e3756db6c28755b8-ssh",
			wantRecv: "zfsrep-manual-run-e3756db6c28755b8-receiver",
			wantSend: "zfsrep-manual-run-e3756db6c28755b8-sender",
		},
		{
			name:     "long name uses thirty character prefix",
			runName:  strings.Repeat("a", 64),
			wantSSH:  "zfsrep-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-ffe054fe7ae0cb6d-ssh",
			wantRecv: "zfsrep-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-ffe054fe7ae0cb6d-receiver",
			wantSend: "zfsrep-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-ffe054fe7ae0cb6d-sender",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := objectNamesForRun(tt.runName)
			if got.SecretName != tt.wantSSH {
				t.Fatalf("SSH Secret name = %q, want %q", got.SecretName, tt.wantSSH)
			}
			if got.ReceiveTaskName != tt.wantRecv {
				t.Fatalf("Receive Task name = %q, want %q", got.ReceiveTaskName, tt.wantRecv)
			}
			if got.SenderName != tt.wantSend {
				t.Fatalf("sender Job name = %q, want %q", got.SenderName, tt.wantSend)
			}
			if len(got.ReceiveTaskName) > dnsLabelMaxLength {
				t.Fatalf("longest child name length = %d, want at most %d", len(got.ReceiveTaskName), dnsLabelMaxLength)
			}
		})
	}
}

func TestObjectNamesForRunDistinguishesNormalizationCollisions(t *testing.T) {
	dotted := objectNamesForRun("a.b")
	hyphenated := objectNamesForRun("a-b")

	if dotted.SecretName == hyphenated.SecretName ||
		dotted.ReceiveTaskName == hyphenated.ReceiveTaskName ||
		dotted.SenderName == hyphenated.SenderName {
		t.Fatalf("object names collide after normalization: %#v", dotted)
	}
	if dotted.SecretName != "zfsrep-a-b-2e7336dc8eba87ef-ssh" {
		t.Fatalf("dotted SSH Secret name = %q", dotted.SecretName)
	}
	if hyphenated.SecretName != "zfsrep-a-b-ssh" {
		t.Fatalf("readable hyphenated SSH Secret name = %q", hyphenated.SecretName)
	}
}
