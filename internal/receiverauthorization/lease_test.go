package receiverauthorization

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReplaceReportsExactNextDeadlineAndRendersConservativeUTCExpiry(t *testing.T) {
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	expiresAt := time.Date(2026, time.July, 20, 14, 10, 0, 500_000_000, time.FixedZone("CEST", 2*60*60))
	module := New(filepath.Join(t.TempDir(), "authorized_keys"))
	module.now = func() time.Time { return now }

	activation, err := module.Replace([]Candidate{
		validSnapshotCandidate("11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey, "tank/first", expiresAt),
		validSnapshotCandidate("99999999-8888-7777-6666-555555555555", otherAuthorizedPublicKey, "tank/second", now.Add(20*time.Minute)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !activation.NextDeadline.Equal(expiresAt) {
		t.Fatalf("next deadline = %v, want exact lease deadline %v", activation.NextDeadline, expiresAt)
	}
	manifest, err := os.ReadFile(module.manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(manifest, []byte(`expiry-time="20260720120959Z"`)) {
		t.Fatalf("authorized_keys = %q, want conservative UTC expiry", manifest)
	}
}

func TestLeaseLapseIsProcessLocalTerminalWithoutCancellingAdmittedPlan(t *testing.T) {
	startedAt := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	now := startedAt
	manifestPath := filepath.Join(t.TempDir(), "authorized_keys")
	module := New(manifestPath)
	module.now = func() time.Time { return now }
	candidate := validSnapshotCandidate("11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey, "tank/dst", startedAt.Add(10*time.Minute))

	if _, err := module.Replace([]Candidate{candidate}); err != nil {
		t.Fatal(err)
	}
	reference := snapshotReference(t, activeSnapshotID(t, manifestPath), grantID(candidate.TaskUID))
	plan, err := module.Admit(reference, "echo -n admitted")
	if err != nil {
		t.Fatal(err)
	}

	now = candidate.ExpiresAt
	if _, err := module.Admit(reference, "echo -n denied"); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("exact-boundary Admit() error = %v, want independent expiry rejection", err)
	}
	now = candidate.ExpiresAt.Add(time.Nanosecond)
	if _, err := module.Admit(reference, "echo -n denied"); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("post-boundary Admit() error = %v, want independent expiry rejection", err)
	}
	module.hooks.removeGeneration = func(string) error { return errors.New("cleanup delayed") }
	activation, err := module.Replace([]Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	if !activation.Changed || activation.Warning == nil {
		t.Fatalf("lapse activation = %#v, want authority committed with cleanup warning", activation)
	}
	if outcome := activation.Outcomes()[0]; !strings.Contains(outcome.Rejection, "expired") {
		t.Fatalf("lapse outcome = %#v, want expiry rejection", outcome)
	}
	if _, err := module.Admit(reference, "echo -n denied"); err == nil || !strings.Contains(err.Error(), "snapshot is not active") {
		t.Fatalf("post-lapse Admit() error = %v, want inactive snapshot rejection", err)
	}

	renewed := candidate
	renewed.ExpiresAt = now.Add(10 * time.Minute)
	module.hooks.removeGeneration = nil
	activation, err = module.Replace([]Candidate{renewed})
	if err != nil {
		t.Fatal(err)
	}
	if outcome := activation.Outcomes()[0]; !strings.Contains(outcome.Rejection, "lapsed") {
		t.Fatalf("same-process renewed outcome = %#v, want terminal lapse rejection", outcome)
	}
	var stdout bytes.Buffer
	if err := plan.Execute(context.Background(), nil, &stdout, nil); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "admitted" {
		t.Fatalf("admitted plan output = %q, want completion unchanged", stdout.String())
	}

	restarted := New(manifestPath)
	restarted.now = func() time.Time { return now }
	if err := restarted.Reset(); err != nil {
		t.Fatal(err)
	}
	activation, err = restarted.Replace([]Candidate{renewed})
	if err != nil {
		t.Fatal(err)
	}
	if outcome := activation.Outcomes()[0]; outcome.Rejection != "" {
		t.Fatalf("post-restart renewed outcome = %#v, want fresh evaluation", outcome)
	}
	if activation.NextDeadline.IsZero() {
		t.Fatal("post-restart activation did not report renewed deadline")
	}
}

func TestReplaceEnforcesMaximumLeaseHorizon(t *testing.T) {
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	module := New(filepath.Join(t.TempDir(), "authorized_keys"))
	module.now = func() time.Time { return now }
	atLimit := validSnapshotCandidate("11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey, "tank/accepted", now.Add(35*time.Minute))
	beyondLimit := validSnapshotCandidate("99999999-8888-7777-6666-555555555555", otherAuthorizedPublicKey, "tank/rejected", now.Add(35*time.Minute+time.Nanosecond))

	activation, err := module.Replace([]Candidate{beyondLimit, atLimit})
	if err != nil {
		t.Fatal(err)
	}
	outcomes := activation.Outcomes()
	if !strings.Contains(outcomes[0].Rejection, "maximum horizon") {
		t.Fatalf("beyond-limit outcome = %#v, want horizon rejection", outcomes[0])
	}
	if outcomes[1].Rejection != "" {
		t.Fatalf("at-limit outcome = %#v, want acceptance", outcomes[1])
	}
	if !activation.NextDeadline.Equal(atLimit.ExpiresAt) {
		t.Fatalf("next deadline = %v, want accepted limit %v", activation.NextDeadline, atLimit.ExpiresAt)
	}
}
