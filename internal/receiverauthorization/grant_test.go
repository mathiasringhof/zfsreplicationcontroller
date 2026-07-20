package receiverauthorization

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testAuthorizedPublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOOBMEh4NBNCYArCdegKrXOfyIVEEhfvFoOYNYjsBP41 receiver"

func TestCompileCandidatesUsesExactTaskUIDAndPreservesMountedReceiveDenial(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	candidate := Candidate{
		TaskUID:                 "11111111-2222-3333-4444-555555555555",
		AuthorizedPublicKey:     testAuthorizedPublicKey,
		ExpiresAt:               now.Add(10 * time.Minute),
		TargetDataset:           "tank/dst",
		ReceiverDatasetPrefixes: []string{"tank"},
		ReceiveUnmounted:        false,
		AllowMount:              false,
		Compression:             "none",
	}

	compiled := compileCandidates(now, []Candidate{candidate})
	outcomes := compiled.outcomes
	if len(outcomes) != 1 || outcomes[0].Rejection != "" {
		t.Fatalf("compilation outcomes = %#v, want one accepted grant", outcomes)
	}

	dir := t.TempDir()
	module := New(filepath.Join(dir, "authorized_keys"))
	keys, err := activateTestKeys(module, compiled)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("authorized keys = %v, want one", keys)
	}
	if !strings.Contains(keys[0], `expiry-time="20260720120959Z"`) {
		t.Fatalf("authorized key = %q, want conservative native expiry", keys[0])
	}
	for _, disallowed := range []string{"storage", "receive-task", "tank/dst", candidate.TaskUID} {
		if strings.Contains(keys[0], disallowed) {
			t.Fatalf("authorized key contains user-controlled authority value %q: %q", disallowed, keys[0])
		}
	}
	snapshotID := activeSnapshotID(t, module.manifestPath)
	reference, err := ReferenceFromArgs([]string{"--snapshot-id", snapshotID, "--grant-id", grantID(candidate.TaskUID)})
	if err != nil {
		t.Fatal(err)
	}
	module.now = func() time.Time { return now }
	if _, err := module.Admit(reference, "zfs receive tank/dst"); err == nil {
		t.Fatal("Admit() error = nil, want mounted receive denial after production serialization round trip")
	}

	replacement := candidate
	replacement.TaskUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	replacement.TargetDataset = "tank/replacement"
	replacement.ReceiveUnmounted = true
	if grantID(replacement.TaskUID) == grantID(candidate.TaskUID) {
		t.Fatalf("replacement grant ID = %q, want identity distinct from old task incarnation", grantID(replacement.TaskUID))
	}
	replacementCompilation := compileCandidates(now, []Candidate{replacement})
	if _, err := activateTestKeys(module, replacementCompilation); err != nil {
		t.Fatal(err)
	}
	if _, err := module.Admit(reference, "zfs receive tank/dst"); err == nil {
		t.Fatal("old task reference admitted replacement authority")
	}
	replacementSnapshotID := activeSnapshotID(t, module.manifestPath)
	replacementReference, err := ReferenceFromArgs([]string{"--snapshot-id", replacementSnapshotID, "--grant-id", grantID(replacement.TaskUID)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := module.Admit(replacementReference, "zfs receive -u tank/replacement"); err != nil {
		t.Fatalf("replacement task Admit() error = %v, want nil", err)
	}
}

func TestReferenceFromArgsRejectsInvalidGrantReference(t *testing.T) {
	t.Parallel()
	validSnapshot := "snapshot-0000000000000000000000000000000000000000000000000000000000000000"
	for _, args := range [][]string{
		nil,
		{"--policy-id", "storage/receive-task"},
		{"--snapshot-id", "snapshot-../../replacement", "--grant-id", grantID("11111111-2222-3333-4444-555555555555")},
		{"--snapshot-id", validSnapshot, "--grant-id", "storage/receive-task"},
		{"--snapshot-id", validSnapshot, "--grant-id", "grant-../../replacement"},
		{"--snapshot-id", validSnapshot, "--grant-id", grantID("11111111-2222-3333-4444-555555555555"), "extra"},
	} {
		if _, err := ReferenceFromArgs(args); err == nil {
			t.Fatalf("ReferenceFromArgs(%q) error = nil, want rejection", args)
		}
	}
}

func TestCompileCandidatesRejectsInvalidGrantIndependently(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	valid := validCandidate(now, "11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey)
	tests := []struct {
		name   string
		mutate func(*Candidate)
	}{
		{name: "reference", mutate: func(candidate *Candidate) { candidate.TaskUID = "" }},
		{name: "key", mutate: func(candidate *Candidate) { candidate.AuthorizedPublicKey = "not a key" }},
		{name: "dataset", mutate: func(candidate *Candidate) { candidate.TargetDataset = "tank/dst;id" }},
		{name: "receiver prefix", mutate: func(candidate *Candidate) { candidate.ReceiverDatasetPrefixes = []string{"tank;id"} }},
		{name: "dataset outside receiver prefix", mutate: func(candidate *Candidate) { candidate.ReceiverDatasetPrefixes = []string{"backup"} }},
		{name: "compression", mutate: func(candidate *Candidate) { candidate.Compression = "shell" }},
		{name: "sync identifier", mutate: func(candidate *Candidate) { candidate.SyncSnapshotIdentifier = "bad;id" }},
		{name: "capability combination", mutate: func(candidate *Candidate) {
			candidate.AllowSyncSnapshotDestroy = true
			candidate.SyncSnapshotIdentifier = ""
		}},
		{name: "expired lease", mutate: func(candidate *Candidate) { candidate.ExpiresAt = now }},
		{name: "lease beyond horizon", mutate: func(candidate *Candidate) { candidate.ExpiresAt = now.Add(35*time.Minute + time.Nanosecond) }},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			invalid := valid
			invalid.TaskUID = "invalid-" + string(rune('a'+i))
			invalid.AuthorizedPublicKey = otherTestAuthorizedPublicKey
			invalid.ReceiverDatasetPrefixes = append([]string(nil), valid.ReceiverDatasetPrefixes...)
			tt.mutate(&invalid)
			outcomes := compileCandidates(now, []Candidate{invalid, valid}).outcomes
			if outcomes[0].Rejection == "" {
				t.Fatalf("invalid candidate outcome = %#v, want rejection", outcomes[0])
			}
			if outcomes[1].Rejection != "" {
				t.Fatalf("unrelated valid candidate outcome = %#v, want acceptance", outcomes[1])
			}
		})
	}
}

func TestCompileCandidatesStoresExplicitDefaultCompression(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	candidate := validCandidate(now, "11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey)
	candidate.Compression = ""
	compiled := compileCandidates(now, []Candidate{candidate})
	if outcome := compiled.outcomes[0]; outcome.Rejection != "" {
		t.Fatalf("outcome = %#v, want API default accepted", outcome)
	}
	dir := t.TempDir()
	module := New(filepath.Join(dir, "authorized_keys"))
	if _, err := activateTestKeys(module, compiled); err != nil {
		t.Fatal(err)
	}
	snapshotID := activeSnapshotID(t, module.manifestPath)
	data, err := os.ReadFile(filepath.Join(module.generationPath(snapshotID), "grants", grantID(candidate.TaskUID)+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if got := string(fields["compression"]); got != `"none"` {
		t.Fatalf("persisted compression = %s, want explicit API default %q", got, "none")
	}
}

func TestCompileCandidatesRejectsEveryCanonicalDuplicateKey(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	first := validCandidate(now, "11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey)
	duplicate := validCandidate(now, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", strings.TrimSuffix(testAuthorizedPublicKey, " receiver")+" another-comment")
	unrelated := validCandidate(now, "99999999-8888-7777-6666-555555555555", "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPFmCq6yib3eYpmpYpK91ZyY8LfFdU2GWDhP9f7k7j8H unrelated")

	outcomes := compileCandidates(now, []Candidate{first, duplicate, unrelated}).outcomes
	if outcomes[0].Rejection == "" || outcomes[1].Rejection == "" {
		t.Fatalf("duplicate outcomes = %#v, want both rejected", outcomes[:2])
	}
	if outcomes[2].Rejection != "" {
		t.Fatalf("unrelated outcome = %#v, want accepted", outcomes[2])
	}
}

func TestCompileCandidatesDuplicateKeyConflictOverridesAnotherCandidateError(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	invalid := validCandidate(now, "11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey)
	invalid.TargetDataset = "tank/dst;invalid"
	duplicate := validCandidate(now, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", strings.TrimSuffix(testAuthorizedPublicKey, " receiver")+" another-comment")

	outcomes := compileCandidates(now, []Candidate{invalid, duplicate}).outcomes
	for i, outcome := range outcomes {
		if !strings.Contains(outcome.Rejection, "ambiguous") {
			t.Fatalf("outcome %d = %#v, want duplicate-key ambiguity", i, outcome)
		}
	}
}

func TestCompileCandidatesRejectsEveryDuplicateTaskUID(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	first := validCandidate(now, "11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey)
	second := validCandidate(now, first.TaskUID, otherTestAuthorizedPublicKey)
	second.TargetDataset = "tank/replacement"

	compiled := compileCandidates(now, []Candidate{first, second})
	for i, outcome := range compiled.outcomes {
		if !strings.Contains(outcome.Rejection, "task UID") {
			t.Fatalf("outcome %d = %#v, want duplicate task UID rejection", i, outcome)
		}
	}
	dir := t.TempDir()
	keys, err := activateTestKeys(New(filepath.Join(dir, "authorized_keys")), compiled)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("authorized keys = %v, want no widened authority", keys)
	}
}

func TestAdmitRejectsMalformedOrUnknownPersistedGrantFields(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name   string
		mutate func(map[string]json.RawMessage)
	}{
		{name: "missing explicit capability", mutate: func(fields map[string]json.RawMessage) { delete(fields, "allowMount") }},
		{name: "unknown field", mutate: func(fields map[string]json.RawMessage) { fields["futureAuthority"] = json.RawMessage("true") }},
		{name: "malformed field", mutate: func(fields map[string]json.RawMessage) { fields["allowMount"] = json.RawMessage(`"false"`) }},
		{name: "null capability", mutate: func(fields map[string]json.RawMessage) { fields["allowMount"] = json.RawMessage("null") }},
		{name: "noncanonical compression", mutate: func(fields map[string]json.RawMessage) { fields["compression"] = json.RawMessage(`""`) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			compiled := compileCandidates(now, []Candidate{validCandidate(now, "11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey)})
			dir := t.TempDir()
			module := New(filepath.Join(dir, "authorized_keys"))
			if _, err := activateTestKeys(module, compiled); err != nil {
				t.Fatal(err)
			}
			snapshotID := activeSnapshotID(t, module.manifestPath)
			path := filepath.Join(module.generationPath(snapshotID), "grants", grantID("11111111-2222-3333-4444-555555555555")+".json")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			tt.mutate(fields)
			data, err = json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, data, 0o400); err != nil {
				t.Fatal(err)
			}
			reference, err := ReferenceFromArgs([]string{"--snapshot-id", snapshotID, "--grant-id", grantID("11111111-2222-3333-4444-555555555555")})
			if err != nil {
				t.Fatal(err)
			}
			module.now = func() time.Time { return now }
			if _, err := module.Admit(reference, "zfs receive -u tank/dst"); err == nil {
				t.Fatal("Admit() error = nil, want persisted grant rejection")
			}
		})
	}
}

func TestAdmitRejectsDuplicatePersistedGrantFields(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.July, 20, 12, 0, 0, 0, time.UTC)
	uid := "11111111-2222-3333-4444-555555555555"
	compiled := compileCandidates(now, []Candidate{validCandidate(now, uid, testAuthorizedPublicKey)})
	dir := t.TempDir()
	module := New(filepath.Join(dir, "authorized_keys"))
	if _, err := activateTestKeys(module, compiled); err != nil {
		t.Fatal(err)
	}
	snapshotID := activeSnapshotID(t, module.manifestPath)
	path := filepath.Join(module.generationPath(snapshotID), "grants", grantID(uid)+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data = []byte(strings.Replace(string(data), `"allowMount":false`, `"allowMount":false,"allowMount":true`, 1))
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	module.now = func() time.Time { return now }
	reference, err := ReferenceFromArgs([]string{"--snapshot-id", snapshotID, "--grant-id", grantID(uid)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := module.Admit(reference, "zfs receive tank/dst"); err == nil {
		t.Fatal("Admit() error = nil, want duplicate persisted field rejection")
	}
}

func validCandidate(now time.Time, uid, publicKey string) Candidate {
	return Candidate{
		TaskUID:                 uid,
		AuthorizedPublicKey:     publicKey,
		ExpiresAt:               now.Add(10 * time.Minute),
		TargetDataset:           "tank/dst",
		ReceiverDatasetPrefixes: []string{"tank"},
		ReceiveUnmounted:        true,
		Compression:             "none",
	}
}

func activateTestKeys(module Module, compilation compilation) ([]string, error) {
	if _, err := module.activate(compilation); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(module.manifestPath)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) == 1 {
		return nil, nil
	}
	return lines[1:], nil
}

const otherTestAuthorizedPublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPFmCq6yib3eYpmpYpK91ZyY8LfFdU2GWDhP9f7k7j8H unrelated"
