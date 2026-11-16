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

	"golang.org/x/crypto/ssh"
)

func TestAuthenticatedGrantAcrossSnapshotUpdates(t *testing.T) {
	for _, name := range []string{
		"unrelated grant added", "unrelated grant removed", "unrelated grant renewed",
		"own lease renewed", "own grant revoked", "own grant expired", "receive task replaced",
		"same task different key",
	} {
		t.Run(name, func(t *testing.T) {
			startedAt := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
			now := startedAt
			module := New(filepath.Join(t.TempDir(), "authorized_keys"))
			module.now = func() time.Time { return now }
			own := validSnapshotCandidate(testTaskUID, testAuthorizedPublicKey, "tank/first", now.Add(10*time.Minute))
			other := validSnapshotCandidate("other-task", otherAuthorizedPublicKey, "tank/second", now.Add(20*time.Minute))
			initial := []Candidate{own, other}
			if name == "unrelated grant added" {
				initial = []Candidate{own}
			}
			if _, err := module.Replace(initial); err != nil {
				t.Fatal(err)
			}
			reference := authenticatedReference(t, module, own.AuthorizedPublicKey)
			if _, err := module.Admit(reference, "zfs get -H name tank/first"); err != nil {
				t.Fatal(err)
			}
			plan, err := module.Admit(reference, "echo -n admitted")
			if err != nil {
				t.Fatal(err)
			}

			now = startedAt.Add(5 * time.Minute)
			updated := []Candidate{own, other}
			wantDenied := false
			switch name {
			case "unrelated grant removed":
				updated = []Candidate{own}
			case "unrelated grant renewed":
				updated[1].ExpiresAt = startedAt.Add(30 * time.Minute)
			case "own lease renewed":
				updated[0].ExpiresAt = startedAt.Add(30 * time.Minute)
			case "own grant revoked":
				updated = []Candidate{other}
				wantDenied = true
			case "own grant expired":
				wantDenied = true
			case "receive task replaced":
				updated = []Candidate{own}
				updated[0].TaskUID = "replacement-task"
				updated[0].AuthorizedPublicKey = otherAuthorizedPublicKey
				wantDenied = true
			case "same task different key":
				updated = []Candidate{own}
				updated[0].AuthorizedPublicKey = otherAuthorizedPublicKey
				wantDenied = true
			}
			if _, err := module.Replace(updated); err != nil {
				t.Fatal(err)
			}
			if name == "own lease renewed" || name == "own grant expired" {
				now = own.ExpiresAt // The renewed deadline must supersede the original one.
			}
			_, err = module.Admit(reference, "zfs get -H name tank/first")
			if (err != nil) != wantDenied {
				t.Fatalf("later admission = %v, want denied = %v", err, wantDenied)
			}
			if name == "own lease renewed" {
				now = updated[0].ExpiresAt
				if _, err := module.Admit(reference, "zfs get -H name tank/first"); err == nil || !strings.Contains(err.Error(), "expired") {
					t.Fatalf("renewed deadline admission = %v, want expiry rejection", err)
				}
			}
			if name == "receive task replaced" || name == "same task different key" {
				fresh := authenticatedReference(t, module, updated[0].AuthorizedPublicKey)
				if _, err := module.Admit(fresh, "zfs get -H name tank/first"); err != nil {
					t.Fatalf("fresh replacement admission: %v", err)
				}
			}
			var stdout bytes.Buffer
			if err := plan.Execute(context.Background(), nil, &stdout, nil); err != nil || stdout.String() != "admitted" {
				t.Fatalf("already admitted plan = %q, %v", stdout.String(), err)
			}
		})
	}
}

func TestAuthenticatedGrantIdentityBindsTaskAndCanonicalKey(t *testing.T) {
	for _, name := range []string{"task UID", "public key", "key comment", "lease expiry"} {
		t.Run(name, func(t *testing.T) {
			now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
			module := New(filepath.Join(t.TempDir(), "authorized_keys"))
			module.now = func() time.Time { return now }
			candidate := validCandidate(now, testTaskUID, testAuthorizedPublicKey)
			if _, err := module.Replace([]Candidate{candidate}); err != nil {
				t.Fatal(err)
			}
			before := authenticatedReference(t, module, candidate.AuthorizedPublicKey)
			switch name {
			case "task UID":
				candidate.TaskUID = "replacement-task"
			case "public key":
				candidate.AuthorizedPublicKey = otherAuthorizedPublicKey
			case "key comment":
				candidate.AuthorizedPublicKey += " changed comment"
			case "lease expiry":
				candidate.ExpiresAt = now.Add(30 * time.Minute)
			}
			if _, err := module.Replace([]Candidate{candidate}); err != nil {
				t.Fatal(err)
			}
			after := authenticatedReference(t, module, candidate.AuthorizedPublicKey)
			wantSame := name == "key comment" || name == "lease expiry"
			if (before.grantID == after.grantID) != wantSame {
				t.Fatalf("grant identity equality = %v, want %v", before.grantID == after.grantID, wantSame)
			}
		})
	}
}

func TestAdmitRechecksLeaseAtAdmissionInstant(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	module := New(filepath.Join(t.TempDir(), "authorized_keys"))
	module.now = func() time.Time { return now }
	candidate := validCandidate(now, testTaskUID, testAuthorizedPublicKey)
	if _, err := module.Replace([]Candidate{candidate}); err != nil {
		t.Fatal(err)
	}
	reference := authenticatedReference(t, module, candidate.AuthorizedPublicKey)
	module.hooks.beforeAdmissionRecheck = func() { now = candidate.ExpiresAt }
	if _, err := module.Admit(reference, "echo -n denied"); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("admission at expiry = %v, want expiry rejection", err)
	}
}

func TestAdmitFailsClosedWhenCleanupRemovesGenerationDuringRead(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	module := New(filepath.Join(t.TempDir(), "authorized_keys"))
	module.now = func() time.Time { return now }
	candidate := validCandidate(now, testTaskUID, testAuthorizedPublicKey)
	if _, err := module.Replace([]Candidate{candidate}); err != nil {
		t.Fatal(err)
	}
	reference := authenticatedReference(t, module, candidate.AuthorizedPublicKey)
	oldGeneration := module.generationPath(activeSnapshotID(t, module.manifestPath))
	// The clock is read after the grant set is verified, before the selected
	// grant file is read. Publish and clean up at that exact interleaving.
	module.now = func() time.Time {
		module.now = func() time.Time { return now }
		other := validCandidate(now, "other-task", otherAuthorizedPublicKey)
		if _, err := module.Replace([]Candidate{candidate, other}); err != nil {
			t.Fatal(err)
		}
		return now
	}
	if _, err := module.Admit(reference, "echo -n denied"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("admission during cleanup = %v, want missing retired generation rejection", err)
	}
	if _, err := os.Stat(oldGeneration); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retired generation = %v, want removed", err)
	}
	if _, err := module.Admit(reference, "echo -n admitted"); err != nil {
		t.Fatalf("retry against current snapshot: %v", err)
	}
}

func authenticatedReference(t *testing.T, module Module, publicKey string) Reference {
	t.Helper()
	want, err := canonicalPublicKey(publicKey)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(module.manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	for len(bytes.TrimSpace(data)) != 0 {
		key, _, options, rest, err := ssh.ParseAuthorizedKey(data)
		if err != nil {
			t.Fatal(err)
		}
		data = rest
		if strings.TrimSpace(string(ssh.MarshalAuthorizedKey(key))) != want {
			continue
		}
		for _, option := range options {
			if command, ok := strings.CutPrefix(option, `command="`); ok {
				command = strings.TrimSuffix(command, `"`)
				args := strings.Fields(command)
				if len(args) < 2 || args[0] != receiverCommandPath || args[1] != "exec" {
					t.Fatalf("unexpected forced command %q", command)
				}
				reference, err := ReferenceFromArgs(args[2:])
				if err != nil {
					t.Fatal(err)
				}
				return reference
			}
		}
	}
	t.Fatal("public key has no forced command in active manifest")
	return Reference{}
}
