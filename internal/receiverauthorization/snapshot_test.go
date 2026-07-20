package receiverauthorization

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const otherAuthorizedPublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPFmCq6yib3eYpmpYpK91ZyY8LfFdU2GWDhP9f7k7j8H unrelated"

func TestActivateUsesDeterministicImmutableSnapshot(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	first := validSnapshotCandidate("11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey, "tank/first", now.Add(10*time.Minute))
	second := validSnapshotCandidate("99999999-8888-7777-6666-555555555555", otherAuthorizedPublicKey, "tank/second", now.Add(20*time.Minute))
	manifest := filepath.Join(t.TempDir(), "authorized_keys")
	module := New(manifest)
	module.now = func() time.Time { return now }

	activation, err := module.Replace([]Candidate{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if !activation.Changed || activation.Warning != nil {
		t.Fatalf("activation = %#v, want changed without warning", activation)
	}
	firstSnapshot := activeSnapshotID(t, manifest)
	assertSnapshotID(t, firstSnapshot)
	content, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"# receiver-authorization-snapshot " + firstSnapshot + "\n",
		"--snapshot-id " + firstSnapshot + " --grant-id " + grantID(first.TaskUID),
		"--snapshot-id " + firstSnapshot + " --grant-id " + grantID(second.TaskUID),
	} {
		if !strings.Contains(string(content), want) {
			t.Fatalf("authorized_keys = %q, want %q", content, want)
		}
	}
	assertMode(t, manifest, 0o600)
	assertMode(t, module.generationPath(firstSnapshot), 0o700)
	assertMode(t, filepath.Join(module.generationPath(firstSnapshot), "authorized_keys"), 0o400)
	assertMode(t, filepath.Join(module.generationPath(firstSnapshot), "grants", grantID(first.TaskUID)+".json"), 0o400)

	activation, err = module.Replace([]Candidate{second, first})
	if err != nil {
		t.Fatal(err)
	}
	if activation.Changed {
		t.Fatal("equivalent authority in a different order changed the active snapshot")
	}
	if got := activeSnapshotID(t, manifest); got != firstSnapshot {
		t.Fatalf("snapshot ID = %q, want unchanged %q", got, firstSnapshot)
	}

	second.AllowDestroy = true
	activation, err = module.Replace([]Candidate{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if !activation.Changed {
		t.Fatal("authority-bearing change was reported unchanged")
	}
	if got := activeSnapshotID(t, manifest); got == firstSnapshot {
		t.Fatalf("authority-bearing change retained snapshot ID %q", got)
	}
}

func TestSnapshotIdentityIncludesEveryAuthorityBearingGrantField(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	baseline := validSnapshotCandidate("11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey, "tank/dst", now.Add(10*time.Minute))
	baseline.SyncSnapshotIdentifier = "rel123"
	_, baselineID, err := canonicalSnapshot(compileCandidates(now, []Candidate{baseline}).grants)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*Candidate){
		"task UID":                 func(c *Candidate) { c.TaskUID = "99999999-8888-7777-6666-555555555555" },
		"public key":               func(c *Candidate) { c.AuthorizedPublicKey = otherAuthorizedPublicKey },
		"lease expiry":             func(c *Candidate) { c.ExpiresAt = c.ExpiresAt.Add(time.Second) },
		"destination":              func(c *Candidate) { c.TargetDataset = "tank/other" },
		"receive unmounted":        func(c *Candidate) { c.ReceiveUnmounted = false },
		"receive resumable":        func(c *Candidate) { c.ReceiveResumable = true },
		"rollback":                 func(c *Candidate) { c.AllowRollback = true },
		"destroy":                  func(c *Candidate) { c.AllowDestroy = true },
		"mount":                    func(c *Candidate) { c.AllowMount = true },
		"sync snapshot destroy":    func(c *Candidate) { c.AllowSyncSnapshotDestroy = true },
		"target snapshot destroy":  func(c *Candidate) { c.AllowTargetSnapshotDestroy = true },
		"sync snapshot identifier": func(c *Candidate) { c.SyncSnapshotIdentifier = "rel456" },
		"compression":              func(c *Candidate) { c.Compression = "zstd" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			changed := baseline
			mutate(&changed)
			compilation := compileCandidates(now, []Candidate{changed})
			if outcome := compilation.outcomes[0]; outcome.Rejection != "" {
				t.Fatalf("changed candidate rejected: %s", outcome.Rejection)
			}
			_, changedID, err := canonicalSnapshot(compilation.grants)
			if err != nil {
				t.Fatal(err)
			}
			if changedID == baselineID {
				t.Fatalf("authority-bearing %s change retained snapshot ID %q", name, changedID)
			}
		})
	}
}

func TestLeaseRenewalActivatesNewSnapshotWithoutInvalidatingAdmittedPlan(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	manifest := filepath.Join(t.TempDir(), "authorized_keys")
	module := New(manifest)
	module.now = func() time.Time { return now }
	candidate := validSnapshotCandidate(testTaskUID, testAuthorizedPublicKey, "tank/dst", now.Add(10*time.Minute))
	if _, err := module.Replace([]Candidate{candidate}); err != nil {
		t.Fatal(err)
	}
	oldSnapshot := activeSnapshotID(t, manifest)
	oldReference := snapshotReference(t, oldSnapshot, grantID(candidate.TaskUID))
	plan, err := module.Admit(oldReference, "zfs receive -u tank/dst")
	if err != nil {
		t.Fatal(err)
	}

	candidate.ExpiresAt = now.Add(30 * time.Minute)
	activation, err := module.Replace([]Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	if !activation.Changed {
		t.Fatal("renewed lease did not activate a new snapshot")
	}
	renewedSnapshot := activeSnapshotID(t, manifest)
	if renewedSnapshot == oldSnapshot {
		t.Fatalf("renewed lease retained snapshot ID %q", oldSnapshot)
	}
	if _, err := module.Admit(oldReference, "zfs receive -u tank/dst"); err == nil || !strings.Contains(err.Error(), "snapshot is not active") {
		t.Fatalf("old snapshot admitted a later command after renewal: %v", err)
	}
	renewedReference := snapshotReference(t, renewedSnapshot, grantID(candidate.TaskUID))
	if _, err := module.Admit(renewedReference, "zfs receive -u tank/dst"); err != nil {
		t.Fatalf("renewed snapshot admission failed: %v", err)
	}
	if plan.command.kind == "" {
		t.Fatal("plan admitted before renewal was invalidated")
	}
}

func TestRestartAcceptsDurablyRenewedFreshTaskState(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	manifest := filepath.Join(t.TempDir(), "authorized_keys")
	oldProcess := New(manifest)
	oldProcess.now = func() time.Time { return now }
	candidate := validSnapshotCandidate(testTaskUID, testAuthorizedPublicKey, "tank/dst", now)
	if _, err := oldProcess.Replace([]Candidate{candidate}); err != nil {
		t.Fatal(err)
	}

	renewedState := candidate
	renewedState.ExpiresAt = now.Add(30 * time.Minute)
	newProcess := New(manifest)
	newProcess.now = func() time.Time { return now }
	if err := newProcess.Reset(); err != nil {
		t.Fatal(err)
	}
	activation, err := newProcess.Replace([]Candidate{renewedState})
	if err != nil {
		t.Fatal(err)
	}
	if !activation.Changed || len(activation.Outcomes()) != 1 || activation.Outcomes()[0].Rejection != "" {
		t.Fatalf("restart activation = %#v, want renewed fresh task accepted", activation)
	}
	reference := snapshotReference(t, activeSnapshotID(t, manifest), grantID(renewedState.TaskUID))
	if _, err := newProcess.Admit(reference, "zfs receive -u tank/dst"); err != nil {
		t.Fatalf("renewed task admission after restart failed: %v", err)
	}
}

func TestActivateEmptySnapshotAndResetDoNotRecoverPriorAuthority(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	manifest := filepath.Join(t.TempDir(), "authorized_keys")
	module := New(manifest)
	module.now = func() time.Time { return now }
	if _, err := module.Replace([]Candidate{validSnapshotCandidate("11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey, "tank/dst", now.Add(10*time.Minute))}); err != nil {
		t.Fatal(err)
	}
	if err := module.Reset(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(manifest); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest after reset: %v, want not exist", err)
	}
	activation, err := module.Replace(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !activation.Changed {
		t.Fatal("initial empty snapshot was reported unchanged")
	}
	content, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Split(strings.TrimSuffix(string(content), "\n"), "\n"); len(lines) != 1 || !strings.HasPrefix(lines[0], "# receiver-authorization-snapshot snapshot-") {
		t.Fatalf("empty manifest = %q, want strict header only", content)
	}
}

func TestActivateDoesNotTreatTruncatedActiveManifestAsEquivalent(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	candidate := validSnapshotCandidate("11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey, "tank/dst", now.Add(10*time.Minute))
	manifest := filepath.Join(t.TempDir(), "authorized_keys")
	module := New(manifest)
	module.now = func() time.Time { return now }
	if _, err := module.Replace([]Candidate{candidate}); err != nil {
		t.Fatal(err)
	}
	snapshotID := activeSnapshotID(t, manifest)
	if err := os.WriteFile(manifest, []byte(manifestHeaderPrefix+snapshotID+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := module.Replace([]Candidate{candidate}); err == nil || !strings.Contains(err.Error(), "manifest content mismatch") {
		t.Fatalf("Activate() error = %v, want altered active manifest rejection", err)
	}
}

func TestActiveUsableRejectsManifestThatNoLongerMatchesItsGrantSet(t *testing.T) {
	dir := t.TempDir()
	manifest := filepath.Join(dir, "authorized_keys")
	module := New(manifest)
	candidate := validSnapshotCandidate("11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey, "tank/dst", time.Now().Add(10*time.Minute))
	if _, err := module.Replace([]Candidate{candidate}); err != nil {
		t.Fatal(err)
	}
	if err := module.activeUsable(); err != nil {
		t.Fatalf("activeUsable() = %v, want valid active snapshot", err)
	}
	snapshotID := activeSnapshotID(t, manifest)
	truncated := []byte(manifestHeaderPrefix + snapshotID + "\n")
	if err := os.WriteFile(manifest, truncated, 0o600); err != nil {
		t.Fatal(err)
	}
	generationManifest := filepath.Join(module.generationPath(snapshotID), "authorized_keys")
	if err := os.Chmod(generationManifest, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(generationManifest, truncated, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := module.activeUsable(); err == nil || !strings.Contains(err.Error(), "manifest content mismatch") {
		t.Fatalf("activeUsable() = %v, want manifest/grant mismatch", err)
	}
}

func TestActivateFailureAroundCommitKeepsAuthorityOrReturnsWarning(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	manifest := filepath.Join(t.TempDir(), "authorized_keys")
	module := New(manifest)
	module.now = func() time.Time { return now }
	first := validSnapshotCandidate("11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey, "tank/first", now.Add(10*time.Minute))
	second := validSnapshotCandidate("99999999-8888-7777-6666-555555555555", otherAuthorizedPublicKey, "tank/second", now.Add(10*time.Minute))
	if _, err := module.Replace([]Candidate{first}); err != nil {
		t.Fatal(err)
	}
	firstSnapshot := activeSnapshotID(t, manifest)

	module.hooks.beforeManifestCommit = func() error { return errors.New("injected pre-commit failure") }
	_, err := module.Replace([]Candidate{second})
	if err == nil || !strings.Contains(err.Error(), "injected pre-commit failure") {
		t.Fatalf("Activate() error = %v, want injected pre-commit failure", err)
	}
	var classified interface{ ActiveAuthorityUsable() bool }
	if !errors.As(err, &classified) || !classified.ActiveAuthorityUsable() {
		t.Fatalf("replacement error = %v, want retained active authority classification", err)
	}
	if got := activeSnapshotID(t, manifest); got != firstSnapshot {
		t.Fatalf("snapshot after pre-commit failure = %q, want %q", got, firstSnapshot)
	}

	module.hooks.beforeManifestCommit = nil
	module.hooks.removeGeneration = func(string) error { return errors.New("injected cleanup failure") }
	activation, err := module.Replace([]Candidate{second})
	if err != nil {
		t.Fatalf("post-commit cleanup made activation fail: %v", err)
	}
	if !activation.Changed || activation.Warning == nil || !strings.Contains(activation.Warning.Error(), "injected cleanup failure") {
		t.Fatalf("activation = %#v, want changed with cleanup warning", activation)
	}
	if got := activeSnapshotID(t, manifest); got == firstSnapshot {
		t.Fatalf("post-commit cleanup failure retained old active snapshot %q", got)
	}
	module.hooks.removeGeneration = nil
	activation, err = module.Replace([]Candidate{second})
	if err != nil {
		t.Fatal(err)
	}
	if activation.Changed || activation.Warning != nil {
		t.Fatalf("housekeeping retry activation = %#v, want unchanged without warning", activation)
	}
	entries, err := os.ReadDir(module.generationsRoot())
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("generations after housekeeping retry = %v, want only active generation", entries)
	}
}

func TestAdmitRequiresExactActiveSnapshotAndRechecksBeforeReturning(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	manifest := filepath.Join(t.TempDir(), "authorized_keys")
	module := New(manifest)
	module.now = func() time.Time { return now }
	first := validSnapshotCandidate("11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey, "tank/first", now.Add(10*time.Minute))
	second := validSnapshotCandidate("99999999-8888-7777-6666-555555555555", otherAuthorizedPublicKey, "tank/second", now.Add(10*time.Minute))
	if _, err := module.Replace([]Candidate{first}); err != nil {
		t.Fatal(err)
	}
	firstSnapshot := activeSnapshotID(t, manifest)
	firstReference := snapshotReference(t, firstSnapshot, grantID(first.TaskUID))
	plan, err := module.Admit(firstReference, "zfs receive -u tank/first")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := module.Replace([]Candidate{second}); err != nil {
		t.Fatal(err)
	}
	if _, err := module.Admit(firstReference, "zfs receive -u tank/first"); err == nil || !strings.Contains(err.Error(), "snapshot is not active") {
		t.Fatalf("retired snapshot admission error = %v, want inactive rejection", err)
	}
	if plan.command.kind == "" {
		t.Fatal("plan admitted before activation was invalidated")
	}

	secondSnapshot := activeSnapshotID(t, manifest)
	wrongGrant := snapshotReference(t, secondSnapshot, grantID(first.TaskUID))
	if _, err := module.Admit(wrongGrant, "zfs receive -u tank/first"); err == nil {
		t.Fatal("snapshot/grant mismatch admission succeeded")
	}
	secondReference := snapshotReference(t, secondSnapshot, grantID(second.TaskUID))
	module.hooks.beforeAdmissionRecheck = func() {
		module.hooks.beforeAdmissionRecheck = nil
		_, activateErr := module.Replace([]Candidate{first})
		if activateErr != nil {
			t.Fatalf("concurrent Activate() error = %v", activateErr)
		}
	}
	if _, err := module.Admit(secondReference, "zfs receive -u tank/second"); err == nil || !strings.Contains(err.Error(), "snapshot is not active") {
		t.Fatalf("concurrent admission error = %v, want final active-snapshot rejection", err)
	}
}

func TestAdmitFailsClosedForMalformedManifestAndSymlinkedGenerationPath(t *testing.T) {
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	manifest := filepath.Join(t.TempDir(), "authorized_keys")
	module := New(manifest)
	module.now = func() time.Time { return now }
	candidate := validSnapshotCandidate("11111111-2222-3333-4444-555555555555", testAuthorizedPublicKey, "tank/dst", now.Add(10*time.Minute))
	if _, err := module.Replace([]Candidate{candidate}); err != nil {
		t.Fatal(err)
	}
	snapshotID := activeSnapshotID(t, manifest)
	reference := snapshotReference(t, snapshotID, grantID(candidate.TaskUID))
	activeData, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, append(activeData, []byte("# unexpected\n")...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := module.Admit(reference, "zfs receive -u tank/dst"); err == nil || !strings.Contains(err.Error(), "manifest content mismatch") {
		t.Fatalf("altered manifest admission error = %v, want exact-content rejection", err)
	}
	if err := os.WriteFile(manifest, activeData, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(manifest, 0o622); err != nil {
		t.Fatal(err)
	}
	if _, err := module.Admit(reference, "zfs receive -u tank/dst"); err == nil || !strings.Contains(err.Error(), "group or world writable") {
		t.Fatalf("unsafe manifest mode admission error = %v, want fail-closed mode rejection", err)
	}
	if err := os.Remove(manifest); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(module.generationPath(snapshotID), "authorized_keys"), manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := module.Admit(reference, "zfs receive -u tank/dst"); err == nil || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("symlinked manifest admission error = %v, want fail-closed symlink rejection", err)
	}
	if err := os.Remove(manifest); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, activeData, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(manifest, []byte("# malformed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := module.Admit(reference, "zfs receive -u tank/dst"); err == nil || !strings.Contains(err.Error(), "malformed snapshot header") {
		t.Fatalf("malformed manifest admission error = %v, want fail-closed header rejection", err)
	}

	generation := module.generationPath(snapshotID)
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.Rename(generation, outside); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, generation); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(outside, "authorized_keys"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := module.Admit(reference, "zfs receive -u tank/dst"); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlinked generation admission error = %v, want fail-closed path rejection", err)
	}
}

func TestReplaceRejectsSymlinkedRuntimeAncestor(t *testing.T) {
	base := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	worldWritable := filepath.Join(outside, "world-writable")
	if err := os.MkdirAll(worldWritable, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(worldWritable, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(base, "linked")); err != nil {
		t.Fatal(err)
	}
	module := New(filepath.Join(base, "linked", "world-writable", "runtime", "authorized_keys"))
	if _, err := module.Replace(nil); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("Replace() error = %v, want symlinked ancestor rejection", err)
	}
}

func snapshotReference(t *testing.T, snapshotID, grantID string) Reference {
	t.Helper()
	reference, err := ReferenceFromArgs([]string{"--snapshot-id", snapshotID, "--grant-id", grantID})
	if err != nil {
		t.Fatal(err)
	}
	return reference
}

func activeSnapshotID(t *testing.T, manifest string) string {
	t.Helper()
	data, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	id, err := parseManifestSnapshotID(data)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func assertSnapshotID(t *testing.T, id string) {
	t.Helper()
	if len(id) != len("snapshot-")+64 || !strings.HasPrefix(id, "snapshot-") {
		t.Fatalf("snapshot ID = %q, want full SHA-256 identity", id)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("mode for %s = %o, want %o", path, got, want)
	}
}

func validSnapshotCandidate(uid, key, dataset string, expiry time.Time) Candidate {
	return Candidate{
		TaskUID:             uid,
		AuthorizedPublicKey: key,
		ExpiresAt:           expiry,
		TargetDataset:       dataset,
		ReceiveUnmounted:    true,
		Compression:         "none",
	}
}
