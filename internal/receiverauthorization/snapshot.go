package receiverauthorization

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	snapshotPrefix       = "snapshot-"
	manifestHeaderPrefix = "# receiver-authorization-snapshot "
	receiverCommandPath  = "/usr/local/bin/zfsrep-receiver"
)

// Activation reports whether authority changed. A warning means authority was
// committed successfully but retired-generation housekeeping should be retried.
type Activation struct {
	Changed      bool
	Warning      error
	NextDeadline time.Time
	outcomes     []Outcome
}

// Outcomes returns one isolated result for each submitted candidate in input order.
func (a Activation) Outcomes() []Outcome {
	return append([]Outcome(nil), a.outcomes...)
}

type moduleHooks struct {
	beforeManifestCommit   func() error
	beforeAdmissionRecheck func()
	removeGeneration       func(string) error
}

// Reset starts a Receiver process with no active authority and discards
// private generations left by an earlier process.
func (m Module) Reset() error {
	if err := validateManifestPath(m.manifestPath); err != nil {
		return err
	}
	if err := ensureManifestParent(m.manifestPath); err != nil {
		return err
	}
	if err := removePathWithoutFollowing(m.manifestPath); err != nil {
		return fmt.Errorf("remove active receiver authorization manifest: %w", err)
	}
	if err := removePathWithoutFollowing(m.runtimeRoot()); err != nil {
		return fmt.Errorf("discard receiver authorization generations: %w", err)
	}
	return nil
}

// activeUsable verifies that the currently published snapshot remains readable
// and internally consistent after an attempted replacement fails.
func (m Module) activeUsable() error {
	snapshotID, _, err := readActiveManifest(m.manifestPath)
	if err != nil {
		return err
	}
	grants, err := readGenerationGrants(filepath.Join(m.generationPath(snapshotID), "grants"))
	if err != nil {
		return err
	}
	canonicalGrants, canonicalID, err := canonicalSnapshot(grants)
	if err != nil {
		return err
	}
	if canonicalID != snapshotID {
		return fmt.Errorf("active receiver authorization grant set does not match snapshot")
	}
	expectedManifest, err := renderManifest(canonicalGrants, snapshotID)
	if err != nil {
		return err
	}
	activeManifest, err := readSafeRegularFile(m.manifestPath, "receiver authorization manifest")
	if err != nil {
		return err
	}
	if !bytes.Equal(activeManifest, expectedManifest) {
		return fmt.Errorf("active receiver authorization manifest content mismatch")
	}
	return validateGeneration(m.generationPath(snapshotID), canonicalGrants, snapshotID, expectedManifest)
}

type replacementError struct {
	cause                 error
	activeAuthorityUsable bool
}

func (e replacementError) Error() string { return e.cause.Error() }
func (e replacementError) Unwrap() error { return e.cause }

// ActiveAuthorityUsable reports whether a replacement failure retained a
// complete enforceable snapshot.
func (e replacementError) ActiveAuthorityUsable() bool { return e.activeAuthorityUsable }

// Replace compiles and atomically activates one complete candidate view as an
// immutable Receiver Authorization Snapshot.
func (m Module) Replace(candidates []Candidate) (Activation, error) {
	now := m.now()
	compiled := compileCandidates(now, candidates)
	previouslyLapsed := m.recordAndReturnPreviouslyObservedLapses(now, candidates)
	if len(previouslyLapsed) != 0 {
		for i := range candidates {
			if _, lapsed := previouslyLapsed[candidates[i].TaskUID]; lapsed {
				compiled.outcomes[i].Rejection = "authorization lease has lapsed"
			}
		}
		compiled.grants = removeLapsedGrants(compiled.grants, previouslyLapsed)
	}
	activation, err := m.activate(compiled)
	if err != nil {
		return Activation{}, replacementError{cause: err, activeAuthorityUsable: m.activeUsable() == nil}
	}
	activation.NextDeadline = nextLeaseDeadline(compiled.grants)
	activation.outcomes = append([]Outcome(nil), compiled.outcomes...)
	return activation, nil
}

func (m Module) recordAndReturnPreviouslyObservedLapses(now time.Time, candidates []Candidate) map[string]struct{} {
	m.leases.mu.Lock()
	defer m.leases.mu.Unlock()
	previouslyLapsed := make(map[string]struct{}, len(m.leases.lapsed))
	for uid := range m.leases.lapsed {
		previouslyLapsed[uid] = struct{}{}
	}
	for _, candidate := range candidates {
		if candidate.TaskUID != "" && !candidate.ExpiresAt.After(now) {
			m.leases.lapsed[candidate.TaskUID] = struct{}{}
		}
	}
	return previouslyLapsed
}

func removeLapsedGrants(grants []grant, lapsed map[string]struct{}) []grant {
	active := grants[:0]
	for _, compiledGrant := range grants {
		if _, found := lapsed[compiledGrant.TaskUID]; !found {
			active = append(active, compiledGrant)
		}
	}
	return active
}

func nextLeaseDeadline(grants []grant) time.Time {
	var next time.Time
	for _, compiledGrant := range grants {
		if next.IsZero() || compiledGrant.ExpiresAt.Before(next) {
			next = compiledGrant.ExpiresAt
		}
	}
	return next
}

func (m Module) activate(compilation compilation) (Activation, error) {
	if err := validateManifestPath(m.manifestPath); err != nil {
		return Activation{}, err
	}
	if err := ensureManifestParent(m.manifestPath); err != nil {
		return Activation{}, err
	}
	if err := ensurePrivateDir(m.runtimeRoot()); err != nil {
		return Activation{}, err
	}
	if err := ensurePrivateDir(m.generationsRoot()); err != nil {
		return Activation{}, err
	}

	grants, snapshotID, err := canonicalSnapshot(compilation.grants)
	if err != nil {
		return Activation{}, err
	}
	manifest, err := renderManifest(grants, snapshotID)
	if err != nil {
		return Activation{}, err
	}
	activeID, activeManifest, activeErr := readActiveManifest(m.manifestPath)
	if activeErr == nil && activeID == snapshotID {
		if !bytes.Equal(activeManifest, manifest) {
			return Activation{}, fmt.Errorf("active receiver authorization manifest content mismatch")
		}
		if err := validateGeneration(m.generationPath(snapshotID), grants, snapshotID, manifest); err != nil {
			return Activation{}, fmt.Errorf("validate active receiver authorization generation: %w", err)
		}
		return Activation{Warning: m.cleanupRetiredGenerations(snapshotID)}, nil
	}
	if activeErr != nil && !errors.Is(activeErr, os.ErrNotExist) {
		return Activation{}, activeErr
	}

	if err := m.materializeGeneration(grants, snapshotID, manifest); err != nil {
		return Activation{}, err
	}
	if err := m.commitManifest(manifest); err != nil {
		return Activation{}, err
	}

	return Activation{Changed: true, Warning: m.cleanupRetiredGenerations(snapshotID)}, nil
}

func canonicalSnapshot(grants []grant) ([]grant, string, error) {
	canonical := append([]grant(nil), grants...)
	sort.Slice(canonical, func(i, j int) bool {
		return grantID(canonical[i].TaskUID) < grantID(canonical[j].TaskUID)
	})
	data, err := json.Marshal(canonical)
	if err != nil {
		return nil, "", fmt.Errorf("encode receiver authorization snapshot: %w", err)
	}
	sum := sha256.Sum256(data)
	return canonical, snapshotPrefix + hex.EncodeToString(sum[:]), nil
}

func renderManifest(grants []grant, snapshotID string) ([]byte, error) {
	if !validSnapshotID(snapshotID) {
		return nil, fmt.Errorf("invalid receiver authorization snapshot ID %q", snapshotID)
	}
	var content strings.Builder
	content.WriteString(manifestHeaderPrefix)
	content.WriteString(snapshotID)
	content.WriteByte('\n')
	for _, compiledGrant := range grants {
		id := grantID(compiledGrant.TaskUID)
		forcedCommand := receiverCommandPath + " exec --snapshot-id " + snapshotID + " --grant-id " + id
		expiry := openSSHExpiryTime(compiledGrant.ExpiresAt)
		content.WriteString("restrict,expiry-time=\"")
		content.WriteString(expiry)
		content.WriteString("\",command=\"")
		content.WriteString(escapeAuthorizedKeysOption(forcedCommand))
		content.WriteString("\" ")
		content.WriteString(compiledGrant.AuthorizedPublicKey)
		content.WriteByte('\n')
	}
	return []byte(content.String()), nil
}

func openSSHExpiryTime(deadline time.Time) string {
	// OpenSSH accepts a key while its whole-second expiry equals the current
	// time, so publish the preceding UTC second to avoid extending authority.
	return deadline.UTC().Truncate(time.Second).Add(-time.Second).Format("20060102150405Z")
}

func parseManifestSnapshotID(data []byte) (string, error) {
	line, _, found := bytes.Cut(data, []byte{'\n'})
	if !found || !bytes.HasPrefix(line, []byte(manifestHeaderPrefix)) {
		return "", fmt.Errorf("receiver authorization manifest has malformed snapshot header")
	}
	id := string(bytes.TrimPrefix(line, []byte(manifestHeaderPrefix)))
	if !validSnapshotID(id) || len(line) != len(manifestHeaderPrefix)+len(id) {
		return "", fmt.Errorf("receiver authorization manifest has malformed snapshot header")
	}
	return id, nil
}

func validSnapshotID(id string) bool {
	if len(id) != len(snapshotPrefix)+64 || !strings.HasPrefix(id, snapshotPrefix) {
		return false
	}
	for _, r := range id[len(snapshotPrefix):] {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func (m Module) materializeGeneration(grants []grant, snapshotID string, manifest []byte) (returnErr error) {
	finalPath := m.generationPath(snapshotID)
	if _, err := os.Lstat(finalPath); err == nil {
		return validateGeneration(finalPath, grants, snapshotID, manifest)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat receiver authorization generation: %w", err)
	}

	stagingPath, err := os.MkdirTemp(m.generationsRoot(), ".staging-")
	if err != nil {
		return fmt.Errorf("create receiver authorization staging generation: %w", err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, os.RemoveAll(stagingPath))
	}()
	grantsPath := filepath.Join(stagingPath, "grants")
	if err := os.Mkdir(grantsPath, 0o700); err != nil {
		return fmt.Errorf("create receiver authorization grant directory: %w", err)
	}
	for _, compiledGrant := range grants {
		data, err := json.Marshal(compiledGrant)
		if err != nil {
			return fmt.Errorf("marshal receiver grant: %w", err)
		}
		path := filepath.Join(grantsPath, grantID(compiledGrant.TaskUID)+".json")
		if err := writeNewFile(path, append(data, '\n'), 0o400); err != nil {
			return fmt.Errorf("write receiver grant: %w", err)
		}
	}
	if err := writeNewFile(filepath.Join(stagingPath, "authorized_keys"), manifest, 0o400); err != nil {
		return fmt.Errorf("write receiver authorization generation manifest: %w", err)
	}
	if err := os.Chmod(grantsPath, 0o700); err != nil {
		return fmt.Errorf("seal receiver authorization grant directory: %w", err)
	}
	if err := os.Chmod(stagingPath, 0o700); err != nil {
		return fmt.Errorf("seal receiver authorization generation: %w", err)
	}
	if err := os.Rename(stagingPath, finalPath); err != nil {
		return fmt.Errorf("publish receiver authorization generation: %w", err)
	}
	return nil
}

func validateGeneration(path string, grants []grant, snapshotID string, expectedManifest []byte) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("receiver authorization generation is unsafe")
	}
	manifestPath := filepath.Join(path, "authorized_keys")
	manifestData, err := readSafeRegularFile(manifestPath, "receiver authorization generation manifest")
	if err != nil {
		return err
	}
	if id, err := parseManifestSnapshotID(manifestData); err != nil || id != snapshotID {
		return fmt.Errorf("receiver authorization generation manifest identity mismatch")
	}
	if !bytes.Equal(manifestData, expectedManifest) {
		return fmt.Errorf("receiver authorization generation manifest content mismatch")
	}
	grantDir := filepath.Join(path, "grants")
	storedGrants, err := readGenerationGrants(grantDir)
	if err != nil {
		return err
	}
	canonicalStored, storedSnapshotID, err := canonicalSnapshot(storedGrants)
	if err != nil {
		return err
	}
	if storedSnapshotID != snapshotID || len(canonicalStored) != len(grants) {
		return fmt.Errorf("receiver authorization generation grant set mismatch")
	}
	for i := range grants {
		if canonicalStored[i] != grants[i] {
			return fmt.Errorf("receiver authorization generation grant set mismatch")
		}
	}
	return nil
}

func (m Module) commitManifest(data []byte) (returnErr error) {
	file, err := os.CreateTemp(filepath.Dir(m.manifestPath), ".authorized_keys-")
	if err != nil {
		return fmt.Errorf("create receiver authorization manifest: %w", err)
	}
	temporaryPath := file.Name()
	defer func() {
		if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			returnErr = errors.Join(returnErr, err)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return errors.Join(fmt.Errorf("set receiver authorization manifest mode: %w", err), file.Close())
	}
	if _, err := file.Write(data); err != nil {
		return errors.Join(fmt.Errorf("write receiver authorization manifest: %w", err), file.Close())
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close receiver authorization manifest: %w", err)
	}
	if m.hooks.beforeManifestCommit != nil {
		if err := m.hooks.beforeManifestCommit(); err != nil {
			return fmt.Errorf("commit receiver authorization manifest: %w", err)
		}
	}
	if err := os.Rename(temporaryPath, m.manifestPath); err != nil {
		return fmt.Errorf("commit receiver authorization manifest: %w", err)
	}
	return nil
}

func (m Module) cleanupRetiredGenerations(activeID string) error {
	entries, err := os.ReadDir(m.generationsRoot())
	if err != nil {
		return fmt.Errorf("list retired receiver authorization generations: %w", err)
	}
	var cleanupErrors []error
	for _, entry := range entries {
		if entry.Name() == activeID {
			continue
		}
		path := filepath.Join(m.generationsRoot(), entry.Name())
		remove := removeGeneration
		if m.hooks.removeGeneration != nil {
			remove = m.hooks.removeGeneration
		}
		if err := remove(path); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove retired receiver authorization generation %q: %w", entry.Name(), err))
		}
	}
	return errors.Join(cleanupErrors...)
}

func (m Module) runtimeRoot() string {
	return filepath.Join(filepath.Dir(m.manifestPath), "receiver-authorization")
}

func (m Module) generationsRoot() string {
	return filepath.Join(m.runtimeRoot(), "generations")
}

func (m Module) generationPath(snapshotID string) string {
	return filepath.Join(m.generationsRoot(), snapshotID)
}

func ensureManifestParent(manifestPath string) error {
	parent := filepath.Dir(manifestPath)
	if err := ensurePrivateDir(parent); err != nil {
		return fmt.Errorf("prepare receiver authorization runtime directory: %w", err)
	}
	return nil
}

func validateManifestPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Base(path) == "." || filepath.Base(path) == string(filepath.Separator) {
		return fmt.Errorf("receiver authorization manifest path is unsafe")
	}
	return nil
}

func ensurePrivateDir(path string) error {
	if err := rejectSymlinkPathComponents(path); err != nil {
		return err
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	if err := rejectSymlinkPathComponents(path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("path must not be a symlink")
	}
	if !info.IsDir() {
		return fmt.Errorf("path is not a directory")
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("set private directory mode: %w", err)
	}
	return nil
}

func writeNewFile(path string, data []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return errors.Join(writeErr, closeErr)
	}
	return os.Chmod(path, mode)
}

func readActiveManifest(path string) (string, []byte, error) {
	data, err := readSafeRegularFile(path, "receiver authorization manifest")
	if err != nil {
		return "", nil, err
	}
	id, err := parseManifestSnapshotID(data)
	if err != nil {
		return "", nil, err
	}
	return id, data, nil
}

func readSafeRegularFile(path, name string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%s must not be a symlink", name)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a regular file", name)
	}
	if info.Mode().Perm()&0o022 != 0 {
		return nil, fmt.Errorf("%s must not be group or world writable", name)
	}
	return os.ReadFile(path)
}

func requireSafeGenerationDirectory(paths ...string) error {
	for _, path := range paths {
		if err := rejectSymlinkPathComponents(path); err != nil {
			return err
		}
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("stat receiver authorization generation path: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
			return fmt.Errorf("receiver authorization generation path is unsafe")
		}
	}
	return nil
}

func rejectSymlinkPathComponents(path string) error {
	current := filepath.Clean(path)
	for current != string(filepath.Separator) && current != "." {
		info, err := os.Lstat(current)
		if err == nil {
			if info.Mode()&os.ModeSymlink != 0 {
				if !trustedTopLevelSystemSymlink(current, info) {
					return fmt.Errorf("receiver authorization path component %q must not be a symlink", current)
				}
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("stat receiver authorization path component %q: %w", current, err)
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}
	return nil
}

func trustedTopLevelSystemSymlink(path string, info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0 && filepath.Dir(path) == string(filepath.Separator)
}

func verifySnapshotGrantSet(grantDir, expectedSnapshotID string) error {
	grants, err := readGenerationGrants(grantDir)
	if err != nil {
		return err
	}
	_, actualSnapshotID, err := canonicalSnapshot(grants)
	if err != nil {
		return err
	}
	if actualSnapshotID != expectedSnapshotID {
		return fmt.Errorf("receiver authorization grant set does not match snapshot")
	}
	return nil
}

func readGenerationGrants(grantDir string) ([]grant, error) {
	if err := requireSafeGenerationDirectory(grantDir); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(grantDir)
	if err != nil {
		return nil, fmt.Errorf("list receiver grants: %w", err)
	}
	grants := make([]grant, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json") {
			return nil, fmt.Errorf("receiver authorization generation contains unexpected grant entry %q", name)
		}
		id := strings.TrimSuffix(name, ".json")
		if !validGrantID(id) {
			return nil, fmt.Errorf("receiver authorization generation contains invalid grant entry %q", name)
		}
		data, err := readSafeRegularFile(filepath.Join(grantDir, name), "receiver grant")
		if err != nil {
			return nil, err
		}
		compiledGrant, err := decodeGrant(data)
		if err != nil {
			return nil, fmt.Errorf("parse receiver grant: %w", err)
		}
		if grantID(compiledGrant.TaskUID) != id {
			return nil, fmt.Errorf("receiver grant identity does not match filename")
		}
		grants = append(grants, compiledGrant)
	}
	return grants, nil
}

func removeGeneration(path string) error {
	if err := filepath.Walk(path, func(current string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			return os.Chmod(current, 0o700)
		}
		return nil
	}); err != nil {
		return err
	}
	return os.RemoveAll(path)
}

func removePathWithoutFollowing(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
		return removeGeneration(path)
	}
	return os.Remove(path)
}
