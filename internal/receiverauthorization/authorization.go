package receiverauthorization

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	maxReceiverCommandLength       = 8192
	maxReceiverDestroyBatchCommand = 32
)

// Module owns receiver command admission.
type Module struct {
	manifestPath string
	now          func() time.Time
	hooks        moduleHooks
	leases       *leaseState
}

type leaseState struct {
	mu     sync.Mutex
	lapsed map[string]struct{}
}

// Reference identifies the authenticated Receive Task and canonical public key,
// independently of the snapshot active when SSH authentication took place.
type Reference struct {
	grantID string
}

// ReferenceFromArgs parses the authorization reference carried by a
// forced-command invocation.
func ReferenceFromArgs(args []string) (Reference, error) {
	fs := flag.NewFlagSet("exec", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var grantID string
	fs.StringVar(&grantID, "grant-id", "", "receiver authorization grant ID")
	if err := fs.Parse(args); err != nil {
		return Reference{}, err
	}
	if fs.NArg() != 0 {
		return Reference{}, fmt.Errorf("unexpected receiver authorization reference arguments")
	}
	if grantID == "" {
		return Reference{}, fmt.Errorf("receiver authorization grant ID is required")
	}
	if !validGrantID(grantID) {
		return Reference{}, fmt.Errorf("invalid receiver authorization grant ID %q", grantID)
	}
	return Reference{grantID: grantID}, nil
}

// New constructs a Receiver Authorization module backed by one stable
// authorized_keys manifest.
func New(manifestPath string) Module {
	return Module{
		manifestPath: filepath.Clean(manifestPath),
		now:          time.Now,
		leases:       &leaseState{lapsed: make(map[string]struct{})},
	}
}

// Admit resolves the grant in the currently active snapshot and authorizes the
// original SSH command into an opaque executable plan.
func (m Module) Admit(reference Reference, originalCommand string) (Plan, error) {
	if err := validateManifestPath(m.manifestPath); err != nil {
		return Plan{}, err
	}
	snapshotID, err := m.activeSnapshot()
	if err != nil {
		return Plan{}, err
	}
	grantDir := filepath.Join(m.generationPath(snapshotID), "grants")
	if err := requireSafeGenerationDirectory(filepath.Dir(m.manifestPath), m.runtimeRoot(), m.generationsRoot(), m.generationPath(snapshotID), grantDir); err != nil {
		return Plan{}, err
	}
	if err := verifySnapshotGrantSet(grantDir, snapshotID); err != nil {
		return Plan{}, err
	}
	grantPath, err := grantPathForID(grantDir, reference.grantID)
	if err != nil {
		return Plan{}, err
	}
	policy, err := readGrantPolicy(grantPath, reference.grantID, m.now())
	if err != nil {
		return Plan{}, err
	}
	if strings.TrimSpace(originalCommand) == "" {
		return Plan{}, fmt.Errorf("missing SSH_ORIGINAL_COMMAND")
	}
	command, err := authorizeReceiverCommand(originalCommand, policy)
	if err != nil {
		return Plan{}, fmt.Errorf("receiver command denied: %w", err)
	}
	if m.hooks.beforeAdmissionRecheck != nil {
		m.hooks.beforeAdmissionRecheck()
	}
	// Expiry may have passed during authorization. A concurrent publication or
	// cleanup may also retire this generation; missing files fail closed, while
	// an already returned Plan owns its command independently of these files.
	if _, err := readGrantPolicy(grantPath, reference.grantID, m.now()); err != nil {
		return Plan{}, err
	}
	if err := m.requireActiveSnapshot(snapshotID); err != nil {
		return Plan{}, err
	}
	return Plan{command: command}, nil
}

func (m Module) requireActiveSnapshot(expected string) error {
	active, err := m.activeSnapshot()
	if err != nil {
		return err
	}
	if active != expected {
		return fmt.Errorf("receiver authorization snapshot is not active")
	}
	return nil
}

func (m Module) activeSnapshot() (string, error) {
	active, manifest, err := readActiveManifest(m.manifestPath)
	if err != nil {
		return "", fmt.Errorf("read active receiver authorization snapshot: %w", err)
	}
	generationPath := m.generationPath(active)
	if err := requireSafeGenerationDirectory(filepath.Dir(m.manifestPath), m.runtimeRoot(), m.generationsRoot(), generationPath); err != nil {
		return "", err
	}
	generationManifest, err := readSafeRegularFile(filepath.Join(generationPath, "authorized_keys"), "receiver authorization generation manifest")
	if err != nil {
		return "", err
	}
	if !bytes.Equal(manifest, generationManifest) {
		return "", fmt.Errorf("active receiver authorization manifest content mismatch")
	}
	return active, nil
}

// Plan is an admitted command whose authorized arguments are private to this
// module.
type Plan struct {
	command receiverCommandPlan
}

// Execute runs an admitted plan with context and streams supplied by the
// forced-command execution adapter.
func (p Plan) Execute(ctx context.Context, stdin io.Reader, stdout, stderr io.Writer) error {
	return executeReceiverCommandPlan(ctx, commandStreams{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}, p.command)
}

type commandPolicy struct {
	TargetDataset              string
	ReceiveUnmounted           bool
	ReceiveResumable           bool
	AllowRollback              bool
	AllowDestroy               bool
	AllowMount                 bool
	AllowSyncSnapshotDestroy   bool
	AllowTargetSnapshotDestroy bool
	SyncSnapshotIdentifier     string
	Compression                string
}

type receiverCommandPlan struct {
	kind          receiverCommandKind
	echoArgs      []string
	lookupCommand string
	pipeline      []receiverCommandStep
	batch         []receiverCommandPlan
}

type receiverCommandKind string

const (
	receiverCommandExit     receiverCommandKind = "exit"
	receiverCommandEcho     receiverCommandKind = "echo"
	receiverCommandLookup   receiverCommandKind = "lookup"
	receiverCommandPS       receiverCommandKind = "ps"
	receiverCommandPipeline receiverCommandKind = "pipeline"
	receiverCommandBatch    receiverCommandKind = "batch"
)

type receiverCommandStep struct {
	Name           string
	Args           []string
	StdoutNull     bool
	StderrNull     bool
	StderrToStdout bool
}

type commandStreams struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
}

type exitError struct {
	code int
	msg  string
}

func (e exitError) Error() string {
	return e.msg
}

func (e exitError) ExitCode() int {
	return e.code
}

func grantPathForID(dir, id string) (string, error) {
	if !validGrantID(id) {
		return "", fmt.Errorf("invalid receiver authorization grant ID %q", id)
	}
	return filepath.Join(dir, id+".json"), nil
}

func validGrantID(id string) bool {
	if len(id) != len("grant-")+64 || !strings.HasPrefix(id, "grant-") {
		return false
	}
	for _, r := range id[len("grant-"):] {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func readGrantPolicy(path, expectedGrantID string, now time.Time) (commandPolicy, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return commandPolicy{}, fmt.Errorf("stat receiver grant: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return commandPolicy{}, fmt.Errorf("receiver grant must not be a symlink")
	}
	if !info.Mode().IsRegular() {
		return commandPolicy{}, fmt.Errorf("receiver grant must be a regular file")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return commandPolicy{}, fmt.Errorf("receiver grant must not be group or world writable")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return commandPolicy{}, fmt.Errorf("read receiver grant: %w", err)
	}
	compiledGrant, err := decodeGrant(data)
	if err != nil {
		return commandPolicy{}, fmt.Errorf("parse receiver grant: %w", err)
	}
	if grantID(compiledGrant.TaskUID, compiledGrant.AuthorizedPublicKey) != expectedGrantID {
		return commandPolicy{}, fmt.Errorf("receiver grant identity does not match reference")
	}
	if err := validateGrant(now, compiledGrant); err != nil {
		return commandPolicy{}, fmt.Errorf("validate receiver grant: %w", err)
	}
	return commandPolicy{
		TargetDataset:              compiledGrant.TargetDataset,
		ReceiveUnmounted:           compiledGrant.ReceiveUnmounted,
		ReceiveResumable:           compiledGrant.ReceiveResumable,
		AllowRollback:              compiledGrant.AllowRollback,
		AllowDestroy:               compiledGrant.AllowDestroy,
		AllowMount:                 compiledGrant.AllowMount,
		AllowSyncSnapshotDestroy:   compiledGrant.AllowSyncSnapshotDestroy,
		AllowTargetSnapshotDestroy: compiledGrant.AllowTargetSnapshotDestroy,
		SyncSnapshotIdentifier:     compiledGrant.SyncSnapshotIdentifier,
		Compression:                compiledGrant.Compression,
	}, nil
}
