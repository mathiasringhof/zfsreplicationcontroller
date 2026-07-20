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

// Reference is an opaque receiver authorization reference parsed from a
// forced-command invocation.
type Reference struct {
	snapshotID string
	grantID    string
}

// ReferenceFromArgs parses the authorization reference carried by a
// forced-command invocation.
func ReferenceFromArgs(args []string) (Reference, error) {
	fs := flag.NewFlagSet("exec", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var grantID string
	var snapshotID string
	fs.StringVar(&snapshotID, "snapshot-id", "", "receiver authorization snapshot ID")
	fs.StringVar(&grantID, "grant-id", "", "receiver authorization grant ID")
	if err := fs.Parse(args); err != nil {
		return Reference{}, err
	}
	if fs.NArg() != 0 {
		return Reference{}, fmt.Errorf("unexpected receiver authorization reference arguments")
	}
	if snapshotID == "" {
		return Reference{}, fmt.Errorf("receiver authorization snapshot ID is required")
	}
	if !validSnapshotID(snapshotID) {
		return Reference{}, fmt.Errorf("invalid receiver authorization snapshot ID %q", snapshotID)
	}
	if grantID == "" {
		return Reference{}, fmt.Errorf("receiver authorization grant ID is required")
	}
	if !validGrantID(grantID) {
		return Reference{}, fmt.Errorf("invalid receiver authorization grant ID %q", grantID)
	}
	return Reference{snapshotID: snapshotID, grantID: grantID}, nil
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

// Admit resolves the referenced policy and authorizes the original SSH
// command into an opaque executable plan.
func (m Module) Admit(reference Reference, originalCommand string) (Plan, error) {
	if err := validateManifestPath(m.manifestPath); err != nil {
		return Plan{}, err
	}
	if err := m.requireActiveSnapshot(reference.snapshotID); err != nil {
		return Plan{}, err
	}
	grantDir := filepath.Join(m.generationPath(reference.snapshotID), "grants")
	if err := requireSafeGenerationDirectory(filepath.Dir(m.manifestPath), m.runtimeRoot(), m.generationsRoot(), m.generationPath(reference.snapshotID), grantDir); err != nil {
		return Plan{}, err
	}
	if err := verifySnapshotGrantSet(grantDir, reference.snapshotID); err != nil {
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
	if err := m.requireActiveSnapshot(reference.snapshotID); err != nil {
		return Plan{}, err
	}
	return Plan{command: command}, nil
}

func (m Module) requireActiveSnapshot(expected string) error {
	active, manifest, err := readActiveManifest(m.manifestPath)
	if err != nil {
		return fmt.Errorf("read active receiver authorization snapshot: %w", err)
	}
	if active != expected {
		return fmt.Errorf("receiver authorization snapshot is not active")
	}
	generationPath := m.generationPath(active)
	if err := requireSafeGenerationDirectory(filepath.Dir(m.manifestPath), m.runtimeRoot(), m.generationsRoot(), generationPath); err != nil {
		return err
	}
	generationManifest, err := readSafeRegularFile(filepath.Join(generationPath, "authorized_keys"), "receiver authorization generation manifest")
	if err != nil {
		return err
	}
	if !bytes.Equal(manifest, generationManifest) {
		return fmt.Errorf("active receiver authorization manifest content mismatch")
	}
	return nil
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
	if grantID(compiledGrant.TaskUID) != expectedGrantID {
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
