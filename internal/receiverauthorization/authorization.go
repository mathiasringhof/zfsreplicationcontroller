package receiverauthorization

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/replication"
)

const (
	maxReceiverCommandLength       = 8192
	maxReceiverDestroyBatchCommand = 32
)

// Module owns receiver command admission.
type Module struct {
	policyDir string
}

// New constructs a Receiver Authorization module backed by the current
// receiver policy directory.
func New(policyDir string) Module {
	return Module{policyDir: filepath.Clean(policyDir)}
}

// Admit resolves the referenced policy and authorizes the original SSH
// command into an opaque executable plan.
func (m Module) Admit(policyID, originalCommand string) (Plan, error) {
	if strings.TrimSpace(originalCommand) == "" {
		return Plan{}, fmt.Errorf("missing SSH_ORIGINAL_COMMAND")
	}
	policyPath, err := policyPathForID(m.policyDir, policyID)
	if err != nil {
		return Plan{}, err
	}
	policy, err := readPolicy(policyPath)
	if err != nil {
		return Plan{}, err
	}
	command, err := authorizeReceiverCommand(originalCommand, policy)
	if err != nil {
		return Plan{}, fmt.Errorf("receiver command denied: %w", err)
	}
	return Plan{command: command}, nil
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
	TargetDataset string `json:"targetDataset"`
	zfsv1.ReceiveTaskPolicy
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

func policyPathForID(dir, id string) (string, error) {
	if !validPolicyID(id) {
		return "", fmt.Errorf("invalid receiver policy ID %q", id)
	}
	return filepath.Join(dir, id+".json"), nil
}

func validPolicyID(id string) bool {
	if len(id) != len("policy-")+32 || !strings.HasPrefix(id, "policy-") {
		return false
	}
	for _, r := range id[len("policy-"):] {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func readPolicy(path string) (commandPolicy, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return commandPolicy{}, fmt.Errorf("stat receiver policy: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return commandPolicy{}, fmt.Errorf("receiver policy must not be a symlink")
	}
	if !info.Mode().IsRegular() {
		return commandPolicy{}, fmt.Errorf("receiver policy must be a regular file")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return commandPolicy{}, fmt.Errorf("receiver policy must not be group or world writable")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return commandPolicy{}, fmt.Errorf("read receiver policy: %w", err)
	}
	var policy commandPolicy
	if err := json.Unmarshal(data, &policy); err != nil {
		return commandPolicy{}, fmt.Errorf("parse receiver policy: %w", err)
	}
	if err := normalizePolicy(data, &policy); err != nil {
		return commandPolicy{}, fmt.Errorf("parse receiver policy fields: %w", err)
	}
	policy.Compression = replication.CompressionDefault(policy.Compression)
	return policy, nil
}

func normalizePolicy(data []byte, policy *commandPolicy) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if _, ok := fields["allowMount"]; !ok && !policy.ReceiveUnmounted {
		policy.AllowMount = true
	}
	return nil
}
