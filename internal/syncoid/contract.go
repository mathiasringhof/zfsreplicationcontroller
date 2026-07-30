package syncoid

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/replication"
)

const (
	defaultCompression        = CompressionNone
	relationshipIDPrefix      = "zrc-"
	relationshipIDDigestBytes = 12
)

// Contract is the matched sender and Receiver side of one Syncoid Replication
// Contract. Its normalized state is intentionally opaque.
type Contract struct {
	sourceDataset  string
	targetDataset  string
	options        options
	receiverPolicy zfsv1.ReceiveTaskPolicy
}

// Connection contains sender-only SSH connection details supplied when the
// controller constructs the sender Job.
type Connection struct {
	TargetHost     string
	SSHKeyFile     string
	KnownHostsFile string
	SSHPort        int32
}

type options struct {
	noSyncSnap            bool
	noRollback            bool
	forceDelete           bool
	deleteTargetSnapshots bool
	compression           string
	identifier            string
	receiveUnmounted      bool
	receiveResumable      bool
	includeSnapshots      []string
	excludeSnapshots      []string
}

// Translate produces both sides of the Syncoid Replication Contract from one
// immutable Replication Run.
func Translate(run *zfsv1.ZFSReplicationRun) (Contract, error) {
	if run == nil {
		return Contract{}, fmt.Errorf("replication run is required")
	}
	if !replication.ValidDatasetName(run.Spec.Source.Dataset) {
		return Contract{}, fmt.Errorf("invalid source dataset %q", run.Spec.Source.Dataset)
	}
	if !replication.ValidDatasetName(run.Spec.Target.Dataset) {
		return Contract{}, fmt.Errorf("invalid target dataset %q", run.Spec.Target.Dataset)
	}
	normalized := normalize(run.Spec.Syncoid)
	if !CompressionSupported(normalized.compression) {
		return Contract{}, fmt.Errorf("unsupported compression %q", normalized.compression)
	}
	normalized.identifier = relationshipIdentifier(run)

	return Contract{
		sourceDataset: run.Spec.Source.Dataset,
		targetDataset: run.Spec.Target.Dataset,
		options:       normalized,
		receiverPolicy: zfsv1.ReceiveTaskPolicy{
			ReceiveUnmounted:           normalized.receiveUnmounted,
			ReceiveResumable:           normalized.receiveResumable,
			AllowRollback:              !normalized.noRollback,
			AllowDestroy:               normalized.forceDelete,
			AllowMount:                 !normalized.receiveUnmounted,
			AllowSyncSnapshotDestroy:   !normalized.noSyncSnap,
			AllowTargetSnapshotDestroy: normalized.deleteTargetSnapshots,
			SyncSnapshotIdentifier:     normalized.identifier,
			Compression:                normalized.compression,
		},
	}, nil
}

// ReceiverPolicy returns the Receiver Authorization policy before a Receiver
// connection is known.
func (c Contract) ReceiverPolicy() zfsv1.ReceiveTaskPolicy {
	return c.receiverPolicy
}

// SenderArguments produces the final Syncoid argument vector after the
// Receiver connection is known.
func (c Contract) SenderArguments(connection Connection) ([]string, error) {
	if err := validateConnection(connection); err != nil {
		return nil, err
	}
	compression, ok := senderCompression(c.options.compression)
	if !ok {
		return nil, fmt.Errorf("unsupported compression %q", c.options.compression)
	}
	return arguments(c.sourceDataset, c.targetDataset, connection, c.options, compression), nil
}

func validateConnection(connection Connection) error {
	if connection.TargetHost == "" {
		return fmt.Errorf("target host is required")
	}
	if connection.SSHKeyFile == "" {
		return fmt.Errorf("SSH key file is required")
	}
	if connection.KnownHostsFile == "" {
		return fmt.Errorf("known hosts file is required")
	}
	if connection.SSHPort < 1 || connection.SSHPort > 65535 {
		return fmt.Errorf("invalid SSH port %d", connection.SSHPort)
	}
	return nil
}

func normalize(spec zfsv1.SyncoidSpec) options {
	return options{
		noSyncSnap:            boolDefault(spec.NoSyncSnap, false),
		noRollback:            boolDefault(spec.NoRollback, true),
		forceDelete:           boolDefault(spec.ForceDelete, false),
		deleteTargetSnapshots: boolDefault(spec.DeleteTargetSnapshots, false),
		compression:           stringDefault(spec.Compress, defaultCompression),
		receiveUnmounted:      boolDefault(spec.ReceiveUnmounted, true),
		receiveResumable:      boolDefault(spec.ReceiveResumable, true),
		includeSnapshots:      normalizeSnapshotPatterns(spec.IncludeSnaps),
		excludeSnapshots:      normalizeSnapshotPatterns(spec.ExcludeSnaps),
	}
}

func arguments(sourceDataset, targetDataset string, connection Connection, normalized options, compression string) []string {
	var result []string
	if normalized.noSyncSnap {
		result = append(result, "--no-sync-snap")
	}
	if normalized.noRollback {
		result = append(result, "--no-rollback")
	}
	result = append(result,
		"--no-privilege-elevation",
		"--compress="+compression,
		"--identifier="+normalized.identifier,
	)
	if normalized.deleteTargetSnapshots {
		result = append(result, "--delete-target-snapshots")
	}
	result = append(result,
		"--sshoption=UserKnownHostsFile="+connection.KnownHostsFile,
		"--sshoption=StrictHostKeyChecking=yes",
		"--sshoption=IdentitiesOnly=yes",
		"--sshkey="+connection.SSHKeyFile,
		"--sshport="+strconv.FormatInt(int64(connection.SSHPort), 10),
	)
	if normalized.receiveUnmounted {
		result = append(result, "--recvoptions=u")
	}
	if !normalized.receiveResumable {
		result = append(result, "--no-resume")
	}
	for _, include := range normalized.includeSnapshots {
		result = append(result, "--include-snaps="+include)
	}
	for _, exclude := range normalized.excludeSnapshots {
		result = append(result, "--exclude-snaps="+exclude)
	}
	if normalized.forceDelete {
		result = append(result, "--force-delete")
	}
	return append(result, sourceDataset, connection.TargetHost+":"+targetDataset)
}

func relationshipIdentifier(run *zfsv1.ZFSReplicationRun) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		run.Namespace,
		run.Spec.Source.NodeName,
		run.Spec.Source.Dataset,
		run.Spec.Target.NodeName,
		run.Spec.Target.Dataset,
	}, "\x00")))
	return relationshipIDPrefix + hex.EncodeToString(sum[:relationshipIDDigestBytes])
}

func ValidIdentifier(identifier string) bool {
	if identifier == "" {
		return false
	}
	for _, character := range identifier {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			character == '_' || character == '-' || character == '.' || character == ':' {
			continue
		}
		return false
	}
	return true
}

func SnapshotOwnedByIdentifier(snapshot, identifier string) bool {
	return ValidIdentifier(identifier) &&
		replication.ValidSnapshotName(snapshot) &&
		strings.HasPrefix(snapshot, "syncoid_"+identifier+"_")
}

func normalizeSnapshotPatterns(patterns []string) []string {
	var normalized []string
	for _, pattern := range patterns {
		for _, line := range strings.Split(pattern, "\n") {
			line = strings.TrimSpace(line)
			if line != "" {
				normalized = append(normalized, line)
			}
		}
	}
	return normalized
}

func boolDefault(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func stringDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
