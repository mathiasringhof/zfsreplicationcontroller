package syncoid

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/replication"
	corev1 "k8s.io/api/core/v1"
)

const (
	envSourceDataset          = "SRC_DATASET"
	envTargetHost             = "DST_HOST"
	envTargetDataset          = "DST_DATASET"
	envSSHKeyFile             = "SSH_KEY_FILE"
	envKnownHostsFile         = "KNOWN_HOSTS_FILE"
	envSSHPort                = "SSH_PORT"
	envNoSyncSnap             = "SYNCOID_NO_SYNC_SNAP"
	envNoRollback             = "SYNCOID_NO_ROLLBACK"
	envForceDelete            = "SYNCOID_FORCE_DELETE"
	envDeleteTargetSnapshots  = "SYNCOID_DELETE_TARGET_SNAPSHOTS"
	envCompression            = "SYNCOID_COMPRESS"
	envIdentifier             = "SYNCOID_IDENTIFIER"
	envReceiveUnmounted       = "RECEIVE_UNMOUNTED"
	envReceiveResumable       = "RECEIVE_RESUMABLE"
	envIncludeSnapshots       = "SYNCOID_INCLUDE_SNAPS"
	envExcludeSnapshots       = "SYNCOID_EXCLUDE_SNAPS"
	defaultCompression        = CompressionNone
	relationshipIDPrefix      = "zrc-"
	relationshipIDDigestBytes = 12
)

// Translation is the matched sender and Receiver side of one Syncoid
// Replication Contract.
type Translation struct {
	SenderEnvironment []corev1.EnvVar
	ReceiverPolicy    zfsv1.ReceiveTaskPolicy
}

// Connection contains sender-only SSH connection details supplied when the
// controller constructs the sender Job.
type Connection struct {
	TargetHost     string
	SSHKeyFile     string
	KnownHostsFile string
	SSHPort        string
}

// Invocation is a strictly decoded, ready-to-execute Syncoid invocation.
// Its normalized state is intentionally private.
type Invocation struct {
	arguments []string
	summary   string
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
func Translate(run *zfsv1.ZFSReplicationRun) (Translation, error) {
	if run == nil {
		return Translation{}, fmt.Errorf("replication run is required")
	}
	normalized := normalize(run.Spec.Syncoid)
	if !CompressionSupported(normalized.compression) {
		return Translation{}, fmt.Errorf("unsupported compression %q", normalized.compression)
	}
	normalized.identifier = relationshipIdentifier(run)

	return Translation{
		SenderEnvironment: encodeEnvironment(run, normalized),
		ReceiverPolicy: zfsv1.ReceiveTaskPolicy{
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

// ConnectionEnvironment encodes sender-only connection data using the same
// private environment transport decoded by DecodeSenderEnvironment.
func ConnectionEnvironment(connection Connection) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: envTargetHost, Value: connection.TargetHost},
		{Name: envSSHKeyFile, Value: connection.SSHKeyFile},
		{Name: envKnownHostsFile, Value: connection.KnownHostsFile},
		{Name: envSSHPort, Value: connection.SSHPort},
	}
}

// DecodeSenderEnvironment strictly decodes the private environment transport
// and constructs the final Syncoid invocation.
func DecodeSenderEnvironment(lookup func(string) (string, bool)) (Invocation, error) {
	sourceDataset, err := requiredString(lookup, envSourceDataset)
	if err != nil {
		return Invocation{}, err
	}
	if !replication.ValidDatasetName(sourceDataset) {
		return Invocation{}, fmt.Errorf("invalid source dataset %q", sourceDataset)
	}
	targetDataset, err := requiredString(lookup, envTargetDataset)
	if err != nil {
		return Invocation{}, err
	}
	if !replication.ValidDatasetName(targetDataset) {
		return Invocation{}, fmt.Errorf("invalid target dataset %q", targetDataset)
	}
	connection, err := decodeConnection(lookup)
	if err != nil {
		return Invocation{}, err
	}
	normalized, err := decodeOptions(lookup)
	if err != nil {
		return Invocation{}, err
	}
	compression, ok := senderCompression(normalized.compression)
	if !ok {
		return Invocation{}, fmt.Errorf("unsupported compression %q", normalized.compression)
	}

	arguments := arguments(sourceDataset, targetDataset, connection, normalized, compression)
	return Invocation{
		arguments: arguments,
		summary: fmt.Sprintf(
			"sourceDataset=%s targetDataset=%s targetHost=%s sshPort=%s syncoidIdentifier=%s noSyncSnap=%t noRollback=%t forceDelete=%t deleteTargetSnapshots=%t compress=%s receiveUnmounted=%t receiveResumable=%t includeSnaps=%q excludeSnaps=%q",
			sourceDataset,
			targetDataset,
			connection.TargetHost,
			connection.SSHPort,
			normalized.identifier,
			normalized.noSyncSnap,
			normalized.noRollback,
			normalized.forceDelete,
			normalized.deleteTargetSnapshots,
			normalized.compression,
			normalized.receiveUnmounted,
			normalized.receiveResumable,
			strings.Join(normalized.includeSnapshots, ","),
			strings.Join(normalized.excludeSnapshots, ","),
		),
	}, nil
}

func (i Invocation) Arguments() []string {
	return slices.Clone(i.arguments)
}

func (i Invocation) Summary() string {
	return i.summary
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

func encodeEnvironment(run *zfsv1.ZFSReplicationRun, normalized options) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: envSourceDataset, Value: run.Spec.Source.Dataset},
		{Name: envTargetDataset, Value: run.Spec.Target.Dataset},
		{Name: envNoSyncSnap, Value: strconv.FormatBool(normalized.noSyncSnap)},
		{Name: envNoRollback, Value: strconv.FormatBool(normalized.noRollback)},
		{Name: envForceDelete, Value: strconv.FormatBool(normalized.forceDelete)},
		{Name: envDeleteTargetSnapshots, Value: strconv.FormatBool(normalized.deleteTargetSnapshots)},
		{Name: envCompression, Value: normalized.compression},
		{Name: envIdentifier, Value: normalized.identifier},
		{Name: envReceiveUnmounted, Value: strconv.FormatBool(normalized.receiveUnmounted)},
		{Name: envReceiveResumable, Value: strconv.FormatBool(normalized.receiveResumable)},
		{Name: envIncludeSnapshots, Value: strings.Join(normalized.includeSnapshots, "\n")},
		{Name: envExcludeSnapshots, Value: strings.Join(normalized.excludeSnapshots, "\n")},
	}
}

func decodeConnection(lookup func(string) (string, bool)) (Connection, error) {
	connection := Connection{}
	var err error
	if connection.TargetHost, err = requiredString(lookup, envTargetHost); err != nil {
		return Connection{}, err
	}
	if connection.SSHKeyFile, err = requiredString(lookup, envSSHKeyFile); err != nil {
		return Connection{}, err
	}
	if connection.KnownHostsFile, err = requiredString(lookup, envKnownHostsFile); err != nil {
		return Connection{}, err
	}
	if connection.SSHPort, err = requiredString(lookup, envSSHPort); err != nil {
		return Connection{}, err
	}
	port, err := strconv.ParseUint(connection.SSHPort, 10, 16)
	if err != nil || port == 0 {
		return Connection{}, fmt.Errorf("invalid SSH port %q", connection.SSHPort)
	}
	return connection, nil
}

func decodeOptions(lookup func(string) (string, bool)) (options, error) {
	var decoded options
	var err error
	if decoded.noSyncSnap, err = requiredBool(lookup, envNoSyncSnap); err != nil {
		return options{}, err
	}
	if decoded.noRollback, err = requiredBool(lookup, envNoRollback); err != nil {
		return options{}, err
	}
	if decoded.forceDelete, err = requiredBool(lookup, envForceDelete); err != nil {
		return options{}, err
	}
	if decoded.deleteTargetSnapshots, err = requiredBool(lookup, envDeleteTargetSnapshots); err != nil {
		return options{}, err
	}
	if decoded.compression, err = requiredString(lookup, envCompression); err != nil {
		return options{}, err
	}
	if !CompressionSupported(decoded.compression) {
		return options{}, fmt.Errorf("unsupported compression %q", decoded.compression)
	}
	if decoded.identifier, err = requiredString(lookup, envIdentifier); err != nil {
		return options{}, err
	}
	if !ValidIdentifier(decoded.identifier) {
		return options{}, fmt.Errorf("unsafe Syncoid identifier %q", decoded.identifier)
	}
	if decoded.receiveUnmounted, err = requiredBool(lookup, envReceiveUnmounted); err != nil {
		return options{}, err
	}
	if decoded.receiveResumable, err = requiredBool(lookup, envReceiveResumable); err != nil {
		return options{}, err
	}
	if decoded.includeSnapshots, err = requiredList(lookup, envIncludeSnapshots); err != nil {
		return options{}, err
	}
	if decoded.excludeSnapshots, err = requiredList(lookup, envExcludeSnapshots); err != nil {
		return options{}, err
	}
	return decoded, nil
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
		"--sshport="+connection.SSHPort,
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

func requiredString(lookup func(string) (string, bool), name string) (string, error) {
	value, ok := lookup(name)
	if !ok || value == "" {
		return "", fmt.Errorf("required sender environment value %s is missing", name)
	}
	return value, nil
}

func requiredBool(lookup func(string) (string, bool), name string) (bool, error) {
	value, ok := lookup(name)
	if !ok {
		return false, fmt.Errorf("required sender environment value %s is missing", name)
	}
	switch value {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("parse sender environment value %s: expected true or false", name)
	}
}

func requiredList(lookup func(string) (string, bool), name string) ([]string, error) {
	value, ok := lookup(name)
	if !ok {
		return nil, fmt.Errorf("required sender environment value %s is missing", name)
	}
	if value == "" {
		return nil, nil
	}
	values := strings.Split(value, "\n")
	for _, item := range values {
		if item == "" || strings.ContainsAny(item, "\r\n") {
			return nil, fmt.Errorf("sender environment value %s contains a malformed list", name)
		}
	}
	return values, nil
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
