package datamover

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/mathias/zfsreplicationcontroller/internal/replication"
)

const (
	EnvRole              = "ZFSREP_ROLE"
	EnvSrcDataset        = "SRC_DATASET"
	EnvDstHost           = "DST_HOST"
	EnvDstDataset        = "DST_DATASET"
	EnvSSHKeyFile        = "SSH_KEY_FILE"
	EnvKnownHostsFile    = "KNOWN_HOSTS_FILE"
	EnvSSHPort           = "SSH_PORT"
	EnvNoSyncSnap        = "SYNCOID_NO_SYNC_SNAP"
	EnvNoRollback        = "SYNCOID_NO_ROLLBACK"
	EnvForceDelete       = "SYNCOID_FORCE_DELETE"
	EnvCompress          = "SYNCOID_COMPRESS"
	EnvSyncoidIdentifier = "SYNCOID_IDENTIFIER"
	EnvReceiveUnmounted  = "RECEIVE_UNMOUNTED"
	EnvReceiveResumable  = "RECEIVE_RESUMABLE"
	EnvIncludeSnaps      = "SYNCOID_INCLUDE_SNAPS"
	EnvExcludeSnaps      = "SYNCOID_EXCLUDE_SNAPS"
	EnvExpectedNodeName  = "EXPECTED_NODE_NAME"
	EnvActualNodeName    = "ACTUAL_NODE_NAME"

	RoleSender            = "sender"
	DefaultSSHKeyFile     = "/var/run/zfsrep/ssh/id_rsa"
	DefaultKnownHostsFile = "/var/run/zfsrep/ssh/known_hosts"
	DefaultSSHPort        = "2222"
)

type SenderConfig struct {
	SrcDataset        string
	DstHost           string
	DstDataset        string
	SSHKeyFile        string
	KnownHostsFile    string
	SSHPort           string
	NoSyncSnap        bool
	NoRollback        bool
	ForceDelete       bool
	Compress          string
	SyncoidIdentifier string
	ReceiveUnmounted  bool
	ReceiveResumable  bool
	IncludeSnaps      []string
	ExcludeSnaps      []string
	ExpectedNode      string
	ActualNode        string
}

func SenderConfigFromEnv() SenderConfig {
	return SenderConfigFromLookup(os.Getenv)
}

func SenderConfigFromLookup(lookup func(string) string) SenderConfig {
	return SenderConfig{
		SrcDataset:        lookup(EnvSrcDataset),
		DstHost:           lookup(EnvDstHost),
		DstDataset:        lookup(EnvDstDataset),
		SSHKeyFile:        lookup(EnvSSHKeyFile),
		KnownHostsFile:    lookup(EnvKnownHostsFile),
		SSHPort:           lookup(EnvSSHPort),
		NoSyncSnap:        boolLookupDefault(lookup, EnvNoSyncSnap, false),
		NoRollback:        boolLookupDefault(lookup, EnvNoRollback, true),
		ForceDelete:       boolLookupDefault(lookup, EnvForceDelete, false),
		Compress:          lookupDefault(lookup, EnvCompress, replication.CompressionNone),
		SyncoidIdentifier: lookup(EnvSyncoidIdentifier),
		ReceiveUnmounted:  lookupDefault(lookup, EnvReceiveUnmounted, "true") == "true",
		ReceiveResumable:  lookupDefault(lookup, EnvReceiveResumable, "true") == "true",
		IncludeSnaps:      listLookup(lookup, EnvIncludeSnaps),
		ExcludeSnaps:      listLookup(lookup, EnvExcludeSnaps),
		ExpectedNode:      lookup(EnvExpectedNodeName),
		ActualNode:        lookup(EnvActualNodeName),
	}
}

func RunSender(ctx context.Context, cfg SenderConfig, r CommandRunner) error {
	if err := validateNode(cfg.ExpectedNode, cfg.ActualNode); err != nil {
		return err
	}
	compress, err := replication.SyncoidCompression(cfg.Compress)
	if err != nil {
		return err
	}
	var args []string
	if cfg.NoSyncSnap {
		args = append(args, "--no-sync-snap")
	}
	if cfg.NoRollback {
		args = append(args, "--no-rollback")
	}
	args = append(args, "--no-privilege-elevation")
	if compress != "" {
		args = append(args, "--compress="+compress)
	}
	if cfg.SyncoidIdentifier != "" {
		if !replication.ValidSyncoidIdentifier(cfg.SyncoidIdentifier) {
			return fmt.Errorf("unsupported syncoid identifier %q", cfg.SyncoidIdentifier)
		}
		args = append(args, "--identifier="+cfg.SyncoidIdentifier)
	}
	if cfg.DstHost != "" && cfg.KnownHostsFile == "" {
		return fmt.Errorf("known hosts file is required for SSH replication")
	}
	if cfg.KnownHostsFile != "" {
		args = append(args,
			"--sshoption=UserKnownHostsFile="+cfg.KnownHostsFile,
			"--sshoption=StrictHostKeyChecking=yes",
			"--sshoption=IdentitiesOnly=yes",
		)
	}
	if cfg.SSHKeyFile != "" {
		args = append(args, "--sshkey="+cfg.SSHKeyFile)
	}
	if cfg.SSHPort != "" {
		args = append(args, "--sshport="+cfg.SSHPort)
	}
	if cfg.ReceiveUnmounted {
		args = append(args, "--recvoptions=u")
	}
	if !cfg.ReceiveResumable {
		args = append(args, "--no-resume")
	}
	for _, include := range cfg.IncludeSnaps {
		args = append(args, "--include-snaps="+include)
	}
	for _, exclude := range cfg.ExcludeSnaps {
		args = append(args, "--exclude-snaps="+exclude)
	}
	if cfg.ForceDelete {
		args = append(args, "--force-delete")
	}
	args = append(args, cfg.SrcDataset, syncoidTarget(cfg.DstHost, cfg.DstDataset))
	if _, stderr, err := r.Run(ctx, "syncoid", args...); err != nil {
		return fmt.Errorf("syncoid failed: %s", clean(stderr, err))
	}
	return nil
}

func syncoidTarget(host, dataset string) string {
	if host == "" {
		return dataset
	}
	return host + ":" + dataset
}

func lookupDefault(lookup func(string) string, key, def string) string {
	if v := lookup(key); v != "" {
		return v
	}
	return def
}

func boolLookupDefault(lookup func(string) string, key string, def bool) bool {
	v := lookup(key)
	if v == "" {
		return def
	}
	return v == "true"
}

func listLookup(lookup func(string) string, key string) []string {
	var out []string
	for _, line := range strings.Split(lookup(key), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

func clean(stderr string, err error) string {
	stderr = strings.TrimSpace(stderr)
	if stderr != "" {
		return stderr
	}
	return err.Error()
}

func validateNode(expected, actual string) error {
	if expected == "" {
		return nil
	}
	if actual != expected {
		return fmt.Errorf("node verification failed: expected %q, got %q", expected, actual)
	}
	return nil
}
