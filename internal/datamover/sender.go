package datamover

import (
	"context"
	"fmt"
	"os"
	"strings"
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
	return SenderConfig{
		SrcDataset:        os.Getenv("SRC_DATASET"),
		DstHost:           os.Getenv("DST_HOST"),
		DstDataset:        os.Getenv("DST_DATASET"),
		SSHKeyFile:        os.Getenv("SSH_KEY_FILE"),
		KnownHostsFile:    os.Getenv("KNOWN_HOSTS_FILE"),
		SSHPort:           os.Getenv("SSH_PORT"),
		NoSyncSnap:        boolEnvDefault("SYNCOID_NO_SYNC_SNAP", false),
		NoRollback:        boolEnvDefault("SYNCOID_NO_ROLLBACK", true),
		ForceDelete:       boolEnvDefault("SYNCOID_FORCE_DELETE", false),
		Compress:          getenv("SYNCOID_COMPRESS", "none"),
		SyncoidIdentifier: os.Getenv("SYNCOID_IDENTIFIER"),
		ReceiveUnmounted:  getenv("RECEIVE_UNMOUNTED", "true") == "true",
		ReceiveResumable:  getenv("RECEIVE_RESUMABLE", "true") == "true",
		IncludeSnaps:      listEnv("SYNCOID_INCLUDE_SNAPS"),
		ExcludeSnaps:      listEnv("SYNCOID_EXCLUDE_SNAPS"),
		ExpectedNode:      os.Getenv("EXPECTED_NODE_NAME"),
		ActualNode:        os.Getenv("ACTUAL_NODE_NAME"),
	}
}

func RunSender(ctx context.Context, cfg SenderConfig, r CommandRunner) error {
	if err := validateNode(cfg.ExpectedNode, cfg.ActualNode); err != nil {
		return err
	}
	compress, err := syncoidCompression(cfg.Compress)
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
		if !validSyncoidIdentifier(cfg.SyncoidIdentifier) {
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

func validSyncoidIdentifier(identifier string) bool {
	for _, r := range identifier {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' ||
			r == '_' || r == '-' || r == '.' || r == ':' {
			continue
		}
		return false
	}
	return identifier != ""
}

func syncoidCompression(compress string) (string, error) {
	switch compress {
	case "", "none":
		return "none", nil
	case "gzip", "xz", "lz4":
		return compress, nil
	case "pigz":
		return "pigz-fast", nil
	case "zstd":
		return "zstd-fast", nil
	case "zstdmt":
		return "zstdmt-fast", nil
	case "lzop":
		return "lzo", nil
	default:
		return "", fmt.Errorf("unsupported compression %q", compress)
	}
}

func syncoidTarget(host, dataset string) string {
	if host == "" {
		return dataset
	}
	return host + ":" + dataset
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func boolEnvDefault(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v == "true"
}

func listEnv(key string) []string {
	var out []string
	for _, line := range strings.Split(os.Getenv(key), "\n") {
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
