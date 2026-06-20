package datamover

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
)

const BootstrapDestroyTargetAndReceiveFull = "DestroyTargetAndReceiveFull"

type SenderConfig struct {
	RunID            string
	SnapshotPrefix   string
	SnapshotName     string
	SrcDataset       string
	DstHost          string
	DstDataset       string
	SSHKeyFile       string
	SSHPort          string
	BaseSnapshot     string
	BootstrapMode    string
	ReceiveUnmounted bool
	ReceiveResumable bool
	ExpectedNode     string
	ActualNode       string
}

func SenderConfigFromEnv() SenderConfig {
	return SenderConfig{
		RunID:            os.Getenv("RUN_ID"),
		SnapshotPrefix:   getenv("SNAPSHOT_PREFIX", "zsync"),
		SnapshotName:     os.Getenv("SNAPSHOT_NAME"),
		SrcDataset:       os.Getenv("SRC_DATASET"),
		DstHost:          os.Getenv("DST_HOST"),
		DstDataset:       os.Getenv("DST_DATASET"),
		SSHKeyFile:       os.Getenv("SSH_KEY_FILE"),
		SSHPort:          os.Getenv("SSH_PORT"),
		BaseSnapshot:     os.Getenv("BASE_SNAPSHOT"),
		BootstrapMode:    os.Getenv("BOOTSTRAP_MODE"),
		ReceiveUnmounted: getenv("RECEIVE_UNMOUNTED", "true") == "true",
		ReceiveResumable: getenv("RECEIVE_RESUMABLE", "true") == "true",
		ExpectedNode:     os.Getenv("EXPECTED_NODE_NAME"),
		ActualNode:       os.Getenv("ACTUAL_NODE_NAME"),
	}
}

func RunSender(ctx context.Context, cfg SenderConfig, r CommandRunner) (guid string, err error) {
	if err := validateNode(cfg.ExpectedNode, cfg.ActualNode); err != nil {
		return "", err
	}
	if cfg.SnapshotName == "" {
		cfg.SnapshotName = cfg.SnapshotPrefix + "-" + cfg.RunID
	}
	if cfg.BootstrapMode == "" {
		cfg.BootstrapMode = "FailIfNoBase"
	}
	srcSnap := cfg.SrcDataset + "@" + cfg.SnapshotName
	if !snapshotExists(ctx, r, srcSnap) {
		if _, stderr, err := r.Run(ctx, "zfs", "snapshot", srcSnap); err != nil {
			return "", fmt.Errorf("zfs snapshot failed: %s", clean(stderr, err))
		}
	}

	baseExists := cfg.BaseSnapshot != "" && snapshotExists(ctx, r, cfg.SrcDataset+"@"+cfg.BaseSnapshot)
	if !baseExists && cfg.BootstrapMode != BootstrapDestroyTargetAndReceiveFull {
		return "", fmt.Errorf("no base snapshot and destructive bootstrap disabled")
	}

	args := []string{
		"--no-sync-snap",
		"--no-rollback",
		"--compress=none",
		"--sshoption=StrictHostKeyChecking=no",
		"--sshoption=UserKnownHostsFile=/dev/null",
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
	args = append(args, "--include-snaps=^"+regexp.QuoteMeta(cfg.SnapshotName)+"$")
	if baseExists {
		args = append(args, "--include-snaps=^"+regexp.QuoteMeta(cfg.BaseSnapshot)+"$")
	}
	if cfg.BootstrapMode == BootstrapDestroyTargetAndReceiveFull {
		args = append(args, "--force-delete")
	}
	args = append(args, cfg.SrcDataset, syncoidTarget(cfg.DstHost, cfg.DstDataset))
	if _, stderr, err := r.Run(ctx, "syncoid", args...); err != nil {
		return "", fmt.Errorf("syncoid failed: %s", clean(stderr, err))
	}
	return snapshotGUID(ctx, r, srcSnap), nil
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
