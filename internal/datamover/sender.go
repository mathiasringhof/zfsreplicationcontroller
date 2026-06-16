package datamover

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

const BootstrapDestroyTargetAndReceiveFull = "DestroyTargetAndReceiveFull"

type SenderConfig struct {
	RunID          string
	SnapshotPrefix string
	SnapshotName   string
	SrcDataset     string
	BaseSnapshot   string
	ReceiverURL    string
	TokenFile      string
	BootstrapMode  string
	ExpectedNode   string
	ActualNode     string
}

func SenderConfigFromEnv() SenderConfig {
	return SenderConfig{
		RunID:          os.Getenv("RUN_ID"),
		SnapshotPrefix: getenv("SNAPSHOT_PREFIX", "zsync"),
		SnapshotName:   os.Getenv("SNAPSHOT_NAME"),
		SrcDataset:     os.Getenv("SRC_DATASET"),
		BaseSnapshot:   os.Getenv("BASE_SNAPSHOT"),
		ReceiverURL:    os.Getenv("RECEIVER_URL"),
		TokenFile:      os.Getenv("TOKEN_FILE"),
		BootstrapMode:  os.Getenv("BOOTSTRAP_MODE"),
		ExpectedNode:   os.Getenv("EXPECTED_NODE_NAME"),
		ActualNode:     os.Getenv("ACTUAL_NODE_NAME"),
	}
}

func RunSender(ctx context.Context, cfg SenderConfig, r CommandRunner, client *http.Client) (guid string, err error) {
	if err := validateNode(cfg.ExpectedNode, cfg.ActualNode); err != nil {
		return "", err
	}
	if cfg.SnapshotName == "" {
		cfg.SnapshotName = cfg.SnapshotPrefix + "-" + cfg.RunID
	}
	if cfg.BootstrapMode == "" {
		cfg.BootstrapMode = "FailIfNoBase"
	}
	if client == nil {
		client = http.DefaultClient
	}
	tokenBytes, err := os.ReadFile(cfg.TokenFile)
	if err != nil {
		return "", fmt.Errorf("read token: %w", err)
	}
	token := strings.TrimSpace(string(tokenBytes))
	srcSnap := cfg.SrcDataset + "@" + cfg.SnapshotName
	if !snapshotExists(ctx, r, srcSnap) {
		if _, stderr, err := r.Run(ctx, "zfs", "snapshot", srcSnap); err != nil {
			return "", fmt.Errorf("zfs snapshot failed: %s", clean(stderr, err))
		}
	}

	mode := "full"
	args := []string{"send", srcSnap}
	if cfg.BaseSnapshot != "" && snapshotExists(ctx, r, cfg.SrcDataset+"@"+cfg.BaseSnapshot) {
		mode = "incremental"
		args = []string{"send", "-i", cfg.SrcDataset + "@" + cfg.BaseSnapshot, srcSnap}
	} else if cfg.BootstrapMode != BootstrapDestroyTargetAndReceiveFull {
		return "", fmt.Errorf("no base snapshot and destructive bootstrap disabled")
	}

	body, done, err := r.StartPipe(ctx, "zfs", args...)
	if err != nil {
		return "", fmt.Errorf("zfs send failed: %w", err)
	}
	defer func() {
		if closeErr := body.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close zfs send pipe: %w", closeErr)
		}
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.ReceiverURL, body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-ZFSRep-Run-ID", cfg.RunID)
	req.Header.Set("X-ZFSRep-Snapshot", cfg.SnapshotName)
	req.Header.Set("X-ZFSRep-Mode", mode)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := client.Do(req)
	if err != nil {
		<-done
		return "", fmt.Errorf("HTTP stream failed: %w", err)
	}
	respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if closeErr := resp.Body.Close(); closeErr != nil {
		return "", fmt.Errorf("close HTTP response body: %w", closeErr)
	}
	if readErr != nil {
		return "", fmt.Errorf("read HTTP response body: %w", readErr)
	}
	if err := <-done; err != nil {
		return "", fmt.Errorf("zfs send failed: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("HTTP stream failed: %s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}
	return snapshotGUID(ctx, r, srcSnap), nil
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
