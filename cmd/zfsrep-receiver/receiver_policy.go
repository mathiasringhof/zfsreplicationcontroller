package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	"github.com/mathias/zfsreplicationcontroller/internal/replication"
)

func readReceiverPolicy(path string) (receiverCommandPolicy, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return receiverCommandPolicy{}, fmt.Errorf("stat receiver policy: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return receiverCommandPolicy{}, fmt.Errorf("receiver policy must not be a symlink")
	}
	if !info.Mode().IsRegular() {
		return receiverCommandPolicy{}, fmt.Errorf("receiver policy must be a regular file")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return receiverCommandPolicy{}, fmt.Errorf("receiver policy must not be group or world writable")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return receiverCommandPolicy{}, fmt.Errorf("read receiver policy: %w", err)
	}
	var policy receiverCommandPolicy
	if err := json.Unmarshal(data, &policy); err != nil {
		return receiverCommandPolicy{}, fmt.Errorf("parse receiver policy: %w", err)
	}
	policy.Compression = replication.CompressionDefault(policy.Compression)
	return policy, nil
}

func receiveTaskAuthorization(cfg receiverConfig, task *zfsv1.ZFSReceiveTask) receiverTaskAuthorization {
	policyID := receiverPolicyID(task)
	policyPath, err := receiverPolicyPathForID(receiverPolicyDir(cfg), policyID)
	if err != nil {
		panic(err)
	}
	policy := receiverCommandPolicy{
		TargetDataset:     task.Spec.Destination.Dataset,
		ReceiveTaskPolicy: task.Spec.Policy,
	}
	policy.Compression = replication.CompressionDefault(policy.Compression)
	forcedCommand := receiverCommandPath + " exec --policy-id " + policyID
	key := "restrict,command=\"" + escapeAuthorizedKeysOption(forcedCommand) + "\" " + strings.TrimSpace(task.Spec.SSH.AuthorizedPublicKey)
	return receiverTaskAuthorization{
		AuthorizedKey: key,
		PolicyID:      policyID,
		PolicyPath:    policyPath,
		Policy:        policy,
	}
}

func receiverPolicyDir(cfg receiverConfig) string {
	return filepath.Join(filepath.Dir(cfg.AuthorizedKeysFile), "policies")
}

func receiverPolicyID(task *zfsv1.ZFSReceiveTask) string {
	sum := sha256.Sum256([]byte(task.Namespace + "\x00" + task.Name))
	return "policy-" + hex.EncodeToString(sum[:])[:32]
}

func receiverPolicyPathForID(dir, id string) (string, error) {
	if !validReceiverPolicyID(id) {
		return "", fmt.Errorf("invalid receiver policy ID %q", id)
	}
	return filepath.Join(filepath.Clean(dir), id+".json"), nil
}

func validReceiverPolicyID(id string) bool {
	if len(id) != len("policy-")+32 || !strings.HasPrefix(id, "policy-") {
		return false
	}
	for _, r := range id[len("policy-"):] {
		if !isLowerHex(r) {
			return false
		}
	}
	return true
}

func isLowerHex(r rune) bool {
	return r >= '0' && r <= '9' || r >= 'a' && r <= 'f'
}

func escapeAuthorizedKeysOption(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `"`, `\"`)
}

func writeReceiverPolicies(dir string, policies map[string]receiverCommandPolicy) error {
	if err := ensureReceiverPolicyDir(dir); err != nil {
		return err
	}
	active := map[string]struct{}{}
	ids := make([]string, 0, len(policies))
	for id := range policies {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		path, err := receiverPolicyPathForID(dir, id)
		if err != nil {
			return err
		}
		data, err := json.Marshal(policies[id])
		if err != nil {
			return fmt.Errorf("marshal receiver policy: %w", err)
		}
		if err := writeReceiverPolicyFile(path, append(data, '\n')); err != nil {
			return fmt.Errorf("write receiver policy: %w", err)
		}
		active[filepath.Base(path)] = struct{}{}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("list receiver policy directory: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		if _, ok := active[entry.Name()]; ok {
			continue
		}
		stalePath := filepath.Join(dir, entry.Name())
		if err := removeReceiverPolicyFile(stalePath); err != nil {
			return fmt.Errorf("remove stale receiver policy: %w", err)
		}
	}
	return nil
}

func ensureReceiverPolicyDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create receiver policy directory: %w", err)
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("stat receiver policy directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("receiver policy directory must not be a symlink")
	}
	if !info.IsDir() {
		return fmt.Errorf("receiver policy path is not a directory")
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("set receiver policy directory mode: %w", err)
	}
	return nil
}

func writeReceiverPolicyFile(path string, data []byte) error {
	if info, err := os.Lstat(path); err == nil && info.IsDir() {
		return fmt.Errorf("receiver policy path is a directory")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmp := path + ".tmp"
	if info, err := os.Lstat(tmp); err == nil {
		if info.IsDir() {
			return fmt.Errorf("receiver policy temporary path is a directory")
		}
		if err := os.Remove(tmp); err != nil {
			return err
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil {
		return errors.Join(writeErr, os.Remove(tmp))
	}
	if closeErr != nil {
		return errors.Join(closeErr, os.Remove(tmp))
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		return errors.Join(err, os.Remove(tmp))
	}
	if err := os.Rename(tmp, path); err != nil {
		return errors.Join(err, os.Remove(tmp))
	}
	return os.Chmod(path, 0o600)
}

func removeReceiverPolicyFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if info.IsDir() {
		return nil
	}
	return os.Remove(path)
}
