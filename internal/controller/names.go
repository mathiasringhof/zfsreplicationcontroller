package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

var invalidDNSLabel = regexp.MustCompile(`[^a-z0-9-]+`)

const dnsLabelMaxLength = 63

func sanitizeName(parts ...string) string {
	raw := strings.ToLower(strings.Join(parts, "-"))
	name := raw
	name = invalidDNSLabel.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	for strings.Contains(name, "--") {
		name = strings.ReplaceAll(name, "--", "-")
	}
	if name == "" {
		name = "zfsrep"
	}
	if len(name) > dnsLabelMaxLength {
		sum := sha256.Sum256([]byte(raw))
		suffix := hex.EncodeToString(sum[:])[:10]
		prefixLength := dnsLabelMaxLength - len(suffix) - 1
		prefix := strings.Trim(name[:prefixLength], "-")
		if prefix == "" {
			prefix = "zfsrep"
		}
		name = prefix + "-" + suffix
	}
	return name
}

func baseName(replicationName string) string {
	return sanitizeName("zfsrep", replicationName)
}

func runName(replicationName, runID string) string {
	return sanitizeName("zfsrep", replicationName, runID)
}

func snapshotPrefix(prefix string) string {
	if prefix == "" {
		return "zsync"
	}
	return prefix
}

func bootstrapMode(mode string) string {
	if mode == "" {
		return "FailIfNoBase"
	}
	return mode
}

func boolDefault(value *bool, def bool) bool {
	if value == nil {
		return def
	}
	return *value
}
