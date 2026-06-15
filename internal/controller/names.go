package controller

import (
	"regexp"
	"strings"
)

var invalidDNSLabel = regexp.MustCompile(`[^a-z0-9-]+`)

func sanitizeName(parts ...string) string {
	name := strings.ToLower(strings.Join(parts, "-"))
	name = invalidDNSLabel.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	for strings.Contains(name, "--") {
		name = strings.ReplaceAll(name, "--", "-")
	}
	if name == "" {
		name = "zfsrep"
	}
	if len(name) > 63 {
		name = strings.Trim(name[:63], "-")
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
