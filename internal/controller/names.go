package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

var invalidDNSLabel = regexp.MustCompile(`[^a-z0-9-]+`)

const dnsLabelMaxLength = 63

const labelPrefix = "zfsreplication.ringhof.io"

type runObjects struct {
	SecretName      string
	ReceiveTaskName string
	SenderName      string
}

func sanitizeName(parts ...string) string {
	raw := strings.ToLower(strings.Join(parts, "-"))
	name := normalizeDNSLabel(raw)
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

func normalizeDNSLabel(name string) string {
	name = strings.ToLower(name)
	name = invalidDNSLabel.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	for strings.Contains(name, "--") {
		name = strings.ReplaceAll(name, "--", "-")
	}
	if name == "" {
		name = "zfsrep"
	}
	return name
}

func boolDefault(value *bool, def bool) bool {
	if value == nil {
		return def
	}
	return *value
}
