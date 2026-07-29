package runchildren

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

const (
	maxNameLength         = 63
	readablePrefixMaxSize = 30
)

var invalidDNSLabel = regexp.MustCompile(`[^a-z0-9-]+`)

type Names struct {
	SSHSecret   string
	ReceiveTask string
	SenderJob   string
}

func ForRun(runName string) Names {
	return Names{
		SSHSecret:   childName(runName, "ssh"),
		ReceiveTask: childName(runName, "receiver"),
		SenderJob:   childName(runName, "sender"),
	}
}

func childName(runName, role string) string {
	readable := "zfsrep-" + runName + "-" + role
	normalizedRunName := normalizeDNSLabel(runName)
	if normalizedRunName == runName && len(readable) <= maxNameLength {
		return readable
	}

	if len(normalizedRunName) > readablePrefixMaxSize {
		normalizedRunName = normalizedRunName[:readablePrefixMaxSize]
	}
	normalizedRunName = strings.Trim(normalizedRunName, "-")
	if normalizedRunName == "" {
		normalizedRunName = "run"
	}
	sum := sha256.Sum256([]byte(runName))
	hash := hex.EncodeToString(sum[:])[:16]
	return "zfsrep-" + normalizedRunName + "-" + hash + "-" + role
}

func normalizeDNSLabel(name string) string {
	name = strings.ToLower(name)
	name = invalidDNSLabel.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	for strings.Contains(name, "--") {
		name = strings.ReplaceAll(name, "--", "-")
	}
	return name
}
