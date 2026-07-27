package replication

import (
	"strings"
	"unicode"
)

func ValidDatasetName(dataset string) bool {
	if dataset == "" ||
		strings.HasPrefix(dataset, "/") ||
		strings.HasSuffix(dataset, "/") ||
		strings.Contains(dataset, "//") ||
		strings.ContainsAny(dataset, "@# \t\r\n;|&`$()<>\\\"'*?[") {
		return false
	}
	for _, part := range strings.Split(dataset, "/") {
		if part == "" || part == "." || part == ".." || strings.ContainsFunc(part, unicode.IsControl) {
			return false
		}
	}
	return true
}

func DatasetOrChild(value, target string) bool {
	return ValidDatasetName(value) &&
		ValidDatasetName(target) &&
		(value == target || strings.HasPrefix(value, target+"/"))
}

func SplitSnapshotTarget(value string) (string, string, bool) {
	dataset, snapshot, ok := strings.Cut(value, "@")
	if !ok || strings.Contains(snapshot, "@") || !ValidDatasetName(dataset) || !ValidSnapshotName(snapshot) {
		return "", "", false
	}
	return dataset, snapshot, true
}

func ValidSnapshotName(snapshot string) bool {
	if snapshot == "" || snapshot == "." || snapshot == ".." {
		return false
	}
	for _, r := range snapshot {
		if validSnapshotRune(r) {
			continue
		}
		return false
	}
	return true
}

func validSnapshotRune(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' ||
		r == '_' || r == '-' || r == '.' || r == ':'
}

func TargetPool(dataset string) string {
	if i := strings.IndexByte(dataset, '/'); i >= 0 {
		return dataset[:i]
	}
	return dataset
}
