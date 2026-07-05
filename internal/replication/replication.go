package replication

import (
	"fmt"
	"slices"
	"strings"
	"unicode"
)

const CompressionNone = "none"

type SyncoidOptions struct {
	NoSyncSnap       bool
	NoRollback       bool
	ForceDelete      bool
	Compress         string
	ReceiveUnmounted bool
	ReceiveResumable bool
	IncludeSnaps     []string
	ExcludeSnaps     []string
}

type SyncoidOptionInput struct {
	NoSyncSnap       *bool
	NoRollback       *bool
	ForceDelete      *bool
	Compress         string
	ReceiveUnmounted *bool
	ReceiveResumable *bool
	IncludeSnaps     []string
	ExcludeSnaps     []string
}

func DefaultSyncoidOptions() SyncoidOptions {
	return SyncoidOptions{
		NoRollback:       true,
		Compress:         CompressionNone,
		ReceiveUnmounted: true,
		ReceiveResumable: true,
	}
}

func NormalizeSyncoidOptions(in SyncoidOptionInput) SyncoidOptions {
	out := DefaultSyncoidOptions()
	out.NoSyncSnap = boolDefault(in.NoSyncSnap, out.NoSyncSnap)
	out.NoRollback = boolDefault(in.NoRollback, out.NoRollback)
	out.ForceDelete = boolDefault(in.ForceDelete, out.ForceDelete)
	out.Compress = CompressionDefault(in.Compress)
	out.ReceiveUnmounted = boolDefault(in.ReceiveUnmounted, out.ReceiveUnmounted)
	out.ReceiveResumable = boolDefault(in.ReceiveResumable, out.ReceiveResumable)
	out.IncludeSnaps = slices.Clone(in.IncludeSnaps)
	out.ExcludeSnaps = slices.Clone(in.ExcludeSnaps)
	return out
}

type compressionSpec struct {
	syncoid      string
	decompressor string
	allowedArgs  [][]string
	aliases      []decompressorAlias
}

type decompressorAlias struct {
	name string
	args []string
}

var compressionSpecs = map[string]compressionSpec{
	"": {
		syncoid: CompressionNone,
	},
	CompressionNone: {
		syncoid: CompressionNone,
	},
	"gzip": {
		syncoid:      "gzip",
		decompressor: "gzip",
		allowedArgs:  [][]string{{"-dc"}},
		aliases: []decompressorAlias{
			{name: "zcat"},
		},
	},
	"pigz": {
		syncoid:      "pigz-fast",
		decompressor: "pigz",
		allowedArgs:  [][]string{{"-dc"}},
	},
	"zstd": {
		syncoid:      "zstd-fast",
		decompressor: "zstd",
		allowedArgs:  [][]string{{"-dc"}},
	},
	"zstdmt": {
		syncoid:      "zstdmt-fast",
		decompressor: "zstdmt",
		allowedArgs:  [][]string{{"-dc"}},
	},
	"xz": {
		syncoid:      "xz",
		decompressor: "xz",
		allowedArgs:  [][]string{{"-d"}, {"-dc"}, {"-d", "-c"}},
	},
	"lzop": {
		syncoid:      "lzo",
		decompressor: "lzop",
		allowedArgs:  [][]string{{"-dfc"}, {"-dc"}},
	},
	"lz4": {
		syncoid:      "lz4",
		decompressor: "lz4",
		allowedArgs:  [][]string{{"-dc"}},
	},
}

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
		if validSyncoidIdentifierRune(r) {
			continue
		}
		return false
	}
	return true
}

func SyncoidSnapshotTarget(snapshot, identifier string) bool {
	if identifier == "" || !ValidSyncoidIdentifier(identifier) || !ValidSnapshotName(snapshot) {
		return false
	}
	return strings.HasPrefix(snapshot, "syncoid_"+identifier+"_")
}

func ValidSyncoidIdentifier(identifier string) bool {
	if identifier == "" {
		return false
	}
	for _, r := range identifier {
		if validSyncoidIdentifierRune(r) {
			continue
		}
		return false
	}
	return true
}

func validSyncoidIdentifierRune(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' ||
		r == '_' || r == '-' || r == '.' || r == ':'
}

func boolDefault(value *bool, def bool) bool {
	if value == nil {
		return def
	}
	return *value
}

func TargetPool(dataset string) string {
	if i := strings.IndexByte(dataset, '/'); i >= 0 {
		return dataset[:i]
	}
	return dataset
}

func CompressionDefault(compression string) string {
	if compression == "" {
		return CompressionNone
	}
	return compression
}

func CompressionSupported(compression string) bool {
	_, ok := compressionSpecs[compression]
	return ok
}

func SyncoidCompression(compression string) (string, error) {
	spec, ok := compressionSpecs[compression]
	if !ok {
		return "", fmt.Errorf("unsupported compression %q", compression)
	}
	return spec.syncoid, nil
}

func CompressorAllowed(name string) bool {
	for compression, spec := range compressionSpecs {
		if compression == "" || compression == CompressionNone {
			continue
		}
		if spec.decompressor == name {
			return true
		}
	}
	return false
}

func DecompressorAllowed(name string, args []string, compression string) bool {
	spec, ok := compressionSpecs[compression]
	if !ok || compression == "" || compression == CompressionNone {
		return false
	}
	for _, alias := range spec.aliases {
		if name == alias.name && slices.Equal(args, alias.args) {
			return true
		}
	}
	if name != spec.decompressor {
		return false
	}
	for _, allowed := range spec.allowedArgs {
		if slices.Equal(args, allowed) {
			return true
		}
	}
	return false
}
