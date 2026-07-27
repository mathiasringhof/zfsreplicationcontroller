package syncoid

import "slices"

const CompressionNone = "none"

type compressionSpec struct {
	sender       string
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
		sender: CompressionNone,
	},
	CompressionNone: {
		sender: CompressionNone,
	},
	"gzip": {
		sender:       "gzip",
		decompressor: "gzip",
		allowedArgs:  [][]string{{"-dc"}},
		aliases:      []decompressorAlias{{name: "zcat"}},
	},
	"pigz": {
		sender:       "pigz-fast",
		decompressor: "pigz",
		allowedArgs:  [][]string{{"-dc"}},
	},
	"zstd": {
		sender:       "zstd-fast",
		decompressor: "zstd",
		allowedArgs:  [][]string{{"-dc"}},
	},
	"zstdmt": {
		sender:       "zstdmt-fast",
		decompressor: "zstdmt",
		allowedArgs:  [][]string{{"-dc"}},
	},
	"xz": {
		sender:       "xz",
		decompressor: "xz",
		allowedArgs:  [][]string{{"-d"}, {"-dc"}, {"-d", "-c"}},
	},
	"lzop": {
		sender:       "lzo",
		decompressor: "lzop",
		allowedArgs:  [][]string{{"-dfc"}, {"-dc"}},
	},
	"lz4": {
		sender:       "lz4",
		decompressor: "lz4",
		allowedArgs:  [][]string{{"-dc"}},
	},
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

func senderCompression(compression string) (string, bool) {
	spec, ok := compressionSpecs[compression]
	return spec.sender, ok
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
