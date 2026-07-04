package replication

import "testing"

func TestValidDatasetName(t *testing.T) {
	for _, dataset := range []string{
		"tank/app",
		"tank/app/child",
		"tank-1/app_2",
		"tank/app.with:colon",
	} {
		t.Run("valid/"+dataset, func(t *testing.T) {
			if !ValidDatasetName(dataset) {
				t.Fatalf("ValidDatasetName(%q) = false, want true", dataset)
			}
		})
	}

	for _, dataset := range []string{
		"",
		"/tank/app",
		"tank/app/",
		"tank//app",
		"tank/.",
		"tank/..",
		"tank/app@snap",
		"tank/a#b",
		"tank/a*b",
		"tank/a\"b",
		"tank/a[b",
		"tank/a?b",
		"tank/a b",
		"tank/a\x01b",
	} {
		t.Run("invalid/"+dataset, func(t *testing.T) {
			if ValidDatasetName(dataset) {
				t.Fatalf("ValidDatasetName(%q) = true, want false", dataset)
			}
		})
	}
}

func TestSyncoidIdentifierAndSnapshotHelpers(t *testing.T) {
	for _, identifier := range []string{"zrc-123", "rel_123", "rel.123", "rel:123"} {
		if !ValidSyncoidIdentifier(identifier) {
			t.Fatalf("ValidSyncoidIdentifier(%q) = false, want true", identifier)
		}
	}
	for _, identifier := range []string{"", "bad/id", "bad id", "bad;id"} {
		if ValidSyncoidIdentifier(identifier) {
			t.Fatalf("ValidSyncoidIdentifier(%q) = true, want false", identifier)
		}
	}

	dataset, snapshot, ok := SplitSnapshotTarget("tank/app@syncoid_rel123_worker_2026")
	if !ok || dataset != "tank/app" || snapshot != "syncoid_rel123_worker_2026" {
		t.Fatalf("SplitSnapshotTarget() = %q, %q, %v", dataset, snapshot, ok)
	}
	if !SyncoidSnapshotTarget(snapshot, "rel123") {
		t.Fatalf("SyncoidSnapshotTarget(%q, rel123) = false, want true", snapshot)
	}
	for _, value := range []string{"tank/app", "tank/app@snap@again", "tank/app@bad,snap"} {
		if _, _, ok := SplitSnapshotTarget(value); ok {
			t.Fatalf("SplitSnapshotTarget(%q) ok = true, want false", value)
		}
	}
	if !DatasetOrChild("tank/app/child", "tank/app") {
		t.Fatalf("DatasetOrChild(tank/app/child, tank/app) = false, want true")
	}
	if DatasetOrChild("tank/app2", "tank/app") {
		t.Fatalf("DatasetOrChild(tank/app2, tank/app) = true, want false")
	}
	if got := TargetPool("tank/app/child"); got != "tank" {
		t.Fatalf("TargetPool() = %q, want tank", got)
	}
}

func TestCompressionMetadata(t *testing.T) {
	for _, tt := range []struct {
		compression string
		syncoid     string
		command     string
		args        []string
	}{
		{compression: "", syncoid: "none"},
		{compression: "none", syncoid: "none"},
		{compression: "gzip", syncoid: "gzip", command: "zcat"},
		{compression: "pigz", syncoid: "pigz-fast", command: "pigz", args: []string{"-dc"}},
		{compression: "zstd", syncoid: "zstd-fast", command: "zstd", args: []string{"-dc"}},
		{compression: "zstdmt", syncoid: "zstdmt-fast", command: "zstdmt", args: []string{"-dc"}},
		{compression: "xz", syncoid: "xz", command: "xz", args: []string{"-d"}},
		{compression: "lzop", syncoid: "lzo", command: "lzop", args: []string{"-dfc"}},
		{compression: "lz4", syncoid: "lz4", command: "lz4", args: []string{"-dc"}},
	} {
		t.Run(tt.compression, func(t *testing.T) {
			if !CompressionSupported(tt.compression) {
				t.Fatalf("CompressionSupported(%q) = false, want true", tt.compression)
			}
			got, err := SyncoidCompression(tt.compression)
			if err != nil {
				t.Fatalf("SyncoidCompression(%q) error = %v", tt.compression, err)
			}
			if got != tt.syncoid {
				t.Fatalf("SyncoidCompression(%q) = %q, want %q", tt.compression, got, tt.syncoid)
			}
			if tt.command != "" && !DecompressorAllowed(tt.command, tt.args, tt.compression) {
				t.Fatalf("DecompressorAllowed(%q, %#v, %q) = false, want true", tt.command, tt.args, tt.compression)
			}
		})
	}

	if CompressionSupported("sh") {
		t.Fatalf("CompressionSupported(sh) = true, want false")
	}
	if _, err := SyncoidCompression("sh"); err == nil {
		t.Fatalf("SyncoidCompression(sh) error = nil, want error")
	}
	if DecompressorAllowed("gzip", []string{"-dc"}, "zstd") {
		t.Fatalf("DecompressorAllowed(gzip, -dc, zstd) = true, want false")
	}
}
