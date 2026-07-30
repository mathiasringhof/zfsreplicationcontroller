package syncoid

import (
	"slices"
	"strings"
	"testing"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTranslateDefaultReplicationContract(t *testing.T) {
	contract, err := Translate(testRun())
	if err != nil {
		t.Fatal(err)
	}
	policy := contract.ReceiverPolicy()
	if !policy.ReceiveUnmounted ||
		!policy.ReceiveResumable ||
		policy.AllowRollback ||
		policy.AllowDestroy ||
		policy.AllowMount ||
		!policy.AllowSyncSnapshotDestroy ||
		policy.AllowTargetSnapshotDestroy ||
		policy.Compression != "none" {
		t.Fatalf("default receiver policy = %#v", policy)
	}
	if policy.SyncSnapshotIdentifier == "" {
		t.Fatal("receiver policy identifier is empty")
	}

	want := []string{
		"--no-rollback",
		"--no-privilege-elevation",
		"--compress=none",
		"--identifier=" + policy.SyncSnapshotIdentifier,
		"--sshoption=UserKnownHostsFile=/var/run/zfsrep/ssh/known_hosts",
		"--sshoption=StrictHostKeyChecking=yes",
		"--sshoption=IdentitiesOnly=yes",
		"--sshkey=/var/run/zfsrep/ssh/id_rsa",
		"--sshport=2222",
		"--recvoptions=u",
		"tank/src",
		"zfs-recv@10.0.0.42:tank/dst",
	}
	if got := senderArguments(t, contract); !slices.Equal(got, want) {
		t.Fatalf("arguments = %#v, want %#v", got, want)
	}
}

func TestTranslateReplicationContractOptions(t *testing.T) {
	for _, tt := range []struct {
		name             string
		spec             zfsv1.SyncoidSpec
		wantArgument     string
		unwantedArgument string
		wantPolicy       func(zfsv1.ReceiveTaskPolicy) bool
	}{
		{
			name:         "no sync snapshot",
			spec:         zfsv1.SyncoidSpec{NoSyncSnap: pointer(true)},
			wantArgument: "--no-sync-snap",
			wantPolicy:   func(policy zfsv1.ReceiveTaskPolicy) bool { return !policy.AllowSyncSnapshotDestroy },
		},
		{
			name:             "rollback enabled explicitly",
			spec:             zfsv1.SyncoidSpec{NoRollback: pointer(false)},
			unwantedArgument: "--no-rollback",
			wantPolicy:       func(policy zfsv1.ReceiveTaskPolicy) bool { return policy.AllowRollback },
		},
		{
			name:         "force delete",
			spec:         zfsv1.SyncoidSpec{ForceDelete: pointer(true)},
			wantArgument: "--force-delete",
			wantPolicy:   func(policy zfsv1.ReceiveTaskPolicy) bool { return policy.AllowDestroy },
		},
		{
			name:         "delete target snapshots",
			spec:         zfsv1.SyncoidSpec{DeleteTargetSnapshots: pointer(true)},
			wantArgument: "--delete-target-snapshots",
			wantPolicy: func(policy zfsv1.ReceiveTaskPolicy) bool {
				return policy.AllowTargetSnapshotDestroy
			},
		},
		{
			name:             "mounted receive",
			spec:             zfsv1.SyncoidSpec{ReceiveUnmounted: pointer(false)},
			unwantedArgument: "--recvoptions=u",
			wantPolicy: func(policy zfsv1.ReceiveTaskPolicy) bool {
				return policy.AllowMount && !policy.ReceiveUnmounted
			},
		},
		{
			name:         "non-resumable receive",
			spec:         zfsv1.SyncoidSpec{ReceiveResumable: pointer(false)},
			wantArgument: "--no-resume",
			wantPolicy:   func(policy zfsv1.ReceiveTaskPolicy) bool { return !policy.ReceiveResumable },
		},
		{
			name: "ordered patterns",
			spec: zfsv1.SyncoidSpec{
				IncludeSnaps: []string{"^daily-", "^manual$"},
				ExcludeSnaps: []string{"-tmp$", "-broken$"},
			},
			wantArgument: "--include-snaps=^daily- --include-snaps=^manual$ --exclude-snaps=-tmp$ --exclude-snaps=-broken$",
			wantPolicy:   func(zfsv1.ReceiveTaskPolicy) bool { return true },
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			run := testRun()
			run.Spec.Syncoid = tt.spec
			contract, err := Translate(run)
			if err != nil {
				t.Fatal(err)
			}
			arguments := strings.Join(senderArguments(t, contract), " ")
			if tt.wantArgument != "" && !strings.Contains(arguments, tt.wantArgument) {
				t.Fatalf("arguments = %q, want sequence %q", arguments, tt.wantArgument)
			}
			if tt.unwantedArgument != "" && strings.Contains(arguments, tt.unwantedArgument) {
				t.Fatalf("arguments = %q, do not want %q", arguments, tt.unwantedArgument)
			}
			if !tt.wantPolicy(contract.ReceiverPolicy()) {
				t.Fatalf("receiver policy = %#v", contract.ReceiverPolicy())
			}
		})
	}
}

func TestTranslateCompressionMappings(t *testing.T) {
	for _, tt := range []struct {
		compression        string
		wantSyncoid        string
		decompressor       string
		decompressorArgs   []string
		wantDecompressorOK bool
	}{
		{compression: "", wantSyncoid: "none"},
		{compression: "none", wantSyncoid: "none"},
		{compression: "gzip", wantSyncoid: "gzip", decompressor: "zcat", wantDecompressorOK: true},
		{compression: "pigz", wantSyncoid: "pigz-fast", decompressor: "pigz", decompressorArgs: []string{"-dc"}, wantDecompressorOK: true},
		{compression: "zstd", wantSyncoid: "zstd-fast", decompressor: "zstd", decompressorArgs: []string{"-dc"}, wantDecompressorOK: true},
		{compression: "zstdmt", wantSyncoid: "zstdmt-fast", decompressor: "zstdmt", decompressorArgs: []string{"-dc"}, wantDecompressorOK: true},
		{compression: "xz", wantSyncoid: "xz", decompressor: "xz", decompressorArgs: []string{"-d"}, wantDecompressorOK: true},
		{compression: "lzop", wantSyncoid: "lzo", decompressor: "lzop", decompressorArgs: []string{"-dfc"}, wantDecompressorOK: true},
		{compression: "lz4", wantSyncoid: "lz4", decompressor: "lz4", decompressorArgs: []string{"-dc"}, wantDecompressorOK: true},
	} {
		t.Run(tt.compression, func(t *testing.T) {
			run := testRun()
			run.Spec.Syncoid.Compress = tt.compression
			contract, err := Translate(run)
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Join(senderArguments(t, contract), " "); !strings.Contains(got, "--compress="+tt.wantSyncoid) {
				t.Fatalf("arguments = %q, want compression %q", got, tt.wantSyncoid)
			}
			wantPolicyCompression := tt.compression
			if wantPolicyCompression == "" {
				wantPolicyCompression = "none"
			}
			if got := contract.ReceiverPolicy().Compression; got != wantPolicyCompression {
				t.Fatalf("policy compression = %q, want %q", got, wantPolicyCompression)
			}
			if got := DecompressorAllowed(tt.decompressor, tt.decompressorArgs, wantPolicyCompression); got != tt.wantDecompressorOK {
				t.Fatalf("DecompressorAllowed(%q, %#v, %q) = %t, want %t", tt.decompressor, tt.decompressorArgs, wantPolicyCompression, got, tt.wantDecompressorOK)
			}
		})
	}
	if DecompressorAllowed("gzip", []string{"-dc"}, "zstd") {
		t.Fatal("gzip decompressor accepted for zstd policy")
	}
}

func TestTranslatePreservesSnapshotPatternNormalization(t *testing.T) {
	run := testRun()
	run.Spec.Syncoid = zfsv1.SyncoidSpec{
		IncludeSnaps: []string{"  ^daily$  ", "", "^manual$\n  ^monthly$"},
		ExcludeSnaps: []string{"  -tmp$  "},
	}
	contract, err := Translate(run)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(senderArguments(t, contract), " ")
	want := "--include-snaps=^daily$ --include-snaps=^manual$ --include-snaps=^monthly$ --exclude-snaps=-tmp$"
	if !strings.Contains(got, want) {
		t.Fatalf("arguments = %q, want preserved normalized patterns %q", got, want)
	}
}

func TestTranslateRejectsInvalidRunConfiguration(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*zfsv1.ZFSReplicationRun)
		want   string
	}{
		{name: "nil run", mutate: nil, want: "replication run is required"},
		{name: "source dataset", mutate: func(run *zfsv1.ZFSReplicationRun) { run.Spec.Source.Dataset = "tank/src;other" }, want: "invalid source dataset"},
		{name: "target dataset", mutate: func(run *zfsv1.ZFSReplicationRun) { run.Spec.Target.Dataset = "tank/dst;other" }, want: "invalid target dataset"},
		{name: "compression", mutate: func(run *zfsv1.ZFSReplicationRun) { run.Spec.Syncoid.Compress = "shell" }, want: "unsupported compression"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var run *zfsv1.ZFSReplicationRun
			if tt.mutate != nil {
				run = testRun()
				tt.mutate(run)
			}
			if _, err := Translate(run); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Translate() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestSenderArgumentsRejectsIncompleteConnection(t *testing.T) {
	contract, err := Translate(testRun())
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name       string
		connection Connection
		want       string
	}{
		{name: "target host", connection: Connection{SSHKeyFile: "key", KnownHostsFile: "hosts", SSHPort: 22}, want: "target host"},
		{name: "SSH key file", connection: Connection{TargetHost: "host", KnownHostsFile: "hosts", SSHPort: 22}, want: "SSH key file"},
		{name: "known hosts file", connection: Connection{TargetHost: "host", SSHKeyFile: "key", SSHPort: 22}, want: "known hosts file"},
		{name: "SSH port", connection: Connection{TargetHost: "host", SSHKeyFile: "key", KnownHostsFile: "hosts"}, want: "SSH port"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := contract.SenderArguments(tt.connection); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("SenderArguments() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestRelationshipIdentifierIsStableAndIsolated(t *testing.T) {
	base := testRun()
	first, err := Translate(base)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Translate(base.DeepCopy())
	if err != nil {
		t.Fatal(err)
	}
	firstID := first.ReceiverPolicy().SyncSnapshotIdentifier
	if firstID != "zrc-ec76c43a582d81b1db463599" {
		t.Fatalf("identifier = %q, want preserved relationship identifier format", firstID)
	}
	if firstID != second.ReceiverPolicy().SyncSnapshotIdentifier {
		t.Fatalf("stable identifier = %q then %q", firstID, second.ReceiverPolicy().SyncSnapshotIdentifier)
	}
	if strings.ContainsAny(firstID, " \t\r\n;|&`$()<>\\") {
		t.Fatalf("identifier = %q, want shell-safe", firstID)
	}

	for _, mutate := range []func(*zfsv1.ZFSReplicationRun){
		func(run *zfsv1.ZFSReplicationRun) { run.Namespace = "other" },
		func(run *zfsv1.ZFSReplicationRun) { run.Spec.Source.NodeName = "worker-c" },
		func(run *zfsv1.ZFSReplicationRun) { run.Spec.Source.Dataset = "tank/other-src" },
		func(run *zfsv1.ZFSReplicationRun) { run.Spec.Target.NodeName = "worker-c" },
		func(run *zfsv1.ZFSReplicationRun) { run.Spec.Target.Dataset = "tank/other-dst" },
	} {
		changed := base.DeepCopy()
		mutate(changed)
		translated, err := Translate(changed)
		if err != nil {
			t.Fatal(err)
		}
		if translated.ReceiverPolicy().SyncSnapshotIdentifier == firstID {
			t.Fatalf("changed relationship reused identifier %q for %#v", firstID, changed.Spec)
		}
	}
}

func senderArguments(t *testing.T, contract Contract) []string {
	t.Helper()
	arguments, err := contract.SenderArguments(Connection{
		TargetHost:     "zfs-recv@10.0.0.42",
		SSHKeyFile:     "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile: "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:        2222,
	})
	if err != nil {
		t.Fatal(err)
	}
	return arguments
}

func testRun() *zfsv1.ZFSReplicationRun {
	return &zfsv1.ZFSReplicationRun{
		ObjectMeta: metav1.ObjectMeta{Namespace: "storage"},
		Spec: zfsv1.ZFSReplicationRunSpec{
			Source: zfsv1.DatasetRef{NodeName: "worker-a", Dataset: "tank/src"},
			Target: zfsv1.DatasetRef{NodeName: "worker-b", Dataset: "tank/dst"},
		},
	}
}

func pointer[T any](value T) *T {
	return &value
}
