package syncoid

import (
	"slices"
	"strings"
	"testing"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTranslateDefaultReplicationContract(t *testing.T) {
	run := testRun()

	contract, err := Translate(run)
	if err != nil {
		t.Fatal(err)
	}
	if contract.ReceiverPolicy.ReceiveUnmounted != true ||
		contract.ReceiverPolicy.ReceiveResumable != true ||
		contract.ReceiverPolicy.AllowRollback ||
		contract.ReceiverPolicy.AllowDestroy ||
		contract.ReceiverPolicy.AllowMount ||
		!contract.ReceiverPolicy.AllowSyncSnapshotDestroy ||
		contract.ReceiverPolicy.AllowTargetSnapshotDestroy ||
		contract.ReceiverPolicy.Compression != "none" {
		t.Fatalf("default receiver policy = %#v", contract.ReceiverPolicy)
	}

	values := environmentValues(contract.SenderEnvironment)
	for key, want := range map[string]string{
		"SRC_DATASET":                     "tank/src",
		"DST_DATASET":                     "tank/dst",
		"SYNCOID_NO_SYNC_SNAP":            "false",
		"SYNCOID_NO_ROLLBACK":             "true",
		"SYNCOID_FORCE_DELETE":            "false",
		"SYNCOID_DELETE_TARGET_SNAPSHOTS": "false",
		"SYNCOID_COMPRESS":                "none",
		"RECEIVE_UNMOUNTED":               "true",
		"RECEIVE_RESUMABLE":               "true",
		"SYNCOID_INCLUDE_SNAPS":           "",
		"SYNCOID_EXCLUDE_SNAPS":           "",
	} {
		if got := values[key]; got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	identifier := values["SYNCOID_IDENTIFIER"]
	if identifier == "" {
		t.Fatal("SYNCOID_IDENTIFIER is empty")
	}
	if contract.ReceiverPolicy.SyncSnapshotIdentifier != identifier {
		t.Fatalf("receiver identifier = %q, sender identifier = %q", contract.ReceiverPolicy.SyncSnapshotIdentifier, identifier)
	}

	for _, entry := range ConnectionEnvironment(Connection{
		TargetHost:     "zfs-recv@10.0.0.42",
		SSHKeyFile:     "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile: "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:        "2222",
	}) {
		values[entry.Name] = entry.Value
	}
	invocation, err := DecodeSenderEnvironment(mapLookup(values))
	if err != nil {
		t.Fatal(err)
	}
	wantArgs := []string{
		"--no-rollback",
		"--no-privilege-elevation",
		"--compress=none",
		"--identifier=" + identifier,
		"--sshoption=UserKnownHostsFile=/var/run/zfsrep/ssh/known_hosts",
		"--sshoption=StrictHostKeyChecking=yes",
		"--sshoption=IdentitiesOnly=yes",
		"--sshkey=/var/run/zfsrep/ssh/id_rsa",
		"--sshport=2222",
		"--recvoptions=u",
		"tank/src",
		"zfs-recv@10.0.0.42:tank/dst",
	}
	if got := invocation.Arguments(); !slices.Equal(got, wantArgs) {
		t.Fatalf("arguments = %#v, want %#v", got, wantArgs)
	}
}

func TestTranslateReplicationContractOptions(t *testing.T) {
	for _, tt := range []struct {
		name              string
		spec              zfsv1.SyncoidSpec
		wantArgument      string
		unwantedArgument  string
		wantPolicy        func(zfsv1.ReceiveTaskPolicy) bool
		wantCompression   string
		wantSyncoidOption string
	}{
		{
			name:         "no sync snapshot",
			spec:         zfsv1.SyncoidSpec{NoSyncSnap: pointer(true)},
			wantArgument: "--no-sync-snap",
			wantPolicy: func(policy zfsv1.ReceiveTaskPolicy) bool {
				return !policy.AllowSyncSnapshotDestroy
			},
		},
		{
			name:             "rollback enabled explicitly",
			spec:             zfsv1.SyncoidSpec{NoRollback: pointer(false)},
			unwantedArgument: "--no-rollback",
			wantPolicy: func(policy zfsv1.ReceiveTaskPolicy) bool {
				return policy.AllowRollback
			},
		},
		{
			name:         "force delete",
			spec:         zfsv1.SyncoidSpec{ForceDelete: pointer(true)},
			wantArgument: "--force-delete",
			wantPolicy: func(policy zfsv1.ReceiveTaskPolicy) bool {
				return policy.AllowDestroy
			},
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
			wantPolicy: func(policy zfsv1.ReceiveTaskPolicy) bool {
				return !policy.ReceiveResumable
			},
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
			values := environmentValues(contract.SenderEnvironment)
			addConnection(values)
			invocation, err := DecodeSenderEnvironment(mapLookup(values))
			if err != nil {
				t.Fatal(err)
			}
			gotArguments := strings.Join(invocation.Arguments(), " ")
			if tt.wantArgument != "" && !strings.Contains(gotArguments, tt.wantArgument) {
				t.Fatalf("arguments = %q, want sequence %q", gotArguments, tt.wantArgument)
			}
			if tt.unwantedArgument != "" && strings.Contains(gotArguments, tt.unwantedArgument) {
				t.Fatalf("arguments = %q, do not want %q", gotArguments, tt.unwantedArgument)
			}
			if !tt.wantPolicy(contract.ReceiverPolicy) {
				t.Fatalf("receiver policy = %#v", contract.ReceiverPolicy)
			}
		})
	}
}

func TestTranslateExplicitDefaultBooleanValues(t *testing.T) {
	run := testRun()
	run.Spec.Syncoid = zfsv1.SyncoidSpec{
		NoSyncSnap:            pointer(false),
		NoRollback:            pointer(true),
		ForceDelete:           pointer(false),
		DeleteTargetSnapshots: pointer(false),
		ReceiveUnmounted:      pointer(true),
		ReceiveResumable:      pointer(true),
	}

	contract, err := Translate(run)
	if err != nil {
		t.Fatal(err)
	}
	values := environmentValues(contract.SenderEnvironment)
	for name, want := range map[string]string{
		"SYNCOID_NO_SYNC_SNAP":            "false",
		"SYNCOID_NO_ROLLBACK":             "true",
		"SYNCOID_FORCE_DELETE":            "false",
		"SYNCOID_DELETE_TARGET_SNAPSHOTS": "false",
		"RECEIVE_UNMOUNTED":               "true",
		"RECEIVE_RESUMABLE":               "true",
	} {
		if got := values[name]; got != want {
			t.Fatalf("%s = %q, want explicit value %q", name, got, want)
		}
	}
	addConnection(values)
	invocation, err := DecodeSenderEnvironment(mapLookup(values))
	if err != nil {
		t.Fatal(err)
	}
	arguments := strings.Join(invocation.Arguments(), " ")
	for _, want := range []string{"--no-rollback", "--recvoptions=u"} {
		if !strings.Contains(arguments, want) {
			t.Fatalf("arguments = %q, want %q for explicit value", arguments, want)
		}
	}
	for _, unwanted := range []string{"--no-sync-snap", "--force-delete", "--delete-target-snapshots", "--no-resume"} {
		if strings.Contains(arguments, unwanted) {
			t.Fatalf("arguments = %q, do not want %q for explicit value", arguments, unwanted)
		}
	}
	if !contract.ReceiverPolicy.ReceiveUnmounted ||
		!contract.ReceiverPolicy.ReceiveResumable ||
		contract.ReceiverPolicy.AllowRollback ||
		contract.ReceiverPolicy.AllowDestroy ||
		contract.ReceiverPolicy.AllowMount ||
		!contract.ReceiverPolicy.AllowSyncSnapshotDestroy ||
		contract.ReceiverPolicy.AllowTargetSnapshotDestroy {
		t.Fatalf("receiver policy = %#v, want explicit default permissions", contract.ReceiverPolicy)
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
			values := environmentValues(contract.SenderEnvironment)
			addConnection(values)
			invocation, err := DecodeSenderEnvironment(mapLookup(values))
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Join(invocation.Arguments(), " "); !strings.Contains(got, "--compress="+tt.wantSyncoid) {
				t.Fatalf("arguments = %q, want compression %q", got, tt.wantSyncoid)
			}
			wantPolicyCompression := tt.compression
			if wantPolicyCompression == "" {
				wantPolicyCompression = "none"
			}
			if contract.ReceiverPolicy.Compression != wantPolicyCompression {
				t.Fatalf("policy compression = %q, want %q", contract.ReceiverPolicy.Compression, wantPolicyCompression)
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

func TestTranslateRejectsUnsupportedCompression(t *testing.T) {
	run := testRun()
	run.Spec.Syncoid.Compress = "shell"

	if _, err := Translate(run); err == nil || !strings.Contains(err.Error(), "unsupported compression") {
		t.Fatalf("Translate() error = %v, want unsupported compression", err)
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
	values := environmentValues(contract.SenderEnvironment)
	addConnection(values)
	invocation, err := DecodeSenderEnvironment(mapLookup(values))
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(invocation.Arguments(), " ")
	want := "--include-snaps=^daily$ --include-snaps=^manual$ --include-snaps=^monthly$ --exclude-snaps=-tmp$"
	if !strings.Contains(got, want) {
		t.Fatalf("arguments = %q, want preserved normalized patterns %q", got, want)
	}
}

func TestDecodeSenderEnvironmentRejectsMalformedInput(t *testing.T) {
	run := testRun()
	contract, err := Translate(run)
	if err != nil {
		t.Fatal(err)
	}
	base := environmentValues(contract.SenderEnvironment)
	addConnection(base)

	for _, tt := range []struct {
		name   string
		mutate func(map[string]string)
		want   string
	}{
		{name: "missing required value", mutate: func(values map[string]string) { delete(values, "SYNCOID_NO_SYNC_SNAP") }, want: "missing"},
		{name: "malformed boolean", mutate: func(values map[string]string) { values["SYNCOID_NO_ROLLBACK"] = "sometimes" }, want: "parse sender environment"},
		{name: "non-canonical boolean", mutate: func(values map[string]string) { values["SYNCOID_NO_ROLLBACK"] = "1" }, want: "parse sender environment"},
		{name: "malformed list", mutate: func(values map[string]string) { values["SYNCOID_INCLUDE_SNAPS"] = "daily\n\nmanual" }, want: "malformed list"},
		{name: "unsupported compression", mutate: func(values map[string]string) { values["SYNCOID_COMPRESS"] = "shell" }, want: "unsupported compression"},
		{name: "unsafe identifier", mutate: func(values map[string]string) { values["SYNCOID_IDENTIFIER"] = "bad;id" }, want: "unsafe Syncoid identifier"},
		{name: "incomplete SSH input", mutate: func(values map[string]string) { delete(values, "KNOWN_HOSTS_FILE") }, want: "missing"},
		{name: "invalid source dataset", mutate: func(values map[string]string) { values["SRC_DATASET"] = "tank/src;other" }, want: "invalid source dataset"},
		{name: "malformed SSH port", mutate: func(values map[string]string) { values["SSH_PORT"] = "not-a-port" }, want: "invalid SSH port"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			values := cloneMap(base)
			tt.mutate(values)
			if _, err := DecodeSenderEnvironment(mapLookup(values)); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("DecodeSenderEnvironment() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestDecodeSenderEnvironmentRequiresEveryPrivateValue(t *testing.T) {
	run := testRun()
	contract, err := Translate(run)
	if err != nil {
		t.Fatal(err)
	}
	base := environmentValues(contract.SenderEnvironment)
	addConnection(base)

	for _, name := range []string{
		"SRC_DATASET",
		"DST_DATASET",
		"DST_HOST",
		"SSH_KEY_FILE",
		"KNOWN_HOSTS_FILE",
		"SSH_PORT",
		"SYNCOID_NO_SYNC_SNAP",
		"SYNCOID_NO_ROLLBACK",
		"SYNCOID_FORCE_DELETE",
		"SYNCOID_DELETE_TARGET_SNAPSHOTS",
		"SYNCOID_COMPRESS",
		"SYNCOID_IDENTIFIER",
		"RECEIVE_UNMOUNTED",
		"RECEIVE_RESUMABLE",
		"SYNCOID_INCLUDE_SNAPS",
		"SYNCOID_EXCLUDE_SNAPS",
	} {
		t.Run(name, func(t *testing.T) {
			values := cloneMap(base)
			delete(values, name)
			if _, err := DecodeSenderEnvironment(mapLookup(values)); err == nil || !strings.Contains(err.Error(), "missing") {
				t.Fatalf("DecodeSenderEnvironment() error = %v, want missing %s", err, name)
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
	firstID := first.ReceiverPolicy.SyncSnapshotIdentifier
	if firstID != "zrc-ec76c43a582d81b1db463599" {
		t.Fatalf("identifier = %q, want preserved relationship identifier format", firstID)
	}
	if firstID != second.ReceiverPolicy.SyncSnapshotIdentifier {
		t.Fatalf("stable identifier = %q then %q", firstID, second.ReceiverPolicy.SyncSnapshotIdentifier)
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
		if translated.ReceiverPolicy.SyncSnapshotIdentifier == firstID {
			t.Fatalf("changed relationship reused identifier %q for %#v", firstID, changed.Spec)
		}
	}
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

func environmentValues(environment []corev1.EnvVar) map[string]string {
	values := make(map[string]string, len(environment))
	for _, entry := range environment {
		values[entry.Name] = entry.Value
	}
	return values
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}

func addConnection(values map[string]string) {
	for _, entry := range ConnectionEnvironment(Connection{
		TargetHost:     "zfs-recv@10.0.0.42",
		SSHKeyFile:     "/var/run/zfsrep/ssh/id_rsa",
		KnownHostsFile: "/var/run/zfsrep/ssh/known_hosts",
		SSHPort:        "2222",
	}) {
		values[entry.Name] = entry.Value
	}
}

func cloneMap(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func pointer[T any](value T) *T {
	return &value
}
