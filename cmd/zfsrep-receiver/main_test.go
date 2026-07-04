package main

import (
	"context"
	"strings"
	"testing"

	zfsv1 "github.com/mathias/zfsreplicationcontroller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRenderSSHDConfigAllowsRootMappedReceiverUser(t *testing.T) {
	cfg := receiverConfig{
		AuthorizedKeysFile: "/run/zfs-receiver/authorized_keys",
		SSHPort:            2222,
	}

	config := renderSSHDConfig(cfg)

	for _, want := range []string{
		"PermitRootLogin prohibit-password",
		"AllowUsers zfs-recv",
		"PasswordAuthentication no",
		"KbdInteractiveAuthentication no",
		"AuthorizedKeysFile /run/zfs-receiver/authorized_keys",
	} {
		if !strings.Contains(config, want) {
			t.Fatalf("sshd_config missing %q:\n%s", want, config)
		}
	}
	if strings.Contains(config, "PermitRootLogin no") {
		t.Fatalf("sshd_config rejects the root-mapped zfs-recv account:\n%s", config)
	}
}

func TestReceiveTaskAuthorizationForcesPolicyCommand(t *testing.T) {
	task := &zfsv1.ZFSReceiveTask{
		ObjectMeta: metav1.ObjectMeta{Name: "recv-1", Namespace: "storage"},
		Spec: zfsv1.ZFSReceiveTaskSpec{
			Destination: zfsv1.ReceiveDestination{Dataset: "tank/dst"},
			SSH: zfsv1.ReceiveTaskSSHSpec{
				AuthorizedPublicKey: "ssh-rsa AAAATEST zfsreplication-controller",
			},
			Policy: zfsv1.ReceiveTaskPolicy{
				ReceiveUnmounted:         true,
				ReceiveResumable:         true,
				AllowSyncSnapshotDestroy: true,
				Compression:              "none",
			},
		},
	}
	cfg := receiverConfig{AuthorizedKeysFile: "/run/zfs-receiver/authorized_keys"}

	auth := receiveTaskAuthorization(cfg, task)

	wantCommand := `restrict,command="/usr/local/bin/zfsrep-receiver exec --policy /run/zfs-receiver/policies/storage_recv-1.json"`
	if !strings.HasPrefix(auth.AuthorizedKey, wantCommand) {
		t.Fatalf("authorized key = %q, want prefix %q", auth.AuthorizedKey, wantCommand)
	}
	if !strings.HasSuffix(auth.AuthorizedKey, " ssh-rsa AAAATEST zfsreplication-controller") {
		t.Fatalf("authorized key = %q, want public key suffix", auth.AuthorizedKey)
	}
	if auth.Policy.TargetDataset != "tank/dst" {
		t.Fatalf("policy target = %q, want tank/dst", auth.Policy.TargetDataset)
	}
	if !auth.Policy.ReceiveUnmounted || !auth.Policy.ReceiveResumable || !auth.Policy.AllowSyncSnapshotDestroy {
		t.Fatalf("policy flags = %#v, want receive flags and sync snapshot destroy", auth.Policy)
	}
}

func TestAuthorizeReceiverCommandAllowsSyncoidTargetCommands(t *testing.T) {
	policy := receiverCommandPolicy{
		TargetDataset:            "tank/dst",
		ReceiveUnmounted:         true,
		ReceiveResumable:         true,
		AllowSyncSnapshotDestroy: true,
		Compression:              "none",
	}

	for _, cmd := range []string{
		"exit",
		"echo -n hello",
		"command -v mbuffer",
		"command -v mbuffer 2>/dev/null",
		"zpool get -o value -H feature@extensible_dataset tank 2>/dev/null",
		"zpool get -o value -H feature@extensible_dataset tank | grep '(active|enabled)' >/dev/null 2>&1",
		"zpool get -o value -H feature@extensible_dataset tank 2>/dev/null | grep '\\(active\\|enabled\\)' >/dev/null 2>&1",
		"ps -Ao args=",
		"zfs get -H name tank/dst",
		"zfs get -H receive_resume_token tank/dst",
		"zfs get -Hpd 1 -t snapshot guid,creation tank/dst",
		"zfs get -Hpd 1 type,guid,creation tank/dst",
		"zfs get -H -p used tank/dst",
		"mbuffer -q -s 128k -m 16M | zfs receive -u -s tank/dst 2>&1",
		"zfs receive -A tank/dst",
		"zfs destroy tank/dst@syncoid_2026-07-04:10:00:00-GMT00:00",
		"zfs destroy tank/dst@syncoid_old; zfs destroy tank/dst@syncoid_older",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := authorizeReceiverCommand(cmd, policy); err != nil {
				t.Fatalf("authorizeReceiverCommand() error = %v, want nil", err)
			}
		})
	}
}

func TestAuthorizeReceiverCommandRejectsCommandsOutsidePolicy(t *testing.T) {
	policy := receiverCommandPolicy{
		TargetDataset:    "tank/dst",
		ReceiveUnmounted: true,
		ReceiveResumable: true,
		Compression:      "none",
	}

	for _, cmd := range []string{
		"id",
		"sh -c id",
		"zfs list",
		"zfs get -H name tank/other",
		"zfs receive -u -s tank/other 2>&1",
		"zfs receive -u -s tank/dst; id",
		"zfs receive -u -s tank/dst $(id)",
		"zfs destroy tank/dst@syncoid_2026-07-04",
		"zfs destroy tank/dst@syncoid_old; id",
		"zfs destroy -r tank/dst",
		"command -v busybox",
		"command -v zstd",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := authorizeReceiverCommand(cmd, policy); err == nil {
				t.Fatal("authorizeReceiverCommand() error = nil, want rejection")
			}
		})
	}
}

func TestAuthorizeReceiverCommandAllowsForceDeletePolicy(t *testing.T) {
	policy := receiverCommandPolicy{
		TargetDataset:    "tank/dst",
		ReceiveUnmounted: true,
		ReceiveResumable: true,
		AllowDestroy:     true,
		Compression:      "none",
	}

	for _, cmd := range []string{
		"zfs destroy -r tank/dst",
		"zfs destroy tank/dst",
		"zfs destroy -r tank/dst;",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := authorizeReceiverCommand(cmd, policy); err != nil {
				t.Fatalf("authorizeReceiverCommand() error = %v, want nil", err)
			}
		})
	}
}

func TestAuthorizeReceiverCommandUsesCompressionPolicy(t *testing.T) {
	policy := receiverCommandPolicy{
		TargetDataset:    "tank/dst",
		ReceiveUnmounted: true,
		ReceiveResumable: true,
		Compression:      "zstd",
	}

	for _, cmd := range []string{
		"command -v zstd",
		"mbuffer -q | zstd -dc | zfs receive -u -s tank/dst 2>&1",
	} {
		t.Run(cmd, func(t *testing.T) {
			if _, err := authorizeReceiverCommand(cmd, policy); err != nil {
				t.Fatalf("authorizeReceiverCommand() error = %v, want nil", err)
			}
		})
	}
	if _, err := authorizeReceiverCommand("mbuffer -q | gzip -dc | zfs receive -u -s tank/dst 2>&1", policy); err == nil {
		t.Fatal("authorizeReceiverCommand() error = nil, want compressor mismatch rejection")
	}
}

func TestAuthorizeReceiverCommandAllowsSyncoidDecompressorForms(t *testing.T) {
	for _, tt := range []struct {
		name        string
		compression string
		command     string
	}{
		{name: "gzip", compression: "gzip", command: "mbuffer -q | zcat | zfs receive -u -s tank/dst 2>&1"},
		{name: "pigz", compression: "pigz", command: "mbuffer -q | pigz -dc | zfs receive -u -s tank/dst 2>&1"},
		{name: "xz", compression: "xz", command: "mbuffer -q | xz -d | zfs receive -u -s tank/dst 2>&1"},
		{name: "lzop", compression: "lzop", command: "mbuffer -q | lzop -dfc | zfs receive -u -s tank/dst 2>&1"},
		{name: "lz4", compression: "lz4", command: "mbuffer -q | lz4 -dc | zfs receive -u -s tank/dst 2>&1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := receiverCommandPolicy{
				TargetDataset:    "tank/dst",
				ReceiveUnmounted: true,
				ReceiveResumable: true,
				Compression:      tt.compression,
			}
			if _, err := authorizeReceiverCommand(tt.command, policy); err != nil {
				t.Fatalf("authorizeReceiverCommand() error = %v, want nil", err)
			}
		})
	}
}

func TestRunForcedCommandRejectsMissingOriginalCommand(t *testing.T) {
	err := runForcedCommand(context.Background(), forcedCommandConfig{
		OriginalCommand: "",
		Policy: receiverCommandPolicy{
			TargetDataset: "tank/dst",
			Compression:   "none",
		},
	})
	if err == nil {
		t.Fatal("runForcedCommand() error = nil, want missing command rejection")
	}
}
