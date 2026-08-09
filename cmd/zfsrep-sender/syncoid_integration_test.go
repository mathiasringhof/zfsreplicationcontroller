package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/mathias/zfsreplicationcontroller/internal/syncoidruntime"
)

const syncoidFixtureSHA256 = "c9347951017f3712665519ed3edc3590a478cd4afef7d092b0ab786e8c556fd7"

func TestPatchedSyncoidPipelineOutcomeThroughSender(t *testing.T) {
	syncoidPath := patchedSyncoidArtifact(t)
	installFakeSyncoidCommands(t, syncoidPath)

	for _, tt := range []struct {
		name          string
		sendExitCode  string
		wantExitCode  int
		wantOutput    string
		wantError     string
		wantPublished string
	}{
		{
			name:          "early producer failure is not masked by successful consumer",
			sendExitCode:  "23",
			wantExitCode:  2,
			wantOutput:    "fake receiver output",
			wantError:     "fake producer error",
			wantPublished: "syncoid exited with status 2",
		},
		{
			name:         "all transfer stages succeed",
			sendExitCode: "0",
			wantExitCode: 0,
			wantOutput:   "fake receiver output",
			wantError:    "fake producer error",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("FAKE_ZFS_SEND_EXIT", tt.sendExitCode)

			var stdout, stderr bytes.Buffer
			terminationMessagePath := filepath.Join(t.TempDir(), "termination-message")
			arguments := []string{
				"--no-command-checks",
				"--no-resume",
				"--no-rollback",
				"--no-privilege-elevation",
				"--no-sync-snap",
				"--no-stream",
				"--compress=none",
				"--quiet",
				"tank/src",
				"zfs-recv@example.invalid:tank/dst",
			}

			exitCode := run(
				context.Background(),
				arguments,
				&stdout,
				&stderr,
				func(ctx context.Context, arguments []string, stdout, stderr io.Writer) runtimeOutcome {
					return syncoidruntime.Run(ctx, arguments, stdout, stderr)
				},
				filePublisher{path: terminationMessagePath},
			)

			if exitCode != tt.wantExitCode {
				t.Fatalf("Sender exit code = %d, want %d\nstdout:\n%s\nstderr:\n%s", exitCode, tt.wantExitCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stdout.String(), tt.wantOutput) {
				t.Fatalf("Sender stdout = %q, want marker %q", stdout.String(), tt.wantOutput)
			}
			if !strings.Contains(stderr.String(), tt.wantError) {
				t.Fatalf("Sender stderr = %q, want marker %q", stderr.String(), tt.wantError)
			}

			published, err := os.ReadFile(terminationMessagePath)
			if tt.wantPublished == "" {
				if !os.IsNotExist(err) {
					t.Fatalf("termination message error = %v, want no publication", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("read termination message: %v", err)
			}
			if got := string(published); got != tt.wantPublished {
				t.Fatalf("termination message = %q, want %q", got, tt.wantPublished)
			}
		})
	}
}

func patchedSyncoidArtifact(t *testing.T) string {
	t.Helper()

	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration test source")
	}
	testDir := filepath.Dir(sourceFile)
	t.Setenv("PERL5LIB", filepath.Join(testDir, "testdata"))
	fixturePath := filepath.Join(testDir, "testdata/syncoid-2.3.0")
	fixture, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read Syncoid fixture: %v", err)
	}
	fixtureSHA256 := sha256.Sum256(fixture)
	if got := hex.EncodeToString(fixtureSHA256[:]); got != syncoidFixtureSHA256 {
		t.Fatalf("Syncoid fixture SHA-256 = %s, want %s", got, syncoidFixtureSHA256)
	}

	artifactDir := t.TempDir()
	syncoidPath := filepath.Join(artifactDir, "syncoid")
	if err := os.WriteFile(syncoidPath, fixture, 0o755); err != nil {
		t.Fatalf("write Syncoid: %v", err)
	}

	patchBytes, err := os.ReadFile(filepath.Join(testDir, "../../patches/syncoid-pipefail.patch"))
	if err != nil {
		t.Fatalf("read Syncoid patch: %v", err)
	}
	patchCommand := exec.Command("patch", "--batch", "--forward", "--fuzz=0", syncoidPath)
	patchCommand.Stdin = bytes.NewReader(patchBytes)
	patchOutput, err := patchCommand.CombinedOutput()
	if err != nil {
		t.Fatalf("apply Syncoid patch: %v\n%s", err, patchOutput)
	}
	return syncoidPath
}

func installFakeSyncoidCommands(t *testing.T, syncoidPath string) {
	t.Helper()
	commandDir := t.TempDir()
	for name, contents := range map[string]string{
		"ssh": `#!/bin/sh
set -eu

host_seen=
remote_command=
for argument in "$@"; do
  if [ -n "${host_seen}" ]; then
    if [ -n "${remote_command}" ]; then
      remote_command="${remote_command} ${argument}"
    else
      remote_command="${argument}"
    fi
  elif [ "${argument}" = "zfs-recv@example.invalid" ]; then
    host_seen=1
  fi
done

[ -n "${host_seen}" ]
case "${remote_command}" in
  *"echo -n"*|*"-O exit"*|exit*) exit 0 ;;
esac
exec /bin/sh -c "${remote_command}"
`,
		"mbuffer": `#!/bin/sh
cat
`,
		"ps": `#!/bin/sh
exit 0
`,
		"zfs": `#!/bin/sh
set -eu

case "${1:-}" in
  get)
    case "$*" in
	      *"syncoid:sync"*) printf 'tank/src\tsyncoid:sync\t-' ;;
	      *"-H name"*) printf 'not-found\n'; exit 1 ;;
      *"-Hpd 1 -t snapshot guid,creation"*)
        printf 'tank/src@base\tguid\t100\n'
        printf 'tank/src@base\tcreation\t100\n'
        ;;
      *"receive_resume_token"*) printf 'tank/dst\treceive_resume_token\t-\n' ;;
    esac
    ;;
  send)
    case "$*" in
      *"-nvP"*) printf 'size\t4096\n' ;;
      *)
        printf 'fake producer output\n'
        printf 'fake producer error\n' >&2
        exit "${FAKE_ZFS_SEND_EXIT:-0}"
        ;;
    esac
    ;;
  receive)
    cat >/dev/null
    printf 'fake receiver output\n'
    ;;
esac
`,
		"syncoid": "",
	} {
		path := filepath.Join(commandDir, name)
		if name == "syncoid" {
			if err := os.Symlink(syncoidPath, path); err != nil {
				t.Fatalf("link patched Syncoid: %v", err)
			}
			continue
		}
		if err := os.WriteFile(path, []byte(contents), 0o755); err != nil {
			t.Fatalf("write fake %s: %v", name, err)
		}
	}

	t.Setenv("PATH", commandDir+string(os.PathListSeparator)+os.Getenv("PATH"))
}
