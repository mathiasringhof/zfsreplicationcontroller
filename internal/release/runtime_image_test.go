package release_test

import (
	"os"
	"strings"
	"testing"
)

func TestRuntimeImagePinsSyncoid230(t *testing.T) {
	data, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	dockerfile := string(data)
	for _, want := range []string{
		"ARG SANOID_VERSION=2.3.0",
		"ARG SANOID_SHA256=1d8735a271a34ec87ea46313a66f6f20bd38b583886924574d3c1f72ea173620",
		"https://github.com/jimsalterjrs/sanoid/archive/refs/tags/v${SANOID_VERSION}.tar.gz",
		"syncoid --version | grep -F \"${SANOID_VERSION}\"",
		"syncoid --help 2>&1 | grep -F -- \"--include-snaps\"",
		"syncoid --help 2>&1 | grep -F -- \"--identifier\"",
		"syncoid --help 2>&1 | grep -F -- \"--delete-target-snapshots\"",
		"openssh-server",
		"go build -o /out/zfsrep-receiver ./cmd/zfsrep-receiver",
		"COPY --from=build /out/zfsrep-receiver /usr/local/bin/zfsrep-receiver",
		"useradd -o -u 0 -g 0 -M -d /run/zfs-receiver -s /bin/sh zfs-recv",
		"usermod -p '*' zfs-recv",
	} {
		if !strings.Contains(dockerfile, want) {
			t.Fatalf("Dockerfile missing %q", want)
		}
	}
	if strings.Contains(dockerfile, " zfsutils-linux sanoid ") {
		t.Fatalf("Dockerfile still installs distro sanoid package")
	}
}

func TestRuntimeImageAppliesSyncoidPipefailPatch(t *testing.T) {
	dockerfileBytes, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatal(err)
	}

	patchBytes, err := os.ReadFile("../../patches/syncoid-pipefail.patch")
	if err != nil {
		t.Fatal(err)
	}

	dockerfile := string(dockerfileBytes)
	for _, want := range []string{
		"bash \\",
		"COPY patches/syncoid-pipefail.patch /tmp/syncoid-pipefail.patch",
		"patch --batch --forward",
		"/tmp/syncoid-pipefail.patch",
	} {
		if !strings.Contains(dockerfile, want) {
			t.Fatalf("Dockerfile missing %q", want)
		}
	}

	patch := string(patchBytes)
	for _, want := range []string{
		"system(\"bash\", \"-o\", \"pipefail\", \"-c\", $synccmd)",
	} {
		if !strings.Contains(patch, want) {
			t.Fatalf("Syncoid patch missing %q", want)
		}
	}
}

func TestSyncoidPatchUsesCanonicalGNUContext(t *testing.T) {
	patchBytes, err := os.ReadFile("../../patches/syncoid-pipefail.patch")
	if err != nil {
		t.Fatal(err)
	}

	expectedHunk := `@@ -947,11 +947,11 @@
	my $ret;
	if ($pvsize == 0) {
		($stdout, $ret) = tee_stderr {
-			system("$synccmd");
+			system("bash", "-o", "pipefail", "-c", $synccmd);
		};
	} else {
		($stdout, $ret) = tee_stdout {
-			system("$synccmd");
+			system("bash", "-o", "pipefail", "-c", $synccmd);
		};
	}
` + " \n"
	expectedHunk = strings.ReplaceAll(expectedHunk, "\n\t", "\n \t")
	if !strings.Contains(string(patchBytes), expectedHunk) {
		t.Fatalf("Syncoid patch is not in canonical GNU patch form:\n%s", patchBytes)
	}
}
