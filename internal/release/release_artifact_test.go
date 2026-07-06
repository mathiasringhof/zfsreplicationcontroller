package release_test

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestReleaseRendererRejectsMainTag(t *testing.T) {
	t.Parallel()

	cmd := exec.Command("../../hack/render-release-manifest.sh", "v0.1.0", "ghcr.io/mathiasringhof/zfsreplicationcontroller:main")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err == nil {
		t.Fatal("render release manifest succeeded with mutable main tag")
	}
	artifactRequireContains(t, "stderr", stderr.String(), "release image must not use mutable tag main")
}

func TestReleaseRendererRendersPinnedManifest(t *testing.T) {
	t.Parallel()

	releaseImage := "ghcr.io/mathiasringhof/zfsreplicationcontroller:v0.1.0"
	out, err := exec.Command("../../hack/render-release-manifest.sh", "v0.1.0", releaseImage).CombinedOutput()
	if err != nil {
		t.Fatalf("render release manifest: %v\n%s", err, out)
	}
	manifest := string(out)

	for _, want := range []string{
		"image: " + releaseImage,
		"value: " + releaseImage,
		"app.kubernetes.io/version: v0.1.0",
	} {
		artifactRequireContains(t, "manifest", manifest, want)
	}
	for _, forbidden := range []string{
		"ghcr.io/mathiasringhof/zfsreplicationcontroller:main",
		"imagePullPolicy: Always",
	} {
		artifactRequireNotContains(t, "manifest", manifest, forbidden)
	}
}

func TestREADMEDocumentsReleaseInstallArtifact(t *testing.T) {
	t.Parallel()

	readme := artifactReadFile(t, "../../README.md")
	for _, want := range []string{
		"https://github.com/mathiasringhof/zfsreplicationcontroller/releases/download/v0.1.2/zfsreplicationcontroller-v0.1.2.yaml",
		"kubectl apply -f zfsreplicationcontroller-v0.1.2.yaml",
		"The `0.1.x` releases are alpha releases",
	} {
		artifactRequireContains(t, "README.md", readme, want)
	}
}

func TestContainerWorkflowUploadsReleaseManifest(t *testing.T) {
	t.Parallel()

	workflow := artifactReadFile(t, "../../.github/workflows/container.yaml")
	for _, want := range []string{
		"contents: write",
		"type=ref,event=tag",
		"Render release manifest",
		"./hack/render-release-manifest.sh",
		"softprops/action-gh-release@v2",
		"files: dist/zfsreplicationcontroller-${{ github.ref_name }}.yaml",
		"actions/upload-artifact@v4",
		"dist/zfsreplicationcontroller-${{ github.ref_name }}.yaml",
	} {
		artifactRequireContains(t, ".github/workflows/container.yaml", workflow, want)
	}
}

func TestContainerWorkflowGatesTagReleasePublication(t *testing.T) {
	t.Parallel()

	workflow := artifactReadFile(t, "../../.github/workflows/container.yaml")
	for _, want := range []string{
		"release-go-checks:",
		"publish-release:",
		"needs: [release-go-checks]",
		"if: github.event_name != 'push' || !startsWith(github.ref, 'refs/tags/v')",
	} {
		artifactRequireContains(t, ".github/workflows/container.yaml", workflow, want)
	}
	artifactRequireNotContains(t, ".github/workflows/container.yaml", workflow, "release-e2e:")
	artifactRequireNotContains(t, ".github/workflows/container.yaml", workflow, "zfsreplication-e2e")
}

func TestE2EWorkflowRunsSeparatelyFromReleasePublication(t *testing.T) {
	t.Parallel()

	workflow := artifactReadFile(t, "../../.github/workflows/e2e.yaml")
	for _, want := range []string{
		"workflow_dispatch:",
		"- \"v*\"",
		"zfsreplication-e2e",
		"./test/e2e/doctor.sh",
		"./test/e2e/run.sh",
	} {
		artifactRequireContains(t, ".github/workflows/e2e.yaml", workflow, want)
	}
}

func TestE2EScriptsUseFullyQualifiedCustomResources(t *testing.T) {
	t.Parallel()

	for _, script := range []string{
		"../../test/e2e/collect.sh",
		"../../test/e2e/status.sh",
	} {
		contents := artifactReadFile(t, script)
		for _, want := range []string{
			"zfsreceivetasks.zfsreplication.ringhof.io",
			"zfsreplicationruns.zfsreplication.ringhof.io",
			"zfsreplicationschedules.zfsreplication.ringhof.io",
		} {
			artifactRequireContains(t, script, contents, want)
		}
		for _, forbidden := range []string{
			"get zfsreceivetasks ",
			"get zfsreplicationruns ",
			"get zfsreplicationschedules ",
		} {
			artifactRequireNotContains(t, script, contents, forbidden)
		}
	}
}

func artifactReadFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func artifactRequireContains(t *testing.T, name, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Fatalf("%s does not contain %q", name, needle)
	}
}

func artifactRequireNotContains(t *testing.T, name, haystack, needle string) {
	t.Helper()
	if strings.Contains(haystack, needle) {
		t.Fatalf("%s contains %q", name, needle)
	}
}
