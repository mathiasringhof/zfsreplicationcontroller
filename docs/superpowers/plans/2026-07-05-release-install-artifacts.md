# Release Install Artifacts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce install artifacts for an alpha `0.1.0` release that do not default users to mutable `main` images.

**Architecture:** Keep `config/` as the development/default Kustomize base. Add a release renderer that takes a version and exact image reference, renders the base manifests with controller, receiver, and data mover image values replaced consistently, and writes a single install YAML for GitHub releases.

**Tech Stack:** Bash, `kubectl kustomize`, Kubernetes manifests, Go tests for script and README guards, GitHub Actions container workflow.

---

## File Structure

- Create: `hack/render-release-manifest.sh`
  - Render a single release install manifest from the existing `config/` base.
- Create: `internal/release/release_artifact_test.go`
  - Guard the release renderer contract and README release artifact text.
- Modify: `.github/workflows/container.yaml`
  - Upload rendered install manifests as release artifacts for `v*` tags.
- Modify: `README.md`
  - Show alpha users how to install `v0.1.0` artifacts and explain the mutable `main` default.
- Create when cutting a release: `dist/zfsreplicationcontroller-v0.1.0.yaml`
  - Generated release manifest for local inspection before attaching to the release.

### Task 1: Add Release Artifact Guard Tests

**Files:**
- Create: `internal/release/release_artifact_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/release/release_artifact_test.go`:

```go
package release_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestReleaseRendererRejectsMutableMainImage(t *testing.T) {
	cmd := exec.Command("bash", "../../hack/render-release-manifest.sh", "v0.1.0", "ghcr.io/mathiasringhof/zfsreplicationcontroller:main")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("render-release-manifest accepted mutable main image; output:\n%s", out)
	}
	if !strings.Contains(string(out), "release image must not use mutable tag main") {
		t.Fatalf("renderer error = %q, want mutable main explanation", string(out))
	}
}

func TestReleaseRendererSetsAllImages(t *testing.T) {
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl is required to render kustomize release manifests")
	}
	cmd := exec.Command("bash", "../../hack/render-release-manifest.sh", "v0.1.0", "ghcr.io/mathiasringhof/zfsreplicationcontroller:v0.1.0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("render-release-manifest failed: %v\n%s", err, out)
	}
	rendered := string(out)
	for _, want := range []string{
		"image: ghcr.io/mathiasringhof/zfsreplicationcontroller:v0.1.0",
		"value: ghcr.io/mathiasringhof/zfsreplicationcontroller:v0.1.0",
		"app.kubernetes.io/version: v0.1.0",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered manifest missing %q:\n%s", want, rendered)
		}
	}
	for _, forbidden := range []string{
		"ghcr.io/mathiasringhof/zfsreplicationcontroller:main",
		"imagePullPolicy: Always",
	} {
		if strings.Contains(rendered, forbidden) {
			t.Fatalf("rendered manifest contains forbidden %q:\n%s", forbidden, rendered)
		}
	}
}

func TestREADMEDocumentsReleaseInstallArtifact(t *testing.T) {
	data, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	readme := string(data)
	for _, want := range []string{
		"https://github.com/mathiasringhof/zfsreplicationcontroller/releases/download/v0.1.0/zfsreplicationcontroller-v0.1.0.yaml",
		"kubectl apply -f zfsreplicationcontroller-v0.1.0.yaml",
		"The `0.1.x` releases are alpha releases",
	} {
		if !strings.Contains(readme, want) {
			t.Fatalf("README missing release install text %q", want)
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```sh
go test ./internal/release -run 'TestReleaseRenderer|TestREADMEDocumentsReleaseInstallArtifact' -count=1
```

Expected: FAIL because `hack/render-release-manifest.sh` does not exist and README lacks release artifact install text.

- [ ] **Step 3: Commit failing release artifact tests**

```sh
git add internal/release/release_artifact_test.go
git commit -m "test: guard release install artifacts"
```

### Task 2: Create Release Manifest Renderer

**Files:**
- Create: `hack/render-release-manifest.sh`

- [ ] **Step 1: Write the renderer**

Create `hack/render-release-manifest.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'usage: %s <version> <release-image>\n' "$0" >&2
  exit 2
}

die() {
  printf 'render-release-manifest: %s\n' "$*" >&2
  exit 1
}

[[ "$#" -eq 2 ]] || usage

version="$1"
image="$2"

[[ "${version}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]] || die "version must look like v0.1.0"
[[ "${image}" == *"/"* ]] || die "release image must include a registry and repository"
[[ "${image}" != *":main" ]] || die "release image must not use mutable tag main"
[[ "${image}" != *":latest" ]] || die "release image must not use mutable tag latest"
[[ "${image}" == *"@sha256:"* || "${image}" == *":${version}" ]] || die "release image must be pinned by digest or tagged with ${version}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
cleanup() {
  rm -rf "${tmp}"
}
trap cleanup EXIT

cp -R "${repo_root}/config" "${tmp}/config"
mkdir -p "${tmp}/overlay"
cat > "${tmp}/overlay/kustomization.yaml" <<EOF
resources:
  - ../config
patches:
  - target:
      kind: Deployment
      name: zfsreplication-controller
    patch: |-
      - op: add
        path: /metadata/labels/app.kubernetes.io~1version
        value: ${version}
      - op: add
        path: /spec/template/metadata/labels/app.kubernetes.io~1version
        value: ${version}
      - op: replace
        path: /spec/template/spec/containers/0/image
        value: ${image}
      - op: replace
        path: /spec/template/spec/containers/0/imagePullPolicy
        value: IfNotPresent
      - op: replace
        path: /spec/template/spec/containers/0/env/0/value
        value: ${image}
  - target:
      kind: DaemonSet
      name: zfs-receiver
    patch: |-
      - op: add
        path: /metadata/labels/app.kubernetes.io~1version
        value: ${version}
      - op: add
        path: /spec/template/metadata/labels/app.kubernetes.io~1version
        value: ${version}
      - op: replace
        path: /spec/template/spec/containers/0/image
        value: ${image}
      - op: replace
        path: /spec/template/spec/containers/0/imagePullPolicy
        value: IfNotPresent
EOF

kubectl kustomize "${tmp}/overlay"
```

- [ ] **Step 2: Make it executable**

Run:

```sh
chmod +x hack/render-release-manifest.sh
```

- [ ] **Step 3: Run renderer tests**

Run:

```sh
go test ./internal/release -run 'TestReleaseRendererRejectsMutableMainImage|TestReleaseRendererSetsAllImages' -count=1
```

Expected: PASS.

- [ ] **Step 4: Commit renderer**

```sh
git add hack/render-release-manifest.sh internal/release/release_artifact_test.go
git commit -m "build: render pinned release manifests"
```

### Task 3: Update Container Workflow To Upload Manifest Artifacts

**Files:**
- Modify: `.github/workflows/container.yaml`

- [ ] **Step 1: Add manifest render and upload steps**

Append these steps after the existing `Build image` step in `.github/workflows/container.yaml`:

```yaml
      - name: Render release manifest
        if: startsWith(github.ref, 'refs/tags/v')
        run: |
          mkdir -p dist
          ./hack/render-release-manifest.sh "${GITHUB_REF_NAME}" "${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}:${GITHUB_REF_NAME}" > "dist/zfsreplicationcontroller-${GITHUB_REF_NAME}.yaml"

      - name: Upload release manifest artifact
        if: startsWith(github.ref, 'refs/tags/v')
        uses: actions/upload-artifact@v4
        with:
          name: zfsreplicationcontroller-${{ github.ref_name }}-manifest
          path: dist/zfsreplicationcontroller-${{ github.ref_name }}.yaml
          if-no-files-found: error
```

- [ ] **Step 2: Add workflow guard to release artifact test**

Append this test to `internal/release/release_artifact_test.go`:

```go
func TestContainerWorkflowUploadsReleaseManifest(t *testing.T) {
	data, err := os.ReadFile("../../.github/workflows/container.yaml")
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(data)
	for _, want := range []string{
		"Render release manifest",
		"./hack/render-release-manifest.sh",
		"actions/upload-artifact@v4",
		"dist/zfsreplicationcontroller-${{ github.ref_name }}.yaml",
	} {
		if !strings.Contains(workflow, want) {
			t.Fatalf("container workflow missing %q", want)
		}
	}
}
```

- [ ] **Step 3: Run workflow artifact guard**

Run:

```sh
go test ./internal/release -run TestContainerWorkflowUploadsReleaseManifest -count=1
```

Expected: PASS.

- [ ] **Step 4: Commit workflow artifact upload**

```sh
git add .github/workflows/container.yaml internal/release/release_artifact_test.go
git commit -m "ci: upload release install manifest artifacts"
```

### Task 4: Document Alpha Release Installation

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add release install instructions**

In the `Install` section of `README.md`, before the existing `kubectl apply -k config` command, add:

````markdown
For an alpha release, prefer the rendered release manifest attached to the
GitHub release instead of the mutable `main` manifests in the repository:

```sh
curl -LO https://github.com/mathiasringhof/zfsreplicationcontroller/releases/download/v0.1.0/zfsreplicationcontroller-v0.1.0.yaml
kubectl apply -f zfsreplicationcontroller-v0.1.0.yaml
```

The `0.1.x` releases are alpha releases. The Kubernetes API remains
`zfsreplication.ringhof.io/v1alpha1`, and incompatible API changes may happen
before a stable `1.0.0`.
````

- [ ] **Step 2: Run README release artifact test**

Run:

```sh
go test ./internal/release -run TestREADMEDocumentsReleaseInstallArtifact -count=1
```

Expected: PASS.

- [ ] **Step 3: Commit README install docs**

```sh
git add README.md internal/release/release_artifact_test.go
git commit -m "docs: describe alpha release installation"
```

### Task 5: Render And Inspect `v0.1.0` Manifest

**Files:**
- Create: `dist/zfsreplicationcontroller-v0.1.0.yaml`

- [ ] **Step 1: Render local release manifest**

Run:

```sh
mkdir -p dist
./hack/render-release-manifest.sh v0.1.0 ghcr.io/mathiasringhof/zfsreplicationcontroller:v0.1.0 > dist/zfsreplicationcontroller-v0.1.0.yaml
```

Expected: command exits 0 and writes a manifest under `dist/`.

- [ ] **Step 2: Inspect image references**

Run:

```sh
grep -n 'ghcr.io/mathiasringhof/zfsreplicationcontroller' dist/zfsreplicationcontroller-v0.1.0.yaml
grep -n 'imagePullPolicy' dist/zfsreplicationcontroller-v0.1.0.yaml
```

Expected: every image and `DATA_MOVER_IMAGE` value uses `ghcr.io/mathiasringhof/zfsreplicationcontroller:v0.1.0`, and image pull policies are `IfNotPresent`.

- [ ] **Step 3: Commit renderer output only if this branch is the release preparation branch**

```sh
git add dist/zfsreplicationcontroller-v0.1.0.yaml
git commit -m "release: render v0.1.0 install manifest"
```

If this is not the release preparation branch, leave `dist/zfsreplicationcontroller-v0.1.0.yaml` untracked and attach the workflow artifact from the tagged release instead.

### Task 6: Full Verification

**Files:**
- No planned source changes

- [ ] **Step 1: Format**

Run:

```sh
go fmt ./...
git diff --exit-code
```

Expected: both commands exit 0.

- [ ] **Step 2: Unit and integration tests**

Run:

```sh
go test ./... -count=1
```

Expected: PASS.

- [ ] **Step 3: Lint**

Run:

```sh
golangci-lint run
```

Expected: `0 issues.`

- [ ] **Step 4: Release renderer smoke**

Run:

```sh
./hack/render-release-manifest.sh v0.1.0 ghcr.io/mathiasringhof/zfsreplicationcontroller:v0.1.0 > /tmp/zfsreplicationcontroller-v0.1.0.yaml
grep -q 'value: ghcr.io/mathiasringhof/zfsreplicationcontroller:v0.1.0' /tmp/zfsreplicationcontroller-v0.1.0.yaml
```

Expected: both commands exit 0.

- [ ] **Step 5: Full E2E with release image tag**

After the `v0.1.0` image has been built locally or published, run:

```sh
E2E_IMAGE_TAG=ghcr.io/mathiasringhof/zfsreplicationcontroller:v0.1.0 ./test/e2e/run.sh
```

Expected: PASS, including `ok github.com/mathias/zfsreplicationcontroller/test/e2e`.

## Self-Review

- Spec coverage: Covers item 4 by adding pinned release install manifests, upload artifacts, README install flow, and a release-image E2E verification path.
- Placeholder scan: No placeholders remain.
- Type consistency: Renderer tests call the exact script and paths created in the plan.
