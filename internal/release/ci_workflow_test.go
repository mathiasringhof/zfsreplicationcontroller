package release_test

import (
	"os"
	"strings"
	"testing"
)

func TestFastCIWorkflowHasReleaseGates(t *testing.T) {
	workflow := ciWorkflowFile(t, "../../.github/workflows/test.yaml")

	ciRequireWorkflowEventValue(t, workflow, "pull_request", "branches", "main")
	ciRequireWorkflowEventValue(t, workflow, "push", "branches", "main")
	ciRequireWorkflowEventValue(t, workflow, "push", "tags", "v*")
	ciRequireLine(t, workflow, "go-test:")
	for _, command := range []string{
		"go fmt ./...",
		"git diff --exit-code",
		"go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2",
		"golangci-lint run",
		"go test ./... -count=1",
		"go test -race ./... -count=1",
	} {
		ciRequireRunCommand(t, workflow, command)
	}
}

func TestGitHubActionsDoesNotDefineE2EWorkflow(t *testing.T) {
	if _, err := os.Stat("../../.github/workflows/e2e.yaml"); err == nil {
		t.Fatal("GitHub Actions E2E workflow exists; E2E must only run manually outside GitHub Actions")
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat GitHub Actions E2E workflow: %v", err)
	}
}

func ciWorkflowFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read workflow %s: %v", path, err)
	}
	return string(data)
}

func ciRequireLine(t *testing.T, contents, want string) {
	t.Helper()
	for _, line := range strings.Split(contents, "\n") {
		if strings.TrimSpace(line) == want {
			return
		}
	}
	t.Fatalf("workflow missing line %q", want)
}

func ciRequireRunCommand(t *testing.T, contents, want string) {
	t.Helper()
	for _, line := range strings.Split(contents, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == want || strings.TrimPrefix(trimmed, "run: ") == want {
			return
		}
	}
	t.Fatalf("workflow missing run command %q", want)
}

func ciRequireWorkflowEventValue(t *testing.T, contents, event, key, value string) {
	t.Helper()
	onBlock := ciRequireYAMLBlock(t, ciYAMLLines(contents), "on:")
	eventBlock := ciRequireYAMLBlock(t, onBlock, event+":")
	keyBlock := ciRequireYAMLBlock(t, eventBlock, key+":")
	ciRequireYAMLLine(t, keyBlock, "- "+value)
}

type ciYAMLLine struct {
	indent int
	text   string
}

func ciYAMLLines(contents string) []ciYAMLLine {
	var lines []ciYAMLLine
	for _, line := range strings.Split(contents, "\n") {
		text := strings.TrimSpace(line)
		if text == "" {
			continue
		}
		lines = append(lines, ciYAMLLine{
			indent: len(line) - len(strings.TrimLeft(line, " ")),
			text:   text,
		})
	}
	return lines
}

func ciRequireYAMLBlock(t *testing.T, lines []ciYAMLLine, want string) []ciYAMLLine {
	t.Helper()
	for i, line := range lines {
		if line.text != want {
			continue
		}
		var block []ciYAMLLine
		for _, child := range lines[i+1:] {
			if child.indent <= line.indent {
				break
			}
			block = append(block, child)
		}
		return block
	}
	t.Fatalf("workflow missing YAML key %q", want)
	return nil
}

func ciRequireYAMLLine(t *testing.T, lines []ciYAMLLine, want string) {
	t.Helper()
	for _, line := range lines {
		if line.text == want || ciYAMLListValue(line.text) == strings.TrimPrefix(want, "- ") {
			return
		}
	}
	t.Fatalf("workflow missing YAML line %q", want)
}

func ciYAMLListValue(text string) string {
	value, ok := strings.CutPrefix(text, "- ")
	if !ok {
		return ""
	}
	return strings.Trim(value, `"'`)
}
