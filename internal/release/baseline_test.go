package release_test

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestSupportedDependencyBaseline(t *testing.T) {
	files := map[string]string{
		"Dockerfile":                        readFile(t, "../../Dockerfile"),
		"test/e2e/env.sh":                   readFile(t, "../../test/e2e/env.sh"),
		"api/v1alpha1/groupversion_info.go": readFile(t, "../../api/v1alpha1/groupversion_info.go"),
	}

	goMod := parseGoMod(t, "../../go.mod")
	if goMod.Go != "1.26.0" {
		t.Fatalf("go.mod Go directive = %q, want %q", goMod.Go, "1.26.0")
	}
	requireDirectRequire(t, goMod, "k8s.io/api", "v0.35.6")
	requireDirectRequire(t, goMod, "k8s.io/apimachinery", "v0.35.6")
	requireRequire(t, goMod, "k8s.io/apiextensions-apiserver", "v0.35.6")
	requireDirectRequire(t, goMod, "k8s.io/client-go", "v0.35.6")
	requireDirectRequire(t, goMod, "sigs.k8s.io/controller-runtime", "v0.23.0")

	requireEqual(t, "Dockerfile first line", firstLine(files["Dockerfile"]), "FROM docker.io/library/golang:1.26.4 AS build")
	requireContains(t, "test/e2e/env.sh", files["test/e2e/env.sh"], `K3S_VERSION="${E2E_K3S_VERSION:-v1.35.6+k3s1}"`)
	group, version := groupVersionConsts(t, "../../api/v1alpha1/groupversion_info.go")
	requireEqual(t, "api/v1alpha1 Group const", group, "zfsreplication.ringhof.io")
	requireEqual(t, "api/v1alpha1 Version const", version, "v1alpha1")
	requireNoOldAPIGroupReferences(t)
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

type goModFile struct {
	Go      string         `json:"Go"`
	Require []goModRequire `json:"Require"`
}

type goModRequire struct {
	Path     string `json:"Path"`
	Version  string `json:"Version"`
	Indirect bool   `json:"Indirect"`
}

func parseGoMod(t *testing.T, path string) goModFile {
	t.Helper()
	out, err := exec.Command("go", "mod", "edit", "-json", path).Output()
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	var file goModFile
	if err := json.Unmarshal(out, &file); err != nil {
		t.Fatalf("parse %s JSON: %v", path, err)
	}
	return file
}

func requireDirectRequire(t *testing.T, file goModFile, path, version string) {
	t.Helper()
	for _, req := range file.Require {
		if req.Path != path {
			continue
		}
		if req.Indirect {
			t.Fatalf("go.mod require %s is indirect, want direct", path)
		}
		if req.Version != version {
			t.Fatalf("go.mod require %s = %q, want %q", path, req.Version, version)
		}
		return
	}
	t.Fatalf("go.mod missing direct require %s %s", path, version)
}

func requireRequire(t *testing.T, file goModFile, path, version string) {
	t.Helper()
	for _, req := range file.Require {
		if req.Path != path {
			continue
		}
		if req.Version != version {
			t.Fatalf("go.mod require %s = %q, want %q", path, req.Version, version)
		}
		return
	}
	t.Fatalf("go.mod missing require %s %s", path, version)
}

func firstLine(contents string) string {
	line, _, _ := strings.Cut(contents, "\n")
	return strings.TrimSuffix(line, "\r")
}

func requireEqual(t *testing.T, name, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %q, want %q", name, got, want)
	}
}

func requireContains(t *testing.T, name, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Fatalf("%s does not contain %q", name, needle)
	}
}

func groupVersionConsts(t *testing.T, path string) (string, string) {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if file.Name.Name != "v1alpha1" {
		t.Fatalf("%s package = %q, want %q", path, file.Name.Name, "v1alpha1")
	}
	constants := make(map[string]string)
	for _, decl := range file.Decls {
		general, ok := decl.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range value.Names {
				if name.Name != "Group" && name.Name != "Version" {
					continue
				}
				if i >= len(value.Values) {
					t.Fatalf("%s %s const has no value", path, name.Name)
				}
				lit, ok := value.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					t.Fatalf("%s %s const is not a string literal", path, name.Name)
				}
				constValue, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatalf("unquote %s %s const: %v", path, name.Name, err)
				}
				constants[name.Name] = constValue
			}
		}
	}
	if constants["Group"] == "" {
		t.Fatalf("%s missing Group const", path)
	}
	if constants["Version"] == "" {
		t.Fatalf("%s missing Version const", path)
	}
	return constants["Group"], constants["Version"]
}

func requireNoOldAPIGroupReferences(t *testing.T) {
	t.Helper()
	oldAPIGroup := "zfsreplication." + "example.com"
	root := filepath.Clean("../..")

	cmd := exec.Command("git", "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("list repository files: %v", err)
	}

	for _, file := range strings.Split(string(out), "\x00") {
		if file == "" {
			continue
		}
		path := filepath.Join(root, file)
		contents, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("read %s: %v", path, err)
		}
		if strings.Contains(string(contents), oldAPIGroup) {
			t.Fatalf("%s still references %s", path, oldAPIGroup)
		}
	}
}
