package controller

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestControllerClusterRoleHasRequiredPermissions(t *testing.T) {
	t.Helper()

	rolePath := filepath.Join("..", "..", "config", "rbac", "role.yaml")
	data, err := os.ReadFile(rolePath)
	if err != nil {
		t.Fatalf("read %s: %v", rolePath, err)
	}

	var role struct {
		Rules []struct {
			APIGroups []string `yaml:"apiGroups"`
			Resources []string `yaml:"resources"`
			Verbs     []string `yaml:"verbs"`
		} `yaml:"rules"`
	}
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatalf("parse %s: %v", rolePath, err)
	}

	verbs := verbsForResource(role.Rules, "coordination.k8s.io", "leases")
	for _, verb := range []string{"create", "get", "list", "watch", "update", "patch"} {
		if !contains(verbs, verb) {
			t.Fatalf("leases RBAC verbs = %v, missing %q", verbs, verb)
		}
	}

	verbs = verbsForResource(role.Rules, "", "pods/log")
	if !contains(verbs, "get") {
		t.Fatalf("pods/log RBAC verbs = %v, missing get", verbs)
	}

	verbs = verbsForResource(role.Rules, "", "secrets")
	for _, verb := range []string{"create", "get", "list", "watch", "update", "patch", "delete"} {
		if !contains(verbs, verb) {
			t.Fatalf("secrets RBAC verbs = %v, missing %q", verbs, verb)
		}
	}
}

func verbsForResource(rules []struct {
	APIGroups []string `yaml:"apiGroups"`
	Resources []string `yaml:"resources"`
	Verbs     []string `yaml:"verbs"`
}, apiGroup, resource string) []string {
	for _, rule := range rules {
		if contains(rule.APIGroups, apiGroup) && contains(rule.Resources, resource) {
			return rule.Verbs
		}
	}
	return nil
}

func contains(items []string, needle string) bool {
	for _, item := range items {
		if item == needle {
			return true
		}
	}
	return false
}
