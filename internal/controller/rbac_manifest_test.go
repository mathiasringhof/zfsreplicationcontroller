package controller

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

type validationRule struct {
	Rule    string `yaml:"rule"`
	Message string `yaml:"message"`
}

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

	verbs := verbsForResource(role.Rules, "zfsreplication.example.com", "zfsreplicationruns")
	for _, verb := range []string{"create", "get", "list", "watch"} {
		if !contains(verbs, verb) {
			t.Fatalf("zfsreplicationruns RBAC verbs = %v, missing %q", verbs, verb)
		}
	}

	verbs = verbsForResource(role.Rules, "zfsreplication.example.com", "zfsreplicationruns/status")
	for _, verb := range []string{"get", "update", "patch"} {
		if !contains(verbs, verb) {
			t.Fatalf("zfsreplicationruns/status RBAC verbs = %v, missing %q", verbs, verb)
		}
	}

	verbs = verbsForResource(role.Rules, "zfsreplication.example.com", "zfsreplicationschedules/status")
	for _, verb := range []string{"get", "update", "patch"} {
		if !contains(verbs, verb) {
			t.Fatalf("zfsreplicationschedules/status RBAC verbs = %v, missing %q", verbs, verb)
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

func TestCRDSchemaExposesSyncoidOptions(t *testing.T) {
	t.Helper()

	crdPath := filepath.Join("..", "..", "config", "crd", "zfsreplication.example.com_zfsreplicationruns.yaml")
	data, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("read %s: %v", crdPath, err)
	}

	type schemaNode struct {
		Default                any                   `yaml:"default"`
		Properties             map[string]schemaNode `yaml:"properties"`
		Items                  *schemaNode           `yaml:"items"`
		Type                   string                `yaml:"type"`
		XKubernetesValidations []validationRule      `yaml:"x-kubernetes-validations"`
	}
	var crd struct {
		Spec struct {
			Versions []struct {
				Schema struct {
					OpenAPIV3Schema schemaNode `yaml:"openAPIV3Schema"`
				} `yaml:"schema"`
			} `yaml:"versions"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("parse %s: %v", crdPath, err)
	}
	if len(crd.Spec.Versions) == 0 {
		t.Fatalf("%s has no versions", crdPath)
	}
	runSpec := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]
	specProps := runSpec.Properties
	syncoidProps := specProps["syncoid"].Properties

	if syncoidProps["noSyncSnap"].Type != "boolean" {
		t.Fatalf("noSyncSnap schema = %#v", syncoidProps["noSyncSnap"])
	}
	if syncoidProps["includeSnaps"].Type != "array" || syncoidProps["includeSnaps"].Items == nil || syncoidProps["includeSnaps"].Items.Type != "string" {
		t.Fatalf("includeSnaps schema = %#v", syncoidProps["includeSnaps"])
	}
	if syncoidProps["excludeSnaps"].Type != "array" || syncoidProps["excludeSnaps"].Items == nil || syncoidProps["excludeSnaps"].Items.Type != "string" {
		t.Fatalf("excludeSnaps schema = %#v", syncoidProps["excludeSnaps"])
	}
	if !hasValidationRule(runSpec.XKubernetesValidations, "self == oldSelf", "spec is immutable") {
		t.Fatalf("spec validations = %#v, want immutable spec rule", runSpec.XKubernetesValidations)
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

func hasValidationRule(rules []validationRule, rule, message string) bool {
	for _, candidate := range rules {
		if candidate.Rule == rule && candidate.Message == message {
			return true
		}
	}
	return false
}
