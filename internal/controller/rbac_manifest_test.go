package controller

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const smokeNamespace = "zfsreplication-smoke"

type validationRule struct {
	Rule    string `yaml:"rule"`
	Message string `yaml:"message"`
}

type manifestObject struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
	Spec struct {
		Template struct {
			Spec struct {
				Containers []manifestContainer `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type manifestContainer struct {
	Name string           `yaml:"name"`
	Args []string         `yaml:"args"`
	Env  []manifestEnvVar `yaml:"env"`
}

type manifestEnvVar struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
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

	verbs = verbsForResource(role.Rules, "zfsreplication.example.com", "zfsreceivetasks")
	for _, verb := range []string{"create", "get", "list", "watch"} {
		if !contains(verbs, verb) {
			t.Fatalf("zfsreceivetasks RBAC verbs = %v, missing %q", verbs, verb)
		}
	}

	verbs = verbsForResource(role.Rules, "zfsreplication.example.com", "zfsreplicationruns/status")
	for _, verb := range []string{"get", "update", "patch"} {
		if !contains(verbs, verb) {
			t.Fatalf("zfsreplicationruns/status RBAC verbs = %v, missing %q", verbs, verb)
		}
	}

	verbs = verbsForResource(role.Rules, "zfsreplication.example.com", "zfsreceivetasks/status")
	for _, verb := range []string{"get", "update", "patch"} {
		if !contains(verbs, verb) {
			t.Fatalf("zfsreceivetasks/status RBAC verbs = %v, missing %q", verbs, verb)
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

	verbs = verbsForResource(role.Rules, "", "pods")
	for _, verb := range []string{"get", "list", "watch", "delete"} {
		if !contains(verbs, verb) {
			t.Fatalf("pods RBAC verbs = %v, missing %q", verbs, verb)
		}
	}

	verbs = verbsForResource(role.Rules, "", "secrets")
	for _, verb := range []string{"create", "get", "list", "watch", "update", "patch", "delete"} {
		if !contains(verbs, verb) {
			t.Fatalf("secrets RBAC verbs = %v, missing %q", verbs, verb)
		}
	}
}

func TestNamespacedRBACRestrictsWorkloadPermissionsToWatchedNamespace(t *testing.T) {
	t.Helper()

	rolePath := filepath.Join("..", "..", "config", "rbac", "namespaced_role.yaml")
	data, err := os.ReadFile(rolePath)
	if err != nil {
		t.Fatalf("read %s: %v", rolePath, err)
	}

	var role struct {
		Kind     string `yaml:"kind"`
		Metadata struct {
			Namespace string `yaml:"namespace"`
		} `yaml:"metadata"`
		Rules []struct {
			APIGroups []string `yaml:"apiGroups"`
			Resources []string `yaml:"resources"`
			Verbs     []string `yaml:"verbs"`
		} `yaml:"rules"`
	}
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatalf("parse %s: %v", rolePath, err)
	}
	if role.Kind != "Role" {
		t.Fatalf("namespaced RBAC kind = %q, want Role", role.Kind)
	}
	if role.Metadata.Namespace != smokeNamespace {
		t.Fatalf("namespaced RBAC namespace = %q, want %s", role.Metadata.Namespace, smokeNamespace)
	}

	for _, tt := range []struct {
		apiGroup string
		resource string
		verbs    []string
	}{
		{apiGroup: "zfsreplication.example.com", resource: "zfsreplicationschedules", verbs: []string{"get", "list", "watch"}},
		{apiGroup: "zfsreplication.example.com", resource: "zfsreplicationruns", verbs: []string{"create", "get", "list", "watch"}},
		{apiGroup: "zfsreplication.example.com", resource: "zfsreceivetasks", verbs: []string{"create", "get", "list", "watch"}},
		{apiGroup: "zfsreplication.example.com", resource: "zfsreplicationruns/status", verbs: []string{"get", "update", "patch"}},
		{apiGroup: "zfsreplication.example.com", resource: "zfsreceivetasks/status", verbs: []string{"get", "update", "patch"}},
		{apiGroup: "zfsreplication.example.com", resource: "zfsreplicationschedules/status", verbs: []string{"get", "update", "patch"}},
		{apiGroup: "batch", resource: "jobs", verbs: []string{"create", "get", "list", "watch", "update", "patch", "delete"}},
		{apiGroup: "", resource: "secrets", verbs: []string{"create", "get", "list", "watch", "update", "patch", "delete"}},
		{apiGroup: "", resource: "pods", verbs: []string{"get", "list", "watch", "delete"}},
		{apiGroup: "", resource: "pods/log", verbs: []string{"get"}},
		{apiGroup: "", resource: "events", verbs: []string{"create", "patch"}},
	} {
		verbs := verbsForResource(role.Rules, tt.apiGroup, tt.resource)
		for _, verb := range tt.verbs {
			if !contains(verbs, verb) {
				t.Fatalf("%s/%s namespaced RBAC verbs = %v, missing %q", tt.apiGroup, tt.resource, verbs, verb)
			}
		}
	}
}

func TestReceiverNamespacedRBACRestrictsTaskPermissionsToWatchedNamespace(t *testing.T) {
	t.Helper()

	rolePath := filepath.Join("..", "..", "config", "rbac", "receiver_namespaced_role.yaml")
	data, err := os.ReadFile(rolePath)
	if err != nil {
		t.Fatalf("read %s: %v", rolePath, err)
	}

	var role struct {
		Kind     string `yaml:"kind"`
		Metadata struct {
			Namespace string `yaml:"namespace"`
		} `yaml:"metadata"`
		Rules []struct {
			APIGroups []string `yaml:"apiGroups"`
			Resources []string `yaml:"resources"`
			Verbs     []string `yaml:"verbs"`
		} `yaml:"rules"`
	}
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatalf("parse %s: %v", rolePath, err)
	}
	if role.Kind != "Role" {
		t.Fatalf("receiver namespaced RBAC kind = %q, want Role", role.Kind)
	}
	if role.Metadata.Namespace != smokeNamespace {
		t.Fatalf("receiver namespaced RBAC namespace = %q, want %s", role.Metadata.Namespace, smokeNamespace)
	}
	for _, tt := range []struct {
		resource string
		verbs    []string
	}{
		{resource: "zfsreceivetasks", verbs: []string{"get", "list", "watch"}},
		{resource: "zfsreceivetasks/status", verbs: []string{"get", "update", "patch"}},
	} {
		verbs := verbsForResource(role.Rules, "zfsreplication.example.com", tt.resource)
		for _, verb := range tt.verbs {
			if !contains(verbs, verb) {
				t.Fatalf("%s receiver namespaced RBAC verbs = %v, missing %q", tt.resource, verbs, verb)
			}
		}
	}
}

func TestNamespacedOverlayUsesNamespacedRBACAndWatchNamespace(t *testing.T) {
	t.Helper()

	kustomizationPath := filepath.Join("..", "..", "kustomization.yaml")
	data, err := os.ReadFile(kustomizationPath)
	if err != nil {
		t.Fatalf("read %s: %v", kustomizationPath, err)
	}
	var kustomization struct {
		Resources []string `yaml:"resources"`
	}
	if err := yaml.Unmarshal(data, &kustomization); err != nil {
		t.Fatalf("parse %s: %v", kustomizationPath, err)
	}
	for _, forbidden := range []string{"config/rbac/role.yaml", "config/rbac/role_binding.yaml"} {
		if contains(kustomization.Resources, forbidden) {
			t.Fatalf("namespaced overlay includes cluster-wide RBAC resource %q", forbidden)
		}
	}
	for _, required := range []string{
		"config/crd/zfsreplication.example.com_zfsreceivetasks.yaml",
		"config/rbac/namespaced_role.yaml",
		"config/rbac/namespaced_role_binding.yaml",
		"config/rbac/receiver_namespaced_role.yaml",
		"config/rbac/receiver_namespaced_role_binding.yaml",
		"config/receiver/daemonset.yaml",
	} {
		if !contains(kustomization.Resources, required) {
			t.Fatalf("namespaced overlay resources = %v, missing %q", kustomization.Resources, required)
		}
	}

	patchPath := filepath.Join("..", "..", "config", "namespaced", "manager_watch_namespace_patch.yaml")
	data, err = os.ReadFile(patchPath)
	if err != nil {
		t.Fatalf("read %s: %v", patchPath, err)
	}
	var deployment struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []manifestContainer `yaml:"containers"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(data, &deployment); err != nil {
		t.Fatalf("parse %s: %v", patchPath, err)
	}
	manager := findContainer(deployment.Spec.Template.Spec.Containers, "manager")
	if manager == nil {
		t.Fatalf("watch namespace patch has no manager container")
	}
	if !contains(manager.Args, "--watch-namespace=$(WATCH_NAMESPACE)") {
		t.Fatalf("manager args = %v, missing watch namespace arg", manager.Args)
	}
	if got := manifestEnvValue(manager.Env, "WATCH_NAMESPACE"); got != smokeNamespace {
		t.Fatalf("WATCH_NAMESPACE env = %q, want %s", got, smokeNamespace)
	}

	patchPath = filepath.Join("..", "..", "config", "namespaced", "receiver_watch_namespace_patch.yaml")
	data, err = os.ReadFile(patchPath)
	if err != nil {
		t.Fatalf("read %s: %v", patchPath, err)
	}
	var daemonSet struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []manifestContainer `yaml:"containers"`
				} `yaml:"spec"`
			} `yaml:"template"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(data, &daemonSet); err != nil {
		t.Fatalf("parse %s: %v", patchPath, err)
	}
	receiver := findContainer(daemonSet.Spec.Template.Spec.Containers, "receiver")
	if receiver == nil {
		t.Fatalf("receiver watch namespace patch has no receiver container")
	}
	if got := manifestEnvValue(receiver.Env, "WATCH_NAMESPACE"); got != smokeNamespace {
		t.Fatalf("receiver WATCH_NAMESPACE env = %q, want %s", got, smokeNamespace)
	}
}

func TestNamespacedOverlayRenderedManifestStaysNamespaced(t *testing.T) {
	t.Helper()

	objects := renderKustomize(t, filepath.Join("..", ".."))
	var seenRole, seenRoleBinding, seenDeployment, seenReceiverRole, seenReceiverRoleBinding, seenReceiverDaemonSet, seenSmokeNamespace bool
	for _, obj := range objects {
		switch obj.Kind {
		case "ClusterRole", "ClusterRoleBinding":
			t.Fatalf("namespaced overlay rendered forbidden %s/%s", obj.Kind, obj.Metadata.Name)
		case "Namespace":
			if obj.Metadata.Name == smokeNamespace {
				seenSmokeNamespace = true
			}
		case "Role":
			if obj.Metadata.Name == "zfsreplication-controller" {
				seenRole = true
				if obj.Metadata.Namespace != smokeNamespace {
					t.Fatalf("rendered Role namespace = %q, want %s", obj.Metadata.Namespace, smokeNamespace)
				}
			}
			if obj.Metadata.Name == "zfs-receiver" {
				seenReceiverRole = true
				if obj.Metadata.Namespace != smokeNamespace {
					t.Fatalf("rendered receiver Role namespace = %q, want %s", obj.Metadata.Namespace, smokeNamespace)
				}
			}
		case "RoleBinding":
			if obj.Metadata.Name == "zfsreplication-controller" {
				seenRoleBinding = true
				if obj.Metadata.Namespace != smokeNamespace {
					t.Fatalf("rendered RoleBinding namespace = %q, want %s", obj.Metadata.Namespace, smokeNamespace)
				}
			}
			if obj.Metadata.Name == "zfs-receiver" {
				seenReceiverRoleBinding = true
				if obj.Metadata.Namespace != smokeNamespace {
					t.Fatalf("rendered receiver RoleBinding namespace = %q, want %s", obj.Metadata.Namespace, smokeNamespace)
				}
			}
		case "Deployment":
			if obj.Metadata.Name == "zfsreplication-controller" {
				seenDeployment = true
				if obj.Metadata.Namespace != "zfsreplication-system" {
					t.Fatalf("rendered Deployment namespace = %q, want zfsreplication-system", obj.Metadata.Namespace)
				}
				manager := findContainer(obj.Spec.Template.Spec.Containers, "manager")
				if manager == nil {
					t.Fatalf("rendered Deployment has no manager container")
				}
				if got := manifestEnvValue(manager.Env, "WATCH_NAMESPACE"); got != smokeNamespace {
					t.Fatalf("rendered WATCH_NAMESPACE env = %q, want %s", got, smokeNamespace)
				}
				if !contains(manager.Args, "--watch-namespace=$(WATCH_NAMESPACE)") {
					t.Fatalf("rendered manager args = %v, missing watch namespace arg", manager.Args)
				}
			}
		case "DaemonSet":
			if obj.Metadata.Name == "zfs-receiver" {
				seenReceiverDaemonSet = true
				if obj.Metadata.Namespace != "zfsreplication-system" {
					t.Fatalf("rendered receiver DaemonSet namespace = %q, want zfsreplication-system", obj.Metadata.Namespace)
				}
				receiver := findContainer(obj.Spec.Template.Spec.Containers, "receiver")
				if receiver == nil {
					t.Fatalf("rendered receiver DaemonSet has no receiver container")
				}
				if got := manifestEnvValue(receiver.Env, "WATCH_NAMESPACE"); got != smokeNamespace {
					t.Fatalf("rendered receiver WATCH_NAMESPACE env = %q, want %s", got, smokeNamespace)
				}
			}
		}
	}
	for name, seen := range map[string]bool{
		"smoke Namespace":                 seenSmokeNamespace,
		"namespaced Role":                 seenRole,
		"namespaced RoleBinding":          seenRoleBinding,
		"watch-scoped manager Deployment": seenDeployment,
		"namespaced receiver Role":        seenReceiverRole,
		"namespaced receiver RoleBinding": seenReceiverRoleBinding,
		"watch-scoped receiver DaemonSet": seenReceiverDaemonSet,
	} {
		if !seen {
			t.Fatalf("rendered namespaced overlay missing %s", name)
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

func TestReceiveTaskCRDSchemaExposesMVP1Fields(t *testing.T) {
	t.Helper()

	crdPath := filepath.Join("..", "..", "config", "crd", "zfsreplication.example.com_zfsreceivetasks.yaml")
	data, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("read %s: %v", crdPath, err)
	}

	type schemaNode struct {
		Properties             map[string]schemaNode `yaml:"properties"`
		Required               []string              `yaml:"required"`
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
	spec := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]
	for _, required := range []string{"runRef", "nodeName", "destination", "ssh"} {
		if !contains(spec.Required, required) {
			t.Fatalf("receive task required fields = %v, missing %q", spec.Required, required)
		}
	}
	if spec.Properties["ssh"].Properties["authorizedPublicKey"].Type != "string" {
		t.Fatalf("authorizedPublicKey schema = %#v", spec.Properties["ssh"].Properties["authorizedPublicKey"])
	}
	if spec.Properties["ssh"].Properties["expiresAt"].Type != "string" {
		t.Fatalf("expiresAt schema = %#v", spec.Properties["ssh"].Properties["expiresAt"])
	}
	if spec.Properties["policy"].Properties["allowRollback"].Type != "boolean" {
		t.Fatalf("allowRollback schema = %#v", spec.Properties["policy"].Properties["allowRollback"])
	}
	if spec.Properties["policy"].Properties["receiveResumable"].Type != "boolean" {
		t.Fatalf("receiveResumable schema = %#v", spec.Properties["policy"].Properties["receiveResumable"])
	}
	if spec.Properties["policy"].Properties["allowSyncSnapshotDestroy"].Type != "boolean" {
		t.Fatalf("allowSyncSnapshotDestroy schema = %#v", spec.Properties["policy"].Properties["allowSyncSnapshotDestroy"])
	}
	if spec.Properties["policy"].Properties["compression"].Type != "string" {
		t.Fatalf("compression schema = %#v", spec.Properties["policy"].Properties["compression"])
	}
	status := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["status"]
	if status.Properties["endpoint"].Properties["host"].Type != "string" {
		t.Fatalf("endpoint.host schema = %#v", status.Properties["endpoint"].Properties["host"])
	}
	if status.Properties["ssh"].Properties["hostKey"].Type != "string" {
		t.Fatalf("ssh.hostKey schema = %#v", status.Properties["ssh"].Properties["hostKey"])
	}
	if !hasValidationRule(spec.XKubernetesValidations, "self == oldSelf", "spec is immutable") {
		t.Fatalf("receive task spec validations = %#v, want immutable spec rule", spec.XKubernetesValidations)
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

func findContainer(containers []manifestContainer, name string) *manifestContainer {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

func manifestEnvValue(env []manifestEnvVar, name string) string {
	for _, item := range env {
		if item.Name == name {
			return item.Value
		}
	}
	return ""
}

func renderKustomize(t *testing.T, path string) []manifestObject {
	t.Helper()

	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skipf("kubectl not found: %v", err)
	}
	cmd := exec.Command("kubectl", "kustomize", path)
	rendered, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl kustomize %s: %v\n%s", path, err, rendered)
	}

	var objects []manifestObject
	for _, doc := range strings.Split(string(rendered), "\n---") {
		doc = strings.TrimSpace(doc)
		if doc == "" {
			continue
		}
		var obj manifestObject
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("parse rendered manifest: %v\n%s", err, doc)
		}
		objects = append(objects, obj)
	}
	return objects
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
