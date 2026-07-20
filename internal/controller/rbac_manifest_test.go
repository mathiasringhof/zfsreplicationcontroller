package controller

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const smokeNamespace = "zfsreplication-smoke"

type validationRule struct {
	Rule    string `yaml:"rule"`
	Message string `yaml:"message"`
}

type crdSchemaProperty struct {
	Type       string                       `yaml:"type"`
	Format     string                       `yaml:"format"`
	Default    any                          `yaml:"default"`
	Minimum    *int64                       `yaml:"minimum"`
	Properties map[string]crdSchemaProperty `yaml:"properties"`
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
	Rules    []manifestPolicyRule `yaml:"rules"`
	RoleRef  manifestRoleRef      `yaml:"roleRef"`
	Subjects []manifestSubject    `yaml:"subjects"`
}

type manifestRoleRef struct {
	Kind string `yaml:"kind"`
	Name string `yaml:"name"`
}

type manifestSubject struct {
	Kind      string `yaml:"kind"`
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
}

type manifestPolicyRule struct {
	APIGroups []string `yaml:"apiGroups"`
	Resources []string `yaml:"resources"`
	Verbs     []string `yaml:"verbs"`
}

type manifestContainer struct {
	Image          string           `yaml:"image"`
	Name           string           `yaml:"name"`
	Args           []string         `yaml:"args"`
	Env            []manifestEnvVar `yaml:"env"`
	ReadinessProbe manifestProbe    `yaml:"readinessProbe"`
}

func TestRenderedRuntimeUsesOneReleaseImage(t *testing.T) {
	objects := renderKustomize(t, filepath.Join("..", "..", "config"))
	var manager, receiver *manifestContainer
	for _, obj := range objects {
		switch obj.Kind {
		case "Deployment":
			if obj.Metadata.Name == "zfsreplication-controller" {
				manager = findContainer(obj.Spec.Template.Spec.Containers, "manager")
			}
		case "DaemonSet":
			if obj.Metadata.Name == "zfs-receiver" {
				receiver = findContainer(obj.Spec.Template.Spec.Containers, "receiver")
			}
		}
	}
	if manager == nil || receiver == nil {
		t.Fatalf("rendered manager = %v, receiver = %v", manager != nil, receiver != nil)
	}
	if manager.Image == "" || receiver.Image != manager.Image {
		t.Fatalf("manager image = %q, receiver image = %q", manager.Image, receiver.Image)
	}
	if got := manifestEnvValue(manager.Env, "RELEASE_IMAGE"); got != manager.Image {
		t.Fatalf("RELEASE_IMAGE = %q, want manager image %q", got, manager.Image)
	}
	if got := manifestEnvValue(manager.Env, "DATA_MOVER_IMAGE"); got != "" {
		t.Fatalf("DATA_MOVER_IMAGE = %q, want removed", got)
	}
	for _, arg := range manager.Args {
		if strings.Contains(arg, "datamover-image") {
			t.Fatalf("manager args contain independent image override: %v", manager.Args)
		}
	}
}

type manifestProbe struct {
	Exec      *manifestExecAction      `yaml:"exec"`
	TCPSocket *manifestTCPSocketAction `yaml:"tcpSocket"`
}

type manifestExecAction struct {
	Command []string `yaml:"command"`
}

type manifestTCPSocketAction struct {
	Port string `yaml:"port"`
}

type manifestEnvVar struct {
	Name      string               `yaml:"name"`
	Value     string               `yaml:"value"`
	ValueFrom manifestEnvVarSource `yaml:"valueFrom"`
}

type manifestEnvVarSource struct {
	FieldRef manifestObjectFieldSelector `yaml:"fieldRef"`
}

type manifestObjectFieldSelector struct {
	FieldPath string `yaml:"fieldPath"`
}

func TestControllerClusterRoleHasRequiredPermissions(t *testing.T) {
	t.Helper()

	rolePath := filepath.Join("..", "..", "config", "rbac", "role.yaml")
	data, err := os.ReadFile(rolePath)
	if err != nil {
		t.Fatalf("read %s: %v", rolePath, err)
	}

	var role struct {
		Rules []manifestPolicyRule `yaml:"rules"`
	}
	if err := yaml.Unmarshal(data, &role); err != nil {
		t.Fatalf("parse %s: %v", rolePath, err)
	}

	verbs := verbsForResource(role.Rules, "zfsreplication.ringhof.io", "zfsreplicationruns")
	for _, verb := range []string{"create", "get", "list", "watch", "delete"} {
		if !contains(verbs, verb) {
			t.Fatalf("zfsreplicationruns RBAC verbs = %v, missing %q", verbs, verb)
		}
	}

	verbs = verbsForResource(role.Rules, "zfsreplication.ringhof.io", "zfsreceivetasks")
	for _, verb := range []string{"create", "get", "list", "watch"} {
		if !contains(verbs, verb) {
			t.Fatalf("zfsreceivetasks RBAC verbs = %v, missing %q", verbs, verb)
		}
	}

	verbs = verbsForResource(role.Rules, "zfsreplication.ringhof.io", "zfsreplicationruns/status")
	for _, verb := range []string{"get", "update", "patch"} {
		if !contains(verbs, verb) {
			t.Fatalf("zfsreplicationruns/status RBAC verbs = %v, missing %q", verbs, verb)
		}
	}

	verbs = verbsForResource(role.Rules, "zfsreplication.ringhof.io", "zfsreceivetasks/status")
	for _, verb := range []string{"get", "update", "patch"} {
		if !contains(verbs, verb) {
			t.Fatalf("zfsreceivetasks/status RBAC verbs = %v, missing %q", verbs, verb)
		}
	}

	verbs = verbsForResource(role.Rules, "zfsreplication.ringhof.io", "zfsreplicationschedules/status")
	for _, verb := range []string{"get", "update", "patch"} {
		if !contains(verbs, verb) {
			t.Fatalf("zfsreplicationschedules/status RBAC verbs = %v, missing %q", verbs, verb)
		}
	}

	verbs = verbsForResource(role.Rules, "", "pods/log")
	if len(verbs) != 0 {
		t.Fatalf("pods/log RBAC verbs = %v, want no Pod log access", verbs)
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

func TestRenderedControllerPodReadRBACIsLeastPrivilege(t *testing.T) {
	for _, tt := range []struct {
		name     string
		path     string
		roleKind string
	}{
		{name: "cluster-wide", path: filepath.Join("..", "..", "config"), roleKind: "ClusterRole"},
		{name: "namespaced", path: filepath.Join("..", ".."), roleKind: "Role"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			objects := renderKustomize(t, tt.path)
			var role *manifestObject
			var manager *manifestContainer
			for i := range objects {
				obj := &objects[i]
				if obj.Kind == tt.roleKind && obj.Metadata.Name == "zfsreplication-controller" {
					role = obj
				}
				if obj.Kind == "Deployment" && obj.Metadata.Name == "zfsreplication-controller" {
					manager = findContainer(obj.Spec.Template.Spec.Containers, "manager")
				}
			}
			if role == nil {
				t.Fatalf("rendered %s has no zfsreplication-controller %s", tt.name, tt.roleKind)
			}

			podVerbs := verbsForResource(role.Rules, "", "pods")
			for _, verb := range []string{"get", "list", "watch"} {
				if !contains(podVerbs, verb) {
					t.Fatalf("rendered %s Pod verbs = %v, missing read verb %q", tt.name, podVerbs, verb)
				}
			}
			for _, verb := range []string{"create", "update", "patch"} {
				if contains(podVerbs, verb) {
					t.Fatalf("rendered %s Pod verbs = %v, includes unnecessary mutation verb %q", tt.name, podVerbs, verb)
				}
			}
			if logVerbs := verbsForResource(role.Rules, "", "pods/log"); len(logVerbs) != 0 {
				t.Fatalf("rendered %s pods/log verbs = %v, want none", tt.name, logVerbs)
			}
			if manager == nil {
				t.Fatalf("rendered %s has no manager container", tt.name)
			}
			podNamespace := findManifestEnv(manager.Env, "POD_NAMESPACE")
			if podNamespace == nil || podNamespace.ValueFrom.FieldRef.FieldPath != "metadata.namespace" {
				t.Fatalf("rendered %s POD_NAMESPACE = %#v, want metadata.namespace fieldRef", tt.name, podNamespace)
			}

			if tt.name == "namespaced" {
				var receiverPodRole *manifestObject
				var receiverPodBinding *manifestObject
				for i := range objects {
					obj := &objects[i]
					if obj.Kind == "Role" && obj.Metadata.Name == "zfsreplication-controller-receiver-pod-reader" {
						receiverPodRole = obj
					}
					if obj.Kind == "RoleBinding" && obj.Metadata.Name == "zfsreplication-controller-receiver-pod-reader" {
						receiverPodBinding = obj
					}
				}
				if receiverPodRole == nil {
					t.Fatal("rendered namespaced install has no Receiver Pod reader Role")
				}
				if got := verbsForResource(receiverPodRole.Rules, "", "pods"); !slices.Equal(got, []string{"get"}) {
					t.Fatalf("rendered Receiver Pod reader verbs = %v, want [get]", got)
				}
				assertRenderedRoleBinding(t, receiverPodBinding, "Role", receiverPodRole.Metadata.Name)
			} else {
				var binding *manifestObject
				for i := range objects {
					obj := &objects[i]
					if obj.Kind == "ClusterRoleBinding" && obj.Metadata.Name == "zfsreplication-controller" {
						binding = obj
						break
					}
				}
				assertRenderedRoleBinding(t, binding, "ClusterRole", role.Metadata.Name)
			}
		})
	}
}

func TestScheduleCRDHistoryLimitsHaveCronJobDefaults(t *testing.T) {
	t.Helper()

	crdPath := filepath.Join("..", "..", "config", "crd", "zfsreplication.ringhof.io_zfsreplicationschedules.yaml")
	data, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("read %s: %v", crdPath, err)
	}

	var crd struct {
		Spec struct {
			Versions []struct {
				Schema struct {
					OpenAPIV3Schema crdSchemaProperty `yaml:"openAPIV3Schema"`
				} `yaml:"schema"`
			} `yaml:"versions"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("parse %s: %v", crdPath, err)
	}
	if len(crd.Spec.Versions) != 1 {
		t.Fatalf("CRD versions = %d, want 1", len(crd.Spec.Versions))
	}

	specProps := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties
	for _, tt := range []struct {
		name        string
		wantDefault int64
	}{
		{name: "successfulRunsHistoryLimit", wantDefault: 3},
		{name: "failedRunsHistoryLimit", wantDefault: 1},
	} {
		prop, ok := specProps[tt.name]
		if !ok {
			t.Fatalf("schedule CRD spec properties missing %s", tt.name)
		}
		if prop.Type != "integer" {
			t.Fatalf("%s type = %q, want integer", tt.name, prop.Type)
		}
		if prop.Format != "int32" {
			t.Fatalf("%s format = %q, want int32", tt.name, prop.Format)
		}
		defaultValue, ok := prop.Default.(int)
		if !ok || int64(defaultValue) != tt.wantDefault {
			t.Fatalf("%s default = %#v, want %d", tt.name, prop.Default, tt.wantDefault)
		}
		if prop.Minimum == nil || *prop.Minimum != 0 {
			t.Fatalf("%s minimum = %v, want 0", tt.name, prop.Minimum)
		}
	}
}

func TestReceiverDaemonSetReadinessProbeDoesNotOpenSSHConnection(t *testing.T) {
	t.Helper()

	daemonSetPath := filepath.Join("..", "..", "config", "receiver", "daemonset.yaml")
	data, err := os.ReadFile(daemonSetPath)
	if err != nil {
		t.Fatalf("read %s: %v", daemonSetPath, err)
	}

	var daemonSet manifestObject
	if err := yaml.Unmarshal(data, &daemonSet); err != nil {
		t.Fatalf("parse %s: %v", daemonSetPath, err)
	}

	receiver := findContainer(daemonSet.Spec.Template.Spec.Containers, "receiver")
	if receiver == nil {
		t.Fatalf("receiver DaemonSet has no receiver container")
	}
	if receiver.ReadinessProbe.TCPSocket != nil {
		t.Fatalf("receiver readiness probe opens SSH port %q; want quiet exec probe", receiver.ReadinessProbe.TCPSocket.Port)
	}
	if receiver.ReadinessProbe.Exec == nil {
		t.Fatalf("receiver readiness probe has no exec command")
	}

	command := strings.Join(receiver.ReadinessProbe.Exec.Command, " ")
	for _, want := range []string{
		"/bin/sh",
		"-ec",
		"/run/zfs-receiver/sshd.pid",
		"/usr/sbin/sshd -t -f /run/zfs-receiver/sshd_config",
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("receiver readiness exec command = %q, missing %q", command, want)
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
		Rules []manifestPolicyRule `yaml:"rules"`
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
		{apiGroup: "zfsreplication.ringhof.io", resource: "zfsreplicationschedules", verbs: []string{"get", "list", "watch"}},
		{apiGroup: "zfsreplication.ringhof.io", resource: "zfsreplicationruns", verbs: []string{"create", "get", "list", "watch", "delete"}},
		{apiGroup: "zfsreplication.ringhof.io", resource: "zfsreceivetasks", verbs: []string{"create", "get", "list", "watch"}},
		{apiGroup: "zfsreplication.ringhof.io", resource: "zfsreplicationruns/status", verbs: []string{"get", "update", "patch"}},
		{apiGroup: "zfsreplication.ringhof.io", resource: "zfsreceivetasks/status", verbs: []string{"get", "update", "patch"}},
		{apiGroup: "zfsreplication.ringhof.io", resource: "zfsreplicationschedules/status", verbs: []string{"get", "update", "patch"}},
		{apiGroup: "batch", resource: "jobs", verbs: []string{"create", "get", "list", "watch", "update", "patch", "delete"}},
		{apiGroup: "", resource: "secrets", verbs: []string{"create", "get", "list", "watch", "update", "patch", "delete"}},
		{apiGroup: "", resource: "pods", verbs: []string{"get", "list", "watch", "delete"}},
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
		Rules []manifestPolicyRule `yaml:"rules"`
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
		verbs := verbsForResource(role.Rules, "zfsreplication.ringhof.io", tt.resource)
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
		"config/crd/zfsreplication.ringhof.io_zfsreceivetasks.yaml",
		"config/rbac/namespaced_role.yaml",
		"config/rbac/namespaced_role_binding.yaml",
		"config/rbac/receiver_pod_reader_role.yaml",
		"config/rbac/receiver_pod_reader_role_binding.yaml",
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

	crdPath := filepath.Join("..", "..", "config", "crd", "zfsreplication.ringhof.io_zfsreplicationruns.yaml")
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
	if syncoidProps["deleteTargetSnapshots"].Type != "boolean" || syncoidProps["deleteTargetSnapshots"].Default != false {
		t.Fatalf("deleteTargetSnapshots schema = %#v", syncoidProps["deleteTargetSnapshots"])
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
	statusProps := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["status"].Properties
	for _, field := range []string{"senderJobName", "receiveTaskName", "receiverPodName", "receiverPodIP", "sshSecretName"} {
		if statusProps[field].Type != "string" {
			t.Fatalf("status.%s schema = %#v, want string", field, statusProps[field])
		}
	}
}

func TestReceiveTaskCRDSchemaExposesMVP1Fields(t *testing.T) {
	t.Helper()

	crdPath := filepath.Join("..", "..", "config", "crd", "zfsreplication.ringhof.io_zfsreceivetasks.yaml")
	data, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("read %s: %v", crdPath, err)
	}

	type schemaNode struct {
		Properties             map[string]schemaNode `yaml:"properties"`
		Default                any                   `yaml:"default"`
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
	if spec.Properties["policy"].Properties["allowTargetSnapshotDestroy"].Type != "boolean" {
		t.Fatalf("allowTargetSnapshotDestroy schema = %#v", spec.Properties["policy"].Properties["allowTargetSnapshotDestroy"])
	}
	if spec.Properties["policy"].Properties["allowTargetSnapshotDestroy"].Default != false {
		t.Fatalf("allowTargetSnapshotDestroy default = %#v, want false", spec.Properties["policy"].Properties["allowTargetSnapshotDestroy"].Default)
	}
	if spec.Properties["policy"].Properties["syncSnapshotIdentifier"].Type != "string" {
		t.Fatalf("syncSnapshotIdentifier schema = %#v", spec.Properties["policy"].Properties["syncSnapshotIdentifier"])
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
	if !hasValidationRule(spec.XKubernetesValidations, "self.runRef == oldSelf.runRef && self.nodeName == oldSelf.nodeName && self.destination == oldSelf.destination && self.policy == oldSelf.policy && self.ssh.authorizedPublicKey == oldSelf.ssh.authorizedPublicKey", "only spec.ssh.expiresAt may change") {
		t.Fatalf("receive task spec validations = %#v, want all fields except expiresAt immutable", spec.XKubernetesValidations)
	}
	if !hasValidationRule(spec.XKubernetesValidations, "timestamp(self.ssh.expiresAt) >= timestamp(oldSelf.ssh.expiresAt)", "spec.ssh.expiresAt may only move forward") {
		t.Fatalf("receive task spec validations = %#v, want monotonic expiresAt rule", spec.XKubernetesValidations)
	}
}

func TestScheduleCRDSchemaExposesSyncoidDeleteTargetSnapshots(t *testing.T) {
	t.Helper()

	crdPath := filepath.Join("..", "..", "config", "crd", "zfsreplication.ringhof.io_zfsreplicationschedules.yaml")
	data, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("read %s: %v", crdPath, err)
	}

	type schemaNode struct {
		Default    any                   `yaml:"default"`
		Properties map[string]schemaNode `yaml:"properties"`
		Type       string                `yaml:"type"`
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
	runTemplate := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties["runTemplate"]
	syncoidProps := runTemplate.Properties["syncoid"].Properties
	if syncoidProps["deleteTargetSnapshots"].Type != "boolean" || syncoidProps["deleteTargetSnapshots"].Default != false {
		t.Fatalf("schedule deleteTargetSnapshots schema = %#v", syncoidProps["deleteTargetSnapshots"])
	}
}

func verbsForResource(rules []manifestPolicyRule, apiGroup, resource string) []string {
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

func findManifestEnv(env []manifestEnvVar, name string) *manifestEnvVar {
	for i := range env {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
}

func assertRenderedRoleBinding(t *testing.T, binding *manifestObject, roleKind, roleName string) {
	t.Helper()
	if binding == nil {
		t.Fatalf("rendered manifests have no binding for %s %s", roleKind, roleName)
	}
	if binding.RoleRef.Kind != roleKind || binding.RoleRef.Name != roleName {
		t.Fatalf("rendered binding roleRef = %#v, want %s %s", binding.RoleRef, roleKind, roleName)
	}
	want := manifestSubject{Kind: "ServiceAccount", Name: "zfsreplication-controller", Namespace: "zfsreplication-system"}
	if !slices.Contains(binding.Subjects, want) {
		t.Fatalf("rendered binding subjects = %#v, missing %#v", binding.Subjects, want)
	}
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
	return slices.Contains(items, needle)
}

func hasValidationRule(rules []validationRule, rule, message string) bool {
	for _, candidate := range rules {
		if candidate.Rule == rule && candidate.Message == message {
			return true
		}
	}
	return false
}
