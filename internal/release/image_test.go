package release_test

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/mathias/zfsreplicationcontroller/internal/release"
	"gopkg.in/yaml.v3"
)

func TestRequiredImage(t *testing.T) {
	for _, image := range []string{
		"ghcr.io/mathiasringhof/zfsreplicationcontroller:v0.4.0",
		"ghcr.io/mathiasringhof/zfsreplicationcontroller@sha256:abc123",
		"zfsreplicationcontroller:main",
	} {
		t.Run(image, func(t *testing.T) {
			got, err := release.RequiredImage(func(string) string { return image })
			if err != nil {
				t.Fatal(err)
			}
			if got != image {
				t.Fatalf("release image = %q, want exact reference %q", got, image)
			}
		})
	}

	if _, err := release.RequiredImage(func(string) string { return "  " }); err == nil || !strings.Contains(err.Error(), "RELEASE_IMAGE") {
		t.Fatalf("missing release image error = %v", err)
	}
}

func TestRenderedRuntimeUsesOneReleaseImage(t *testing.T) {
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skipf("kubectl not found: %v", err)
	}
	rendered, err := exec.Command("kubectl", "kustomize", "../../config").CombinedOutput()
	if err != nil {
		t.Fatalf("render config: %v\n%s", err, rendered)
	}

	var manager, receiver *releaseManifestContainer
	for _, document := range strings.Split(string(rendered), "\n---") {
		if strings.TrimSpace(document) == "" {
			continue
		}
		var object releaseManifestObject
		if err := yaml.Unmarshal([]byte(document), &object); err != nil {
			t.Fatalf("parse rendered manifest: %v", err)
		}
		switch {
		case object.Kind == "Deployment" && object.Metadata.Name == "zfsreplication-controller":
			manager = releaseContainer(object.Spec.Template.Spec.Containers, "manager")
		case object.Kind == "DaemonSet" && object.Metadata.Name == "zfs-receiver":
			receiver = releaseContainer(object.Spec.Template.Spec.Containers, "receiver")
		}
	}
	if manager == nil || receiver == nil {
		t.Fatalf("rendered manager = %v, receiver = %v", manager != nil, receiver != nil)
	}
	if manager.Image == "" || receiver.Image != manager.Image {
		t.Fatalf("manager image = %q, receiver image = %q", manager.Image, receiver.Image)
	}
	if got := releaseEnvironmentValue(manager.Env, "RELEASE_IMAGE"); got != manager.Image {
		t.Fatalf("RELEASE_IMAGE = %q, want manager image %q", got, manager.Image)
	}
	if got := releaseEnvironmentValue(manager.Env, "DATA_MOVER_IMAGE"); got != "" {
		t.Fatalf("DATA_MOVER_IMAGE = %q, want removed", got)
	}
	for _, argument := range manager.Args {
		if strings.Contains(argument, "datamover-image") {
			t.Fatalf("manager args contain independent image override: %v", manager.Args)
		}
	}
}

type releaseManifestObject struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Template struct {
			Spec struct {
				Containers []releaseManifestContainer `yaml:"containers"`
			} `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

type releaseManifestContainer struct {
	Name  string                       `yaml:"name"`
	Image string                       `yaml:"image"`
	Args  []string                     `yaml:"args"`
	Env   []releaseManifestEnvironment `yaml:"env"`
}

type releaseManifestEnvironment struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

func releaseContainer(containers []releaseManifestContainer, name string) *releaseManifestContainer {
	for index := range containers {
		if containers[index].Name == name {
			return &containers[index]
		}
	}
	return nil
}

func releaseEnvironmentValue(environment []releaseManifestEnvironment, name string) string {
	for _, entry := range environment {
		if entry.Name == name {
			return entry.Value
		}
	}
	return ""
}
