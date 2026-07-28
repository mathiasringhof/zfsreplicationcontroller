package release_test

import (
	"strings"
	"testing"

	"github.com/mathias/zfsreplicationcontroller/internal/release"
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
