package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestManagerMainReportsExpectedFailureOnce(t *testing.T) {
	var stderr bytes.Buffer
	code := managerMain(&stderr, func() error {
		return errors.New("create manager: control plane unavailable")
	})

	if code != 1 {
		t.Fatalf("managerMain() code = %d, want 1", code)
	}
	if got := stderr.String(); got != "create manager: control plane unavailable\n" {
		t.Fatalf("stderr = %q, want one concise fatal line", got)
	}
}

func TestRequiredReleaseImage(t *testing.T) {
	for _, image := range []string{
		"ghcr.io/mathiasringhof/zfsreplicationcontroller:v0.4.0",
		"ghcr.io/mathiasringhof/zfsreplicationcontroller@sha256:abc123",
		"zfsreplicationcontroller:main",
	} {
		t.Run(image, func(t *testing.T) {
			got, err := requiredReleaseImage(func(string) string { return image })
			if err != nil {
				t.Fatal(err)
			}
			if got != image {
				t.Fatalf("release image = %q, want exact reference %q", got, image)
			}
		})
	}

	if _, err := requiredReleaseImage(func(string) string { return "  " }); err == nil || !strings.Contains(err.Error(), "RELEASE_IMAGE") {
		t.Fatalf("missing release image error = %v", err)
	}
}

func TestManagerOptionsScopeCacheWhenWatchNamespaceSet(t *testing.T) {
	scheme := runtime.NewScheme()

	opts := managerOptions(scheme, "0", ":8081", "storage")
	if _, ok := opts.Cache.DefaultNamespaces["storage"]; !ok {
		t.Fatalf("DefaultNamespaces = %#v, missing storage", opts.Cache.DefaultNamespaces)
	}
}

func TestManagerOptionsWatchAllNamespacesByDefault(t *testing.T) {
	scheme := runtime.NewScheme()

	opts := managerOptions(scheme, "0", ":8081", "")
	if len(opts.Cache.DefaultNamespaces) != 0 {
		t.Fatalf("DefaultNamespaces = %#v, want empty all-namespaces cache", opts.Cache.DefaultNamespaces)
	}
}
