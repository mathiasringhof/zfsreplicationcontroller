package main

import (
	"bytes"
	"errors"
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
