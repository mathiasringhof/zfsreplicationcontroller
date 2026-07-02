package main

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

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
