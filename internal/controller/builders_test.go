package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestImagePullPolicyFor(t *testing.T) {
	tests := []struct {
		name  string
		image string
		want  corev1.PullPolicy
	}{
		{
			name:  "default tag",
			image: "ghcr.io/mathiasringhof/zfsreplicationcontroller",
			want:  corev1.PullAlways,
		},
		{
			name:  "latest tag",
			image: "ghcr.io/mathiasringhof/zfsreplicationcontroller:latest",
			want:  corev1.PullAlways,
		},
		{
			name:  "main tag",
			image: "ghcr.io/mathiasringhof/zfsreplicationcontroller:main",
			want:  corev1.PullAlways,
		},
		{
			name:  "commit tag",
			image: "ghcr.io/mathiasringhof/zfsreplicationcontroller:sha-8bacf3b",
			want:  corev1.PullIfNotPresent,
		},
		{
			name:  "registry port and tag",
			image: "registry.local:5000/zfsreplicationcontroller:v0.1.0",
			want:  corev1.PullIfNotPresent,
		},
		{
			name:  "digest",
			image: "ghcr.io/mathiasringhof/zfsreplicationcontroller@sha256:abc123",
			want:  corev1.PullIfNotPresent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := imagePullPolicyFor(tt.image); got != tt.want {
				t.Fatalf("imagePullPolicyFor(%q) = %q, want %q", tt.image, got, tt.want)
			}
		})
	}
}
