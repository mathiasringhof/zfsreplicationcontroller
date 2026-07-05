#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <version> <release-image>" >&2
}

if [[ $# -ne 2 ]]; then
  usage
  exit 2
fi

version="$1"
release_image="$2"

if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "release version must match vMAJOR.MINOR.PATCH[-PRERELEASE]" >&2
  exit 1
fi

if [[ "$release_image" != */* ]]; then
  echo "release image must include registry/repository" >&2
  exit 1
fi

image_name="${release_image##*/}"
image_without_digest="${release_image%@*}"
image_tag=""
if [[ "$image_name" == *:* ]]; then
  image_tag="${image_without_digest##*:}"
fi

case "$image_tag" in
  main)
    echo "release image must not use mutable tag main" >&2
    exit 1
    ;;
  latest)
    echo "release image must not use mutable tag latest" >&2
    exit 1
    ;;
esac

if [[ "$release_image" != *@sha256:* && "$image_tag" != "$version" ]]; then
  echo "release image must use digest or tag matching $version" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

cp -R "$repo_root/config" "$tmp_dir/config"
mkdir -p "$tmp_dir/release"

cat > "$tmp_dir/release/kustomization.yaml" <<'YAML'
resources:
  - ../config
patches:
  - path: release-deployment.yaml
  - path: release-daemonset.yaml
YAML

cat > "$tmp_dir/release/release-deployment.yaml" <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: zfsreplication-controller
  namespace: zfsreplication-system
  labels:
    app.kubernetes.io/version: ${version}
spec:
  template:
    metadata:
      labels:
        app.kubernetes.io/version: ${version}
    spec:
      containers:
        - name: manager
          image: ${release_image}
          imagePullPolicy: IfNotPresent
          env:
            - name: DATA_MOVER_IMAGE
              value: ${release_image}
YAML

cat > "$tmp_dir/release/release-daemonset.yaml" <<YAML
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: zfs-receiver
  namespace: zfsreplication-system
  labels:
    app.kubernetes.io/version: ${version}
spec:
  template:
    metadata:
      labels:
        app.kubernetes.io/version: ${version}
    spec:
      containers:
        - name: receiver
          image: ${release_image}
          imagePullPolicy: IfNotPresent
YAML

kubectl kustomize "$tmp_dir/release"
