#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

[[ -f "${KUBECONFIG_PATH}" ]] || die "kubeconfig not found: ${KUBECONFIG_PATH}; run test/e2e/up.sh first"

kubectl_cmd get nodes -o wide
kubectl_cmd get pods -A -o wide
kubectl_cmd get jobs -A || true
kubectl_cmd get zfsreplications -A || true
