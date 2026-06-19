#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

[[ -f "${KUBECONFIG_PATH}" ]] || die "kubeconfig not found: ${KUBECONFIG_PATH}; run test/e2e/up.sh first"

log "applying controller manifests"
kubectl_cmd apply -k "${REPO_ROOT}/config"

log "using image ${IMAGE_TAG}"
kubectl_cmd -n zfsreplication-system set image deployment/zfsreplication-controller "manager=${IMAGE_TAG}"
kubectl_cmd -n zfsreplication-system set env deployment/zfsreplication-controller "DATA_MOVER_IMAGE=${IMAGE_TAG}"
kubectl_cmd -n zfsreplication-system set env deployment/zfsreplication-controller "ZFS_SIMULATOR_STATE_HOSTPATH=/var/lib/zfs-sim"
kubectl_cmd -n zfsreplication-system rollout restart deployment/zfsreplication-controller

log "waiting for controller rollout"
kubectl_cmd -n zfsreplication-system rollout status deployment/zfsreplication-controller --timeout=180s
