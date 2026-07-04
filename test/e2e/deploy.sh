#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

[[ -f "${KUBECONFIG_PATH}" ]] || die "kubeconfig not found: ${KUBECONFIG_PATH}; run test/e2e/up.sh first"

log "applying controller manifests"
kubectl_cmd apply -k "${REPO_ROOT}/config"

log "using image ${IMAGE_TAG}"
kubectl_cmd -n zfsreplication-system set image deployment/zfsreplication-controller "manager=${IMAGE_TAG}"
kubectl_cmd -n zfsreplication-system set image daemonset/zfs-receiver "receiver=${IMAGE_TAG}"
kubectl_cmd -n zfsreplication-system patch deployment/zfsreplication-controller --type=json -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]'
kubectl_cmd -n zfsreplication-system patch daemonset/zfs-receiver --type=json -p='[{"op":"replace","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]'
kubectl_cmd -n zfsreplication-system set env deployment/zfsreplication-controller "DATA_MOVER_IMAGE=${IMAGE_TAG}"
kubectl_cmd -n zfsreplication-system rollout restart deployment/zfsreplication-controller
kubectl_cmd -n zfsreplication-system rollout restart daemonset/zfs-receiver

log "waiting for controller and receiver rollouts"
kubectl_cmd -n zfsreplication-system rollout status deployment/zfsreplication-controller --timeout=180s
kubectl_cmd -n zfsreplication-system rollout status daemonset/zfs-receiver --timeout=180s
