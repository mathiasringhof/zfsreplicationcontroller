#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

dest="${1:-${ARTIFACT_DIR}/collected}"
mkdir -p "${dest}"

if [[ -f "${KUBECONFIG_PATH}" ]] && command -v kubectl >/dev/null 2>&1; then
  log "collecting Kubernetes state into ${dest}"
  kubectl_cmd get all -A -o wide > "${dest}/all.txt" || true
  kubectl_cmd get events -A --sort-by=.lastTimestamp > "${dest}/events.txt" || true
  kubectl_cmd get zfsreceivetasks -A -o yaml > "${dest}/zfsreceivetasks.yaml" || true
  kubectl_cmd get zfsreplicationruns -A -o yaml > "${dest}/zfsreplicationruns.yaml" || true
  kubectl_cmd get zfsreplicationschedules -A -o yaml > "${dest}/zfsreplicationschedules.yaml" || true
  kubectl_cmd logs -n zfsreplication-system deployment/zfsreplication-controller > "${dest}/manager.log" || true
  kubectl_cmd get pods -A -o yaml > "${dest}/pods.yaml" || true
  while read -r namespace pod; do
    [[ -n "${namespace}" && -n "${pod}" ]] || continue
    kubectl_cmd logs -n "${namespace}" "${pod}" --all-containers=true > "${dest}/${namespace}-${pod}.log" || true
  done < <(kubectl_cmd get pods -A -o jsonpath='{range .items[*]}{.metadata.namespace}{" "}{.metadata.name}{"\n"}{end}' || true)
fi

if command -v limactl >/dev/null 2>&1; then
  for node in "${E2E_NODES[@]}"; do
    if instance_exists "${node}"; then
      log "collecting real ZFS state from ${node}"
      run_on_node "${node}" sh -lc "sudo zpool status || true; sudo zpool list || true; sudo zfs list -t all || true" > "${dest}/${node}-zfs-state.txt" || true
    fi
  done
fi

log "collected artifacts in ${dest}"
