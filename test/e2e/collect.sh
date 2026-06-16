#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

dest="${1:-${ARTIFACT_DIR}/collected}"
mkdir -p "${dest}"

if [[ -f "${KUBECONFIG_PATH}" ]] && command -v kubectl >/dev/null 2>&1; then
  log "collecting Kubernetes state into ${dest}"
  kubectl_cmd get all -A -o wide > "${dest}/all.txt" || true
  kubectl_cmd get events -A --sort-by=.lastTimestamp > "${dest}/events.txt" || true
  kubectl_cmd get zfsreplications -A -o yaml > "${dest}/zfsreplications.yaml" || true
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
      log "collecting zfs simulator events from ${node}"
      run_on_node "${node}" sh -lc "sudo test -f /var/lib/zfs-sim/events.jsonl && sudo cat /var/lib/zfs-sim/events.jsonl || true" > "${dest}/${node}-zfs-events.jsonl" || true
      run_on_node "${node}" sh -lc "sudo find /var/lib/zfs-sim -maxdepth 3 -type f -print 2>/dev/null || true" > "${dest}/${node}-zfs-files.txt" || true
    fi
  done
fi

log "collected artifacts in ${dest}"
