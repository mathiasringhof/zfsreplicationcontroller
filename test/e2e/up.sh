#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

ensure_artifact_dir
need_cmd limactl
need_cmd kubectl

for node in "${E2E_NODES[@]}"; do
  start_instance "${node}"
done

prepare_real_zfs_worker() {
  local node="$1"
  log "preparing real ZFS pool ${REAL_ZFS_POOL} on ${node}"
  run_on_node "${node}" sh -lc "
    set -eu
    sudo apt-get update
    sudo apt-get install -y --no-install-recommends zfsutils-linux
    if [ -e /dev/zfs ] && [ ! -c /dev/zfs ]; then
      sudo rm -f /dev/zfs
    fi
    sudo modprobe zfs
    sudo mkdir -p '${REAL_ZFS_ROOT}'
    if ! sudo zpool list -H -o name '${REAL_ZFS_POOL}' >/dev/null 2>&1; then
      sudo zpool import -d '${REAL_ZFS_ROOT}' '${REAL_ZFS_POOL}' >/dev/null 2>&1 || true
    fi
    if ! sudo zpool list -H -o name '${REAL_ZFS_POOL}' >/dev/null 2>&1; then
      sudo truncate -s '${REAL_ZFS_SIZE}' '${REAL_ZFS_ROOT}/${REAL_ZFS_POOL}.img'
      sudo zpool create -f -m none -O mountpoint=none '${REAL_ZFS_POOL}' '${REAL_ZFS_ROOT}/${REAL_ZFS_POOL}.img'
    fi
    sudo zfs set mountpoint=none '${REAL_ZFS_POOL}'
  "
}

for worker in "${E2E_WORKERS[@]}"; do
  prepare_real_zfs_worker "${worker}"
done

if ! run_on_node "${CONTROL_PLANE}" test -x /usr/local/bin/k3s; then
  log "installing k3s server on ${CONTROL_PLANE}"
  run_on_node "${CONTROL_PLANE}" sh -lc "curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION='${K3S_VERSION}' sh -s - server --node-name '${CONTROL_PLANE}' --disable traefik --write-kubeconfig-mode 0644"
else
  log "k3s server already installed on ${CONTROL_PLANE}"
fi

cp_ip="$(node_ip "${CONTROL_PLANE}")"
if [[ -z "${cp_ip}" ]]; then
  die "could not determine control-plane IP"
fi
log "control-plane IP is ${cp_ip}"
log "worker join URL is ${K3S_SERVER_URL}"

token="$(run_on_node "${CONTROL_PLANE}" sudo cat /var/lib/rancher/k3s/server/node-token)"
if [[ -z "${token}" ]]; then
  die "could not read k3s join token"
fi

for worker in "${E2E_WORKERS[@]}"; do
  if run_on_node "${worker}" test -x /usr/local/bin/k3s; then
    log "k3s agent already installed on ${worker}"
    continue
  fi
  log "installing k3s agent on ${worker}"
  run_on_node "${worker}" sh -lc "curl -sfL https://get.k3s.io | INSTALL_K3S_VERSION='${K3S_VERSION}' K3S_URL='${K3S_SERVER_URL}' K3S_TOKEN='${token}' sh -s - agent --node-name '${worker}'"
done

wait_for_kubeconfig
wait_for_nodes

kubectl_cmd label node "${WORKER_A}" zfsreplicationcontroller.e2e/source=true --overwrite
kubectl_cmd label node "${WORKER_B}" zfsreplicationcontroller.e2e/target=true --overwrite

log "cluster is ready"
log "kubeconfig: ${KUBECONFIG_PATH}"
