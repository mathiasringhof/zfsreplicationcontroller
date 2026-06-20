#!/usr/bin/env bash
set -euo pipefail

E2E_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${E2E_DIR}/../.." && pwd)"
ARTIFACT_DIR="${E2E_ARTIFACT_DIR:-${E2E_DIR}/.artifacts}"

CONTROL_PLANE="${E2E_CONTROL_PLANE:-zrc-e2e-cp}"
WORKER_A="${E2E_WORKER_A:-worker-a}"
WORKER_B="${E2E_WORKER_B:-worker-b}"
E2E_NODES=("${CONTROL_PLANE}" "${WORKER_A}" "${WORKER_B}")
E2E_WORKERS=("${WORKER_A}" "${WORKER_B}")

K3S_VERSION="${E2E_K3S_VERSION:-v1.31.1+k3s1}"
KUBECONFIG_PATH="${E2E_KUBECONFIG:-${ARTIFACT_DIR}/kubeconfig}"
LIMA_NETWORK="${E2E_LIMA_NETWORK:-lima:user-v2}"
HOST_APISERVER_PORT="${E2E_HOST_APISERVER_PORT:-16443}"
K3S_SERVER_URL="${E2E_K3S_SERVER_URL:-https://lima-${CONTROL_PLANE}.internal:6443}"

DEFAULT_IMAGE_TAG="zfsreplicationcontroller:e2e"
DEFAULT_IMAGE_TAR="${ARTIFACT_DIR}/zfsreplicationcontroller-e2e.tar"
DEFAULT_IMAGE_DOCKERFILE="${REPO_ROOT}/Dockerfile"

IMAGE_TAG="${E2E_IMAGE_TAG:-${DEFAULT_IMAGE_TAG}}"
IMAGE_TAR="${E2E_IMAGE_TAR:-${DEFAULT_IMAGE_TAR}}"
IMAGE_DOCKERFILE="${E2E_IMAGE_DOCKERFILE:-${DEFAULT_IMAGE_DOCKERFILE}}"
REAL_ZFS_POOL="${E2E_REAL_ZFS_POOL:-tank}"
REAL_ZFS_ROOT="${E2E_REAL_ZFS_ROOT:-/var/lib/zfs-real}"
REAL_ZFS_SIZE="${E2E_REAL_ZFS_SIZE:-1024M}"

log() {
  printf '[e2e] %s\n' "$*" >&2
}

die() {
  printf '[e2e] error: %s\n' "$*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

ensure_artifact_dir() {
  mkdir -p "${ARTIFACT_DIR}"
}

limactl_cmd() {
  need_cmd limactl
  limactl "$@"
}

kubectl_cmd() {
  need_cmd kubectl
  kubectl --kubeconfig "${KUBECONFIG_PATH}" "$@"
}

instance_exists() {
  local names
  names="$(limactl_cmd list --format '{{.Name}}')"
  grep -Fxq "$1" <<< "${names}"
}

start_instance() {
  local name="$1"
  if instance_exists "${name}"; then
    log "Lima instance ${name} already exists"
    limactl_cmd start "${name}" >/dev/null
    return
  fi
  log "creating Lima instance ${name}"
  if [[ "${name}" == "${CONTROL_PLANE}" ]]; then
    limactl_cmd start --tty=false --name="${name}" --network="${LIMA_NETWORK}" --port-forward="${HOST_APISERVER_PORT}:6443" "${E2E_DIR}/lima/ubuntu.yaml"
  else
    limactl_cmd start --tty=false --name="${name}" --network="${LIMA_NETWORK}" "${E2E_DIR}/lima/ubuntu.yaml"
  fi
}

node_ip() {
  local name="$1"
  limactl_cmd shell "${name}" sh -lc "ip -4 -o addr show scope global | awk '{ split(\$4, a, \"/\"); if (a[1] !~ /^127\\./) { print a[1]; exit } }'"
}

run_on_node() {
  local name="$1"
  shift
  limactl_cmd shell "${name}" "$@"
}

copy_to_node() {
  local src="$1"
  local name="$2"
  local dst="$3"
  limactl_cmd copy "${src}" "${name}:${dst}"
}

copy_from_node() {
  local name="$1"
  local src="$2"
  local dst="$3"
  limactl_cmd copy "${name}:${src}" "${dst}"
}

wait_for_kubeconfig() {
  local raw="${ARTIFACT_DIR}/k3s.yaml"

  log "copying kubeconfig from ${CONTROL_PLANE}"
  copy_from_node "${CONTROL_PLANE}" /etc/rancher/k3s/k3s.yaml "${raw}"
  sed "s#https://127.0.0.1:6443#https://127.0.0.1:${HOST_APISERVER_PORT}#g" "${raw}" > "${KUBECONFIG_PATH}"
  chmod 0600 "${KUBECONFIG_PATH}"
}

wait_for_nodes() {
  log "waiting for Kubernetes nodes"
  kubectl_cmd wait --for=condition=Ready "node/${CONTROL_PLANE}" --timeout=300s
  kubectl_cmd wait --for=condition=Ready "node/${WORKER_A}" --timeout=300s
  kubectl_cmd wait --for=condition=Ready "node/${WORKER_B}" --timeout=300s
}
