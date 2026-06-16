#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

need_cmd limactl
[[ -f "${IMAGE_TAR}" ]] || die "image tar does not exist: ${IMAGE_TAR}; run test/e2e/build-image.sh first"

for node in "${E2E_NODES[@]}"; do
  log "copying image to ${node}"
  copy_to_node "${IMAGE_TAR}" "${node}" /tmp/zfsreplicationcontroller-e2e.tar
  log "importing image into k3s on ${node}"
  run_on_node "${node}" sudo k3s ctr images import /tmp/zfsreplicationcontroller-e2e.tar
done
