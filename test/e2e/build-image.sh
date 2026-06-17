#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

ensure_artifact_dir

builder="${E2E_IMAGE_BUILDER:-auto}"

builder_works() {
  local cmd="$1"
  command -v "${cmd}" >/dev/null 2>&1 && "${cmd}" info >/dev/null 2>&1
}

choose_builder() {
  if [[ "${builder}" != "auto" ]]; then
    printf '%s\n' "${builder}"
    return
  fi
  if builder_works docker; then
    printf 'docker\n'
    return
  fi
  if builder_works podman; then
    printf 'podman\n'
    return
  fi
  if command -v limactl >/dev/null 2>&1 && instance_exists "${CONTROL_PLANE}"; then
    printf 'vm\n'
    return
  fi
  die "no usable image builder found: start Docker/Podman, or run test/e2e/up.sh first for VM build fallback"
}

build_with_host_builder() {
  local cmd="$1"
  log "building e2e image ${IMAGE_TAG} with ${cmd}"
  "${cmd}" build -f "${E2E_DIR}/image/Dockerfile.e2e" -t "${IMAGE_TAG}" "${REPO_ROOT}"

  log "saving image tar ${IMAGE_TAR}"
  "${cmd}" save "${IMAGE_TAG}" -o "${IMAGE_TAR}"
}

build_with_vm() {
  need_cmd limactl
  instance_exists "${CONTROL_PLANE}" || die "control-plane VM does not exist; run test/e2e/up.sh first"

  local src_tar="${ARTIFACT_DIR}/src.tar"
  log "packing repository for VM build"
  COPYFILE_DISABLE=1 tar --no-xattrs -C "${REPO_ROOT}" \
    --exclude .git \
    --exclude .gocache \
    --exclude .gomodcache \
    --exclude test/e2e/.artifacts \
    -cf "${src_tar}" .

  log "copying repository archive to ${CONTROL_PLANE}"
  copy_to_node "${src_tar}" "${CONTROL_PLANE}" /tmp/zrc-src.tar

  log "preparing buildah in ${CONTROL_PLANE}"
  run_on_node "${CONTROL_PLANE}" sh -lc "if ! command -v buildah >/dev/null 2>&1; then sudo apt-get update && sudo apt-get install -y --no-install-recommends buildah; fi"

  log "building e2e image ${IMAGE_TAG} inside ${CONTROL_PLANE}"
  run_on_node "${CONTROL_PLANE}" sh -lc "rm -rf /tmp/zrc-src && mkdir -p /tmp/zrc-src && tar -C /tmp/zrc-src -xf /tmp/zrc-src.tar && cd /tmp/zrc-src && sudo buildah bud --isolation chroot --format docker -f test/e2e/image/Dockerfile.e2e -t '${IMAGE_TAG}' ."

  log "exporting image archive inside ${CONTROL_PLANE}"
  run_on_node "${CONTROL_PLANE}" sudo rm -f /tmp/zrc-image.tar
  run_on_node "${CONTROL_PLANE}" sh -lc "sudo buildah push '${IMAGE_TAG}' docker-archive:/tmp/zrc-image.tar:'${IMAGE_TAG}'"

  log "copying image tar to ${IMAGE_TAR}"
  copy_from_node "${CONTROL_PLANE}" /tmp/zrc-image.tar "${IMAGE_TAR}"
}

selected="$(choose_builder)"
case "${selected}" in
  docker|podman)
    build_with_host_builder "${selected}"
    ;;
  vm)
    build_with_vm
    ;;
  *)
    die "unknown image builder ${selected}"
    ;;
esac
