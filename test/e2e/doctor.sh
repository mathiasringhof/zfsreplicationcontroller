#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

missing=0

check_cmd() {
  local cmd="$1"
  if command -v "${cmd}" >/dev/null 2>&1; then
    log "found ${cmd}: $(command -v "${cmd}")"
  else
    log "missing ${cmd}"
    missing=1
  fi
}

check_cmd limactl
check_cmd kubectl

if command -v docker >/dev/null 2>&1; then
  if docker info >/dev/null 2>&1; then
    log "found usable docker: $(command -v docker)"
  else
    log "found docker but it is not currently usable"
  fi
elif command -v podman >/dev/null 2>&1; then
  if podman info >/dev/null 2>&1; then
    log "found usable podman: $(command -v podman)"
  else
    log "found podman but it is not currently usable; build-image.sh can fall back to VM build after up.sh"
  fi
else
  log "missing docker or podman; build-image.sh can fall back to VM build after up.sh"
fi

if command -v limactl >/dev/null 2>&1; then
  if limactl network ls >/dev/null 2>&1; then
    log "Lima network command is available"
  else
    log "could not inspect Lima networks; shared networking may need host setup"
    missing=1
  fi
fi

if [[ "${missing}" -ne 0 ]]; then
  die "e2e VM environment prerequisites are not satisfied"
fi

log "e2e VM environment prerequisites look good"
