#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

need_cmd limactl

mode="${1:-delete}"
case "${mode}" in
  delete|destroy)
    for node in "${E2E_NODES[@]}"; do
      if instance_exists "${node}"; then
        log "deleting Lima instance ${node}"
        limactl_cmd delete --force "${node}"
      else
        log "Lima instance ${node} does not exist"
      fi
    done
    ;;
  stop)
    for node in "${E2E_NODES[@]}"; do
      if instance_exists "${node}"; then
        log "stopping Lima instance ${node}"
        limactl_cmd stop "${node}" || true
      else
        log "Lima instance ${node} does not exist"
      fi
    done
    ;;
  *)
    die "usage: $0 [delete|stop]"
    ;;
esac
