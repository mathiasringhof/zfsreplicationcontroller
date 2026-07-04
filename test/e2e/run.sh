#!/usr/bin/env bash
set -euo pipefail

E2E_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${E2E_DIR}/env.sh"

need_cmd go

cleanup() {
  local status=$?
  if [[ "${E2E_KEEP_CLUSTER:-0}" == "1" ]]; then
    log "leaving E2E cluster running because E2E_KEEP_CLUSTER=1"
    exit "${status}"
  fi
  log "tearing down E2E cluster"
  if ! "${E2E_DIR}/down.sh" delete; then
    [[ "${status}" -ne 0 ]] || status=1
  fi
  exit "${status}"
}
trap cleanup EXIT

log "deleting any existing E2E cluster for a clean slate"
"${E2E_DIR}/down.sh" delete

"${E2E_DIR}/up.sh"
"${E2E_DIR}/build-image.sh"
"${E2E_DIR}/import-image.sh"
"${E2E_DIR}/deploy.sh"
"${E2E_DIR}/status.sh"

cat >&2 <<EOF
[e2e] environment is ready.
[e2e] kubeconfig: ${E2E_DIR}/.artifacts/kubeconfig
EOF

test_run="${E2E_TEST_RUN:-TestE2E}"
test_count="${E2E_TEST_COUNT:-1}"
log "running go test ./test/e2e -run ${test_run} -count=${test_count} -v"
KUBECONFIG="${KUBECONFIG_PATH}" go test ./test/e2e -run "${test_run}" -count="${test_count}" -v
