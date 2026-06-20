#!/usr/bin/env bash
set -euo pipefail

E2E_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${E2E_DIR}/up.sh"
"${E2E_DIR}/build-image.sh"
"${E2E_DIR}/import-image.sh"
"${E2E_DIR}/deploy.sh"
"${E2E_DIR}/status.sh"

cat >&2 <<EOF
[e2e] environment is ready.
[e2e] kubeconfig: ${E2E_DIR}/.artifacts/kubeconfig
[e2e]
[e2e] next steps for tests:
[e2e]   go test ./test/e2e -run TestE2E -count=1 -v
[e2e]
[e2e] tear down with:
[e2e]   ${E2E_DIR}/down.sh
EOF
