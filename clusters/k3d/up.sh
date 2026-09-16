#!/usr/bin/env bash
# Uses the k3d setup script of civitas-core-deployment.
set -euo pipefail
: "${CIVITAS_CORE_DEPLOYMENT:?}"
"$CIVITAS_CORE_DEPLOYMENT/dev-deployment/startup.sh" -k
