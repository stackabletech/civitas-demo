#!/usr/bin/env bash
# Uses civitas-core-deployment's k3d bootstrap.
set -euo pipefail
: "${CIVITAS_CORE_DEPLOYMENT:?}"
"$CIVITAS_CORE_DEPLOYMENT/dev-deployment/startup.sh" -k
