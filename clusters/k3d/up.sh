#!/usr/bin/env bash
# k3d/k3s: reuse civitas-core-deployment's dev bootstrap (cluster, ingress-nginx, MetalLB,
# cert-manager, CA issuer, coredns-custom).
set -euo pipefail
: "${CIVITAS_CORE_DEPLOYMENT:?}"
"$CIVITAS_CORE_DEPLOYMENT/dev-deployment/startup.sh" -k
