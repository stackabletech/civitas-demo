#!/usr/bin/env bash
# Bring-your-own cluster: nothing is installed, prerequisites are only checked.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
cat <<'EOF'
Using the current kube context as-is. Required:
  - default StorageClass
  - ingress-nginx (IngressClass "nginx", Service ingress-nginx/ingress-nginx-controller)
  - cert-manager with ClusterIssuer "selfsigned-ca" (Secret cert-manager/ca-secret)
  - in-cluster DNS resolving *.<domain> to the ingress controller
EOF
"$here/../../tests/cluster.sh"
