#!/usr/bin/env bash
# Create (or reuse) a kind cluster and install the common prerequisites.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
name="${KIND_CLUSTER_NAME:-kind}"

if kind get clusters | grep -qx "$name"; then
  echo "Reusing existing kind cluster '$name'"
else
  kind create cluster --name "$name" --config "$here/cluster.yaml"
fi
kubectl config use-context "kind-$name"

"$here/../common/bootstrap.sh" \
  --set controller.hostPort.enabled=true \
  --set controller.service.type=ClusterIP \
  --set controller.publishService.enabled=false
"$here/coredns.sh"
