#!/usr/bin/env bash
# Inside the cluster, send all *.civitas.test names to ingress-nginx.
set -euo pipefail
DOMAIN="${DOMAIN:-civitas.test}"
escaped=${DOMAIN//./\\.}
rule="    rewrite stop name regex (.*\\.)?${escaped}\\.? ingress-nginx-controller.ingress-nginx.svc.cluster.local. answer auto"

corefile=$(kubectl -n kube-system get configmap coredns -o jsonpath='{.data.Corefile}')
if grep -qF "ingress-nginx-controller.ingress-nginx.svc.cluster.local" <<<"$corefile"; then
  echo "CoreDNS already rewrites *.$DOMAIN"; exit 0
fi
# Read the rule from the environment: awk -v changes backslashes differently per awk version.
patched=$(rule="$rule" awk '{print} /^\.:53 \{/{print ENVIRON["rule"]}' <<<"$corefile")
kubectl -n kube-system create configmap coredns --from-literal=Corefile="$patched" --dry-run=client -o yaml \
  | kubectl apply -f -
kubectl -n kube-system rollout restart deployment/coredns
kubectl -n kube-system rollout status deployment/coredns --timeout=120s
