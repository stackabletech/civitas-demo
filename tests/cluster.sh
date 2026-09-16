#!/usr/bin/env bash
# Cluster prerequisites for the deployment layer (any distribution).
source "$(dirname "$0")/lib.sh"
DOMAIN=$(cat "$ROOT/values/default-instance.yaml" | yq '.global.domain')

echo "== ingress-nginx"
ready=$(kubectl -n ingress-nginx get deploy ingress-nginx-controller -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
assert_eq "ingress-nginx controller ready" "1" "${ready:-0}"

echo "== cert-manager"
issuer=$(kubectl get clusterissuer selfsigned-ca -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
assert_eq "ClusterIssuer selfsigned-ca Ready" "True" "${issuer:-missing}"

echo "== in-cluster DNS"
svc_ip=$(kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.spec.clusterIP}' 2>/dev/null || true)
resolved=$(kubectl run dns-check-$RANDOM --rm -i --restart=Never --image=busybox:1.36 --quiet -- nslookup "idm.$DOMAIN" 2>/dev/null | awk '/^Address: /{print $2}' | tail -1 || true)
assert_eq "idm.$DOMAIN resolves to ingress controller" "$svc_ip" "$resolved"

finish
