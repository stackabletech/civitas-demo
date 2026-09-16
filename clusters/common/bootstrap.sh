#!/usr/bin/env bash
# ingress-nginx, cert-manager and the CIVITAS CA ClusterIssuer for any cluster.
set -euo pipefail

: "${CIVITAS_CORE_DEPLOYMENT:?}"
DOMAIN="${DOMAIN:-civitas.test}"
INGRESS_NGINX_VERSION="${INGRESS_NGINX_VERSION:-4.15.1}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.21.2}"
INGRESS_EXTRA_ARGS=("$@")

helm upgrade --install ingress-nginx ingress-nginx \
  --repo https://kubernetes.github.io/ingress-nginx --version "$INGRESS_NGINX_VERSION" \
  --namespace ingress-nginx --create-namespace \
  --set controller.config.annotations-risk-level=Critical \
  --set controller.config.enable-annotation-validation=false \
  --set controller.config.proxy-buffer-size=64k \
  --set-string controller.config.proxy-buffers="4 64k" \
  --set controller.config.proxy-busy-buffers-size=128k \
  "${INGRESS_EXTRA_ARGS[@]}" \
  --wait --timeout 10m

helm upgrade --install cert-manager cert-manager \
  --repo https://charts.jetstack.io --version "$CERT_MANAGER_VERSION" \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true \
  --wait --timeout 10m

SSL="$CIVITAS_CORE_DEPLOYMENT/dev-deployment/.ssl"
CA_CERT=$(base64 -w0 < "$SSL/civitas.crt")
CA_KEY=$(base64 -w0 < "$SSL/civitas.key")
export DOMAIN CA_CERT CA_KEY
envsubst '${DOMAIN} ${CA_CERT} ${CA_KEY}' < "$CIVITAS_CORE_DEPLOYMENT/dev-deployment/ca-template.yaml" | kubectl apply -f -
kubectl wait --for=condition=Ready clusterissuer/selfsigned-ca --timeout=120s
