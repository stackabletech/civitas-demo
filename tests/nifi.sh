#!/usr/bin/env bash
# NiFi: cluster ready, bootstrap Job done, REST API accepts Keycloak client-credentials
# tokens, UI ingress redirects to Keycloak.
source "$(dirname "$0")/lib.sh"
NS=$(instance_ns)
DOMAIN=$(cat "$ROOT/values/default-instance.yaml" | yq '.global.domain')
NIFI_URL="https://nifi-nifi-node-default-0.nifi-nifi-node-default-headless.$NS.svc.cluster.local:8443"
TOKEN_URL="http://keycloak-app-keycloakx-http.$NS.svc.cluster.local/realms/$NS/protocol/openid-connect/token"

echo "== NifiCluster"
kubectl -n "$NS" wait --for=condition=Available nificluster/nifi-nifi --timeout=900s >/dev/null \
  && pass "NifiCluster Available" || fail "NifiCluster Available"
kubectl -n "$NS" wait --for=condition=Complete job -l app.kubernetes.io/name=nifi-bootstrap --timeout=900s >/dev/null \
  && pass "nifi-bootstrap Job complete" || fail "nifi-bootstrap Job complete"

echo "== REST API with client-credentials token (as nifi-bootstrap)"
SECRET=$(kubectl -n "$NS" get secret keycloak-client-nifi-bootstrap -o jsonpath='{.data.client-secret}' | base64 -d)
code=$(in_cluster "$NS" app.kubernetes.io/name=nifi-bootstrap "
    T=\$(curl -fsS -d grant_type=client_credentials -d client_id=nifi-bootstrap -d client_secret='$SECRET' '$TOKEN_URL' | jq -r .access_token)
    curl -ks -o /dev/null -w '%{http_code}' -H \"Authorization: Bearer \$T\" '$NIFI_URL/nifi-api/flow/about'")
assert_eq "GET /nifi-api/flow/about" "200" "$code"

echo "== UI ingress → Keycloak"
ING_IP=$(kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.spec.clusterIP}')
out=$(in_cluster ingress-nginx "" "
    R='--resolve nifi.$DOMAIN:443:$ING_IP --resolve idm.$DOMAIN:443:$ING_IP'
    echo ui=\$(curl -ks \$R -o /dev/null -w '%{http_code}' https://nifi.$DOMAIN/nifi/)
    LOC=\$(curl -ks \$R -o /dev/null -w '%{redirect_url}' https://nifi.$DOMAIN/nifi-api/oauth2/authorization/consumer)
    echo login=\$LOC
    echo keycloak=\$(curl -ks \$R -o /dev/null -w '%{http_code}' \"\$LOC\")")
assert_contains "NiFi UI via ingress" "ui=200" "$out"
assert_contains "NiFi login redirects to Keycloak realm" "login=https://idm.$DOMAIN/realms/$NS/protocol/openid-connect/auth" "$out"
assert_contains "Keycloak accepts NiFi redirect_uri (login page 200)" "keycloak=200" "$out"
echo "== Browser login (authorization code flow) as the demo admin"
if kubectl -n "$NS" get secret nifi-demo-admin-user >/dev/null 2>&1; then
  USER=$(kubectl -n "$NS" get secret nifi-demo-admin-user -o jsonpath='{.data.username}' | base64 -d)
  PW=$(kubectl -n "$NS" get secret nifi-demo-admin-user -o jsonpath='{.data.password}' | base64 -d)
  out=$(in_cluster ingress-nginx "" "
    C='-ks -c /tmp/j -b /tmp/j --resolve nifi.$DOMAIN:443:$ING_IP --resolve idm.$DOMAIN:443:$ING_IP'
    LOGIN=\$(curl \$C -o /dev/null -w '%{redirect_url}' https://nifi.$DOMAIN/nifi-api/oauth2/authorization/consumer)
    ACTION=\$(curl \$C \"\$LOGIN\" | grep -o 'action=\"[^\"]*\"' | head -1 | sed 's/action=\"//;s/\"\$//;s/&amp;/\\&/g')
    CB=\$(curl \$C -o /dev/null -w '%{redirect_url}' --data-urlencode username='$USER' --data-urlencode password='$PW' \"\$ACTION\")
    curl \$C -o /dev/null \"\$CB\"
    echo identity=\$(curl \$C https://nifi.$DOMAIN/nifi-api/flow/current-user | jq -r .identity)
    echo rootWrite=\$(curl \$C https://nifi.$DOMAIN/nifi-api/flow/process-groups/root | jq -r .permissions.canWrite)")
  assert_contains "NiFi session identity is $USER" "identity=$USER" "$out"
  assert_contains "$USER can write the root process group" "rootWrite=true" "$out"
else
  echo "  SKIP (run 'just create-admin-user' first)"
fi

finish
