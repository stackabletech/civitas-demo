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
code=$(kubectl -n "$NS" run nifi-rest-check-$RANDOM --rm -i --restart=Never --quiet \
  --labels app.kubernetes.io/name=nifi-bootstrap \
  --image=docker.io/badouralix/curl-jq:alpine -- sh -c "
    T=\$(curl -fsS -d grant_type=client_credentials -d client_id=nifi-bootstrap -d client_secret='$SECRET' '$TOKEN_URL' | jq -r .access_token)
    curl -ks -o /dev/null -w '%{http_code}' -H \"Authorization: Bearer \$T\" '$NIFI_URL/nifi-api/flow/about'" 2>/dev/null | tail -c 3)
assert_eq "GET /nifi-api/flow/about" "200" "$code"

echo "== UI ingress → Keycloak"
ING_IP=$(kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.spec.clusterIP}')
out=$(kubectl -n ingress-nginx run nifi-ui-check-$RANDOM --rm -i --restart=Never --quiet \
  --image=docker.io/badouralix/curl-jq:alpine -- sh -c "
    R='--resolve nifi.$DOMAIN:443:$ING_IP --resolve idm.$DOMAIN:443:$ING_IP'
    echo ui=\$(curl -ks \$R -o /dev/null -w '%{http_code}' https://nifi.$DOMAIN/nifi/)
    LOC=\$(curl -ks \$R -o /dev/null -w '%{redirect_url}' https://nifi.$DOMAIN/nifi-api/oauth2/authorization/consumer)
    echo login=\$LOC
    echo keycloak=\$(curl -ks \$R -o /dev/null -w '%{http_code}' \"\$LOC\")" 2>/dev/null || true)
assert_contains "NiFi UI via ingress" "ui=200" "$out"
assert_contains "NiFi login redirects to Keycloak realm" "login=https://idm.$DOMAIN/realms/$NS/protocol/openid-connect/auth" "$out"
assert_contains "Keycloak accepts NiFi redirect_uri (login page 200)" "keycloak=200" "$out"
finish
