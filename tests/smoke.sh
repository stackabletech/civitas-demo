#!/usr/bin/env bash
# End-to-end: prerequisites, operators, Kafka, NiFi, config-adapter on Kafka, portal.
source "$(dirname "$0")/lib.sh"
NS=$(instance_ns)
DOMAIN=$(cat "$ROOT/values/default-instance.yaml" | yq '.global.domain')
d="$(dirname "$0")"

for t in cluster operators kafka nifi; do
  echo "#### $t"; "$d/$t.sh" || FAILED=1
done

echo "#### config-adapter"
kubectl -n "$NS" rollout status deploy/config-adapter --timeout=600s >/dev/null \
  && pass "config-adapter ready" || fail "config-adapter ready"
POD=$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=kafka,app.kubernetes.io/instance=kafka-cluster,app.kubernetes.io/component=broker -o jsonpath='{.items[0].metadata.name}')
BS=$(hf instance -l name=config-adapters-adapters build --embed-values 2>/dev/null | grep -o 'kafka-cluster[a-z-]*bootstrap' | head -1).$NS.svc.cluster.local:9092
groups=$(kubectl -n "$NS" exec "$POD" -c kafka -- /stackable/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$BS" --list 2>/dev/null || true)
assert_contains "consumer group config-adapter-group on Stackable Kafka" "config-adapter-group" "$groups"
# Same identity, network path and endpoints config-adapter uses (NIFI_OIDC_CLIENT_ID=nifi-config-adapter).
SECRET=$(kubectl -n "$NS" get secret keycloak-client-nifi-config-adapter -o jsonpath='{.data.client-secret}' | base64 -d)
NIFI_URL="https://nifi-nifi-node-default-0.nifi-nifi-node-default-headless.$NS.svc.cluster.local:8443"
TOKEN_URL="http://keycloak-app-keycloakx-http.$NS.svc.cluster.local/realms/$NS/protocol/openid-connect/token"
# NiFi replicates API calls to its node(s); retry to ride out a transient replication timeout.
code=$(in_cluster "$NS" app.kubernetes.io/name=config-adapter,app.kubernetes.io/instance=config-adapters-adapters "
    T=\$(curl -fsS -d grant_type=client_credentials -d client_id=nifi-config-adapter -d client_secret='$SECRET' '$TOKEN_URL' | jq -r .access_token)
    for i in 1 2 3; do
      c=\$(curl -ks -m 30 -o /dev/null -w '%{http_code}' -H \"Authorization: Bearer \$T\" '$NIFI_URL/nifi-api/flow/process-groups/root')
      [ \"\$c\" = 200 ] && break; sleep 5
    done; echo \$c")
assert_eq "nifi-config-adapter may read the root process group" "200" "$code"
if kubectl -n "$NS" logs deploy/config-adapter --tail=2000 | grep -i 'nifi' | grep -qE ' ERROR |UnknownHost|Connection refused|returned 403'; then
  fail "config-adapter logs show NiFi errors"
else
  pass "no NiFi errors in config-adapter logs"
fi

echo "#### portal"
ING_IP=$(kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.spec.clusterIP}')
code=$(in_cluster ingress-nginx "" "curl -ks -m 30 -o /dev/null -w '%{http_code}' --resolve portal.$DOMAIN:443:$ING_IP https://portal.$DOMAIN/")
case "$code" in 200|301|302|307|308) pass "portal responds ($code)";; *) fail "portal responds (got '$code')";; esac

finish
