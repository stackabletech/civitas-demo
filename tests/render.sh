#!/usr/bin/env bash
# Checks the config without touching the cluster.
source "$(dirname "$0")/lib.sh"

echo "== operators layer"
ops=$(hf operators list 2>&1 || true)
for r in postgres-operator stackable-commons stackable-secret stackable-listener stackable-kafka stackable-nifi; do
  assert_contains "release $r" "$r" "$ops"
done
tpl=$(hf operators -l name=stackable-kafka template 2>&1 || true)
assert_contains "kafka-operator image 26.7.0" "oci.stackable.tech/sdp/kafka-operator:26.7.0" "$tpl"
np=$(hf operators -l policy-name=stackable-operator-webhooks template 2>&1 || true)
assert_contains "webhook NetworkPolicy port 8443" "port: 8443" "$np"

echo "== instance layer"
inst=$(hf instance list 2>&1 || true)
assert_contains "keycloak release present" "keycloak-app" "$inst"
if grep -q "runtime-policies" <<<"$ops$inst"; then fail "runtime-policies deployed"; else pass "no runtime-policies"; fi
if grep -Eq "^(postgres-operator|stackable-(commons|secret|listener|kafka|nifi))[[:space:]]" <<<"$inst"; then fail "operators in instance layer"; else pass "no operators in instance layer"; fi

echo "== config-adapter"
ca=$(hf instance -l name=config-adapters-adapters build --embed-values 2>&1 || true)
mesh=$(grep -A1 'customServiceMesh:' <<<"$ca" | grep -o 'enable: [a-z]*' | head -1 || true)
assert_eq "serviceMesh disabled via values/default-instance.yaml" "enable: false" "$mesh"
assert_contains "Kafka bootstrap is the Stackable service" "kafka-cluster-broker-default-bootstrap" "$ca"
nifi_url=$(grep -A1 'name: NIFI_URL$' <<<"$ca" | grep -o 'value: .*' | head -1 || true)
assert_contains "NIFI_URL is the Stackable pod" "nifi-nifi-node-default-0" "$nifi_url"

tmpd=$(mktemp -d); trap 'rm -rf "$tmpd"' EXIT
tmpv="$tmpd/values.yaml"
printf 'nifi:\n  nifi:\n    url: https://nifi.example:8443\n' > "$tmpv"
ca=$(EXTRA_VALUES="$tmpv" hf instance -l name=config-adapters-adapters build --embed-values 2>&1 || true)
nifi_url=$(grep -A1 'name: NIFI_URL$' <<<"$ca" | grep -o 'value: .*' | head -1 || true)
assert_eq "NIFI_URL follows nifi.nifi.url" "value: https://nifi.example:8443" "$nifi_url"

echo "== kafka addon"
k=$(hf instance -l name=kafka-cluster template 2>&1 || true)
assert_contains "KafkaCluster" "kind: KafkaCluster" "$k"
assert_contains "Kafka 4.2.1" 'productVersion: "4.2.1"' "$k"
assert_contains "KRaft" "metadataManager: kraft" "$k"

echo "== nifi addon"
n=$(hf instance -l name=nifi-nifi template 2>&1 || true)
assert_contains "NifiCluster" "kind: NifiCluster" "$n"
assert_contains "NiFi 2.9.0" 'productVersion: "2.9.0"' "$n"
assert_contains "initial admin nifi-bootstrap" "initialAdminUser: nifi-bootstrap" "$n"
assert_contains "OIDC AuthenticationClass" "kind: AuthenticationClass" "$n"
assert_contains "ES256" "nifi.security.user.oidc.preferred.jwsalgorithm: ES256" "$n"
if grep -Eq "^[[:space:]]*zookeeperConfigMapName:" <<<"$n"; then fail "zookeeperConfigMapName set"; else pass "no ZooKeeper"; fi
b=$(hf instance -l name=nifi-bootstrap template 2>&1 || true)
assert_contains "bootstrap Job targets the Stackable pod" "nifi-nifi-node-default-0" "$b"

finish
