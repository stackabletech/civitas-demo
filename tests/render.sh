#!/usr/bin/env bash
# Offline checks: helmfile state renders with our values and component lists.
source "$(dirname "$0")/lib.sh"

echo "== operators layer"
ops=$(hf operators list 2>&1 || true)
assert_contains "postgres operator in operators layer" "postgres-operator" "$ops"
if grep -q "runtime-policies" <<<"$ops"; then fail "runtime-policies must not be deployed"; else pass "no runtime-policies"; fi

echo "== instance layer"
inst=$(hf instance list 2>&1 || true)
assert_contains "keycloak release present" "keycloak-app" "$inst"
if grep -q "runtime-policies" <<<"$inst"; then fail "runtime-policies must not be deployed"; else pass "no runtime-policies"; fi
if grep -Eq "postgres-operator|kafka-operator" <<<"$inst"; then fail "operator releases leaked into instance layer"; else pass "no operators in instance layer"; fi

echo "== state values override v2 defaults"
built=$(hf instance -l name=config-adapters-adapters build --embed-values 2>&1 || true)
mesh=$(grep -A1 'customServiceMesh:' <<<"$built" | grep -o 'enable: [a-z]*' | head -1 || true)
assert_eq "serviceMesh disabled via values/default-instance.yaml" "enable: false" "$mesh"

echo "== config-adapter uses nifi.nifi.url"
tmpv=$(mktemp "$ROOT/.test-values-XXXX.yaml"); trap 'rm -f "$tmpv"' EXIT
printf 'nifi:\n  nifi:\n    url: https://nifi.example:8443\n' > "$tmpv"
ca=$(EXTRA_VALUES="$tmpv" hf instance -l name=config-adapters-adapters build --embed-values 2>&1 || true)
nifi_url=$(grep -A1 'name: NIFI_URL$' <<<"$ca" | grep -o 'value: .*' | head -1 || true)
assert_eq "NIFI_URL taken from nifi.nifi.url" "value: https://nifi.example:8443" "$nifi_url"

finish
