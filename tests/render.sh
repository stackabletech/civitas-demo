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
mesh=$(grep -A1 'customServiceMesh:' <<<"$built" | grep -o 'enable: [a-z]*' | head -1)
assert_eq "serviceMesh disabled via values/default-instance.yaml" "enable: false" "$mesh"

finish
