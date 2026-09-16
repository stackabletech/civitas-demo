#!/usr/bin/env bash
# Checks that the Stackable operators run.
source "$(dirname "$0")/lib.sh"
NS=civitas-operators

for p in commons secret listener kafka nifi; do
  ok=$(kubectl -n "$NS" get deploy -l "app.kubernetes.io/instance=stackable-$p" -o jsonpath='{.items[0].status.readyReplicas}' 2>/dev/null || true)
  assert_eq "operator stackable-$p ready" "1" "${ok:-0}"
done
for crd in kafkaclusters.kafka.stackable.tech nificlusters.nifi.stackable.tech \
           authenticationclasses.authentication.stackable.tech secretclasses.secrets.stackable.tech \
           listenerclasses.listeners.stackable.tech; do
  kubectl get crd "$crd" >/dev/null 2>&1 && pass "CRD $crd" || fail "CRD $crd"
done
kubectl get secretclass tls >/dev/null 2>&1 && pass "SecretClass tls" || fail "SecretClass tls"
kubectl get listenerclass cluster-internal >/dev/null 2>&1 && pass "ListenerClass cluster-internal" || fail "ListenerClass cluster-internal"
finish
