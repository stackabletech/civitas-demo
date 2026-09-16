#!/usr/bin/env bash
# Produce and consume a message through the Stackable Kafka bootstrap Service.
source "$(dirname "$0")/lib.sh"
NS=$(instance_ns)
SVC=$(hf instance -l name=config-adapters-adapters build --embed-values 2>/dev/null \
  | grep -o 'kafka-cluster[a-z-]*bootstrap' | head -1)

echo "== KafkaCluster"
kubectl -n "$NS" wait --for=condition=Available kafkacluster/kafka-cluster --timeout=600s >/dev/null \
  && pass "KafkaCluster Available" || fail "KafkaCluster Available"
kubectl -n "$NS" get svc "$SVC" >/dev/null 2>&1 && pass "bootstrap Service $SVC exists" || fail "bootstrap Service $SVC exists"

echo "== produce/consume"
POD=$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=kafka,app.kubernetes.io/instance=kafka-cluster,app.kubernetes.io/component=broker -o jsonpath='{.items[0].metadata.name}')
MSG="smoke-$(date +%s)"
TOPIC=de.civitascore.smoke-test
BS="$SVC.$NS.svc.cluster.local:9092"
kubectl -n "$NS" exec "$POD" -c kafka -- bash -c \
  "echo '$MSG' | /stackable/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS --topic $TOPIC" >/dev/null 2>&1 \
  && pass "produce" || fail "produce"
got=$(kubectl -n "$NS" exec "$POD" -c kafka -- bash -c \
  "/stackable/kafka/bin/kafka-console-consumer.sh --bootstrap-server $BS --topic $TOPIC --from-beginning --timeout-ms 20000 2>/dev/null" | grep -x "$MSG" || true)
assert_eq "consume" "$MSG" "$got"
finish
