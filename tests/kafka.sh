#!/usr/bin/env bash
# Sends a message to Kafka and reads it back.
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

echo "== kafka-ui"
kubectl -n "$NS" rollout status deploy/kafka-ui --timeout=300s >/dev/null
port=$((20000 + RANDOM % 10000))
kubectl -n "$NS" port-forward svc/kafka-ui "$port:80" >/dev/null 2>&1 &
pf=$!
clusters=""
for i in $(seq 1 15); do
  clusters=$(curl -s "http://127.0.0.1:$port/api/clusters" 2>/dev/null || true)
  [ -n "$clusters" ] && break
  sleep 1
done
kill "$pf" 2>/dev/null || true
status=$(yq -p json '.[] | select(.name == "kafka-cluster") | .status' <<<"$clusters" 2>/dev/null || true)
assert_eq "kafka-ui shows kafka-cluster as online" "online" "$status"
finish
