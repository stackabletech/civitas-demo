#!/usr/bin/env bash
# Demo pipeline through the CIVITAS portal: MQTT -> NiFi -> PostGIS.
#   demo.sh up     create everything and release the dataset
#   demo.sh down   take the dataset back to draft and stop the MQTT broker
# Talks to the portal on https://portal.<domain> through port 443 of this computer.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$here/.."
values="$root/values/default-instance.yaml"
ns=$(yq '.global.instanceSlug' < "$values")
domain=$(yq '.global.domain' < "$values")
ca="${CIVITAS_CORE_DEPLOYMENT:?}/dev-deployment/.ssl/civitas.crt"

name="Stackable Demo"
ds_name="Stackable Demo Structure"
source_name="Stackable Demo MQTT"
pipeline_name="Stackable Demo Pipeline"
table="stackable_demo"

command -v jq >/dev/null || { echo "jq is needed for the demo"; exit 1; }
kubectl -n "$ns" get secret nifi-demo-admin-user >/dev/null 2>&1 || { echo "run 'just create-admin-user' first"; exit 1; }

secret() { kubectl -n "$ns" get secret "$1" -o jsonpath="{.data.$2}" | base64 -d; }
curl_opts=(-s --cacert "$ca" --resolve "idm.$domain:443:127.0.0.1" --resolve "portal.$domain:443:127.0.0.1")

token=$(curl "${curl_opts[@]}" "https://idm.$domain/realms/$ns/protocol/openid-connect/token" \
  -d grant_type=password -d client_id=portal-frontend \
  -d client_secret="$(secret keycloak-client-portal-frontend client-secret)" \
  -d username="$(secret nifi-demo-admin-user username)" \
  --data-urlencode password="$(secret nifi-demo-admin-user password)" | jq -r .access_token)
[ "$token" != "null" ] || { echo "login to Keycloak failed (is port 443 reachable? see README)"; exit 1; }

# api METHOD PATH [JSON]: call the portal API, fail on HTTP errors.
api() {
  local out code
  out=$(mktemp)
  code=$(curl "${curl_opts[@]}" -o "$out" -w '%{http_code}' -X "$1" \
    -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
    "https://portal.$domain/v1$2" ${3:+--data "$3"})
  if [ "$code" -ge 300 ]; then echo "portal API $1 $2 failed ($code): $(cat "$out")" >&2; rm -f "$out"; return 1; fi
  cat "$out"; rm -f "$out"
}

# find_id PATH NAME: id of the entry with that name, or empty.
find_id() { api GET "$1?size=500" | jq -r --arg n "$2" '(.content // .)[] | select(.name == $n) | .id' | head -n1; }

dataset_status() { api GET "/datasets/$1" | jq -r .dataSetStatus; }

# wait_for DATASET STATUS: poll until the dataset reaches STATUS and no saga is running.
wait_for() {
  for _ in $(seq 1 60); do
    s=$(api GET "/datasets/$1")
    if [ "$(jq -r .dataSetStatus <<<"$s")" = "$2" ] && [ "$(jq -r .pendingSagaType <<<"$s")" = "null" ]; then return 0; fi
    sleep 5
  done
  echo "dataset did not reach $2 in time" >&2; return 1
}

# The portal builds the data structure ID like this (see its src/utils/urn.ts).
urn() {
  python3 - "$1" "$2" "$3" <<'EOF'
import re, sys
name, uuid, version = sys.argv[1:4]
n = "".join(w[:1].upper() + w[1:] for w in re.split(r"[^a-zA-Z0-9]+", name) if w)
v, d = int(uuid.replace("-", ""), 16), ""
while v:
    v, r = divmod(v, 36)
    d = "0123456789abcdefghijklmnopqrstuvwxyz"[r] + d
print(f"urn:core:platform:civitas:datastructure:common:{n}:{d[-10:].rjust(10, '0')}:{version}")
EOF
}

up() {
  echo "== MQTT broker and fake sender"
  kubectl -n "$ns" apply -f "$here/mqtt.yaml" >/dev/null
  kubectl -n "$ns" rollout status deploy/demo-mqtt --timeout=180s >/dev/null

  echo "== data pool"
  pool=$(find_id /datapools "$name")
  [ -n "$pool" ] || pool=$(api POST /datapools "$(jq -nc --arg n "$name" '{name:$n, description:"Stackable demo"}')" | jq -r .id)

  echo "== data structure"
  ds=$(find_id /datastructures "$ds_name")
  if [ -z "$ds" ]; then
    ds=$(api POST /datastructures "$(jq -nc --arg n "$ds_name" \
      '{name:$n, description:"id, name and a point", createdFromDataSource:false, dataStructureVersionIds:[], assignments:[]}')" | jq -r .id)
  fi
  dsv=$(api GET "/datastructures/$ds" | jq -r '.dataStructureVersions[0].id // empty')
  if [ -z "$dsv" ]; then
    dsv=$(api POST "/datastructures/$ds/versions" "$(jq -nc '{
      dataStructureVersionSource:"OWN", version:"1.0.0", description:"Demo", modelName:"StackableDemoModel",
      model:{type:"object", properties:{id:{type:"string"}, name:{type:"string"},
             geom:{"$ref":"https://geojson.org/schema/Point.json", crs:"EPSG:4326"}}},
      styles:{nodes:[{data:{element:{attributes:[{name:"id"},{name:"name"},{name:"geom"}]}}}]}}')" | jq -r .id)
  fi
  if [ "$(api GET "/datastructures/$ds" | jq -r .dataStructureStatus)" != "AVAILABLE" ]; then
    api POST "/datastructures/$ds/versions/$dsv/release" >/dev/null
    api POST "/datastructures/$ds/release" >/dev/null
  fi

  echo "== MQTT data source"
  src=$(find_id /datasources "$source_name")
  if [ -z "$src" ]; then
    src=$(api POST /datasources "$(jq -nc --arg n "$source_name" --arg url "tcp://demo-mqtt.$ns.svc.cluster.local:1883" '{
      name:$n, description:"Fake readings from demo-mqtt", connectorType:"MQTT", assignments:[],
      configuration:{urls:[$url], topics:["demo/sensors"], client_id:"stackable-demo", qos:1,
                     connect_timeout:"5s", keepalive:"30s", tls:{enabled:false}}}')" | jq -r .id)
    api PATCH "/datasources/$src" "$(jq -nc --arg p "$pool" '{datapoolScope:{type:"SPECIFIC", datapoolIds:[$p]}}')" >/dev/null
    api PATCH "/datasources/$src" "$(jq -nc --arg v "$dsv" '{dataStructureVersionId:$v}')" >/dev/null
  fi
  [ "$(api GET "/datasources/$src" | jq -r .dataSourceStatus)" = "AVAILABLE" ] || api POST "/datasources/$src/release" >/dev/null

  echo "== dataset with pipeline"
  set_id=$(find_id /datasets "$name")
  if [ -z "$set_id" ]; then
    set_id=$(api POST /datasets "$(jq -nc --arg n "$name" '{name:$n, description:"Stackable demo", openDataAccess:true, namedApis:[]}')" | jq -r .id)
    api PATCH "/datasets/$set_id" "$(jq -nc --arg p "$pool" '{datapoolId:$p}')" >/dev/null
    sink=$(api POST "/datasets/$set_id/datasinks" "$(jq -nc --arg t "$table" --arg v "$dsv" \
      '{dataSinkType:"POSTGIS", configuration:{tableName:$t, dataStructureVersionId:$v}}')" | jq -r .id)
    pipeline=$(sed -e "s#__PIPELINE_NAME__#$pipeline_name#g" -e "s#__DATA_SOURCE_ID__#$src#g" \
      -e "s#__DATA_SOURCE_NAME__#$source_name#g" -e "s#__DATA_SINK_ID__#$sink#g" -e "s#__TABLE_NAME__#$table#g" \
      -e "s#__DS_NAME__#$ds_name#g" -e "s#__DSV_ID__#$dsv#g" -e "s#__DS_ID__#$ds#g" \
      -e "s#__URN__#$(urn "$ds_name" "$ds" 1.0.0)#g" "$here/pipeline.json")
    api POST "/datasets/$set_id/pipelines" "$pipeline" >/dev/null
  fi

  echo "== release (config-adapter deploys the NiFi flow, messages go over Kafka)"
  [ "$(dataset_status "$set_id")" = "DRAFT" ] && api POST "/datasets/$set_id/stage" >/dev/null
  [ "$(dataset_status "$set_id")" = "READY" ] && api POST "/datasets/$set_id/release" >/dev/null
  wait_for "$set_id" AVAILABLE
  sleep 20
  api GET "/datasets/$set_id" | jq -r '.pipelines[] | "pipeline \(.name): \(.runtimeStatus.state // "no status yet") \(.runtimeStatus.message // "")"'

  cat <<EOF

Done. Have a look:
  Portal:   https://portal.$domain  (Unsere Daten > Datensätze > $name)
  NiFi:     https://nifi.$domain/nifi  (process group pipeline-...)
  kafka-ui: just kafka-ui, then http://localhost:8080
  Rows:     just demo-rows
EOF
}

down() {
  set_id=$(find_id /datasets "$name")
  if [ -n "$set_id" ]; then
    [ "$(dataset_status "$set_id")" = "AVAILABLE" ] && { api POST "/datasets/$set_id/unrelease" >/dev/null; wait_for "$set_id" READY; }
    [ "$(dataset_status "$set_id")" = "READY" ] && api POST "/datasets/$set_id/unstage" >/dev/null
    echo "dataset '$name' is back to draft, the NiFi flow is removed"
  fi
  kubectl -n "$ns" delete -f "$here/mqtt.yaml" --ignore-not-found >/dev/null
  echo "MQTT broker removed"
}

"${1:?usage: demo.sh up|down}"
