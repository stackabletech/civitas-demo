# CIVITAS/CORE on Stackable Kafka & NiFi

This project deploys the **full CIVITAS/CORE v2 platform** from
[`civitas-core-deployment`](../civitas-core-deployment) ("v2" below) with two building blocks swapped
for the [Stackable Data Platform](https://docs.stackable.tech/) (SDP) 26.7:

- **Strimzi Kafka → Stackable `KafkaCluster`** (Kafka 4.2.1, KRaft)
- **apache-nifi-helm NiFi → Stackable `NifiCluster`** (NiFi 2.9.0, Kubernetes clustering backend, Keycloak OIDC)

No ZooKeeper is involved: CIVITAS/CORE v2 does not use it today, Stackable NiFi clusters
via Kubernetes Leases/ConfigMaps and Stackable Kafka runs in KRaft mode. Everything else
(Keycloak, APISIX, PostgreSQL, FROST, config-adapter, portal, GeoServer, Superset, …) is
deployed unchanged from `civitas-core-deployment`.

Verified on a single-node kind cluster (see [Verification](#verification)).

---

## What changes

| Aspect | civitas-core-deployment | this project |
|---|---|---|
| Kafka operator | Strimzi 0.51.0 | Stackable `kafka-operator` 26.7.0 |
| Kafka cluster | `Kafka kafka-cluster` + `KafkaNodePool`s, Kafka 4.1.0 KRaft | `KafkaCluster kafka-cluster`, Kafka 4.2.1 KRaft (1 controller, 1 broker) |
| Kafka client access | `kafka-cluster-kafka-bootstrap.<ns>:9092`, plaintext, no auth | `kafka-cluster-broker-default-bootstrap.<ns>:9092`, plaintext, no auth |
| Kafka inter-node TLS | none | Stackable `tls` SecretClass (automatic) |
| Kafka topics | auto-create + `KafkaTopic kafkasql-journal` | auto-create (the unused journal topic is dropped) |
| NiFi deployment | Helm chart `apache-nifi-helm/nifi` 0.0.11 | Stackable `nifi-operator` 26.7.0, `NifiCluster nifi-nifi` |
| NiFi clustering state | Kubernetes Leases/ConfigMaps | Kubernetes Leases/ConfigMaps (Stackable default) |
| NiFi auth | OIDC via chart values | `AuthenticationClass <slug>-keycloak` (OIDC, Keycloak provider hint) |
| NiFi authorization | file-based, initial admin `nifi-bootstrap` | `authorization.standard.fileBased.initialAdminUser: nifi-bootstrap` |
| NiFi TLS | chart-generated certificate | Stackable secret-operator `tls` SecretClass |
| NiFi REST URL (config-adapter, bootstrap) | `https://nifi-nifi-0.nifi-nifi.<ns>:8443` | `https://nifi-nifi-node-default-0.nifi-nifi-node-default-headless.<ns>.svc.cluster.local:8443` |
| NiFi UI | internal only (port-forward) | `https://nifi.<domain>` via ingress-nginx (toggle) |
| NiFi initial policies | `nifi-bootstrap` Job | same Job/chart, reused from civitas-core-deployment |
| Shared operators | CloudNativePG + Strimzi | CloudNativePG + Stackable commons/secret/listener/kafka/nifi |
| Service mesh / runtime policies | Linkerd + Kyverno | disabled (see [Deviations](#deviations-from-civitas-core-deployment)) |

The Stackable clusters reuse civitas-core-deployment's release names (`kafka-cluster`,
`nifi-nifi`), so pod labels such as `app.kubernetes.io/instance: nifi-nifi` still match the
NetworkPolicies of the other components (e.g. PostgreSQL allowing NiFi).

---

## How it plugs into civitas-core-deployment

civitas-core-deployment loads components from `components/<name>/`, **unless a directory
`deployment/addons/<name>/` exists** – then that directory replaces the component entirely
(helmfile, charts, values, Keycloak clients, secrets, NetworkPolicies). `/deployment` is
gitignored in civitas-core-deployment.

This project owns such a `deployment/` tree and `just link` symlinks it into the v2
checkout:

```
civitas-stackable-demo                       civitas-core-deployment
├─ helmfile-operators.yaml.gotmpl ────────▶ helmfile-root ─▶ prepare, postgres (operator), networkpolicies
│                                                          └▶ deployment/addons/stackable ─▶ 5 Stackable operators
├─ helmfile-instance.yaml.gotmpl  ────────▶ helmfile-root ─▶ v2 components (keycloak, apisix, portal, …)
│                                                          ├▶ deployment/addons/kafka ─▶ KafkaCluster (KRaft) + kafka-ui
│                                                          └▶ deployment/addons/nifi  ─▶ NifiCluster + OIDC + ingress + bootstrap Job
├─ values/default-instance.yaml   (state values passed into helmfile-root)
└─ deployment/ ◀──── symlink ──── civitas-core-deployment/deployment
```

| Path | Purpose |
|---|---|
| `helmfile-operators.yaml.gotmpl` | Operators layer (once per cluster): `prepare`, `postgres` (operator part), `stackable`, `networkpolicies` → namespace `civitas-operators` |
| `helmfile-instance.yaml.gotmpl` | One instance (namespace = `global.instanceSlug`): full v2 component list without `runtime-policies` |
| `values/default-instance.yaml` | The knobs you normally change (domain, slug, product versions, toggles) |
| `deployment/addons/stackable` | Stackable operator charts from `oci://oci.stackable.tech/sdp-charts` |
| `deployment/addons/kafka` | Local chart `kafka-cluster` (KafkaCluster) + kafka-ui |
| `deployment/addons/nifi` | Local chart `nifi-cluster` (NifiCluster, AuthenticationClass, CA SecretClass, OIDC client Secret, Ingress) + v2's `nifi-bootstrap` chart |
| `deployment/environments/local/` | Environment `local` required by v2's helmfile-root |
| `clusters/{kind,k3d,none}` | Cluster bootstrap per distribution (not needed by the deployment itself) |
| `patches/civitas-core-deployment/` | The one required change to civitas-core-deployment |
| `tests/` | Render, cluster and end-to-end checks used by the `just test-*` recipes |

### Required patch to civitas-core-deployment

config-adapter had NiFi's URL hardcoded. The patch
`patches/civitas-core-deployment/0001-configurable-nifi-url.patch` makes it read
`nifi.nifi.url` and keeps the old URL as default (fully backward compatible). It is
committed on branch `feat/configurable-nifi-url` of the local checkout;
`just apply-v2-patch` applies it to another checkout, `just check-tools` verifies it.

---

## Prerequisites

- `kubectl`, `helm` (3.17+ or 4.x) with the **helm-diff** plugin, `helmfile` ≥ 1.0,
  `yq` v4, `envsubst` (gettext), [`just`](https://github.com/casey/just)
- `kind` (default) or `k3d`, and Docker
- civitas-core-deployment checked out next to this repository, or
  `export CIVITAS_CORE_DEPLOYMENT=/path/to/civitas-core-deployment`
- Resources: the full stack requests ≈ 7.4 CPU / 17 GiB memory and used ≈ 12.5 GiB
  memory on the reference machine (47 pods, ~20 GiB PVCs). The first deployment pulls
  many images and takes 20–30 minutes.

```bash
brew install helmfile yq kind            # helm is installed as a helmfile dependency
helm plugin install https://github.com/databus23/helm-diff   # helm 4: add --verify=false
```

---

## Quickstart

```bash
just check-tools          # tools, v2 checkout, v2 patch
just apply-v2-patch       # only if check-tools asks for it
just deploy               # cluster-up → link → operators → instance
just smoke-test           # end-to-end verification
just create-admin-user    # enable admin@civitas.test for UI logins
just credentials          # Keycloak admin + NiFi UI user
```

### Cluster flavors

| `CLUSTER=` | What `just cluster-up` does |
|---|---|
| `kind` (default) | Creates `KIND_CLUSTER_NAME` (default `kind`) from `clusters/kind/cluster.yaml` **or reuses an existing cluster of that name**, installs ingress-nginx (host ports), cert-manager, the CIVITAS CA `ClusterIssuer selfsigned-ca` and a CoreDNS rewrite `*.civitas.test → ingress-nginx` |
| `k3d` | Runs civitas-core-deployment's `dev-deployment/startup.sh -k` (k3d cluster `civitas-local`) |
| `none` | Installs nothing; checks that the current context provides the prerequisites below |

Any other cluster works if it has: a default StorageClass, ingress-nginx
(`IngressClass nginx`, Service `ingress-nginx/ingress-nginx-controller`), cert-manager with
`ClusterIssuer selfsigned-ca` (Secret `cert-manager/ca-secret`) and in-cluster DNS resolving
`*.<domain>` to the ingress controller. `clusters/common/bootstrap.sh` installs the first
three on any cluster.

### Browser access

The UIs use `https://<name>.civitas.test` and a self-signed CA
(`civitas-core-deployment/dev-deployment/.ssl/civitas.crt`; import it into the browser or
accept the warning). Logins only work on port 443, because the Keycloak clients register
redirect URIs without a port.

1. `just add-hosts` maps `idm portal api dashboard nifi .civitas.test` to 127.0.0.1.
2. Make port 443 reach ingress-nginx:
   - kind cluster created by `just cluster-up`: already mapped.
   - any other cluster (e.g. plain `kind create cluster`): keep `just port-forward 443`
     running (sudo). `just port-forward` without argument uses 8443, where pages load but
     logins fail.
3. `just create-admin-user`, then `just credentials` for the password.
4. Open `https://nifi.civitas.test/nifi` and log in as `admin@civitas.test` through Keycloak.

---

## Working with it

| Recipe | Description |
|---|---|
| `just deploy` | Tools check, cluster, link, operators layer, instance layer |
| `just operators` / `just instance` | Sync one layer (`instance` also creates the dummy `keycloak-smtp` secret) |
| `just sync-component <c>` | Sync one component of the instance, e.g. `nifi`, `kafka`, `keycloak` |
| `just template [layer] [selector]` | Render manifests, e.g. `just template instance name=nifi-nifi` |
| `just diff [layer] [selector]` | Pending changes (helm-diff) |
| `just status` | Operators, Stackable resources, pods and ingresses |
| `just credentials` | Keycloak admin and NiFi demo user |
| `just create-admin-user` | Password for `global.initialUserEmail`, clears pending required actions |
| `just add-hosts` / `just port-forward [port]` | Browser access helpers (`443` for logins, sudo) |
| `just test-render` | Offline helmfile render checks |
| `just test-cluster` / `test-operators` / `test-kafka` / `test-nifi` | Checks per layer |
| `just smoke-test` | All of the above plus config-adapter ↔ Kafka/NiFi and portal |
| `just link` / `just unlink` | Manage the `deployment/` symlink in civitas-core-deployment |
| `just apply-v2-patch` | Apply the config-adapter patch |
| `just destroy` | Delete the cluster (`CLUSTER=…`) and unlink |

Running helmfile directly works too, but export the v2 path first (the addons resolve
charts and defaults inside civitas-core-deployment through it):

```bash
export CIVITAS_CORE_DEPLOYMENT=$(realpath ../civitas-core-deployment)
helmfile -f helmfile-instance.yaml.gotmpl -e local -l component=nifi apply
```

Extra values files (e.g. private overrides) can be layered on top of
`values/default-instance.yaml` with `EXTRA_VALUES=my-values.yaml[,more.yaml] just instance`.

### Kafka

```bash
POD=kafka-cluster-broker-default-0
BS=kafka-cluster-broker-default-bootstrap.dev.svc.cluster.local:9092
kubectl -n dev exec $POD -c kafka -- /stackable/kafka/bin/kafka-topics.sh --bootstrap-server $BS --list
kubectl -n dev exec $POD -c kafka -- /stackable/kafka/bin/kafka-consumer-groups.sh --bootstrap-server $BS --describe --group config-adapter-group
kubectl -n dev exec -i $POD -c kafka -- /stackable/kafka/bin/kafka-console-producer.sh --bootstrap-server $BS --topic my-topic
```

kafka-ui is deployed like in v2 (not exposed): `kubectl -n dev port-forward svc/kafka-ui 8080:80`.

### NiFi

- UI: `https://nifi.civitas.test/nifi`, log in with the user from `just credentials`
  (`admin@civitas.test` gets full operator policies from the `nifi-bootstrap` Job).
- REST with a machine identity (what config-adapter does):

```bash
SECRET=$(kubectl -n dev get secret keycloak-client-nifi-config-adapter -o jsonpath='{.data.client-secret}' | base64 -d)
TOKEN=$(curl -s -d grant_type=client_credentials -d client_id=nifi-config-adapter -d client_secret=$SECRET \
  http://keycloak-app-keycloakx-http.dev.svc.cluster.local/realms/dev/protocol/openid-connect/token | jq -r .access_token)
curl -k -H "Authorization: Bearer $TOKEN" \
  https://nifi-nifi-node-default-0.nifi-nifi-node-default-headless.dev.svc.cluster.local:8443/nifi-api/flow/current-user
```

  (Run it from a pod the NetworkPolicies allow, e.g. labelled `app.kubernetes.io/name=config-adapter`;
  see `in_cluster` in `tests/lib.sh`.)
- Stackable resources: `kubectl -n dev get nificluster nifi-nifi -o yaml` (status conditions),
  operator logs: `kubectl -n civitas-operators logs deploy/stackable-nifi-nifi-operator-deployment`.

---

## Configuration reference

`values/default-instance.yaml`:

| Key | Default | Meaning |
|---|---|---|
| `global.domain` | `civitas.test` | Base domain for `idm.`, `portal.`, `api.`, `dashboard.`, `nifi.` |
| `global.instanceSlug` | `dev` | Namespace **and** Keycloak realm |
| `global.initialUserEmail` | `admin@civitas.test` | Human admin; gets NiFi admin policies |
| `global.profile` | `development` | Selects `development`/`production` values files |
| `global.serviceMesh.enable` | `false` | Linkerd (not supported with the Stackable addons yet) |
| `global.runtimePolicies.enabled` | `false` | Kyverno policies (Strimzi-specific rules) |
| `global.ingress.clusterIssuer` / `ingressClass` | `selfsigned-ca` / `nginx` | TLS issuer and ingress class for all ingresses |
| `kafka.cluster.productVersion` | `4.2.1` | Kafka version (SDP 26.7 supports 3.9.2 LTS and 4.2.1) |
| `nifi.nifi.productVersion` | `2.9.0` | NiFi version |
| `nifi.nifi.ingress.enabled` | `true` | Expose the NiFi UI at `https://nifi.<domain>` |

Addon value keys (override in `values/default-instance.yaml` or an `EXTRA_VALUES` file):

- `kafka.cluster.{enabled,namespace,bootstrapService,bootstrapPort,productVersion,rawValues}`,
  `kafka.ui.{enabled,namespace,rawValues}` – `rawValues` go to the `kafka-cluster` chart
  (`deployment/addons/kafka/charts/kafka-cluster/values.yaml`: replicas, resources,
  `broker.properties` overrides) or kafka-ui.
- `nifi.nifi.{enabled,namespace,nodeCount,initialAdminEmail,bootstrapAdminIdentity,url,postgresqlJdbcVersion,ingress.*,rawValues}`,
  `nifi.bootstrap.{enabled,namespace,rawValues}` – `rawValues` go to the `nifi-cluster`
  chart (`deployment/addons/nifi/charts/nifi-cluster/values.yaml`: resources, storage).
- Operator chart versions: `deployment/addons/stackable/charts.yaml`.

---

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `no matches for kind KafkaCluster/NifiCluster` | Operators layer missing: `just operators` (CRDs are installed by the operators at start-up). |
| `open …/defaults/helm-defaults.yaml: no such file` or `requiredEnv CIVITAS_CORE_DEPLOYMENT` | Running helmfile without the env var: export `CIVITAS_CORE_DEPLOYMENT` (absolute path). helmfile resolves the symlinked addons physically, so relative paths into v2 do not work. |
| `nifi-nifi-oidc-client` or `nifi-nifi-keycloak-ca` Secret missing | They are copied (Helm `lookup`) from `keycloak-client-nifi` / `custom-ca-cert` at install time. If those did not exist yet: `just sync-component nifi`. |
| First `nifi-bootstrap` pod in `Error` | NiFi needs ~2–3 min to start; the Job retries and completes (`kubectl -n dev get job nifi-bootstrap`). |
| NiFi `HTTP ERROR 400 Invalid SNI` through an ingress | NiFi's Jetty only accepts hostnames from its certificate (in-cluster Service names). The chart's ingress sets `proxy-ssl-name`/`upstream-vhost` to `nifi-nifi-node.<ns>.svc.cluster.local`; keep these annotations when changing the ingress. |
| NiFi login returns 401 at `/nifi-api/access/oidc/request` | NiFi 2 moved the login endpoint to `/nifi-api/oauth2/authorization/consumer` (the UI uses it automatically). |
| Keycloak: `Account is not fully set up` | v2 creates `admin@civitas.test` with `VERIFY_EMAIL`; without SMTP run `just create-admin-user`. |
| NiFi `No applicable policies could be found` after changing `bootstrapAdminIdentity` | NiFi only applies the initial admin on first start. Delete the NifiCluster's PVCs (`kubectl -n dev delete nificluster nifi-nifi && kubectl -n dev delete pvc -l app.kubernetes.io/instance=nifi-nifi`) and `just sync-component nifi`. |
| Occasional HTTP 500 from NiFi REST right after a restart | NiFi replicates requests to its node(s); the first replication can time out while the node warms up. Retry (config-adapter does). |
| `*.civitas.test` not resolvable inside pods | kind: `clusters/kind/coredns.sh`; k3s: `coredns-custom` from v2's `startup.sh`. |
| Traffic blocked | Every component gets a default-deny NetworkPolicy; allowed peers are in `deployment/addons/*/networkpolicies.yaml` (`kubectl -n dev get networkpolicy`). |
| `brew install helmfile` upgraded helm to 4.x and `helm plugin install` fails verification | `helm plugin install https://github.com/databus23/helm-diff --verify=false` |

---

## Deviations from civitas-core-deployment

- **Experimental versions:** Kafka 4.2.1 and KRaft mode are marked experimental in SDP 26.7
  (3.9.2 is the LTS line; switch with `kafka.cluster.productVersion` – KRaft is set explicitly).
- **Linkerd and Kyverno disabled:** v2's Kyverno rules only allow Strimzi to create pods
  labelled `app.kubernetes.io/name: kafka` and require Linkerd sidecars; Stackable pods would
  need opaque-port annotations and adjusted policies.
- **NiFi UI exposed** through ingress-nginx at `https://nifi.<domain>` (v2: internal only).
  This also makes the redirect URIs the `nifi` Keycloak client already registers usable.
- **`KafkaTopic kafkasql-journal` dropped** – nothing in v2 references it; all topics are auto-created.
- **TLS:** NiFi and Kafka inter-node certificates come from the Stackable secret-operator;
  Kafka's client listener stays plaintext without authentication like in v2.
- **Single node** Kafka and NiFi (`nifi.nifi.nodeCount > 1` is rejected, as in v2).

## Upstream recommendations for civitas-core-deployment

1. Merge the configurable NiFi URL (`nifi.nifi.url`) patch.
2. Make the addon lookup path configurable instead of the fixed `./deployment/addons`, so
   external projects need no symlink.
3. Make the Kyverno runtime policies and Linkerd annotations operator-agnostic (or add
   Stackable variants) so mesh and policies can stay enabled.
4. Decide on NiFi exposure: either an APISIX route/ingress for `nifi.<domain>` or remove the
   unused redirect URIs from the `nifi` Keycloak client.

## Verification

`just smoke-test` checks, against the live cluster:

1. Cluster prerequisites (ingress-nginx, `ClusterIssuer`, in-cluster DNS).
2. Stackable operators ready, CRDs, `SecretClass tls`, `ListenerClass cluster-internal`.
3. `KafkaCluster` available; produce/consume via the bootstrap Service.
4. `NifiCluster` available; `nifi-bootstrap` Job complete; REST call with a Keycloak
   client-credentials token; UI via ingress; login redirect accepted by Keycloak; full
   authorization-code login as the demo admin with write access to the root process group
   (after `just create-admin-user`).
5. config-adapter ready, consumer group `config-adapter-group` on the Stackable Kafka, REST
   access to NiFi as `nifi-config-adapter`, no NiFi errors in its logs.
6. Portal responds through the ingress.
