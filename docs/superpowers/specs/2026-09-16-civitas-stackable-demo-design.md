# CIVITAS/CORE v2 on Stackable Kafka & NiFi — Design

Date: 2026-09-16
Status: approved in brainstorming, pending spec review

## Goal

A small standalone project that deploys the **full CIVITAS/CORE v2 stack**
(`../civitas-core-deployment`) with two components swapped out:

| v2 today | This project |
|---|---|
| Strimzi operator 0.51.0 + `Kafka` 4.1.0 (KRaft) | Stackable kafka-operator 26.7.0 + `KafkaCluster` 4.2.1 (KRaft) |
| NiFi 2.9.0 via `apache-nifi-helm` chart (Kubernetes state) | Stackable nifi-operator 26.7.0 + `NifiCluster` 2.9.0 (Kubernetes clustering backend) |
| — | Stackable commons-, secret-, listener-operator 26.7.0 |

ZooKeeper is **not** used: v2 does not use it today (Strimzi runs KRaft, NiFi uses
Kubernetes state), Stackable NiFi defaults to the Kubernetes clustering backend and
Stackable Kafka supports KRaft (`metadataManager: kraft`, experimental in 26.7).

The deployment layer is cluster-distribution-agnostic. First target is a local
**kind** cluster; **k3d/k3s** and "bring your own cluster" are supported through
separate cluster bootstrap flavors.

`civitas-stackable-migration-guide.md` (earlier brainstorm) is superseded by this
design; it pins outdated versions (24.7, NiFi 1.x, ZooKeeper) and non-existent CRD
fields. It stays in the repo, marked as superseded.

## Non-goals

- Changing tracked files in `civitas-core-deployment`, with ONE exception decided
  during planning: a backward-compatible patch making config-adapter's NiFi URL a
  value (`nifi.nifi.url`), applied on branch `feat/configurable-nifi-url` in the v2
  checkout and shipped here as `patches/civitas-core-deployment/0001-configurable-nifi-url.patch`
  (§2.4). Further upstream ideas are listed in the README.
- Linkerd service mesh and Kyverno runtime policies (disabled:
  `global.serviceMesh.enable=false`, `global.runtimePolicies.enabled=false`;
  `runtime-policies` removed from the component list).
- Kafka authentication/TLS for clients (v2 contract is plaintext, no auth; kept).
- Production sizing, multi-node Kafka/NiFi, ZooKeeper→KRaft migration.
- CIVITAS/CORE v1 (`../civitas-core`, Ansible): contains no Strimzi or NiFi.

## 1. Integration mechanism

v2's `helmfile-components.yaml.gotmpl` includes `./deployment/addons/<component>/helmfile.yaml.gotmpl`
instead of `./components/<component>/…` whenever that directory exists, and
`civitas.configFiles` reads *all* per-component config files (`charts.yaml`,
`images.yaml`, `default-environment.yaml.gotmpl`, `keycloak-clients.yaml`,
`secrets.yaml`, `databases.yaml`, `networkpolicies*.yaml`, `apisix-routes.yaml`)
from the addon directory in that case. Both paths are relative to the v2 repo root,
and `/deployment` is gitignored in v2.

This project therefore **owns a `deployment/` tree that is symlinked to
`../civitas-core-deployment/deployment`**. `just link` creates the symlink and
refuses if a real `deployment/` directory already exists there. The v2 checkout
location is configurable (`CIVITAS_CORE_DEPLOYMENT` env var / just variable,
default `../civitas-core-deployment`).

### Repository layout

```
civitas-stackable-demo/
├── justfile                        # all workflows (see §4)
├── README.md
├── civitas-stackable-migration-guide.md   # superseded brainstorm (kept)
├── clusters/                       # cluster bootstrap ONLY (distribution-specific)
│   ├── kind/                       # cluster.yaml, up.sh, down.sh
│   ├── k3d/                        # up.sh/down.sh delegating to v2 dev-deployment/startup.sh
│   └── common/                     # shared bootstrap: ingress-nginx, cert-manager, CA issuer
├── helmfile-operators.yaml         # includes v2 helmfile-root.yaml.gotmpl, deployLayer=operators
├── helmfile-instance.yaml.gotmpl   # includes v2 helmfile-root.yaml.gotmpl, deployLayer=instance
├── values/
│   └── default-instance.yaml       # user-facing knobs (domain, slug, components, toggles, versions)
├── deployment/                     # → symlinked as <v2>/deployment
│   ├── environments/local/
│   │   └── global.yaml.gotmpl      # required by v2 root (near-empty)
│   └── addons/
│       ├── stackable/              # operator-layer component: 5 Stackable operators
│       ├── kafka/                  # replaces Strimzi
│       └── nifi/                   # replaces apache-nifi-helm
└── docs/superpowers/specs/…
```

### Entrypoints and values

- `helmfile-operators.yaml.gotmpl`: `deployLayer: operators`, components
  `[prepare, postgres, stackable, networkpolicies]`, `global.operators.watchAllNamespaces: true`.
  Operators land in `civitas-operators` (v2 default for the two-layer model).
- `helmfile-instance.yaml.gotmpl`: `deployLayer: instance`, full v2 component list
  minus `runtime-policies` (`stackable` is not listed; it only has operator parts).
- Both entrypoints are `.gotmpl` so the v2 path can come from the
  `CIVITAS_CORE_DEPLOYMENT` env var.
- Both pass `values/default-instance.yaml` down as state values, so it is the single
  user-facing configuration file (domain, `instanceSlug`, `initialUserEmail`,
  component list, mesh/policy toggles, NiFi ingress toggle, product versions).
  **Fallback** if helmfile's nested state-value precedence does not let these
  override v2's environment defaults: move them into
  `deployment/environments/local/global.yaml.gotmpl` and document it.
- Environment name: `local` (profile `development`). Storage class stays `''`
  (cluster default: `standard` on kind, `local-path` on k3s); ingress class
  `nginx`; ClusterIssuer `selfsigned-ca` — all overridable in the values file.

### Addon component conventions

Each addon follows v2's component anatomy (`civitas-component.yaml`, `charts.yaml`,
`images.yaml` if needed, `default-environment.yaml.gotmpl`,
`values/<part>/{base,development,production}-values.yaml.gotmpl`, a helmfile
modelled on `components/kafka/helmfile.yaml.gotmpl` including the `deployLayer`
filter for `extraLabels.operator: 'true'` parts). Custom resources are rendered by
small local charts inside the addon (`charts/…`), not by `bedag/raw`, so they are
templated, versioned and testable with `helm template`. v2 charts are referenced by
relative path where reused (`../../../components/nifi/charts/nifi-bootstrap`).

## 2. Components

### 2.1 `addons/stackable` (operator layer)

Parts (all `extraLabels.operator: 'true'`, namespace = `civitas.operatorNamespace`):
`commons-operator`, `secret-operator`, `listener-operator`, `kafka-operator`,
`nifi-operator`; charts `oci://oci.stackable.tech/sdp-charts/<name>` version `26.7.0`.
`needs`: kafka/nifi operators need commons, secret and listener operators.
Operators apply their own CRDs at startup (charts ship no `crds/`), so instance
releases rely on the operators layer being synced first.

### 2.2 `addons/kafka`

Keeps v2's value contract so consumers need no change:
`kafka.cluster.{enabled,namespace,bootstrapService,bootstrapPort}`, `kafka.ui.*`.

Parts:
- `cluster` — local chart rendering:
  - `KafkaCluster` (name `kafka-cluster`, same as the Strimzi cluster),
    `image.productVersion: 4.2.1`,
    `clusterConfig.metadataManager: kraft`,
    `clusterConfig.tls.serverSecretClass: null` (plaintext client listener, as v2),
    internal TLS left at the default `tls` SecretClass;
    `controllers` roleGroup `default` replicas 1, `brokers` roleGroup `default`
    replicas 1, broker `bootstrapListenerClass: cluster-internal`;
    `configOverrides` for `broker.properties`: replication factors and
    `min.insync.replicas` = 1, `auto.create.topics.enable=true`,
    `log.retention.ms=2592000000` (as v2).
  - Resources/storage sized like v2 development (profile values).
- `ui` — v2's kafka-ui chart and values, `bootstrapServers` pointed at the new service.

`bootstrapService`/`bootstrapPort` are set to the Stackable bootstrap Service
(expected `kafka-cluster-broker-default-bootstrap`, plaintext port 9092 — **verified on the
live cluster during implementation** and documented).

Dropped: Strimzi `KafkaTopic kafkasql-journal` (unreferenced in v2), KafkaUsers
(none configured). Topics are auto-created as in v2.

`networkpolicies.yaml`: same allowed peers as v2 (portal backend, kafka-ui,
config-adapter, nifi) but selecting Stackable Kafka pods
(`app.kubernetes.io/name: kafka`, `app.kubernetes.io/instance: kafka-cluster`) and
allowing broker↔controller traffic.

### 2.3 `addons/nifi`

Keeps v2's value contract: `nifi.nifi.{enabled,namespace,nodeCount,initialAdminEmail,bootstrapAdminIdentity}`,
`nifi.bootstrap.*`; adds `nifi.nifi.ingress.{enabled,subdomain}` (default
`true`, `nifi`).

Files copied from v2 unchanged: `keycloak-clients.yaml` (clients `nifi`,
`nifi-config-adapter`, `nifi-bootstrap` incl. hardcoded-`sub` mappers). Secrets
come from the v2 `secrets` component as before (`keycloak-client-<id>`).

Parts:
- `nifi` — local chart rendering:
  - `SecretClass <slug>-civitas-ca` (`k8sSearch` backend) + labelled Secret holding
    the platform CA (`ca.crt` copied from `custom-ca-cert` created by v2 `prepare`),
    used to trust Keycloak's HTTPS endpoint. Only when
    `global.ingress.clusterIssuer == selfsigned-ca` (same condition v2 uses);
    otherwise the AuthenticationClass uses `caCert.webPki`.
  - `AuthenticationClass <slug>-keycloak` (cluster-scoped, hence slug-prefixed):
    `oidc.hostname: idm.<domain>`, `port: 443`, `rootPath: /realms/<slug>`,
    `scopes: [openid, email, profile]`, `principalClaim: preferred_username`,
    `providerHint: Keycloak`, TLS verification via the SecretClass above.
  - Secret `nifi-oidc-client` with keys `clientId: nifi`,
    `clientSecret: <from keycloak-client-nifi>` (Helm `lookup`). `lookup` is empty
    during `helm template`/`helmfile template`, so the Secret is then skipped with a
    rendered comment instead of failing; on `sync` the `secrets` component has
    already created the source secret (the `nifi` part `needs` it).
  - `NifiCluster` (name `nifi-nifi`, equal to v2's release name, so Stackable's pod
    labels `app.kubernetes.io/name: nifi` + `app.kubernetes.io/instance: nifi-nifi`
    match the existing v2 NetworkPolicies of postgres and kafka unchanged),
    `image.productVersion: 2.9.0`, no
    `zookeeperConfigMapName` (Kubernetes clustering backend),
    `authentication: [{authenticationClass: <slug>-keycloak, oidc: {clientCredentialsSecret: nifi-oidc-client}}]`,
    `authorization.standard.accessPolicyProvider.fileBased.initialAdminUser: nifi-bootstrap`,
    `sensitiveProperties: {keySecret: nifi-sensitive-property-key, autoGenerate: true}`,
    `nodes.roleGroups.default.replicas: 1`, resources/storage from profile values,
    PostgreSQL JDBC driver provided to NiFi (v2 fetches `postgresql-42.7.4.jar` via
    init container; here via `podOverrides` init container + `extraVolumes`,
    same version).
  - Ingress `nifi.<domain>` (ingress class and cluster issuer from globals,
    `nginx.ingress.kubernetes.io/backend-protocol: HTTPS`) when
    `nifi.nifi.ingress.enabled`. This matches the redirect URIs the `nifi`
    Keycloak client already registers.
- `bootstrap` — v2's `nifi-bootstrap` chart via relative path, v2's values with
  `nifi.url` pointed at the Stackable NiFi pod FQDN (expected
  `https://nifi-nifi-node-default-0.nifi-nifi-node-default-headless.<ns>.svc.cluster.local:8443`,
  verified live).

`networkpolicies.yaml`: v2's peers (apisix, config-adapter, nifi-bootstrap, nifi
itself) plus ingress-nginx (for the UI ingress), selecting Stackable NiFi pods.

**Known risk, verified first during implementation:** machine clients send
Keycloak client-credentials access tokens as `Authorization: Bearer` to NiFi's REST
API and rely on NiFi resolving `sub`. This is NiFi 2.9.0 behaviour (same version),
but depends on Stackable rendering equivalent OIDC properties. If it does not work,
fix via `configOverrides` on `nifi.properties`; if that is impossible, stop and
re-discuss.

### 2.4 Consumer re-wiring (`deployment/environments/local/`)

- v2 patch (see Non-goals): config-adapter uses `.Values.nifi.nifi.url` when set,
  otherwise the old hardcoded URL. The nifi addon sets `nifi.nifi.url` in its
  `default-environment.yaml.gotmpl`. Kafka bootstrap already derives from
  `kafka.cluster.*` keys. No `config-adapters.yaml.gotmpl` override file needed.
- portal backend, kafka-ui: no change needed (key-based).
- Keycloak `nifi` client: unchanged.

## 3. Cluster bootstrap (`clusters/`)

Requirements of the deployment layer on any cluster (documented in README):
default StorageClass, ingress-nginx (class `nginx`), cert-manager with
ClusterIssuer `selfsigned-ca` backed by Secret `cert-manager/ca-secret`,
in-cluster DNS resolving `*.<domain>` to the ingress controller Service, host DNS
for `idm/portal/api/dashboard/nifi.<domain>`.

- `clusters/common/`: installs ingress-nginx and cert-manager via Helm and applies
  the CA issuer using v2's committed CA (`dev-deployment/.ssl/civitas.crt|key`) and
  `dev-deployment/ca-template.yaml`.
- `clusters/kind/`: `cluster.yaml` (cluster name from `KIND_CLUSTER_NAME`, default `kind`; an existing cluster with that name is reused as-is; one control-plane
  node labelled `ingress-ready=true`, host ports 80/443 mapped); `up.sh` creates the
  cluster, runs common bootstrap, patches the CoreDNS Corefile with a
  `*.civitas.test` → `ingress-nginx-controller.ingress-nginx.svc.cluster.local`
  rewrite; `down.sh` deletes it.
- `clusters/k3d/`: delegates to v2 `dev-deployment/startup.sh -k` / `-u` (which
  already does ingress-nginx, cert-manager, CA, CoreDNS for k3s).
- `none`: skip bootstrap; user provides the requirements above.

## 4. Workflow (`justfile`)

Variables: `cluster` (`kind` default | `k3d` | `none`), `v2` (path to
civitas-core-deployment), `env` (`local`).

| Recipe | Does |
|---|---|
| `check-tools` | verifies kubectl, helm, helm-diff, helmfile, kind/k3d, prints install hints |
| `cluster-up` / `cluster-down` | runs `clusters/<cluster>/up.sh` / `down.sh` |
| `link` / `unlink` | manages the `<v2>/deployment` symlink safely |
| `operators` | `helmfile -f helmfile-operators.yaml sync -e local` |
| `instance` | creates `keycloak-smtp` dummy secret if missing, then `helmfile -f helmfile-instance.yaml.gotmpl sync -e local` |
| `deploy` | check-tools → cluster-up → link → operators → instance |
| `sync-component c` | `helmfile … -l component=<c> sync` |
| `template` / `diff` | render/diff without applying |
| `status` | Stackable CRs, pods, ingresses |
| `credentials` | Keycloak admin + initial user info (mirrors v2 recipe) |
| `add-hosts` | prints / appends `/etc/hosts` line (sudo, explicit) |
| `smoke-test` | see §5 |
| `destroy` | cluster-down (+ unlink) |

## 5. Verification

Definition of done: `just deploy` on a fresh kind cluster reaches all releases
synced and `just smoke-test` passes:

1. **Kafka**: produce and consume a message on `de.civitascore.smoke-test` through
   the bootstrap Service from an in-cluster client pod.
2. **NiFi REST**: obtain a client-credentials token for `nifi-bootstrap` from
   Keycloak and `GET /nifi-api/flow/about` → 200; the bootstrap Job completed.
3. **config-adapter**: pod ready and consumer group `config-adapter-group` present
   on the Stackable Kafka.
4. **NiFi UI**: `https://nifi.civitas.test/nifi` redirects to
   `idm.civitas.test/realms/<slug>` login.
5. **Portal**: `https://portal.civitas.test` responds (v2 stack otherwise healthy).

`helm template`/`helmfile template` of the addons is run before deploying.

## 6. README contents

What and why; architecture diagram (text); mapping table (Strimzi/Helm → Stackable
resources, service names, value keys); prerequisites and tool install; quickstart
per cluster flavor; day-2 (sync one component, NiFi/Kafka access, logs, CR status);
configuration reference for `values/default-instance.yaml`; troubleshooting
(NiFi auth file reset, CRDs not yet present, DNS); deviations from v2 (KRaft
experimental + Kafka 4.2.1 experimental in SDP 26.7, mesh/Kyverno off, NiFi
exposed, no journal topic); upstream recommendations for v2 (make config-adapter
NiFi URL a value, configurable addon path, Stackable-aware Kyverno rules).

## 7. Additions decided during planning

- **Stackable operator webhooks:** every Stackable operator serves a CRD conversion
  webhook on 8443 (pods labelled `webhook.stackable.tech/conversion: enabled`).
  `addons/stackable/networkpolicies.yaml` allows 8443 from `0.0.0.0/0` to those
  pods (same pattern as v2's `postgres-operator-webhook`), because v2's
  `networkpolicies` component creates a `default-deny-<component>` per component
  and kind ≥ 0.27 enforces NetworkPolicies.
- **Addon part names** in `stackable`: `commons`, `secret`, `listener`, `kafka`,
  `nifi` (releases `stackable-commons`, …; no dashes in value keys).
- **Keycloak signing:** the v2 realm signs with ES256 only; NiFi gets
  `nifi.security.user.oidc.preferred.jwsalgorithm=ES256` via `configOverrides`.
- **JDBC driver:** NiFi pods get `postgresql-42.7.4.jar` at
  `/opt/nifi/drivers/postgresql.jar` (same path as v2) through a `podOverrides`
  init container and an `emptyDir`.
- **Human NiFi admin:** `just create-admin-user` creates the realm user
  `global.initialUserEmail` (the identity the bootstrap Job grants NiFi admin) with
  a generated password stored in Secret `nifi-demo-admin-user`, so the UI login can
  be demonstrated.
