# CIVITAS/CORE v2 on Stackable Kafka & NiFi — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy the full CIVITAS/CORE v2 stack from `../civitas-core-deployment` with Strimzi Kafka and the NiFi Helm chart replaced by Stackable kafka/nifi operators, driven by a `justfile`, verified on a local kind cluster.

**Architecture:** This repo owns a `deployment/` tree that is symlinked into the v2 checkout (where v2 looks for `deployment/addons/<component>` overrides). Three addon components (`stackable`, `kafka`, `nifi`) follow v2's component anatomy; two thin helmfile entrypoints include v2's `helmfile-root.yaml.gotmpl` for the operators and instance layers. Cluster bootstrap is separated per distribution under `clusters/`.

**Tech Stack:** helmfile 1.x, helm 3.17, kubectl, kind 0.27 (k8s 1.32), Stackable Data Platform 26.7.0 (commons/secret/listener/kafka/nifi operators; Kafka 4.2.1 KRaft; NiFi 2.9.0), ingress-nginx, cert-manager, bash, yq v4, just.

**Spec:** `docs/superpowers/specs/2026-09-16-civitas-stackable-demo-design.md`

## Global Constraints

- Stackable operator charts: `oci://oci.stackable.tech/sdp-charts/<name>-operator`, version `26.7.0`.
- Kafka `image.productVersion: "4.2.1"`, `clusterConfig.metadataManager: kraft`, no ZooKeeper anywhere.
- NiFi `image.productVersion: "2.9.0"`, Kubernetes clustering backend (no `zookeeperConfigMapName`).
- Stackable cluster names: `KafkaCluster kafka-cluster`, `NifiCluster nifi-nifi`.
- v2 value keys kept: `kafka.cluster.{enabled,namespace,bootstrapService,bootstrapPort}`, `kafka.ui.*`, `nifi.nifi.{enabled,namespace,nodeCount,initialAdminEmail,bootstrapAdminIdentity}`, `nifi.bootstrap.*`; new: `nifi.nifi.url`, `nifi.nifi.ingress.{enabled,subdomain}`, `kafka.cluster.productVersion`, `nifi.nifi.productVersion`.
- Globals for the demo: `global.serviceMesh.enable: false`, `global.runtimePolicies.enabled: false`; `runtime-policies` not in any component list.
- Helmfile environment name: `local`. v2 path: env `CIVITAS_CORE_DEPLOYMENT`, default `../civitas-core-deployment`.
- Only change to v2 tracked files: config-adapter NiFi URL patch on branch `feat/configurable-nifi-url`.
- kind cluster name: env `KIND_CLUSTER_NAME`, default `kind` (the user's existing cluster is `kind`).
- Every commit message ends with `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`.
- Shell scripts: `#!/usr/bin/env bash` + `set -euo pipefail`, executable bit set.
- yq is the snap build: it cannot open files under `/tmp`; always pipe content via stdin (`cat f | yq ...`).

## File Structure

```
civitas-stackable-demo/
├── .gitignore
├── justfile                                   # all workflows
├── README.md                                  # Task 8
├── civitas-stackable-migration-guide.md       # superseded banner (Task 8)
├── helmfile-operators.yaml.gotmpl             # operators layer entrypoint
├── helmfile-instance.yaml.gotmpl              # instance layer entrypoint
├── values/default-instance.yaml               # user-facing knobs (state values)
├── patches/civitas-core-deployment/0001-configurable-nifi-url.patch
├── clusters/
│   ├── common/bootstrap.sh                    # ingress-nginx, cert-manager, CA issuer (any cluster)
│   ├── kind/{cluster.yaml,up.sh,down.sh,coredns.sh}
│   ├── k3d/{up.sh,down.sh}                    # delegates to v2 dev-deployment/startup.sh
│   └── none/{up.sh,down.sh}                   # verifies prerequisites only
├── deployment/                                # symlinked to <v2>/deployment
│   ├── environments/local/global.yaml.gotmpl
│   └── addons/
│       ├── stackable/  civitas-component.yaml charts.yaml default-environment.yaml.gotmpl
│       │               helmfile.yaml.gotmpl networkpolicies.yaml values/<part>/{base,development,production}-values.yaml.gotmpl
│       ├── kafka/      civitas-component.yaml charts.yaml default-environment.yaml.gotmpl helmfile.yaml.gotmpl
│       │               networkpolicies.yaml charts/kafka-cluster/** values/{cluster,ui}/*
│       └── nifi/       civitas-component.yaml charts.yaml images.yaml default-environment.yaml.gotmpl helmfile.yaml.gotmpl
│                       keycloak-clients.yaml networkpolicies.yaml charts/nifi-cluster/** values/{nifi,bootstrap}/*
└── tests/
    ├── lib.sh                                 # assertion helpers
    ├── render.sh                              # offline helmfile render checks
    ├── cluster.sh                             # cluster bootstrap checks
    ├── operators.sh                           # Stackable operators checks
    ├── kafka.sh                               # Kafka produce/consume
    ├── nifi.sh                                # NiFi REST + UI redirect
    └── smoke.sh                               # everything incl. config-adapter + portal
```

---

### Task 1: Project scaffold, helmfile wiring, render test harness

**Files:**
- Create: `.gitignore`, `justfile`, `values/default-instance.yaml`, `helmfile-operators.yaml.gotmpl`, `helmfile-instance.yaml.gotmpl`, `deployment/environments/local/global.yaml.gotmpl`, `tests/lib.sh`, `tests/render.sh`

**Interfaces:**
- Produces: `just link|unlink|check-tools|template|build`, `tests/lib.sh` functions `pass`, `fail`, `assert_eq NAME EXPECTED ACTUAL`, `assert_contains NAME NEEDLE HAYSTACK`, `finish`, `hf LAYER ARGS...` (runs helmfile on an entrypoint with `-e local`), env `ROOT`, `CIVITAS_CORE_DEPLOYMENT`.

- [ ] **Step 1: Install missing tools (ask the user before installing)**

Run: `brew install helmfile && helm plugin install https://github.com/databus23/helm-diff`
Expected: `helmfile --version` prints `helmfile version 1.x`; `helm plugin list` shows `diff`.

- [ ] **Step 2: Write test helpers `tests/lib.sh`**

```bash
#!/usr/bin/env bash
# Shared helpers for the civitas-stackable-demo test scripts.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CIVITAS_CORE_DEPLOYMENT="${CIVITAS_CORE_DEPLOYMENT:-$ROOT/../civitas-core-deployment}"
FAILED=0

pass() { printf '  \033[32mPASS\033[0m %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$1"; FAILED=1; }

assert_eq() { # name expected actual
  if [ "$2" = "$3" ]; then pass "$1"; else fail "$1 (expected '$2', got '$3')"; fi
}

assert_contains() { # name needle haystack
  if grep -qF -- "$2" <<<"$3"; then pass "$1"; else fail "$1 (missing '$2')"; fi
}

finish() {
  if [ "$FAILED" = 0 ]; then echo "All checks passed"; else echo "Some checks failed"; exit 1; fi
}

hf() { # layer (operators|instance), remaining args passed to helmfile
  local layer=$1; shift
  helmfile -f "$ROOT/helmfile-$layer.yaml.gotmpl" -e local "$@"
}

instance_ns() { cat "$ROOT/values/default-instance.yaml" | yq '.global.instanceSlug'; }
```

- [ ] **Step 3: Write the failing render test `tests/render.sh`**

```bash
#!/usr/bin/env bash
# Offline checks: helmfile state renders with our values and component lists.
source "$(dirname "$0")/lib.sh"

echo "== operators layer"
ops=$(hf operators list 2>&1)
assert_contains "postgres operator in operators layer" "postgres-operator" "$ops"
if grep -q "runtime-policies" <<<"$ops"; then fail "runtime-policies must not be deployed"; else pass "no runtime-policies"; fi

echo "== instance layer"
inst=$(hf instance list 2>&1)
assert_contains "keycloak release present" "keycloak-app" "$inst"
if grep -q "runtime-policies" <<<"$inst"; then fail "runtime-policies must not be deployed"; else pass "no runtime-policies"; fi
if grep -Eq "postgres-operator|kafka-operator" <<<"$inst"; then fail "operator releases leaked into instance layer"; else pass "no operators in instance layer"; fi

echo "== state values override v2 defaults"
built=$(hf instance -l name=config-adapters-adapters build --embed-values 2>&1)
mesh=$(grep -A1 'customServiceMesh:' <<<"$built" | grep -o 'enable: [a-z]*' | head -1)
assert_eq "serviceMesh disabled via values/default-instance.yaml" "enable: false" "$mesh"

finish
```

- [ ] **Step 4: Run it to verify it fails**

Run: `chmod +x tests/*.sh && tests/render.sh`
Expected: FAIL / error `no such file helmfile-operators.yaml.gotmpl`.

- [ ] **Step 5: Create `values/default-instance.yaml`**

```yaml
---
# User-facing configuration of the CIVITAS/CORE + Stackable demo.
# Both entrypoints pass this file as state values into civitas-core-deployment's
# helmfile-root.yaml.gotmpl, so every key here overrides v2's
# defaults/environment/global.yaml and the addons' default-environment files.
global:
  domain: civitas.test
  # Namespace AND Keycloak realm name (v2 singleNamespace mode).
  instanceSlug: dev
  # Realm user that nifi-bootstrap grants full NiFi admin; see `just create-admin-user`.
  initialUserEmail: admin@civitas.test
  profile: development
  serviceMesh:
    enable: false
  runtimePolicies:
    enabled: false
  ingress:
    clusterIssuer: selfsigned-ca
    ingressClass: nginx

kafka:
  cluster:
    productVersion: "4.2.1"

nifi:
  nifi:
    productVersion: "2.9.0"
    ingress:
      # Expose the NiFi UI at https://nifi.<domain> (v2 keeps NiFi internal-only).
      enabled: true
```

- [ ] **Step 6: Create `helmfile-operators.yaml.gotmpl`**

```yaml
---
# Shared cluster operators: CloudNativePG (from civitas-core-deployment) and the
# Stackable operators (deployment/addons/stackable). Sync once per cluster.
#
#   just operators   (== helmfile -f helmfile-operators.yaml.gotmpl -e local sync)
environments:
  local:
    values: []
---
helmfiles:
  - path: '{{ env "CIVITAS_CORE_DEPLOYMENT" | default "../civitas-core-deployment" }}/helmfile-root.yaml.gotmpl'
    values:
      - values/default-instance.yaml
      - environments:
          - local
        deployLayer: operators
        components:
          - prepare
          - postgres
          - stackable
          - networkpolicies
        global:
          operators:
            watchAllNamespaces: true
```

- [ ] **Step 7: Create `helmfile-instance.yaml.gotmpl`**

```yaml
---
# One CIVITAS/CORE instance (namespace = global.instanceSlug) on Stackable Kafka/NiFi.
# `kafka` and `nifi` resolve to deployment/addons/{kafka,nifi} because that directory
# exists in civitas-core-deployment (symlinked by `just link`).
#
#   just instance    (== helmfile -f helmfile-instance.yaml.gotmpl -e local sync)
environments:
  local:
    values: []
---
helmfiles:
  - path: '{{ env "CIVITAS_CORE_DEPLOYMENT" | default "../civitas-core-deployment" }}/helmfile-root.yaml.gotmpl'
    values:
      - values/default-instance.yaml
      - environments:
          - local
        deployLayer: instance
        components:
          - prepare
          - secrets
          - networkpolicies
          - postgres
          - etcd
          - kafka
          - keycloak
          - authz
          - apisix
          - frost
          - nifi
          - config-adapters
          - portal
          - geoserver
          - valkey
          - superset
```

- [ ] **Step 8: Create `deployment/environments/local/global.yaml.gotmpl`**

```yaml
---
# Required by civitas-core-deployment/helmfile-root.yaml.gotmpl for environment "local".
# Configuration lives in civitas-stackable-demo/values/default-instance.yaml.
```

- [ ] **Step 9: Create `.gitignore`**

```
.claude/
```

- [ ] **Step 10: Create `justfile` (core recipes; later tasks append more)**

```just
set shell := ["bash", "-euo", "pipefail", "-c"]

export CIVITAS_CORE_DEPLOYMENT := env("CIVITAS_CORE_DEPLOYMENT", justfile_directory() / ".." / "civitas-core-deployment")
cluster := env("CLUSTER", "kind")
env := "local"

# List recipes
default:
    @just --list

# Verify required CLI tools and the civitas-core-deployment checkout
check-tools:
    #!/usr/bin/env bash
    set -euo pipefail
    missing=0
    for t in kubectl helm helmfile yq envsubst; do
      command -v "$t" >/dev/null || { echo "missing: $t"; missing=1; }
    done
    case "{{cluster}}" in
      kind) command -v kind >/dev/null || { echo "missing: kind"; missing=1; } ;;
      k3d)  command -v k3d  >/dev/null || { echo "missing: k3d";  missing=1; } ;;
    esac
    helm plugin list | grep -q '^diff' || { echo "missing: helm-diff (helm plugin install https://github.com/databus23/helm-diff)"; missing=1; }
    [ -f "$CIVITAS_CORE_DEPLOYMENT/helmfile-root.yaml.gotmpl" ] || { echo "civitas-core-deployment not found at $CIVITAS_CORE_DEPLOYMENT (set CIVITAS_CORE_DEPLOYMENT)"; missing=1; }
    if [ "$missing" = 1 ]; then echo "Install hints: brew install helmfile yq kind k3d gettext"; exit 1; fi
    echo "All tools present."

# Symlink ./deployment into civitas-core-deployment (where v2 looks for addons)
link:
    #!/usr/bin/env bash
    set -euo pipefail
    target="$CIVITAS_CORE_DEPLOYMENT/deployment"
    src="{{justfile_directory()}}/deployment"
    if [ -L "$target" ]; then
      if [ "$(readlink -f "$target")" = "$(readlink -f "$src")" ]; then echo "already linked: $target"; exit 0; fi
      echo "ERROR: $target already links to $(readlink "$target")"; exit 1
    elif [ -e "$target" ]; then
      echo "ERROR: $target exists and is not a symlink; move it away first"; exit 1
    fi
    ln -s "$src" "$target"
    echo "linked $target -> $src"

# Remove the deployment symlink from civitas-core-deployment
unlink:
    #!/usr/bin/env bash
    set -euo pipefail
    target="$CIVITAS_CORE_DEPLOYMENT/deployment"
    if [ -L "$target" ]; then rm "$target" && echo "removed $target"; else echo "no symlink at $target"; fi

# Render manifests of a layer (operators|instance), optional helmfile selector
template layer="instance" selector="":
    helmfile -f helmfile-{{layer}}.yaml.gotmpl -e {{env}} {{ if selector != "" { "-l " + selector } else { "" } }} template

# Show pending changes of a layer (needs helm-diff)
diff layer="instance" selector="":
    helmfile -f helmfile-{{layer}}.yaml.gotmpl -e {{env}} {{ if selector != "" { "-l " + selector } else { "" } }} diff

# Offline render checks
test-render: link
    tests/render.sh
```

- [ ] **Step 11: Link and run the render test**

Run: `just link && just test-render`
Expected: all PASS. If "serviceMesh disabled" fails, helmfile does not let parent state values override v2 env defaults: move the `global:` block of `values/default-instance.yaml` into `deployment/environments/local/global.yaml.gotmpl`, keep only a comment in the values file pointing there, update the spec's "Fallback" note, re-run until PASS. If `build` output format differs, adjust only the grep in the test, not the expectation.

- [ ] **Step 12: Commit**

```bash
git add .gitignore justfile values helmfile-*.gotmpl deployment tests
git commit -m "feat: scaffold helmfile entrypoints wired into civitas-core-deployment

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: v2 patch — configurable config-adapter NiFi URL

**Files:**
- Modify (v2 repo, branch `feat/configurable-nifi-url`): `../civitas-core-deployment/components/config-adapters/values/adapters/base-values.yaml.gotmpl` (the `$nifiUrl` line, ~line 136)
- Create: `patches/civitas-core-deployment/0001-configurable-nifi-url.patch`
- Modify: `tests/render.sh`, `justfile`

**Interfaces:**
- Produces: v2 honours `.Values.nifi.nifi.url`; `just apply-v2-patch`.

- [ ] **Step 1: Add failing assertion to `tests/render.sh` (before `finish`)**

```bash
echo "== config-adapter uses nifi.nifi.url"
ca=$(hf instance -l name=config-adapters-adapters --state-values-set nifi.nifi.url=https://nifi.example:8443 build --embed-values 2>&1)
assert_contains "NIFI_URL taken from nifi.nifi.url" "https://nifi.example:8443" "$ca"
```

- [ ] **Step 2: Run to verify it fails**

Run: `tests/render.sh`
Expected: `FAIL NIFI_URL taken from nifi.nifi.url`.

- [ ] **Step 3: Create branch and patch v2**

```bash
git -C ../civitas-core-deployment switch -c feat/configurable-nifi-url
```
Replace in `components/config-adapters/values/adapters/base-values.yaml.gotmpl`:
```
  # $nifiUrl must be the same as `NIFI_WEB_HTTPS_HOST="${host_name}"` in apache-nifi-helm/templates/configmap.yaml
  {{- $nifiUrl := printf "https://nifi-nifi-0.nifi-nifi.%s:8443" .Values.nifi.nifi.namespace }}
```
with
```
  # Defaults to the apache-nifi-helm pod FQDN, which must match `NIFI_WEB_HTTPS_HOST="${host_name}"`
  # in apache-nifi-helm/templates/configmap.yaml. Replacement NiFi components set `nifi.nifi.url`.
  {{- $nifiUrl := .Values.nifi.nifi.url | default (printf "https://nifi-nifi-0.nifi-nifi.%s:8443" .Values.nifi.nifi.namespace) }}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `tests/render.sh`
Expected: all PASS. If `--state-values-set` does not reach the nested helmfiles (assertion still fails although the template line is patched), change the test to write a temporary values file `values/test-nifi-url.yaml` containing `nifi: {nifi: {url: https://nifi.example:8443}}`, pass it by temporarily adding it as the last entry of the entrypoint's `values:` list inside the test (`sed` to a temp copy of `helmfile-instance.yaml.gotmpl` in the repo root, named `.helmfile-instance-test.yaml.gotmpl`, deleted by a `trap`), and re-run.

- [ ] **Step 5: Commit in v2 and export the patch**

```bash
git -C ../civitas-core-deployment commit -am "feat(config-adapters): make NiFi URL configurable via nifi.nifi.url

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
mkdir -p patches/civitas-core-deployment
git -C ../civitas-core-deployment format-patch -1 --stdout > patches/civitas-core-deployment/0001-configurable-nifi-url.patch
```

- [ ] **Step 6: Add `apply-v2-patch` recipe to `justfile`**

```just
# Apply the required config-adapter patch to civitas-core-deployment (idempotent)
apply-v2-patch:
    #!/usr/bin/env bash
    set -euo pipefail
    f="$CIVITAS_CORE_DEPLOYMENT/components/config-adapters/values/adapters/base-values.yaml.gotmpl"
    if grep -q 'nifi.nifi.url' "$f"; then echo "patch already applied"; exit 0; fi
    git -C "$CIVITAS_CORE_DEPLOYMENT" am "{{justfile_directory()}}/patches/civitas-core-deployment/0001-configurable-nifi-url.patch"
```
And add to `check-tools` before the final `if`:
```bash
    grep -q 'nifi.nifi.url' "$CIVITAS_CORE_DEPLOYMENT/components/config-adapters/values/adapters/base-values.yaml.gotmpl" 2>/dev/null \
      || { echo "civitas-core-deployment lacks the NiFi URL patch: run 'just apply-v2-patch'"; missing=1; }
```

- [ ] **Step 7: Commit**

```bash
git add patches justfile tests/render.sh
git commit -m "feat: ship config-adapter NiFi URL patch for civitas-core-deployment

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Cluster bootstrap (kind / k3d / none)

**Files:**
- Create: `clusters/common/bootstrap.sh`, `clusters/kind/cluster.yaml`, `clusters/kind/up.sh`, `clusters/kind/down.sh`, `clusters/kind/coredns.sh`, `clusters/k3d/up.sh`, `clusters/k3d/down.sh`, `clusters/none/up.sh`, `clusters/none/down.sh`, `tests/cluster.sh`
- Modify: `justfile`

**Interfaces:**
- Consumes: `CIVITAS_CORE_DEPLOYMENT` (CA files `dev-deployment/.ssl/civitas.{crt,key}`, `dev-deployment/ca-template.yaml`).
- Produces: `just cluster-up|cluster-down|test-cluster`; cluster has ingress-nginx (ns `ingress-nginx`, svc `ingress-nginx-controller`), cert-manager, ClusterIssuer `selfsigned-ca`, Secret `cert-manager/ca-secret`, in-cluster DNS `*.civitas.test` → ingress controller.

- [ ] **Step 1: Write failing test `tests/cluster.sh`**

```bash
#!/usr/bin/env bash
# Cluster prerequisites for the deployment layer (any distribution).
source "$(dirname "$0")/lib.sh"
DOMAIN=$(cat "$ROOT/values/default-instance.yaml" | yq '.global.domain')

echo "== ingress-nginx"
ready=$(kubectl -n ingress-nginx get deploy ingress-nginx-controller -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
assert_eq "ingress-nginx controller ready" "1" "${ready:-0}"

echo "== cert-manager"
issuer=$(kubectl get clusterissuer selfsigned-ca -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)
assert_eq "ClusterIssuer selfsigned-ca Ready" "True" "${issuer:-missing}"

echo "== in-cluster DNS"
svc_ip=$(kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.spec.clusterIP}')
resolved=$(kubectl run dns-check-$RANDOM --rm -i --restart=Never --image=busybox:1.36 --quiet -- nslookup "idm.$DOMAIN" 2>/dev/null | awk '/^Address: /{print $2}' | tail -1 || true)
assert_eq "idm.$DOMAIN resolves to ingress controller" "$svc_ip" "$resolved"

finish
```

- [ ] **Step 2: Run to verify it fails**

Run: `chmod +x tests/cluster.sh && KUBECONFIG=${KUBECONFIG:-~/.kube/config} tests/cluster.sh`
Expected: FAIL on all three checks (fresh cluster).

- [ ] **Step 3: Create `clusters/common/bootstrap.sh`**

```bash
#!/usr/bin/env bash
# Distribution-independent cluster prerequisites: ingress-nginx, cert-manager and the
# CIVITAS self-signed CA ClusterIssuer (CA material from civitas-core-deployment).
set -euo pipefail

: "${CIVITAS_CORE_DEPLOYMENT:?}"
DOMAIN="${DOMAIN:-civitas.test}"
INGRESS_NGINX_VERSION="${INGRESS_NGINX_VERSION:-4.13.2}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.18.2}"
INGRESS_EXTRA_ARGS=("$@")   # e.g. --set controller.hostPort.enabled=true

helm upgrade --install ingress-nginx ingress-nginx \
  --repo https://kubernetes.github.io/ingress-nginx --version "$INGRESS_NGINX_VERSION" \
  --namespace ingress-nginx --create-namespace \
  --set controller.config.annotations-risk-level=Critical \
  --set controller.config.enable-annotation-validation=false \
  --set controller.config.proxy-buffer-size=64k \
  --set-string controller.config.proxy-buffers="4 64k" \
  --set controller.config.proxy-busy-buffers-size=128k \
  "${INGRESS_EXTRA_ARGS[@]}" \
  --wait --timeout 10m

helm upgrade --install cert-manager cert-manager \
  --repo https://charts.jetstack.io --version "$CERT_MANAGER_VERSION" \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true \
  --wait --timeout 10m

SSL="$CIVITAS_CORE_DEPLOYMENT/dev-deployment/.ssl"
CA_CERT=$(base64 -w0 < "$SSL/civitas.crt")
CA_KEY=$(base64 -w0 < "$SSL/civitas.key")
export DOMAIN CA_CERT CA_KEY
envsubst '${DOMAIN} ${CA_CERT} ${CA_KEY}' < "$CIVITAS_CORE_DEPLOYMENT/dev-deployment/ca-template.yaml" | kubectl apply -f -
kubectl wait --for=condition=Ready clusterissuer/selfsigned-ca --timeout=120s
echo "Common bootstrap done."
```

- [ ] **Step 4: Create `clusters/kind/cluster.yaml`**

```yaml
# kind cluster for the CIVITAS/CORE Stackable demo: host ports 80/443 reach ingress-nginx.
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 80
        hostPort: 80
        protocol: TCP
      - containerPort: 443
        hostPort: 443
        protocol: TCP
```

- [ ] **Step 5: Create `clusters/kind/coredns.sh`**

```bash
#!/usr/bin/env bash
# Resolve *.<domain> inside the cluster to the ingress-nginx controller Service
# (kind has no k3s-style coredns-custom import, so the Corefile is patched).
set -euo pipefail
DOMAIN="${DOMAIN:-civitas.test}"
escaped=${DOMAIN//./\\.}
rule="    rewrite stop name regex (.*\\.)?${escaped}\\.? ingress-nginx-controller.ingress-nginx.svc.cluster.local. answer auto"

corefile=$(kubectl -n kube-system get configmap coredns -o jsonpath='{.data.Corefile}')
if grep -qF "ingress-nginx-controller.ingress-nginx.svc.cluster.local" <<<"$corefile"; then
  echo "CoreDNS already rewrites *.$DOMAIN"; exit 0
fi
patched=$(awk -v rule="$rule" '{print} /^\.:53 \{/{print rule}' <<<"$corefile")
kubectl -n kube-system create configmap coredns --from-literal=Corefile="$patched" --dry-run=client -o yaml \
  | kubectl apply -f -
kubectl -n kube-system rollout restart deployment/coredns
kubectl -n kube-system rollout status deployment/coredns --timeout=120s
```

- [ ] **Step 6: Create `clusters/kind/up.sh` and `down.sh`**

`up.sh`:
```bash
#!/usr/bin/env bash
# Create (or reuse) a kind cluster and install the common prerequisites.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
name="${KIND_CLUSTER_NAME:-kind}"

if kind get clusters | grep -qx "$name"; then
  echo "Reusing existing kind cluster '$name'"
else
  kind create cluster --name "$name" --config "$here/cluster.yaml"
fi
kubectl config use-context "kind-$name"

# Bind ingress-nginx to the node's host ports so kind's extraPortMappings reach it.
"$here/../common/bootstrap.sh" \
  --set controller.hostPort.enabled=true \
  --set controller.service.type=ClusterIP \
  --set controller.publishService.enabled=false
"$here/coredns.sh"
```
`down.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
kind delete cluster --name "${KIND_CLUSTER_NAME:-kind}"
```

- [ ] **Step 7: Create `clusters/k3d/up.sh`, `down.sh`**

`up.sh`:
```bash
#!/usr/bin/env bash
# k3d/k3s: reuse civitas-core-deployment's dev bootstrap (cluster, ingress-nginx, MetalLB,
# cert-manager, CA issuer, coredns-custom).
set -euo pipefail
: "${CIVITAS_CORE_DEPLOYMENT:?}"
"$CIVITAS_CORE_DEPLOYMENT/dev-deployment/startup.sh" -k
```
`down.sh`:
```bash
#!/usr/bin/env bash
set -euo pipefail
k3d cluster delete civitas-local
```

- [ ] **Step 8: Create `clusters/none/up.sh`, `down.sh`**

`up.sh`:
```bash
#!/usr/bin/env bash
# Bring-your-own cluster: nothing is installed, prerequisites are only checked.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
cat <<'EOF'
Using the current kube context as-is. Required:
  - default StorageClass
  - ingress-nginx (IngressClass "nginx", Service ingress-nginx/ingress-nginx-controller)
  - cert-manager with ClusterIssuer "selfsigned-ca" (Secret cert-manager/ca-secret)
  - in-cluster DNS resolving *.<domain> to the ingress controller
EOF
"$here/../../tests/cluster.sh"
```
`down.sh`:
```bash
#!/usr/bin/env bash
echo "cluster=none: nothing to tear down"
```

- [ ] **Step 9: Append recipes to `justfile`**

```just
# Create/prepare the cluster (CLUSTER=kind|k3d|none, KIND_CLUSTER_NAME for kind)
cluster-up:
    clusters/{{cluster}}/up.sh

# Delete the cluster (CLUSTER=kind|k3d|none)
cluster-down:
    clusters/{{cluster}}/down.sh

# Check cluster prerequisites
test-cluster:
    tests/cluster.sh
```

- [ ] **Step 10: Run bootstrap on the user's existing kind cluster and test**

Run: `chmod +x clusters/*/*.sh && KIND_CLUSTER_NAME=kind just cluster-up && just test-cluster`
Expected: all PASS. If the chart versions do not exist, pick the newest with `helm search repo`/`helm show chart --repo ... --version` and update the defaults. If DNS fails with `rewrite ... answer auto`, replace the rule with a `template` block answering an `A` record with the controller ClusterIP and re-run.

- [ ] **Step 11: Commit**

```bash
git add clusters tests/cluster.sh justfile
git commit -m "feat: cluster bootstrap for kind, k3d and existing clusters

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: `stackable` addon (operators layer)

**Files:**
- Create: `deployment/addons/stackable/{civitas-component.yaml,charts.yaml,default-environment.yaml.gotmpl,helmfile.yaml.gotmpl,networkpolicies.yaml}`, `deployment/addons/stackable/values/{commons,secret,listener,kafka,nifi}/{base,development,production}-values.yaml.gotmpl`, `tests/operators.sh`
- Modify: `tests/render.sh`, `justfile`

**Interfaces:**
- Produces: releases `stackable-commons|secret|listener|kafka|nifi` in `civitas-operators`; CRDs `kafkaclusters.kafka.stackable.tech`, `nificlusters.nifi.stackable.tech`, `authenticationclasses.authentication.stackable.tech`, `secretclasses.secrets.stackable.tech`; SecretClass `tls`; value keys `stackable.<part>.{enabled,namespace}`. The addon helmfile template below is reused verbatim by Task 5 and Task 6.

- [ ] **Step 1: Add failing render assertions to `tests/render.sh` (before `finish`)**

```bash
echo "== stackable operators"
ops=$(hf operators list 2>&1)
for p in commons secret listener kafka nifi; do
  assert_contains "release stackable-$p" "stackable-$p" "$ops"
done
tpl=$(hf operators -l name=stackable-kafka template 2>&1)
assert_contains "kafka-operator image 26.7.0" "oci.stackable.tech/sdp/kafka-operator:26.7.0" "$tpl"
np=$(hf operators -l policy-name=stackable-operator-webhooks template 2>&1)
assert_contains "webhook NetworkPolicy port 8443" "port: 8443" "$np"
```

- [ ] **Step 2: Run to verify it fails**

Run: `tests/render.sh`
Expected: FAIL `release stackable-commons` (component dir missing → helmfile error).

- [ ] **Step 3: Create `deployment/addons/stackable/civitas-component.yaml`**

```yaml
---
# Stackable Data Platform operators shared by the kafka and nifi addons.
component: stackable
parts:
  - name: commons
    extraLabels:
      operator: 'true'
  - name: secret
    extraLabels:
      operator: 'true'
  - name: listener
    extraLabels:
      operator: 'true'
  - name: kafka
    extraLabels:
      operator: 'true'
    needs:
      - stackable.commons
      - stackable.secret
      - stackable.listener
  - name: nifi
    extraLabels:
      operator: 'true'
    needs:
      - stackable.commons
      - stackable.secret
      - stackable.listener
```

- [ ] **Step 4: Create `deployment/addons/stackable/charts.yaml`**

```yaml
---
stackable:
  commons:
    repository: oci.stackable.tech/sdp-charts
    oci: true
    chart: stackable/commons-operator
    version: '26.7.0'
  secret:
    repository: oci.stackable.tech/sdp-charts
    oci: true
    chart: stackable/secret-operator
    version: '26.7.0'
  listener:
    repository: oci.stackable.tech/sdp-charts
    oci: true
    chart: stackable/listener-operator
    version: '26.7.0'
  kafka:
    repository: oci.stackable.tech/sdp-charts
    oci: true
    chart: stackable/kafka-operator
    version: '26.7.0'
  nifi:
    repository: oci.stackable.tech/sdp-charts
    oci: true
    chart: stackable/nifi-operator
    version: '26.7.0'
```

- [ ] **Step 5: Create `deployment/addons/stackable/default-environment.yaml.gotmpl`**

```yaml
---
{{- $ns := include "civitas.operatorNamespace" (dict "global" .Values.global "suffix" "stackable" "deployLayer" .Values.deployLayer) }}
stackable:
  commons:
    enabled: true
    namespace: {{ $ns }}
  secret:
    enabled: true
    namespace: {{ $ns }}
  listener:
    enabled: true
    namespace: {{ $ns }}
  kafka:
    enabled: true
    namespace: {{ $ns }}
  nifi:
    enabled: true
    namespace: {{ $ns }}
```

- [ ] **Step 6: Create the shared addon helmfile `deployment/addons/stackable/helmfile.yaml.gotmpl`**

This is v2's `components/kafka/helmfile.yaml.gotmpl` (deployLayer filter) plus OCI repositories, per-part `timeout`, and v2's `values/<part>/{base,<profile>}-values.yaml.gotmpl` layout. Tasks 5 and 6 copy this file unchanged.

```yaml
---
bases:
  - "../../../defaults/helm-defaults.yaml"
---
{{- $componentData := (readFile "./civitas-component.yaml" | fromYaml) }}

{{- $root := .Values }}
{{- $component := $componentData.component }}
{{- $parts := $componentData.parts }}

repositories:
{{- $alreadyAdded := dict }}
{{- range $parts }}
  {{- $chartCfg := index $root.charts $component .name }}
  {{- $repoUrl := index $chartCfg "repository" | default "" }}
  {{- if ne $repoUrl "" }}
    {{- $repoName := index (splitList "/" $chartCfg.chart) 0 }}
    {{- if not (hasKey $alreadyAdded $repoName) }}
      {{- $_ := set $alreadyAdded $repoName true }}
  - name: cc2-{{ $repoName }}
    url: {{ $repoUrl }}
    {{- if index $chartCfg "oci" }}
    oci: true
    {{- end }}
    {{- end }}
  {{- end }}
{{- end }}

releases:
  {{- $layer := $root.deployLayer | default "" }}
  {{- range $parts }}
  {{- $part := . }}
  {{- $releaseName := printf "%s-%s" $component $part.name }}
  {{- $chartCfg := index $root.charts $component $part.name }}
  {{- $repoUrl := index $chartCfg "repository" | default "" }}
  {{- $partLabels := index $part "extraLabels" | default dict }}
  {{- $isOperator := eq (index $partLabels "operator" | default "") "true" }}
  {{- if or (eq $layer "") (and (eq $layer "operators") $isOperator) (and (eq $layer "instance") (not $isOperator)) }}
  - name: {{ $releaseName }}
    labels:
      release: {{ $releaseName }}
      {{- range $key, $value := $partLabels }}
      {{ $key }}: {{ $value | quote }}
      {{- end }}
    namespace: {{ index $root $component $part.name "namespace" }}
    {{- if ne $repoUrl "" }}
    chart: cc2-{{ $chartCfg.chart }}
    {{- else }}
    chart: {{ $chartCfg.chart }}
    {{- end }}
    {{- if index $chartCfg "version" }}
    version: {{ $chartCfg.version | quote }}
    {{- end }}
    createNamespace: {{ $root.global.createNamespaces }}
    {{- if index $part "timeout" }}
    timeout: {{ $part.timeout }}
    {{- end }}
    installed: {{ index $root $component $part.name "enabled" }}
    condition: {{ $component }}.{{ $part.name }}.enabled
    {{- if index $part "needs" }}
    {{- $renderedNeeds := list }}
    {{- range index $part "needs" }}
      {{- $items := splitList "." . }}
      {{- $depComponent := index $items 0 }}
      {{- $depPart := index $items 1 }}
      {{- $depIsOperator := false }}
      {{- if eq $depComponent $component }}
        {{- range $p := $parts }}
          {{- if eq $p.name $depPart }}
            {{- $depLabels := index $p "extraLabels" | default dict }}
            {{- if eq (index $depLabels "operator" | default "") "true" }}{{- $depIsOperator = true }}{{- end }}
          {{- end }}
        {{- end }}
      {{- end }}
      {{- if not (and (eq $layer "instance") $depIsOperator) }}
        {{- $renderedNeeds = append $renderedNeeds (printf "%s/%s-%s" (index $root $depComponent $depPart "namespace") ($depComponent | kebabcase) $depPart) }}
      {{- end }}
    {{- end }}
    {{- if gt (len $renderedNeeds) 0 }}
    needs:
      {{- range $renderedNeeds }}
      - {{ . }}
      {{- end }}
    {{- end }}
    {{- end }}
    values:
      - values/{{ $part.name }}/base-values.yaml.gotmpl
      - values/{{ $part.name }}/{{ $root.global.profile }}-values.yaml.gotmpl
      - {{- index $root $component $part.name "rawValues" | default dict | toYaml | nindent 8 }}
  {{- end }}
  {{- end }}

commonLabels:
  component: {{ $component }}
```

- [ ] **Step 7: Create operator values files**

For each part `P` in `commons secret listener kafka nifi` create:

`deployment/addons/stackable/values/P/base-values.yaml.gotmpl`:
```yaml
---
# Stackable operator chart values. Defaults of oci.stackable.tech/sdp-charts apply;
# operators watch all namespaces and maintain their own CRDs.
maintenance:
  endOfSupportCheck:
    enabled: false
```
`deployment/addons/stackable/values/P/development-values.yaml.gotmpl`:
```yaml
---
# Development profile: chart defaults.
```
`deployment/addons/stackable/values/P/production-values.yaml.gotmpl`:
```yaml
---
# Production profile: chart defaults.
```

Run to create them:
```bash
for p in commons secret listener kafka nifi; do
  d=deployment/addons/stackable/values/$p; mkdir -p "$d"
  printf -- '---\n# Stackable operator chart values. Defaults of oci.stackable.tech/sdp-charts apply;\n# operators watch all namespaces and maintain their own CRDs.\nmaintenance:\n  endOfSupportCheck:\n    enabled: false\n' > "$d/base-values.yaml.gotmpl"
  printf -- '---\n# Development profile: chart defaults.\n' > "$d/development-values.yaml.gotmpl"
  printf -- '---\n# Production profile: chart defaults.\n' > "$d/production-values.yaml.gotmpl"
done
```

- [ ] **Step 8: Create `deployment/addons/stackable/networkpolicies.yaml`**

```yaml
---
# The kube-apiserver calls each Stackable operator's CRD conversion webhook on :8443.
# The apiserver is not selectable as a NetworkPolicy peer, so (like v2's
# postgres-operator-webhook) allow the port from anywhere, scoped to webhook pods.
# Without it, default-deny-stackable in the operator namespace breaks CR applies.
stackable-operator-webhooks:
  deployLayer: operators
  componentNamespace: stackable.commons
  podSelector:
    webhook.stackable.tech/conversion: enabled
  policyTypes:
    - Ingress
  ingress:
    - from:
        - ipBlock:
            cidr: 0.0.0.0/0
      ports:
        - protocol: TCP
          port: 8443
```

- [ ] **Step 9: Run render test**

Run: `tests/render.sh`
Expected: all PASS. If helmfile cannot resolve `bases: ../../../defaults/helm-defaults.yaml` through the symlink (it resolves relative paths physically instead of lexically), change the `bases` entry to `{{ env "CIVITAS_CORE_DEPLOYMENT" }}/defaults/helm-defaults.yaml` (the justfile and `tests/lib.sh` always export an absolute path) and re-run.

- [ ] **Step 10: Write cluster test `tests/operators.sh`**

```bash
#!/usr/bin/env bash
# Stackable operators are running and their CRDs are served.
source "$(dirname "$0")/lib.sh"
NS=civitas-operators

for p in commons secret listener kafka nifi; do
  ok=$(kubectl -n "$NS" get deploy "stackable-$p-${p}-operator" -o jsonpath='{.status.readyReplicas}' 2>/dev/null \
    || kubectl -n "$NS" get deploy -l "app.kubernetes.io/instance=stackable-$p" -o jsonpath='{.items[0].status.readyReplicas}' 2>/dev/null || true)
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
```

- [ ] **Step 11: Add recipes to `justfile`**

```just
# Sync the shared operators layer (CloudNativePG + Stackable operators)
operators: check-tools link
    helmfile -f helmfile-operators.yaml.gotmpl -e {{env}} sync

# Check Stackable operators
test-operators:
    tests/operators.sh
```

- [ ] **Step 12: Deploy and test**

Run: `chmod +x tests/operators.sh && just operators && just test-operators`
Expected: sync succeeds, all PASS (CRDs can take ~30 s after operator start; re-run test once if CRD checks fail right after sync).

- [ ] **Step 13: Commit**

```bash
git add deployment/addons/stackable tests justfile
git commit -m "feat: stackable operators addon for the operators layer

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: `kafka` addon (Stackable KafkaCluster replaces Strimzi)

**Files:**
- Create: `deployment/addons/kafka/{civitas-component.yaml,charts.yaml,default-environment.yaml.gotmpl,helmfile.yaml.gotmpl,networkpolicies.yaml}`, `deployment/addons/kafka/charts/kafka-cluster/{Chart.yaml,values.yaml,templates/kafkacluster.yaml}`, `deployment/addons/kafka/values/{cluster,ui}/{base,development,production}-values.yaml.gotmpl`, `tests/kafka.sh`
- Modify: `tests/render.sh`, `justfile`

**Interfaces:**
- Consumes: Stackable CRDs from Task 4; helmfile template from Task 4 Step 6.
- Produces: `KafkaCluster kafka-cluster` in `kafka.cluster.namespace`; `kafka.cluster.bootstrapService` = Stackable bootstrap Service name (verified), `bootstrapPort: 9092`; kafka-ui release `kafka-ui`.

- [ ] **Step 1: Add failing render assertions (before `finish` in `tests/render.sh`)**

```bash
echo "== kafka addon"
k=$(hf instance -l name=kafka-cluster template 2>&1)
assert_contains "KafkaCluster kind" "kind: KafkaCluster" "$k"
assert_contains "Kafka 4.2.1" 'productVersion: "4.2.1"' "$k"
assert_contains "KRaft" "metadataManager: kraft" "$k"
if grep -q "strimzi" <<<"$(hf instance list 2>&1)"; then fail "strimzi still referenced"; else pass "no strimzi releases"; fi
ca=$(hf instance -l name=config-adapters-adapters build --embed-values 2>&1)
assert_contains "config-adapter bootstrap → Stackable service" "kafka-cluster-broker-default-bootstrap" "$ca"
```

- [ ] **Step 2: Run to verify it fails**

Run: `tests/render.sh`
Expected: FAIL `KafkaCluster kind` (v2 Strimzi component still used).

- [ ] **Step 3: Create component metadata**

`deployment/addons/kafka/civitas-component.yaml`:
```yaml
---
# Replaces civitas-core-deployment/components/kafka (Strimzi) with a Stackable KafkaCluster.
# The kafka-operator lives in deployment/addons/stackable (operators layer).
component: kafka
parts:
  - name: cluster
    timeout: 600
  - name: ui
    needs:
      - kafka.cluster
```
`deployment/addons/kafka/charts.yaml`:
```yaml
---
kafka:
  cluster:
    chart: charts/kafka-cluster
  ui:
    repository: https://kafbat.github.io/helm-charts
    chart: kafka-ui/kafka-ui
    version: '1.6.4'
```
`deployment/addons/kafka/default-environment.yaml.gotmpl`:
```yaml
---
kafka:
  cluster:
    enabled: true
    namespace: {{ include "civitas.namespace" (dict "global" .Values.global "suffix" "kafka") }}
    productVersion: "4.2.1"
    # Consumers (portal, config-adapter, kafka-ui) build the bootstrap address from these keys.
    # Stackable names the bootstrap Service <cluster>-broker-<roleGroup>-bootstrap.
    bootstrapService: kafka-cluster-broker-default-bootstrap
    bootstrapPort: 9092
  ui:
    enabled: true
    namespace: {{ include "civitas.namespace" (dict "global" .Values.global "suffix" "kafka") }}
    subdomain: kafka-ui
    pathPrefix: /
```

- [ ] **Step 4: Copy the addon helmfile**

Run: `cp deployment/addons/stackable/helmfile.yaml.gotmpl deployment/addons/kafka/helmfile.yaml.gotmpl`

- [ ] **Step 5: Create chart `deployment/addons/kafka/charts/kafka-cluster`**

`Chart.yaml`:
```yaml
apiVersion: v2
name: kafka-cluster
description: Stackable KafkaCluster (KRaft) for CIVITAS/CORE
type: application
version: 0.1.0
appVersion: "4.2.1"
```
`values.yaml`:
```yaml
name: kafka-cluster
productVersion: "4.2.1"
controllers:
  replicas: 1
  resources:
    cpu: { min: 250m, max: "1" }
    memory: { limit: 1Gi }
    storage:
      logDirs: { capacity: 2Gi }
brokers:
  replicas: 1
  resources:
    cpu: { min: 250m, max: "1" }
    memory: { limit: 2Gi }
    storage:
      logDirs: { capacity: 5Gi }
  # broker.properties overrides (single broker ⇒ replication factor 1)
  properties:
    auto.create.topics.enable: "true"
    offsets.topic.replication.factor: "1"
    transaction.state.log.replication.factor: "1"
    transaction.state.log.min.isr: "1"
    default.replication.factor: "1"
    min.insync.replicas: "1"
    log.retention.ms: "2592000000"
```
`templates/kafkacluster.yaml`:
```yaml
apiVersion: kafka.stackable.tech/v1alpha1
kind: KafkaCluster
metadata:
  name: {{ .Values.name }}
  labels:
    app.kubernetes.io/managed-by: {{ .Release.Service }}
    helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
spec:
  image:
    productVersion: {{ .Values.productVersion | quote }}
  clusterConfig:
    metadataManager: kraft
    tls:
      # Plaintext client listener without authentication: same contract as the
      # Strimzi cluster in civitas-core-deployment. Inter-broker/controller TLS stays on.
      serverSecretClass: null
  controllers:
    config:
      resources:
        {{- toYaml .Values.controllers.resources | nindent 8 }}
    roleGroups:
      default:
        replicas: {{ .Values.controllers.replicas }}
  brokers:
    config:
      bootstrapListenerClass: cluster-internal
      brokerListenerClass: cluster-internal
      resources:
        {{- toYaml .Values.brokers.resources | nindent 8 }}
    configOverrides:
      broker.properties:
        {{- toYaml .Values.brokers.properties | nindent 8 }}
    roleGroups:
      default:
        replicas: {{ .Values.brokers.replicas }}
```

- [ ] **Step 6: Create values files**

`deployment/addons/kafka/values/cluster/base-values.yaml.gotmpl`:
```yaml
---
{{- $this := .Values.kafka.cluster }}
productVersion: {{ $this.productVersion | quote }}
```
`deployment/addons/kafka/values/cluster/development-values.yaml.gotmpl`:
```yaml
---
# Development: chart defaults (1 controller, 1 broker, small volumes).
```
`deployment/addons/kafka/values/cluster/production-values.yaml.gotmpl`:
```yaml
---
controllers:
  replicas: 3
brokers:
  replicas: 3
  resources:
    cpu: { min: "1", max: "2" }
    memory: { limit: 4Gi }
    storage:
      logDirs: { capacity: 50Gi }
  properties:
    auto.create.topics.enable: "true"
    offsets.topic.replication.factor: "3"
    transaction.state.log.replication.factor: "3"
    transaction.state.log.min.isr: "2"
    default.replication.factor: "3"
    min.insync.replicas: "2"
    log.retention.ms: "2592000000"
```
`deployment/addons/kafka/values/ui/base-values.yaml.gotmpl` (from v2 `components/kafka/values/ui-values.yaml.gotmpl`):
```yaml
---
yamlApplicationConfig:
  kafka:
    clusters:
      - name: kafka-cluster
        bootstrapServers: {{ .Values.kafka.cluster.bootstrapService }}:{{ .Values.kafka.cluster.bootstrapPort }}

ingress:
  enabled: false

resources:
  requests:
    cpu: 500m
    memory: 512Mi
  limits:
    cpu: 500m
    memory: 512Mi
```
`deployment/addons/kafka/values/ui/development-values.yaml.gotmpl` and `production-values.yaml.gotmpl`:
```yaml
---
# Profile overrides: none.
```

- [ ] **Step 7: Create `deployment/addons/kafka/networkpolicies.yaml`**

```yaml
---
# Same allowed clients as v2's Strimzi policy, selecting Stackable Kafka pods, plus
# broker <-> controller traffic between the Kafka pods themselves.
kafka-cluster:
  podSelector:
    app.kubernetes.io/name: kafka
    app.kubernetes.io/instance: kafka-cluster
  policyTypes:
    - Ingress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: portal
              app.kubernetes.io/component: backend
          componentNamespace: portal
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: kafka-ui
          componentNamespace: kafka
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: config-adapter
          componentNamespace: config-adapters
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: nifi
          componentNamespace: nifi
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: kafka
              app.kubernetes.io/instance: kafka-cluster
          componentNamespace: kafka
```

- [ ] **Step 8: Run render test**

Run: `tests/render.sh`
Expected: all PASS.

- [ ] **Step 9: Write cluster test `tests/kafka.sh`**

```bash
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
```

- [ ] **Step 10: Add recipes to `justfile`**

```just
# Sync one component of the instance layer, e.g. `just sync-component kafka`
sync-component component:
    helmfile -f helmfile-instance.yaml.gotmpl -e {{env}} -l component={{component}} sync

# Check Kafka produce/consume
test-kafka:
    tests/kafka.sh
```

- [ ] **Step 11: Deploy Kafka alone and test**

Run: `chmod +x tests/kafka.sh && just sync-component kafka && just test-kafka`
Expected: all PASS. If the bootstrap Service is named differently (`kubectl -n dev get svc`), set `bootstrapService` in `default-environment.yaml.gotmpl` to the real name, update the render assertion, re-run both tests. If the client port is not 9092 (`kubectl -n dev get svc <name> -o yaml`), set `bootstrapPort` accordingly. If the kafka container or script path differ, adjust `-c kafka` / `/stackable/kafka/bin` from `kubectl -n dev get pod $POD -o yaml`.

- [ ] **Step 12: Commit**

```bash
git add deployment/addons/kafka tests justfile
git commit -m "feat: kafka addon replacing Strimzi with a Stackable KRaft KafkaCluster

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: `nifi` addon (Stackable NifiCluster) and full instance deploy

**Files:**
- Create: `deployment/addons/nifi/{civitas-component.yaml,charts.yaml,images.yaml,default-environment.yaml.gotmpl,helmfile.yaml.gotmpl,keycloak-clients.yaml,networkpolicies.yaml}`, `deployment/addons/nifi/charts/nifi-cluster/{Chart.yaml,values.yaml,templates/_helpers.tpl,templates/ca.yaml,templates/authenticationclass.yaml,templates/oidc-client-secret.yaml,templates/nificluster.yaml,templates/ingress.yaml}`, `deployment/addons/nifi/values/{nifi,bootstrap}/{base,development,production}-values.yaml.gotmpl`, `tests/nifi.sh`
- Modify: `tests/render.sh`, `justfile`

**Interfaces:**
- Consumes: v2 `secrets` (`keycloak-client-nifi`, `keycloak-client-nifi-bootstrap`, `keycloak-client-nifi-config-adapter`), v2 `prepare` (`custom-ca-cert`), v2 chart `components/nifi/charts/nifi-bootstrap`, Keycloak service `keycloak-app-keycloakx-http`, `routes.keycloak.subDomain`, `keycloak.app.{namespace,pathPrefix}`, Task 2 patch (`nifi.nifi.url`).
- Produces: `NifiCluster nifi-nifi`, `AuthenticationClass <slug>-keycloak`, `SecretClass <slug>-civitas-ca`, Secret `nifi-nifi-oidc-client`, Ingress `nifi-nifi` at `nifi.<domain>`, value `nifi.nifi.url`.

- [ ] **Step 1: Add failing render assertions (before `finish` in `tests/render.sh`)**

```bash
echo "== nifi addon"
n=$(hf instance -l name=nifi-nifi template 2>&1)
assert_contains "NifiCluster kind" "kind: NifiCluster" "$n"
assert_contains "NiFi 2.9.0" 'productVersion: "2.9.0"' "$n"
assert_contains "initial admin nifi-bootstrap" "initialAdminUser: nifi-bootstrap" "$n"
assert_contains "OIDC AuthenticationClass" "kind: AuthenticationClass" "$n"
assert_contains "ES256" "nifi.security.user.oidc.preferred.jwsalgorithm: ES256" "$n"
if grep -q "zookeeper" <<<"$n"; then fail "zookeeper referenced"; else pass "no zookeeper"; fi
ca=$(hf instance -l name=config-adapters-adapters build --embed-values 2>&1)
assert_contains "config-adapter NIFI_URL → Stackable pod" "nifi-nifi-node-default-0" "$ca"
b=$(hf instance -l name=nifi-bootstrap template 2>&1)
assert_contains "bootstrap job targets Stackable pod" "nifi-nifi-node-default-0" "$b"
```

- [ ] **Step 2: Run to verify it fails**

Run: `tests/render.sh`
Expected: FAIL `NifiCluster kind`.

- [ ] **Step 3: Create component metadata**

`deployment/addons/nifi/civitas-component.yaml`:
```yaml
---
# Replaces civitas-core-deployment/components/nifi (apache-nifi-helm) with a Stackable
# NifiCluster. The nifi-operator lives in deployment/addons/stackable (operators layer).
component: nifi
parts:
  - name: nifi
    timeout: 600
  - name: bootstrap
    timeout: 900
    needs:
      - nifi.nifi
```
`deployment/addons/nifi/charts.yaml`:
```yaml
---
nifi:
  nifi:
    chart: charts/nifi-cluster
  bootstrap:
    # Reuse civitas-core-deployment's bootstrap Job chart unchanged.
    chart: ../../../components/nifi/charts/nifi-bootstrap
```
`deployment/addons/nifi/images.yaml` (copied from v2):
```yaml
---
nifi:
  bootstrap:
    # Minimal Alpine-based image containing `curl` + `jq` (bootstrap Job, JDBC init container).
    repository: 'docker.io/badouralix/curl-jq'
    tag: 'alpine'
    digest: 'sha256:f0370deaaf07dc61af9bae7474cbcffee62be5b3e83e1f6dd5d25d340e6c16c4'
```
`deployment/addons/nifi/default-environment.yaml.gotmpl`:
```yaml
---
{{- $ns := include "civitas.namespace" (dict "global" .Values.global "suffix" "nifi") }}
nifi:
  nifi:
    enabled: true
    namespace: {{ $ns }}
    productVersion: "2.9.0"
    nodeCount: 1
    initialAdminEmail: {{ .Values.global.initialUserEmail }}
    bootstrapAdminIdentity: nifi-bootstrap
    postgresqlJdbcVersion: "42.7.4"
    # Consumed by config-adapter (civitas-core-deployment patch) and the bootstrap Job.
    url: https://nifi-nifi-node-default-0.nifi-nifi-node-default-headless.{{ $ns }}.svc.cluster.local:8443
    ingress:
      enabled: true
      subdomain: nifi
  bootstrap:
    enabled: true
    namespace: {{ $ns }}
```

- [ ] **Step 4: Copy helmfile and Keycloak clients**

Run:
```bash
cp deployment/addons/stackable/helmfile.yaml.gotmpl deployment/addons/nifi/helmfile.yaml.gotmpl
cp ../civitas-core-deployment/components/nifi/keycloak-clients.yaml deployment/addons/nifi/keycloak-clients.yaml
```
Then prepend to `deployment/addons/nifi/keycloak-clients.yaml` after `---`:
```yaml
# Copied unchanged from civitas-core-deployment/components/nifi/keycloak-clients.yaml
# (an addon replaces all of a component's config files). Keep in sync with v2.
```

- [ ] **Step 5: Create chart `deployment/addons/nifi/charts/nifi-cluster`**

`Chart.yaml`:
```yaml
apiVersion: v2
name: nifi-cluster
description: Stackable NifiCluster with Keycloak OIDC for CIVITAS/CORE
type: application
version: 0.1.0
appVersion: "2.9.0"
```
`values.yaml`:
```yaml
name: nifi-nifi
productVersion: "2.9.0"
replicas: 1
initialAdminUser: nifi-bootstrap
oidc:
  hostname: idm.civitas.test
  port: 443
  realm: dev
  # Name of the generated Keycloak client secret (key client-secret) for client `nifi`.
  clientId: nifi
  clientSecretName: keycloak-client-nifi
  # SecretClass providing the CA that signed Keycloak's certificate; empty = webPki.
  caSecretClass: ""
  # Secret with ca.crt to publish for caSecretClass (copied via lookup).
  caSourceSecret: custom-ca-cert
jdbc:
  image: docker.io/badouralix/curl-jq@sha256:f0370deaaf07dc61af9bae7474cbcffee62be5b3e83e1f6dd5d25d340e6c16c4
  postgresqlVersion: "42.7.4"
resources:
  cpu: { min: 500m, max: "2" }
  memory: { limit: 3Gi }
  storage:
    contentRepo: { capacity: 2Gi }
    databaseRepo: { capacity: 1Gi }
    flowfileRepo: { capacity: 2Gi }
    provenanceRepo: { capacity: 2Gi }
    stateRepo: { capacity: 1Gi }
ingress:
  enabled: false
  host: nifi.civitas.test
  className: nginx
  clusterIssuer: selfsigned-ca
```
`templates/_helpers.tpl`:
```yaml
{{- define "nifi-cluster.authClassName" -}}
{{ .Release.Namespace }}-keycloak
{{- end }}

{{- define "nifi-cluster.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}
```
`templates/ca.yaml`:
```yaml
{{- if .Values.oidc.caSecretClass }}
# CA that signed Keycloak's ingress certificate, published for the Stackable secret-operator.
apiVersion: secrets.stackable.tech/v1alpha1
kind: SecretClass
metadata:
  name: {{ .Values.oidc.caSecretClass }}
  labels:
    {{- include "nifi-cluster.labels" . | nindent 4 }}
spec:
  backend:
    k8sSearch:
      searchNamespace:
        pod: {}
---
{{- $src := lookup "v1" "Secret" .Release.Namespace .Values.oidc.caSourceSecret }}
{{- if $src }}
apiVersion: v1
kind: Secret
metadata:
  name: {{ .Values.name }}-keycloak-ca
  labels:
    {{- include "nifi-cluster.labels" . | nindent 4 }}
    secrets.stackable.tech/class: {{ .Values.oidc.caSecretClass }}
type: Opaque
data:
  ca.crt: {{ index $src.data "ca.crt" }}
{{- else }}
# Secret {{ .Values.oidc.caSourceSecret }} not found (helm template or prepare hook not run yet).
{{- end }}
{{- end }}
```
`templates/authenticationclass.yaml`:
```yaml
apiVersion: authentication.stackable.tech/v1alpha1
kind: AuthenticationClass
metadata:
  name: {{ include "nifi-cluster.authClassName" . }}
  labels:
    {{- include "nifi-cluster.labels" . | nindent 4 }}
spec:
  provider:
    oidc:
      hostname: {{ .Values.oidc.hostname }}
      port: {{ .Values.oidc.port }}
      rootPath: /realms/{{ .Values.oidc.realm }}
      scopes:
        - openid
        - email
        - profile
      principalClaim: preferred_username
      providerHint: Keycloak
      tls:
        verification:
          server:
            caCert:
              {{- if .Values.oidc.caSecretClass }}
              secretClass: {{ .Values.oidc.caSecretClass }}
              {{- else }}
              webPki: {}
              {{- end }}
```
`templates/oidc-client-secret.yaml`:
```yaml
{{- $src := lookup "v1" "Secret" .Release.Namespace .Values.oidc.clientSecretName }}
{{- if $src }}
# Stackable expects clientId/clientSecret keys; civitas-core-deployment generates client-secret.
apiVersion: v1
kind: Secret
metadata:
  name: {{ .Values.name }}-oidc-client
  labels:
    {{- include "nifi-cluster.labels" . | nindent 4 }}
type: Opaque
data:
  clientId: {{ .Values.oidc.clientId | b64enc }}
  clientSecret: {{ index $src.data "client-secret" }}
{{- else }}
# Secret {{ .Values.oidc.clientSecretName }} not found (helm template or secrets component not synced yet).
{{- end }}
```
`templates/nificluster.yaml`:
```yaml
apiVersion: nifi.stackable.tech/v1alpha1
kind: NifiCluster
metadata:
  name: {{ .Values.name }}
  labels:
    {{- include "nifi-cluster.labels" . | nindent 4 }}
spec:
  image:
    productVersion: {{ .Values.productVersion | quote }}
  clusterConfig:
    # No zookeeperConfigMapName: Kubernetes clustering backend (state in Leases/ConfigMaps).
    authentication:
      - authenticationClass: {{ include "nifi-cluster.authClassName" . }}
        oidc:
          clientCredentialsSecret: {{ .Values.name }}-oidc-client
    authorization:
      standard:
        accessPolicyProvider:
          fileBased:
            # Hardcoded `sub` of the nifi-bootstrap Keycloak client (see keycloak-clients.yaml).
            initialAdminUser: {{ .Values.initialAdminUser }}
    sensitiveProperties:
      keySecret: {{ .Values.name }}-sensitive-property-key
      autoGenerate: true
    hostHeaderCheck:
      allowAll: true
  nodes:
    config:
      resources:
        {{- toYaml .Values.resources | nindent 8 }}
    configOverrides:
      nifi.properties:
        # The CIVITAS realm signs tokens with ES256 only.
        nifi.security.user.oidc.preferred.jwsalgorithm: ES256
    podOverrides:
      spec:
        initContainers:
          - name: fetch-postgresql-jdbc
            image: {{ .Values.jdbc.image }}
            command:
              - sh
              - -c
              - curl -fsSL -o /drivers/postgresql.jar https://jdbc.postgresql.org/download/postgresql-{{ .Values.jdbc.postgresqlVersion }}.jar
            volumeMounts:
              - name: jdbc-drivers
                mountPath: /drivers
            resources:
              requests: { cpu: 10m, memory: 16Mi }
              limits: { cpu: 100m, memory: 64Mi }
        containers:
          - name: nifi
            volumeMounts:
              # Same path as civitas-core-deployment's NiFi; config-adapter flows reference it.
              - name: jdbc-drivers
                mountPath: /opt/nifi/drivers
                readOnly: true
        volumes:
          - name: jdbc-drivers
            emptyDir: {}
    roleGroups:
      default:
        replicas: {{ .Values.replicas }}
```
`templates/ingress.yaml`:
```yaml
{{- if .Values.ingress.enabled }}
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: {{ .Values.name }}
  labels:
    {{- include "nifi-cluster.labels" . | nindent 4 }}
  annotations:
    cert-manager.io/cluster-issuer: {{ .Values.ingress.clusterIssuer }}
    nginx.ingress.kubernetes.io/backend-protocol: HTTPS
    nginx.ingress.kubernetes.io/affinity: cookie
spec:
  ingressClassName: {{ .Values.ingress.className }}
  tls:
    - hosts:
        - {{ .Values.ingress.host }}
      secretName: {{ .Values.name }}-tls
  rules:
    - host: {{ .Values.ingress.host }}
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: {{ .Values.name }}-node
                port:
                  name: https
{{- end }}
```

- [ ] **Step 6: Create NiFi values files**

`deployment/addons/nifi/values/nifi/base-values.yaml.gotmpl`:
```yaml
---
{{- $this := .Values.nifi.nifi }}
{{- $global := .Values.global }}
{{- if gt (int $this.nodeCount) 1 }}
{{- fail "addons/nifi: nodeCount > 1 is not supported (nifi.nifi.url, bootstrap Job and config-adapter target node 0)" }}
{{- end }}
productVersion: {{ $this.productVersion | quote }}
replicas: {{ $this.nodeCount }}
initialAdminUser: {{ $this.bootstrapAdminIdentity | quote }}
oidc:
  hostname: {{ .Values.routes.keycloak.subDomain }}.{{ $global.domain }}
  port: 443
  realm: {{ $global.instanceSlug }}
  clientId: nifi
  clientSecretName: keycloak-client-nifi
  {{- if eq $global.ingress.clusterIssuer "selfsigned-ca" }}
  caSecretClass: {{ $global.instanceSlug }}-civitas-ca
  {{- end }}
  caSourceSecret: custom-ca-cert
jdbc:
  image: {{ .Values.images.nifi.bootstrap.repository }}@{{ .Values.images.nifi.bootstrap.digest }}
  postgresqlVersion: {{ $this.postgresqlJdbcVersion | quote }}
ingress:
  enabled: {{ $this.ingress.enabled }}
  host: {{ $this.ingress.subdomain }}.{{ $global.domain }}
  className: {{ $global.ingress.ingressClass }}
  clusterIssuer: {{ $global.ingress.clusterIssuer }}
```
`deployment/addons/nifi/values/nifi/development-values.yaml.gotmpl`:
```yaml
---
# Development: chart defaults (1 node, 3Gi memory, small repositories).
```
`deployment/addons/nifi/values/nifi/production-values.yaml.gotmpl`:
```yaml
---
resources:
  cpu: { min: "2", max: "4" }
  memory: { limit: 12Gi }
  storage:
    contentRepo: { capacity: 50Gi }
    databaseRepo: { capacity: 5Gi }
    flowfileRepo: { capacity: 20Gi }
    provenanceRepo: { capacity: 100Gi }
    stateRepo: { capacity: 5Gi }
```
`deployment/addons/nifi/values/bootstrap/base-values.yaml.gotmpl`: copy v2's `components/nifi/values/bootstrap/base-values.yaml.gotmpl` and change only the `nifi.url` block to:
```yaml
# NiFi REST API endpoint: pod FQDN of the Stackable NiFi node (nifi.nifi.url).
nifi:
  url: {{ $nifi.url | quote }}
  # Certificate issued by the Stackable secret-operator `tls` SecretClass; not mounted here.
  insecureSkipVerify: true
```
Run:
```bash
mkdir -p deployment/addons/nifi/values/bootstrap
cp ../civitas-core-deployment/components/nifi/values/bootstrap/*.gotmpl deployment/addons/nifi/values/bootstrap/
```
then edit `base-values.yaml.gotmpl` as above.

- [ ] **Step 7: Create `deployment/addons/nifi/networkpolicies.yaml`**

```yaml
---
# v2's NiFi peers, selecting Stackable NiFi pods (NifiCluster nifi-nifi), plus the
# ingress controller for the UI at https://nifi.<domain>.
nifi:
  podSelector:
    app.kubernetes.io/name: nifi
    app.kubernetes.io/instance: nifi-nifi
  policyTypes:
    - Ingress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: apisix
          componentNamespace: apisix
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: config-adapter
          componentNamespace: config-adapters
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: nifi-bootstrap
          componentNamespace: nifi
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: nifi
              app.kubernetes.io/instance: nifi-nifi
          componentNamespace: nifi
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: ingress-nginx
```

- [ ] **Step 8: Run render test**

Run: `tests/render.sh`
Expected: all PASS. If the bootstrap chart path `../../../components/nifi/charts/nifi-bootstrap` does not resolve through the symlink (`charts.yaml` is not templated, so it cannot use env vars), change the local-chart branch of the `chart:` line in all three addon helmfiles to:
```
    chart: {{ if hasPrefix "../../../" $chartCfg.chart }}{{ env "CIVITAS_CORE_DEPLOYMENT" }}/{{ trimPrefix "../../../" $chartCfg.chart }}{{ else }}{{ $chartCfg.chart }}{{ end }}
```
and re-run.

- [ ] **Step 9: Write cluster test `tests/nifi.sh`**

```bash
#!/usr/bin/env bash
# NiFi: cluster ready, bootstrap Job done, REST API accepts Keycloak client-credentials
# tokens, UI ingress redirects to Keycloak.
source "$(dirname "$0")/lib.sh"
NS=$(instance_ns)
DOMAIN=$(cat "$ROOT/values/default-instance.yaml" | yq '.global.domain')
NIFI_URL="https://nifi-nifi-node-default-0.nifi-nifi-node-default-headless.$NS.svc.cluster.local:8443"
TOKEN_URL="http://keycloak-app-keycloakx-http.$NS.svc.cluster.local/realms/$NS/protocol/openid-connect/token"

echo "== NifiCluster"
kubectl -n "$NS" wait --for=condition=Available nificluster/nifi-nifi --timeout=900s >/dev/null \
  && pass "NifiCluster Available" || fail "NifiCluster Available"
kubectl -n "$NS" wait --for=condition=Complete job -l app.kubernetes.io/name=nifi-bootstrap --timeout=900s >/dev/null \
  && pass "nifi-bootstrap Job complete" || fail "nifi-bootstrap Job complete"

echo "== REST API with client-credentials token (as nifi-bootstrap)"
SECRET=$(kubectl -n "$NS" get secret keycloak-client-nifi-bootstrap -o jsonpath='{.data.client-secret}' | base64 -d)
code=$(kubectl -n "$NS" run nifi-rest-check-$RANDOM --rm -i --restart=Never --quiet \
  --labels app.kubernetes.io/name=nifi-bootstrap \
  --image=docker.io/badouralix/curl-jq:alpine -- sh -c "
    T=\$(curl -fsS -d grant_type=client_credentials -d client_id=nifi-bootstrap -d client_secret='$SECRET' '$TOKEN_URL' | jq -r .access_token)
    curl -ks -o /dev/null -w '%{http_code}' -H \"Authorization: Bearer \$T\" '$NIFI_URL/nifi-api/flow/about'" 2>/dev/null | tail -c 3)
assert_eq "GET /nifi-api/flow/about" "200" "$code"

echo "== UI ingress → Keycloak"
ING_IP=$(kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.spec.clusterIP}')
loc=$(kubectl -n ingress-nginx run nifi-ui-check-$RANDOM --rm -i --restart=Never --quiet \
  --image=docker.io/badouralix/curl-jq:alpine -- \
  curl -ks -o /dev/null -w '%{redirect_url}' --resolve "nifi.$DOMAIN:443:$ING_IP" \
  "https://nifi.$DOMAIN/nifi-api/access/oidc/request" 2>/dev/null || true)
assert_contains "OIDC request redirects to idm.$DOMAIN" "https://idm.$DOMAIN/realms/$NS/" "$loc"
finish
```

- [ ] **Step 10: Add recipes to `justfile`**

```just
# Sync the instance layer (full CIVITAS/CORE stack on Stackable Kafka/NiFi)
instance: check-tools link
    #!/usr/bin/env bash
    set -euo pipefail
    ns=$(cat values/default-instance.yaml | yq '.global.instanceSlug')
    kubectl get namespace "$ns" >/dev/null 2>&1 || kubectl create namespace "$ns"
    # Keycloak's config job requires an SMTP secret; dummy values like civitas-core-deployment's `just deploy`.
    kubectl -n "$ns" get secret keycloak-smtp >/dev/null 2>&1 || kubectl -n "$ns" create secret generic keycloak-smtp \
      --from-literal=host='smtp.example.com' --from-literal=port='587' \
      --from-literal=from='noreply@example.com' --from-literal=user='noreply@example.com' \
      --from-literal=password='YOUR_SMTP_PASSWORD'
    helmfile -f helmfile-instance.yaml.gotmpl -e {{env}} sync

# Check NiFi (REST with Keycloak token, UI redirect)
test-nifi:
    tests/nifi.sh
```

- [ ] **Step 11: Deploy the full instance and test**

Run: `chmod +x tests/nifi.sh && just instance && just test-nifi && just test-kafka`
Expected: sync completes (takes 15–30 min on first run), all PASS.

Debug order if something fails (use superpowers:systematic-debugging):
1. `kubectl -n dev get nificluster nifi-nifi -o yaml` conditions; `kubectl -n dev logs deploy/stackable-nifi-nifi-operator -n civitas-operators`.
2. Service/pod names: `kubectl -n dev get svc,pod | grep nifi` — if the headless Service is not `nifi-nifi-node-default-headless` or the UI Service is not `nifi-nifi-node` with port `https`, update `nifi.nifi.url` (default-environment), ingress `service.name`/`port.name`, `tests/nifi.sh`, the render assertions.
3. REST 401/403: check NiFi logs for the resolved identity. If bearer tokens are not accepted, look at the rendered `nifi.properties` (`kubectl -n dev get cm nifi-nifi-node-default -o yaml`) versus v2's apache-nifi-helm properties for `nifi.security.user.oidc.*` and add missing keys to `configOverrides`. If impossible → STOP and report to the user (spec risk).
4. OIDC discovery TLS errors: verify Secret `nifi-nifi-keycloak-ca` exists with label `secrets.stackable.tech/class: dev-civitas-ca`; if `custom-ca-cert` was missing at first install, re-run `just sync-component nifi`.
5. Missing `nifi-nifi-oidc-client`: `keycloak-client-nifi` absent at install time → `just sync-component nifi`.

- [ ] **Step 12: Commit**

```bash
git add deployment/addons/nifi tests justfile
git commit -m "feat: nifi addon replacing apache-nifi-helm with a Stackable NifiCluster

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Day-2 recipes and end-to-end smoke test

**Files:**
- Create: `tests/smoke.sh`
- Modify: `justfile`

**Interfaces:**
- Consumes: `tests/kafka.sh`, `tests/nifi.sh`, `tests/cluster.sh`, `tests/operators.sh`.
- Produces: `just deploy|smoke-test|status|credentials|create-admin-user|add-hosts|destroy|nifi-ui`.

- [ ] **Step 1: Write `tests/smoke.sh`**

```bash
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
if kubectl -n "$NS" logs deploy/config-adapter --tail=500 | grep -qi 'nifi.*\(refused\|UnknownHost\|401\|403\)'; then
  fail "config-adapter logs show NiFi connection/auth errors"
else
  pass "no NiFi connection/auth errors in config-adapter logs"
fi

echo "#### portal"
ING_IP=$(kubectl -n ingress-nginx get svc ingress-nginx-controller -o jsonpath='{.spec.clusterIP}')
code=$(kubectl -n ingress-nginx run portal-check-$RANDOM --rm -i --restart=Never --quiet \
  --image=docker.io/badouralix/curl-jq:alpine -- \
  curl -ks -o /dev/null -w '%{http_code}' --resolve "portal.$DOMAIN:443:$ING_IP" "https://portal.$DOMAIN/" 2>/dev/null | tail -c 3 || true)
case "$code" in 200|301|302) pass "portal responds ($code)";; *) fail "portal responds (got '$code')";; esac

finish
```

- [ ] **Step 2: Append day-2 recipes to `justfile`**

```just
# Everything: tools, cluster, link, operators, instance
deploy: check-tools cluster-up link operators instance
    @echo "Deployed. Run 'just smoke-test' and 'just credentials'."

# End-to-end verification
smoke-test:
    tests/smoke.sh

# Stackable CRs, pods and ingresses of the instance
status:
    #!/usr/bin/env bash
    set -euo pipefail
    ns=$(cat values/default-instance.yaml | yq '.global.instanceSlug')
    kubectl get pods -n civitas-operators
    kubectl -n "$ns" get kafkaclusters,nificlusters,authenticationclasses,secretclasses 2>/dev/null || true
    kubectl -n "$ns" get pods,ingress

# Print admin credentials
credentials:
    #!/usr/bin/env bash
    set -euo pipefail
    ns=$(cat values/default-instance.yaml | yq '.global.instanceSlug')
    domain=$(cat values/default-instance.yaml | yq '.global.domain')
    echo "Keycloak admin console: https://idm.$domain/admin  user: admin  password: $(kubectl -n "$ns" get secret keycloak-admin-user -o jsonpath='{.data.password}' | base64 -d)"
    if kubectl -n "$ns" get secret nifi-demo-admin-user >/dev/null 2>&1; then
      echo "NiFi UI: https://nifi.$domain/nifi  user: $(kubectl -n "$ns" get secret nifi-demo-admin-user -o jsonpath='{.data.username}' | base64 -d)  password: $(kubectl -n "$ns" get secret nifi-demo-admin-user -o jsonpath='{.data.password}' | base64 -d)"
    else
      echo "NiFi UI user not created yet: run 'just create-admin-user'"
    fi

# Create the realm user global.initialUserEmail (NiFi admin via nifi-bootstrap) with a generated password
create-admin-user:
    #!/usr/bin/env bash
    set -euo pipefail
    ns=$(cat values/default-instance.yaml | yq '.global.instanceSlug')
    email=$(cat values/default-instance.yaml | yq '.global.initialUserEmail')
    if ! kubectl -n "$ns" get secret nifi-demo-admin-user >/dev/null 2>&1; then
      pw="Civitas$(head -c 12 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 10)1"
      kubectl -n "$ns" create secret generic nifi-demo-admin-user --from-literal=username="$email" --from-literal=password="$pw"
    fi
    pw=$(kubectl -n "$ns" get secret nifi-demo-admin-user -o jsonpath='{.data.password}' | base64 -d)
    admin_pw=$(kubectl -n "$ns" get secret keycloak-admin-user -o jsonpath='{.data.password}' | base64 -d)
    pod=$(kubectl -n "$ns" get pod -l app.kubernetes.io/name=keycloakx -o jsonpath='{.items[0].metadata.name}')
    kubectl -n "$ns" exec "$pod" -- bash -c "
      set -e
      kc=/opt/keycloak/bin/kcadm.sh
      \$kc config credentials --server http://localhost:8080 --realm master --user admin --password '$admin_pw' >/dev/null
      id=\$(\$kc get users -r '$ns' -q exact=true -q username='$email' --fields id --format csv --noquotes | head -n1)
      if [ -z \"\$id\" ]; then
        \$kc create users -r '$ns' -s username='$email' -s email='$email' -s enabled=true -s emailVerified=true -s firstName=Civitas -s lastName=Admin
        id=\$(\$kc get users -r '$ns' -q exact=true -q username='$email' --fields id --format csv --noquotes | head -n1)
      fi
      \$kc set-password -r '$ns' --userid \"\$id\" --new-password '$pw'
    "
    echo "Created/updated $email; see 'just credentials'."

# /etc/hosts entries for browser access (needs sudo; kind needs host ports 80/443)
add-hosts:
    #!/usr/bin/env bash
    set -euo pipefail
    domain=$(cat values/default-instance.yaml | yq '.global.domain')
    line="127.0.0.1 idm.$domain portal.$domain api.$domain dashboard.$domain nifi.$domain"
    if grep -qF "$line" /etc/hosts; then echo "already present"; else echo "$line # civitas-stackable-demo" | sudo tee -a /etc/hosts; fi

# Browser access without host port mappings: forward ingress to https://<host>:8443
port-forward:
    kubectl -n ingress-nginx port-forward svc/ingress-nginx-controller 8443:443

# Tear down the cluster (CLUSTER=kind|k3d|none) and remove the symlink
destroy: cluster-down unlink
```

- [ ] **Step 3: Run the smoke test**

Run: `chmod +x tests/smoke.sh && just smoke-test`
Expected: all PASS. If "no NiFi errors in config-adapter logs" fails, inspect the log lines and fix per Task 6 debug list.

- [ ] **Step 4: Check recipes run**

Run: `just status && just create-admin-user && just credentials`
Expected: CRs listed, user created, credentials printed including NiFi UI user.

- [ ] **Step 5: Commit**

```bash
git add tests/smoke.sh justfile
git commit -m "feat: day-2 recipes and end-to-end smoke test

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: README, superseded guide, final verification

**Files:**
- Create: `README.md`
- Modify: `civitas-stackable-migration-guide.md` (banner at the top)

**Interfaces:**
- Consumes: verified names and behaviour from Tasks 1–7 (use the actual values found, not the "expected" ones, wherever they differed).

- [ ] **Step 1: Add superseded banner to `civitas-stackable-migration-guide.md` (first lines)**

```markdown
> **Superseded.** Early brainstorm without access to the code. It pins outdated versions
> (SDP 24.7, NiFi 1.x, ZooKeeper) and non-existent CRD fields. See `README.md` and
> `docs/superpowers/specs/2026-09-16-civitas-stackable-demo-design.md` for what was built.
```

- [ ] **Step 2: Write `README.md` with these sections and contents**

1. **Title + one paragraph:** full CIVITAS/CORE v2 (`civitas-core-deployment`) with Strimzi Kafka and the apache-nifi-helm NiFi replaced by Stackable Data Platform 26.7 operators; no ZooKeeper.
2. **What changes (table):** rows Kafka operator, Kafka cluster, Kafka version/mode, NiFi deployment, NiFi clustering state, NiFi auth, NiFi TLS, NiFi UI exposure, bootstrap address, NiFi REST URL, shared operators — columns "civitas-core-deployment" vs "this project" with the concrete names (`kafka-cluster-kafka-bootstrap` → verified Stackable service, `nifi-nifi-0.nifi-nifi.<ns>` → verified pod FQDN, `AuthenticationClass <slug>-keycloak`, etc.).
3. **How it plugs into civitas-core-deployment:** the `deployment/addons/<component>` override mechanism, the symlink created by `just link`, the three addons, the entrypoints and `values/default-instance.yaml`, the required v2 patch (`just apply-v2-patch`, branch `feat/configurable-nifi-url`). Include the text architecture diagram:
   ```
   civitas-stackable-demo                     civitas-core-deployment
   ├─ helmfile-operators.yaml.gotmpl ──────▶ helmfile-root ─▶ prepare, postgres(operator), networkpolicies
   │                                                        └▶ deployment/addons/stackable ─▶ 5 Stackable operators
   ├─ helmfile-instance.yaml.gotmpl  ──────▶ helmfile-root ─▶ v2 components (keycloak, apisix, portal, …)
   │                                                        ├▶ deployment/addons/kafka ─▶ KafkaCluster (KRaft) + kafka-ui
   │                                                        └▶ deployment/addons/nifi  ─▶ NifiCluster + OIDC + bootstrap Job
   └─ deployment/ ◀── symlink ── civitas-core-deployment/deployment
   ```
4. **Prerequisites:** kubectl, helm ≥ 3.17 + helm-diff, helmfile ≥ 1.0, yq v4, envsubst, just, kind or k3d; sibling checkout or `CIVITAS_CORE_DEPLOYMENT`; machine sizing observed during Task 6 (report actual `kubectl top node` or requests sum).
5. **Quickstart:** `just apply-v2-patch`, `just deploy` (kind, default cluster name `kind`), `just smoke-test`, `just create-admin-user`, `just credentials`, browser access (`just add-hosts` with kind host ports from `clusters/kind/cluster.yaml`, or `just port-forward` + `/etc/hosts` + `https://nifi.civitas.test:8443`, noting the Keycloak redirect URI only registers the port-less/443 variants so full OIDC login needs host port 443). Variants: `CLUSTER=k3d just deploy`, `CLUSTER=none just deploy` with the prerequisite list from `clusters/none/up.sh`.
6. **Working with it:** table of all `just` recipes; syncing one component (`just sync-component nifi`); rendering (`just template instance name=nifi-nifi`); `just diff`; Kafka CLI example via `kubectl exec` into the broker pod (copy commands from `tests/kafka.sh`); NiFi REST example with client-credentials (from `tests/nifi.sh`); where to change versions (`values/default-instance.yaml`, `deployment/addons/stackable/charts.yaml`).
7. **Configuration reference:** every key in `values/default-instance.yaml` with meaning; addon value keys (`kafka.cluster.*`, `nifi.nifi.*`) and `rawValues` usage.
8. **Troubleshooting:** CRDs missing → run `just operators` first; NiFi 403 after changing `initialAdminUser` → delete NiFi PVCs / NifiCluster and re-sync (Stackable stores `authorizations.xml` on the node's PVC); `nifi-nifi-oidc-client`/CA secret missing → `just sync-component nifi`; DNS for `*.civitas.test` inside the cluster → `clusters/kind/coredns.sh`; NetworkPolicy denies → `kubectl -n dev get networkpolicy` and `deployment/addons/*/networkpolicies.yaml`; plus any issue actually hit during Tasks 3–7 and its fix.
9. **Deviations from civitas-core-deployment:** Kafka 4.2.1 and KRaft are experimental in SDP 26.7; Linkerd and Kyverno disabled (and why: Strimzi-specific Kyverno label rules, mesh opaque ports); NiFi UI exposed via ingress-nginx; Strimzi `KafkaTopic kafkasql-journal` dropped (unused); no KafkaUsers; Stackable-managed TLS certificates for NiFi and inter-broker traffic.
10. **Upstream recommendations for civitas-core-deployment:** the NiFi URL patch; make the addon lookup path configurable; Stackable-aware Kyverno runtime policies and Linkerd annotations; expose NiFi through APISIX or fix the `nifi` client redirect URIs for port-forward usage.

- [ ] **Step 3: Final verification — idempotent re-run**

Run: `just deploy && just smoke-test`
Expected: second `deploy` succeeds without changes that break anything; smoke test all PASS. Ask the user before a from-scratch run (`just destroy && just deploy && just smoke-test`), since `destroy` deletes their kind cluster.

- [ ] **Step 4: Commit**

```bash
git add README.md civitas-stackable-migration-guide.md
git commit -m "docs: README for the CIVITAS/CORE Stackable demo

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```
