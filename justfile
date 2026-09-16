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
    grep -q 'nifi.nifi "url"' "$CIVITAS_CORE_DEPLOYMENT/components/config-adapters/values/adapters/base-values.yaml.gotmpl" 2>/dev/null \
      || { echo "civitas-core-deployment lacks the NiFi URL patch: run 'just apply-v2-patch'"; missing=1; }
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

# Apply the required config-adapter patch to civitas-core-deployment (idempotent)
apply-v2-patch:
    #!/usr/bin/env bash
    set -euo pipefail
    f="$CIVITAS_CORE_DEPLOYMENT/components/config-adapters/values/adapters/base-values.yaml.gotmpl"
    if grep -q 'nifi.nifi "url"' "$f"; then echo "patch already applied"; exit 0; fi
    git -C "$CIVITAS_CORE_DEPLOYMENT" am "{{justfile_directory()}}/patches/civitas-core-deployment/0001-configurable-nifi-url.patch"

# Create/prepare the cluster (CLUSTER=kind|k3d|none, KIND_CLUSTER_NAME for kind)
cluster-up:
    clusters/{{cluster}}/up.sh

# Delete the cluster (CLUSTER=kind|k3d|none)
cluster-down:
    clusters/{{cluster}}/down.sh

# Check cluster prerequisites
test-cluster:
    tests/cluster.sh

# Sync the shared operators layer (CloudNativePG + Stackable operators)
operators: check-tools link
    helmfile -f helmfile-operators.yaml.gotmpl -e {{env}} sync

# Check Stackable operators
test-operators:
    tests/operators.sh

# Sync one component of the instance layer, e.g. `just sync-component kafka`
sync-component component:
    helmfile -f helmfile-instance.yaml.gotmpl -e {{env}} -l component={{component}} sync

# Check Kafka produce/consume
test-kafka:
    tests/kafka.sh

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

# Create/enable the realm user global.initialUserEmail (NiFi admin via nifi-bootstrap) with a generated password
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
      # civitas-core-deployment may already have created this user with VERIFY_EMAIL pending;
      # the demo has no SMTP, so mark the email verified and clear required actions.
      \$kc update users/\$id -r '$ns' -s emailVerified=true -s 'requiredActions=[]'
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
