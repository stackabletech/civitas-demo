set shell := ["bash", "-euo", "pipefail", "-c"]

export CIVITAS_CORE_DEPLOYMENT := env("CIVITAS_CORE_DEPLOYMENT", justfile_directory() / ".." / "civitas-core-deployment")
cluster := env("CLUSTER", "kind")
env := "local"
values := "values/default-instance.yaml"

# Show all commands
default:
    @just --list --unsorted

# --- setup -------------------------------------------------------------------

# Check that all tools and civitas-core-deployment are there
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

# Add the needed NiFi URL patch to civitas-core-deployment
apply-v2-patch:
    #!/usr/bin/env bash
    set -euo pipefail
    f="$CIVITAS_CORE_DEPLOYMENT/components/config-adapters/values/adapters/base-values.yaml.gotmpl"
    if grep -q 'nifi.nifi "url"' "$f"; then echo "patch already applied"; exit 0; fi
    git -C "$CIVITAS_CORE_DEPLOYMENT" am "{{justfile_directory()}}/patches/civitas-core-deployment/0001-configurable-nifi-url.patch"

# Link our deployment folder into civitas-core-deployment
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

# Remove that link again
unlink:
    #!/usr/bin/env bash
    set -euo pipefail
    target="$CIVITAS_CORE_DEPLOYMENT/deployment"
    if [ -L "$target" ]; then rm "$target" && echo "removed $target"; else echo "no symlink at $target"; fi

# Create or prepare the cluster (CLUSTER=kind, k3d or none)
cluster-up:
    clusters/{{cluster}}/up.sh

# Delete the cluster
cluster-down:
    clusters/{{cluster}}/down.sh

# --- deploy ------------------------------------------------------------------

# Do everything: check, cluster, link, operators, platform
deploy: check-tools cluster-up link operators instance
    @echo "Deployed. Run 'just smoke-test' and 'just credentials'."

# Install the operators (PostgreSQL and Stackable)
operators: check-tools link
    helmfile -f helmfile-operators.yaml.gotmpl -e {{env}} sync

# Install the CIVITAS/CORE platform
instance: check-tools link
    #!/usr/bin/env bash
    set -euo pipefail
    ns=$(yq '.global.instanceSlug' < {{values}})
    kubectl get namespace "$ns" >/dev/null 2>&1 || kubectl create namespace "$ns"
    # Keycloak wants mail server settings. Fake ones are fine here.
    kubectl -n "$ns" get secret keycloak-smtp >/dev/null 2>&1 || kubectl -n "$ns" create secret generic keycloak-smtp \
      --from-literal=host='smtp.example.com' --from-literal=port='587' \
      --from-literal=from='noreply@example.com' --from-literal=user='noreply@example.com' \
      --from-literal=password='YOUR_SMTP_PASSWORD'
    helmfile -f helmfile-instance.yaml.gotmpl -e {{env}} sync

# Install one part again, for example `just sync-component nifi`
sync-component component:
    helmfile -f helmfile-instance.yaml.gotmpl -e {{env}} -l component={{component}} sync

# Print the Kubernetes files without installing (operators or instance)
template layer="instance" selector="":
    helmfile -f helmfile-{{layer}}.yaml.gotmpl -e {{env}} {{ if selector != "" { "-l " + selector } else { "" } }} template

# Show what would change (operators or instance)
diff layer="instance" selector="":
    helmfile -f helmfile-{{layer}}.yaml.gotmpl -e {{env}} {{ if selector != "" { "-l " + selector } else { "" } }} diff

# Delete the cluster and remove the link
destroy: cluster-down unlink

# --- day 2 -------------------------------------------------------------------

# Show what is running
status:
    #!/usr/bin/env bash
    set -euo pipefail
    ns=$(yq '.global.instanceSlug' < {{values}})
    kubectl -n civitas-operators get pods
    kubectl -n "$ns" get kafkaclusters,nificlusters 2>/dev/null || true
    kubectl -n "$ns" get pods,ingress

# Show logins for Keycloak and NiFi
credentials:
    #!/usr/bin/env bash
    set -euo pipefail
    ns=$(yq '.global.instanceSlug' < {{values}})
    domain=$(yq '.global.domain' < {{values}})
    secret() { kubectl -n "$ns" get secret "$1" -o jsonpath="{.data.$2}" | base64 -d; }
    echo "Keycloak: https://idm.$domain/admin  user: admin  password: $(secret keycloak-admin-user password)"
    if kubectl -n "$ns" get secret nifi-demo-admin-user >/dev/null 2>&1; then
      echo "NiFi:     https://nifi.$domain/nifi  user: $(secret nifi-demo-admin-user username)  password: $(secret nifi-demo-admin-user password)"
    else
      echo "NiFi:     run 'just create-admin-user' first"
    fi

# Give admin@civitas.test a password so you can log in to NiFi
create-admin-user:
    #!/usr/bin/env bash
    set -euo pipefail
    ns=$(yq '.global.instanceSlug' < {{values}})
    email=$(yq '.global.initialUserEmail' < {{values}})
    if ! kubectl -n "$ns" get secret nifi-demo-admin-user >/dev/null 2>&1; then
      pw="Civitas$(head -c 12 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 10)1"
      kubectl -n "$ns" create secret generic nifi-demo-admin-user --from-literal=username="$email" --from-literal=password="$pw"
    fi
    pw=$(kubectl -n "$ns" get secret nifi-demo-admin-user -o jsonpath='{.data.password}' | base64 -d)
    admin_pw=$(kubectl -n "$ns" get secret keycloak-admin-user -o jsonpath='{.data.password}' | base64 -d)
    pod=$(kubectl -n "$ns" get pod -l app.kubernetes.io/name=keycloakx -o jsonpath='{.items[0].metadata.name}')
    kubectl -n "$ns" exec "$pod" -c keycloak -- bash -c "
      set -e
      kc=/opt/keycloak/bin/kcadm.sh
      \$kc config credentials --server http://localhost:8080 --realm master --user admin --password '$admin_pw' >/dev/null
      id=\$(\$kc get users -r '$ns' -q exact=true -q username='$email' --fields id --format csv --noquotes | head -n1)
      if [ -z \"\$id\" ]; then
        \$kc create users -r '$ns' -s username='$email' -s email='$email' -s enabled=true -s firstName=Civitas -s lastName=Admin
        id=\$(\$kc get users -r '$ns' -q exact=true -q username='$email' --fields id --format csv --noquotes | head -n1)
      fi
      # There is no mail server, so mark the email as checked.
      \$kc update users/\$id -r '$ns' -s emailVerified=true -s 'requiredActions=[]'
      \$kc set-password -r '$ns' --userid \"\$id\" --new-password '$pw'
    "
    echo "Updated $email; see 'just credentials'."

# Add the civitas.test names to /etc/hosts (asks for sudo)
add-hosts:
    #!/usr/bin/env bash
    set -euo pipefail
    domain=$(yq '.global.domain' < {{values}})
    line="127.0.0.1 idm.$domain portal.$domain api.$domain dashboard.$domain nifi.$domain"
    if grep -qF "$line" /etc/hosts; then echo "already present"; else echo "$line" | sudo tee -a /etc/hosts; fi

# Open the platform on localhost (use port 443 for logins, asks for sudo)
port-forward port="8443":
    #!/usr/bin/env bash
    set -euo pipefail
    cmd=("$(command -v kubectl)" --kubeconfig "${KUBECONFIG:-$HOME/.kube/config}" \
      -n ingress-nginx port-forward svc/ingress-nginx-controller {{port}}:443)
    if [ {{port}} -lt 1024 ]; then sudo "${cmd[@]}"; else "${cmd[@]}"; fi

# --- tests -------------------------------------------------------------------

# Check the config without a cluster
test-render: link
    tests/render.sh

# Check the cluster basics
test-cluster:
    tests/cluster.sh

# Check the Stackable operators
test-operators:
    tests/operators.sh

# Check Kafka by sending and reading a message
test-kafka:
    tests/kafka.sh

# Check NiFi API, web page and login
test-nifi:
    tests/nifi.sh

# Run all checks
smoke-test:
    tests/smoke.sh
