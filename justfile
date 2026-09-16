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
