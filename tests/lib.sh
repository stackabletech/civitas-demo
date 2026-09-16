#!/usr/bin/env bash
# Helpers for the test scripts.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export CIVITAS_CORE_DEPLOYMENT="${CIVITAS_CORE_DEPLOYMENT:-$ROOT/.civitas-core-deployment}"
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

# Run SCRIPT in a short lived pod and print what it printed. We read the logs at the
# end because `kubectl run --rm -i` sometimes loses the output of fast pods.
# usage: in_cluster NAMESPACE LABELS SCRIPT   (LABELS: k=v,k=v or "")
in_cluster() {
  local ns=$1 labels=$2 script=$3 pod="check-$RANDOM$RANDOM"
  kubectl -n "$ns" run "$pod" --restart=Never --image=docker.io/badouralix/curl-jq:alpine \
    ${labels:+--labels "$labels"} --command -- sh -c "$script" >/dev/null
  kubectl -n "$ns" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$pod" --timeout=180s >/dev/null 2>&1 \
    || kubectl -n "$ns" wait --for=jsonpath='{.status.phase}'=Failed "pod/$pod" --timeout=5s >/dev/null 2>&1 || true
  kubectl -n "$ns" logs "$pod" 2>/dev/null || true
  kubectl -n "$ns" delete pod "$pod" --wait=false >/dev/null 2>&1 || true
}
