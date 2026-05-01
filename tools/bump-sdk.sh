#!/usr/bin/env bash
#
# Bump the gestalt SDK / gestaltd version pin across the repo.
#
# Usage:
#   tools/bump-sdk.sh                  # apply the pins from .gestalt-sdk-version
#   tools/bump-sdk.sh sdk-go v0.0.1-alpha.13
#   tools/bump-sdk.sh sdk-go v0.0.1-alpha.13 gestaltd v0.0.1-alpha.7
#
# Walks every Go module and rewrites the gestalt SDK require line, removing the
# old local-replace directive. Optionally updates .gestalt-sdk-version in place.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

pin_file=".gestalt-sdk-version"
sdk_go=""
gestaltd=""

read_pin() {
  awk -F= -v key="$1" '$1 == key { print $2 }' "$pin_file"
}

if [ "${#}" -eq 0 ]; then
  if [ ! -f "$pin_file" ]; then
    echo "ERROR: $pin_file not found" >&2
    exit 1
  fi
  sdk_go="$(read_pin SDK_GO)"
  gestaltd="$(read_pin GESTALTD)"
else
  sdk_go="$(read_pin SDK_GO 2>/dev/null || true)"
  gestaltd="$(read_pin GESTALTD 2>/dev/null || true)"
  while [ "${#}" -gt 0 ]; do
    case "$1" in
      sdk-go)   sdk_go="$2"; shift 2 ;;
      gestaltd) gestaltd="$2"; shift 2 ;;
      *)
        echo "unknown component: $1" >&2
        exit 1
        ;;
    esac
  done
  cat > "$pin_file" <<EOF
SDK_GO=${sdk_go}
GESTALTD=${gestaltd}
EOF
fi

if [ -z "$sdk_go" ]; then
  echo "ERROR: SDK_GO pin missing" >&2
  exit 1
fi

echo "Pinning github.com/valon-technologies/gestalt/sdk/go to ${sdk_go}"
while IFS= read -r gomod; do
  if grep -q "^// HOLDOUT" "$gomod"; then
    echo "  skipped $gomod (HOLDOUT)"
    continue
  fi
  if grep -q "github.com/valon-technologies/gestalt/sdk/go" "$gomod"; then
    perl -0777 -i -pe '
      s|^(\s*github\.com/valon-technologies/gestalt/sdk/go)\s+v\S+|$1 '"$sdk_go"'|gm;
      s|^replace\s+github\.com/valon-technologies/gestalt/sdk/go\s*=>\s*\S+\n||gm;
    ' "$gomod"
    echo "  updated $gomod"
  fi
done < <(find . -name go.mod -not -path './.git/*' -not -path './node_modules/*')

echo
echo "Done. Run 'go mod tidy' in each module dir to refresh sums."
