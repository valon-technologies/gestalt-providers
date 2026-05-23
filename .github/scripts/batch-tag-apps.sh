#!/usr/bin/env bash
set -euo pipefail

# Create and optionally push apps/<name>/v<version> release tags from manifest versions.
# Dry-run by default; pass --push to create tags locally and push to origin.

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${ROOT}"

PUSH=false
APP_FILTER=""

usage() {
  cat <<'EOF'
Usage: batch-tag-apps.sh [--push] [--app NAME]

  --push   Create missing tags and push them to origin (triggers release-app.yml).
  --app    Only process apps/NAME (default: all apps/*/manifest.yaml).
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --push)
      PUSH=true
      shift
      ;;
    --app)
      APP_FILTER="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

shopt -s nullglob
manifests=(apps/*/manifest.yaml)
if ((${#manifests[@]} == 0)); then
  echo "no apps/*/manifest.yaml found under ${ROOT}" >&2
  exit 1
fi

tagged=0
skipped=0

for manifest in "${manifests[@]}"; do
  app_dir="${manifest%/manifest.yaml}"
  name="${app_dir#apps/}"
  if [[ -n "${APP_FILTER}" && "${name}" != "${APP_FILTER}" ]]; then
    continue
  fi

  version="$(
    sed -nE "s/^[[:space:]]*version:[[:space:]]*['\"]?([^'\"[:space:]]+)['\"]?[[:space:]]*$/\\1/p" \
      "${manifest}" | head -n1
  )"
  if [[ -z "${version}" ]]; then
    echo "skip ${name}: missing version in manifest.yaml" >&2
    ((skipped += 1)) || true
    continue
  fi

  tag="apps/${name}/v${version}"
  if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null 2>&1; then
    echo "skip existing ${tag}"
    ((skipped += 1)) || true
    continue
  fi

  echo "tag ${tag}"
  if [[ "${PUSH}" == true ]]; then
    git tag "${tag}"
    git push origin "${tag}"
  fi
  ((tagged += 1)) || true
done

echo "done: ${tagged} tag(s) planned or pushed, ${skipped} skipped"
