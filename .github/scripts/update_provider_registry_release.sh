#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "usage: update_provider_registry_release.sh PACKAGE_DIR TAG MANIFEST_PATH METADATA_PATH" >&2
  exit 2
fi

package_dir="$1"
tag="$2"
manifest_path="$3"
metadata_path="$4"

tmp_manifest="${RUNNER_TEMP:-/tmp}/provider-registry-manifest.yaml"
tmp_metadata="${RUNNER_TEMP:-/tmp}/provider-registry-release.yaml"
cp "$manifest_path" "$tmp_manifest"
cp "$metadata_path" "$tmp_metadata"

python3 -m pip install -r .github/scripts/provider_registry_requirements.txt

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git fetch --force origin main --tags
git checkout -B main origin/main
git pull --rebase origin main

regenerate_registry() {
  python3 .github/scripts/generate_provider_index.py \
    --release-metadata "$tmp_metadata" \
    --release-manifest "$tmp_manifest" \
    --release-tag "$tag" \
    --package-dir "$package_dir"
  python3 .github/scripts/generate_provider_index.py --check
  python3 .github/scripts/validate_provider_registry_contract.py
}

regenerate_registry
if git diff --quiet -- provider-index.yaml registry/catalog.json; then
  echo "provider registry already up to date"
  exit 0
fi

git add provider-index.yaml registry/catalog.json
git commit -m "Update provider registry for ${tag}"

for attempt in 1 2 3; do
  if git push origin HEAD:main; then
    exit 0
  fi
  if [ "$attempt" -eq 3 ]; then
    echo "failed to push provider registry update after ${attempt} attempts" >&2
    exit 1
  fi
  git fetch --force origin main --tags
  git rebase origin/main
  regenerate_registry
  git add provider-index.yaml registry/catalog.json
  if git diff --cached --quiet; then
    echo "provider registry became up to date after rebase"
    exit 0
  fi
  git commit --amend --no-edit
done
