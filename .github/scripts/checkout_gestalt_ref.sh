#!/usr/bin/env bash

set -euo pipefail

dest_dir="${1:?destination directory is required}"
ref="${2:?gestalt ref is required}"

: "${GESTALT_REPOSITORY:?GESTALT_REPOSITORY is required}"
: "${PAT_TOKEN:?PAT_TOKEN is required}"

toolchain_url="https://x-access-token:${PAT_TOKEN}@github.com/${GESTALT_REPOSITORY}.git"

rm -rf "$dest_dir"
git clone "$toolchain_url" "$dest_dir"

if git -C "$dest_dir" fetch --depth 1 origin "refs/tags/${ref}"; then
  git -C "$dest_dir" checkout --detach FETCH_HEAD
  exit 0
fi

if git -C "$dest_dir" fetch --depth 1 origin "${ref}"; then
  git -C "$dest_dir" checkout --detach FETCH_HEAD
  exit 0
fi

if git -C "$dest_dir" rev-parse --verify --quiet "${ref}^{commit}" >/dev/null; then
  git -C "$dest_dir" checkout --detach "${ref}"
  exit 0
fi

echo "ERROR: unable to fetch Gestalt ref ${ref}" >&2
exit 1
