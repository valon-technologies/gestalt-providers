#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <plugin-dir> <gestalt-checkout>" >&2
  exit 1
fi

plugin_dir="$1"
toolchain_dir="$2"
ui_dir="${toolchain_dir}/gestaltd/ui"
asset_dir="${plugin_dir}/out"
build_root="$(mktemp -d)"
ui_copy_dir="${build_root}/gestaltd/ui"
admin_out_dir="${build_root}/gestaltd/internal/adminui/out"

cleanup() {
  rm -rf "${build_root}"
}
trap cleanup EXIT

if [ ! -f "${plugin_dir}/plugin.yaml" ]; then
  echo "plugin manifest not found at ${plugin_dir}/plugin.yaml" >&2
  exit 1
fi
if [ ! -f "${ui_dir}/package.json" ]; then
  echo "gestalt web ui source not found at ${ui_dir}" >&2
  exit 1
fi

mkdir -p "${ui_copy_dir}" "${admin_out_dir}"
rsync -a --delete "${ui_dir}/" "${ui_copy_dir}/"

(cd "${ui_copy_dir}" && npm ci && npm run build)

rm -rf "${asset_dir}"
mkdir -p "${asset_dir}"
rsync -a --delete "${ui_copy_dir}/out/" "${asset_dir}/"
