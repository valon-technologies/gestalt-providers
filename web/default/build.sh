#!/bin/sh
set -eu

gestalt_checkout="${GESTALT_CHECKOUT:-../../../gestalt}"
ui_dir="${gestalt_checkout}/gestaltd/ui"

if [ ! -f "${ui_dir}/package.json" ]; then
  echo "gestalt web ui source not found at ${ui_dir}" >&2
  echo "set GESTALT_CHECKOUT to a gestalt checkout before running provider release" >&2
  exit 1
fi

(cd "${ui_dir}" && npm ci && npm run build)

rm -rf out
mkdir -p out
cp -R "${ui_dir}/out/." out/
