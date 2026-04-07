#!/bin/sh

set -eu

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <repository> <ref> <output-path>" >&2
  exit 2
fi

repository="$1"
ref="$2"
output_path="$3"
pat_token="${PAT_TOKEN:-}"

if [ -z "$pat_token" ]; then
  echo "PAT_TOKEN is required to build gestaltd from a private repository" >&2
  exit 1
fi

toolchain_dir="$(mktemp -d)"
cleanup() {
  rm -rf "$toolchain_dir"
}
trap cleanup EXIT INT TERM

git clone "https://x-access-token:${pat_token}@github.com/${repository}.git" "$toolchain_dir"
git -C "$toolchain_dir" fetch --depth 1 origin "$ref"
git -C "$toolchain_dir" checkout FETCH_HEAD

mkdir -p "$(dirname "$output_path")"
(
  cd "$toolchain_dir/gestaltd"
  go build -o "$output_path" ./cmd/gestaltd
)
