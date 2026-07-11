#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Build gestaltd from a pinned Gestalt git ref and install it into GOBIN.

Usage:
  install_gestaltd.sh (--ref REF | --ref-file FILE) [options]

Options:
  --ref REF           Gestalt git ref (commit SHA or tag) to build gestaltd from
  --ref-file FILE     Read the Gestalt git ref from FILE
  --toolchain-dir DIR Gestalt checkout directory (default: ${GITHUB_WORKSPACE}/../gestalt)
  --verify-ref        Assert the checkout HEAD matches --ref (full SHA refs only)
  --cgo-disabled      Build with CGO_ENABLED=0
  --add-to-path       Append GOBIN to GITHUB_PATH when set

Environment:
  GESTALT_REPOSITORY  Required by checkout_gestalt_ref.sh
  GOBIN               Install directory (default: ${RUNNER_TEMP}/bin)
  GITHUB_WORKSPACE    Used for the default toolchain checkout location
EOF
}

ref=""
ref_file=""
toolchain_dir="${GITHUB_WORKSPACE:-$PWD}/../gestalt"
verify_ref=false
cgo_disabled=false
add_to_path=false

while [ $# -gt 0 ]; do
  case "$1" in
    --ref)
      ref="${2:?--ref requires a value}"
      shift 2
      ;;
    --ref-file)
      ref_file="${2:?--ref-file requires a value}"
      shift 2
      ;;
    --toolchain-dir)
      toolchain_dir="${2:?--toolchain-dir requires a value}"
      shift 2
      ;;
    --verify-ref)
      verify_ref=true
      shift
      ;;
    --cgo-disabled)
      cgo_disabled=true
      shift
      ;;
    --add-to-path)
      add_to_path=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [ -n "$ref" ] && [ -n "$ref_file" ]; then
  echo "ERROR: use only one of --ref or --ref-file" >&2
  exit 1
fi

if [ -z "$ref" ] && [ -z "$ref_file" ]; then
  echo "ERROR: --ref or --ref-file is required" >&2
  usage >&2
  exit 1
fi

if [ -n "$ref_file" ]; then
  ref="$(tr -d '[:space:]' <"$ref_file")"
  if [ -z "$ref" ]; then
    echo "ERROR: ref file is empty: $ref_file" >&2
    exit 1
  fi
fi

gobin="${GOBIN:-${RUNNER_TEMP:-/tmp}/bin}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "$gobin"
if [ "$add_to_path" = true ] && [ -n "${GITHUB_PATH:-}" ]; then
  echo "$gobin" >>"$GITHUB_PATH"
fi

"$script_dir/checkout_gestalt_ref.sh" "$toolchain_dir" "$ref"

if [ "$verify_ref" = true ]; then
  if [[ "$ref" =~ ^[0-9a-f]{40}$ ]]; then
    test "$(git -C "$toolchain_dir" rev-parse HEAD)" = "$ref"
  else
    echo "ERROR: --verify-ref requires a full 40-character commit SHA ref" >&2
    exit 1
  fi
fi

build_env=()
if [ "$cgo_disabled" = true ]; then
  build_env=(env CGO_ENABLED=0)
fi

"${build_env[@]}" bash -c "cd \"$toolchain_dir/gestaltd\" && go build -o \"$gobin/gestaltd\" ./cmd/gestaltd"
