#!/usr/bin/env sh
set -eu

target="agent:./src/provider.ts#provider"
output=".gestalt/build/provider"

goos="${GOOS:-}"
goarch="${GOARCH:-}"
if [ -z "$goos" ] && [ -n "${RELEASE_PLATFORM:-}" ]; then
  goos="${RELEASE_PLATFORM%%/*}"
  goarch="${RELEASE_PLATFORM#*/}"
fi
if [ -z "$goos" ]; then
  case "$(uname -s)" in
    Darwin) goos="darwin" ;;
    Linux) goos="linux" ;;
    *) goos="$(uname -s | tr '[:upper:]' '[:lower:]')" ;;
  esac
fi
if [ -z "$goarch" ]; then
  case "$(uname -m)" in
    x86_64|amd64) goarch="amd64" ;;
    arm64|aarch64) goarch="arm64" ;;
    *) goarch="$(uname -m)" ;;
  esac
fi

bun_bin="${GESTALT_BUN:-bun}"
if [ ! -d node_modules ]; then
  "$bun_bin" install --frozen-lockfile
fi

mkdir -p "$(dirname "$output")"
plugin_name="${PWD##*/}"
if [ -n "${GESTALT_TYPESCRIPT_SDK_DIR:-}" ] && [ -f "${GESTALT_TYPESCRIPT_SDK_DIR}/src/build.ts" ]; then
  if [ ! -d "${GESTALT_TYPESCRIPT_SDK_DIR}/node_modules" ]; then
    "$bun_bin" install --cwd "${GESTALT_TYPESCRIPT_SDK_DIR}" --frozen-lockfile
  fi
  "$bun_bin" --cwd "${GESTALT_TYPESCRIPT_SDK_DIR}" "${GESTALT_TYPESCRIPT_SDK_DIR}/src/build.ts" -- "$PWD" "$target" "$PWD/$output" "$plugin_name" "$goos" "$goarch"
else
  "$bun_bin" run --cwd "$PWD" gestalt-ts-build -- "$PWD" "$target" "$PWD/$output" "$plugin_name" "$goos" "$goarch"
fi
