#!/usr/bin/env sh
set -eu

runtime_kind="integration"
target="provider"
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
    armv7l|armv7*) goarch="arm" ;;
    *) goarch="$(uname -m)" ;;
  esac
fi

env_name="GESTALT_PYTHON_$(printf '%s_%s' "$goos" "$goarch" | tr '[:lower:]-/.' '[:upper:]___')"
eval "python_bin=\${$env_name:-}"
if [ -z "$python_bin" ]; then
  python_bin="${GESTALT_PYTHON:-}"
fi
if [ -z "$python_bin" ] && [ -x ".venv/bin/python" ]; then
  python_bin=".venv/bin/python"
fi
if [ -z "$python_bin" ]; then
  uv sync --frozen --no-dev
  python_bin=".venv/bin/python"
fi

mkdir -p "$(dirname "$output")"
plugin_name="${PWD##*/}"
"$python_bin" -m gestalt._build "$PWD" "$target" "$PWD/$output" "$plugin_name" "$runtime_kind" "$goos" "$goarch"
