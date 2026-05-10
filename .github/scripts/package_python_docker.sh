#!/usr/bin/env bash
# Build a Python provider inside a Docker container for a target platform.
# Uses QEMU emulation for non-native architectures (requires docker/setup-qemu-action).
#
# Usage: package_python_docker.sh <docker-platform> <base-image> <gestaltd-binary> \
#                                  <python-env-var> <release-platform> <plugin-dir> <version>
#
# Requires: UV_PYTHON and RUNNER_TEMP environment variables.
# Optionally set GESTALT_SDK_HOST_DIR to a checked-out gestalt/sdk/python
# directory; when present, it replaces the provider lockfile's gestalt-sdk.

set -euo pipefail

docker_platform="$1"
base_image="$2"
gestaltd_bin="$3"
python_env_var="$4"
release_platform="$5"
plugin_dir="$6"
version="$7"

: "${RUNNER_TEMP:?RUNNER_TEMP is required}"

case "$base_image" in
  *alpine*) install_cmd="apk add --no-cache bash build-base ca-certificates git curl zlib-dev cargo rust" ;;
  *)        install_cmd="apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates git curl zlib1g-dev libffi-dev cargo rustc" ;;
esac

sdk_mount=()
if [ -n "${GESTALT_SDK_HOST_DIR:-}" ]; then
  if [ ! -d "${GESTALT_SDK_HOST_DIR}" ]; then
    echo "GESTALT_SDK_HOST_DIR does not exist: ${GESTALT_SDK_HOST_DIR}" >&2
    exit 1
  fi
  sdk_mount=(
    -e GESTALT_SDK_CONTAINER_DIR=/gestalt-sdk
    -v "${GESTALT_SDK_HOST_DIR}:/gestalt-sdk:ro"
  )
fi

echo "=== Packaging ${release_platform} (${base_image}) ==="

docker run --rm --platform "${docker_platform}" \
  -e UV_PYTHON=python3 \
  -e UV_PYTHON_DOWNLOADS=never \
  -e PYTHON_ENV_VAR="${python_env_var}" \
  -e RELEASE_PLATFORM="${release_platform}" \
  -e VERSION="${version}" \
  -e INSTALL_CMD="${install_cmd}" \
  -v "${PWD}:/workspace" \
  -v "${RUNNER_TEMP}/bin/${gestaltd_bin}:/usr/local/bin/gestaltd:ro" \
  "${sdk_mount[@]}" \
  -w "/workspace/${plugin_dir}" \
  "${base_image}" \
  sh -ceu '
    eval "${INSTALL_CMD}"

    if ! command -v uv >/dev/null 2>&1; then
      curl -LsSf https://astral.sh/uv/install.sh | sh
      export PATH="${HOME}/.local/bin:${PATH}"
    fi

    rm -rf .venv
    uv sync --frozen --no-dev --python "${UV_PYTHON}"
    if [ -n "${GESTALT_SDK_CONTAINER_DIR:-}" ]; then
      rm -rf /tmp/gestalt-sdk
      cp -R "${GESTALT_SDK_CONTAINER_DIR}" /tmp/gestalt-sdk
      uv pip install --python "$PWD/.venv/bin/python" --reinstall --no-deps /tmp/gestalt-sdk
    fi
    export "${PYTHON_ENV_VAR}=$PWD/.venv/bin/python"
    gestaltd provider release --version "${VERSION}" --platform "${RELEASE_PLATFORM}"
    chmod -R a+rX dist
  '
