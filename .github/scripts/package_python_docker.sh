#!/usr/bin/env bash
# Build a Python provider inside a Docker container for a target platform.
# Uses QEMU emulation for non-native architectures (requires docker/setup-qemu-action).
#
# Usage: package_python_docker.sh <docker-platform> <base-image> <gestaltd-binary> \
#                                  <python-env-var> <release-platform> <plugin-dir> <version>
#
# Requires: PAT_TOKEN, UV_PYTHON, RUNNER_TEMP environment variables.

set -euo pipefail

docker_platform="$1"
base_image="$2"
gestaltd_bin="$3"
python_env_var="$4"
release_platform="$5"
plugin_dir="$6"
version="$7"

: "${PAT_TOKEN:?PAT_TOKEN is required}"
: "${RUNNER_TEMP:?RUNNER_TEMP is required}"

case "$base_image" in
  *alpine*) install_cmd="apk add --no-cache bash build-base ca-certificates git curl zlib-dev" ;;
  *)        install_cmd="apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates git curl zlib1g-dev libffi-dev" ;;
esac

echo "=== Packaging ${release_platform} (${base_image}) ==="

docker run --rm --platform "${docker_platform}" \
  -e PAT_TOKEN \
  -e UV_PYTHON \
  -e PYTHON_ENV_VAR="${python_env_var}" \
  -e RELEASE_PLATFORM="${release_platform}" \
  -e VERSION="${version}" \
  -e INSTALL_CMD="${install_cmd}" \
  -v "${PWD}:/workspace" \
  -v "${RUNNER_TEMP}/bin/${gestaltd_bin}:/usr/local/bin/gestaltd:ro" \
  -w "/workspace/${plugin_dir}" \
  "${base_image}" \
  sh -ceu '
    eval "${INSTALL_CMD}"

    if ! command -v uv >/dev/null 2>&1; then
      curl -LsSf https://astral.sh/uv/install.sh | sh
      export PATH="${HOME}/.local/bin:${PATH}"
    fi

    printf "machine github.com\n  login x-access-token\n  password %s\n" "${PAT_TOKEN}" > "${HOME}/.netrc"
    chmod 600 "${HOME}/.netrc"

    git config --global url."https://github.com/".insteadOf "ssh://git@github.com/"
    git config --global url."https://github.com/".insteadOf "git@github.com:"

    rm -rf .venv
    uv sync
    export "${PYTHON_ENV_VAR}=$PWD/.venv/bin/python"
    gestaltd provider release --version "${VERSION}" --platform "${RELEASE_PLATFORM}"
    chmod -R a+rX dist
  '
