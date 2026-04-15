#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF' >&2
Usage:
  .github/scripts/release_policy.sh write-github-output <plugin-dir> <version> <output-file>
  .github/scripts/release_policy.sh assert-ci-local <plugin-dir> <version> <artifact-dir>
  .github/scripts/release_policy.sh assert-local <plugin-dir> <version> <artifact-dir>
  .github/scripts/release_policy.sh assert-release <plugin-dir> <version> <repository> <release-tag>
EOF
  exit 1
}

command_name="${1:-}"
[ -n "${command_name}" ] || usage

plugin_dir="${2:-}"
version="${3:-}"
[ -n "${plugin_dir}" ] || usage
[ -n "${version}" ] || usage

manifest_path="${plugin_dir}/manifest.yaml"
if [ ! -f "${manifest_path}" ]; then
  echo "ERROR: manifest not found at ${manifest_path}" >&2
  exit 1
fi

manifest_source="$(sed -nE "s/^[[:space:]]*source:[[:space:]]*['\"]?([^'\"[:space:]]+)['\"]?[[:space:]]*$/\\1/p" "${manifest_path}" | head -n1)"
manifest_kind="$(sed -nE "s/^[[:space:]]*kind:[[:space:]]*([^[:space:]]+)[[:space:]]*$/\\1/p" "${manifest_path}" | head -n1)"

if [ -z "${manifest_source}" ] || [ -z "${manifest_kind}" ]; then
  echo "ERROR: unable to parse source/kind from ${manifest_path}" >&2
  exit 1
fi

package_name="${manifest_source##*/}"
artifact_prefix="gestalt-plugin-${package_name}_v${version}"
generic_asset="${artifact_prefix}.tar.gz"

has_go=false
has_python=false
has_rust=false
plugin_runtime=false

find "${plugin_dir}" -maxdepth 1 -name '*.go' | grep -q . && has_go=true
[ -f "${plugin_dir}/pyproject.toml" ] && has_python=true
[ -f "${plugin_dir}/Cargo.toml" ] && has_rust=true

manifest_has_key() {
  local key="$1"
  grep -Eq "^[[:space:]]*${key}:[[:space:]]*" "${manifest_path}"
}

runtime_sensitive=false
case "${manifest_kind}" in
  auth|indexeddb|cache|secrets)
    runtime_sensitive=true
    ;;
  plugin)
    if manifest_has_key "entrypoint" || manifest_has_key "artifacts" || manifest_has_key "release"; then
      plugin_runtime=true
    fi
    if [ "${plugin_runtime}" = "true" ]; then
      runtime_sensitive=true
    fi
    ;;
esac

package_mode="generic"
allow_generic=true
required_assets=("${generic_asset}")
optional_assets=()

if [ "${runtime_sensitive}" = "true" ]; then
  package_mode="runtime"
  allow_generic=false
  required_assets=(
    "${artifact_prefix}_linux_amd64.tar.gz"
    "${artifact_prefix}_linux_arm64.tar.gz"
  )
  if [ "${has_go}" = "true" ] || [ "${has_python}" = "true" ]; then
    required_assets+=("${artifact_prefix}_darwin_arm64.tar.gz")
  fi

  if [ "${has_go}" = "true" ]; then
    optional_assets+=(
      "${artifact_prefix}_darwin_amd64.tar.gz"
      "${artifact_prefix}_linux_arm.tar.gz"
    )
  elif [ "${has_python}" = "true" ] || [ "${has_rust}" = "true" ]; then
    optional_assets+=("${artifact_prefix}_linux_arm.tar.gz")
  fi
fi

assert_required_assets() {
  local actual_assets=("$@")
  local asset
  local missing=()

  for asset in "${required_assets[@]}"; do
    if ! printf '%s\n' "${actual_assets[@]}" | grep -Fx -- "${asset}" >/dev/null; then
      missing+=("${asset}")
    fi
  done

  if [ "${#missing[@]}" -gt 0 ]; then
    printf 'ERROR: missing required release assets for %s\n' "${plugin_dir}" >&2
    printf '  %s\n' "${missing[@]}" >&2
    exit 1
  fi
}

assert_release_shape() {
  local actual_assets=("$@")
  local asset
  local prefixed_assets=()
  local platform_specific=()

  while IFS= read -r asset; do
    [ -n "${asset}" ] || continue
    prefixed_assets+=("${asset}")
  done < <(printf '%s\n' "${actual_assets[@]}" | grep -E "^${artifact_prefix}(_.*)?\\.tar\\.gz$" || true)

  if [ "${allow_generic}" = "true" ]; then
    while IFS= read -r asset; do
      [ -n "${asset}" ] || continue
      platform_specific+=("${asset}")
    done < <(printf '%s\n' "${prefixed_assets[@]}" | grep -E "^${artifact_prefix}_.+\\.tar\\.gz$" || true)
    if [ "${#platform_specific[@]}" -gt 0 ]; then
      echo "ERROR: platform-neutral packages must not publish platform-specific archives" >&2
      printf '  %s\n' "${platform_specific[@]}" >&2
      exit 1
    fi
  else
    if printf '%s\n' "${actual_assets[@]}" | grep -Fx -- "${generic_asset}" >/dev/null; then
      echo "ERROR: runtime-sensitive packages must not publish a generic archive" >&2
      echo "  ${generic_asset}" >&2
      exit 1
    fi
  fi
}

assert_asset_policy() {
  local actual_assets=("$@")
  assert_required_assets "${actual_assets[@]}"
  assert_release_shape "${actual_assets[@]}"
}

assert_ci_asset_policy() {
  local actual_assets=("$@")
  local ci_required_assets=("${generic_asset}")

  if [ "${runtime_sensitive}" = "true" ]; then
    ci_required_assets=("${artifact_prefix}_linux_amd64.tar.gz")
  fi

  required_assets=("${ci_required_assets[@]}")
  assert_required_assets "${actual_assets[@]}"
  assert_release_shape "${actual_assets[@]}"
}

case "${command_name}" in
  write-github-output)
    output_file="${4:-}"
    [ -n "${output_file}" ] || usage
    {
      echo "kind=${manifest_kind}"
      echo "package-mode=${package_mode}"
      echo "allow-generic=${allow_generic}"
      echo "runtime-sensitive=${runtime_sensitive}"
      echo "required-assets<<EOF"
      printf '%s\n' "${required_assets[@]}"
      echo "EOF"
      echo "optional-assets<<EOF"
      if [ "${#optional_assets[@]}" -gt 0 ]; then
        printf '%s\n' "${optional_assets[@]}"
      fi
      echo "EOF"
    } >> "${output_file}"
    ;;
  assert-local)
    artifact_dir="${4:-}"
    [ -n "${artifact_dir}" ] || usage
    if [ ! -d "${artifact_dir}" ]; then
      echo "ERROR: artifact directory not found at ${artifact_dir}" >&2
      exit 1
    fi
    local_assets=()
    while IFS= read -r asset; do
      [ -n "${asset}" ] || continue
      local_assets+=("${asset}")
    done < <(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -type f -exec basename {} \; | sort)
    assert_asset_policy "${local_assets[@]}"
    ;;
  assert-ci-local)
    artifact_dir="${4:-}"
    [ -n "${artifact_dir}" ] || usage
    if [ ! -d "${artifact_dir}" ]; then
      echo "ERROR: artifact directory not found at ${artifact_dir}" >&2
      exit 1
    fi
    local_assets=()
    while IFS= read -r asset; do
      [ -n "${asset}" ] || continue
      local_assets+=("${asset}")
    done < <(find "${artifact_dir}" -mindepth 1 -maxdepth 1 -type f -exec basename {} \; | sort)
    assert_ci_asset_policy "${local_assets[@]}"
    ;;
  assert-release)
    repository="${4:-}"
    release_tag="${5:-}"
    [ -n "${repository}" ] || usage
    [ -n "${release_tag}" ] || usage
    encoded_tag="${release_tag//\//%2F}"
    release_assets=()
    while IFS= read -r asset; do
      [ -n "${asset}" ] || continue
      release_assets+=("${asset}")
    done < <(gh api "repos/${repository}/releases/tags/${encoded_tag}" --jq '.assets[].name' | sort)
    if ! printf '%s\n' "${release_assets[@]}" | grep -Fx -- "checksums.txt" >/dev/null; then
      echo "ERROR: published release is missing checksums.txt" >&2
      exit 1
    fi
    assert_asset_policy "${release_assets[@]}"
    ;;
  *)
    usage
    ;;
esac
