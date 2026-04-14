#!/usr/bin/env bash

set -euo pipefail

plugin_dir="${1:?plugin directory is required}"

cache_test_containers=()

cleanup_go_cache_test_env() {
  local name
  for name in "${cache_test_containers[@]:-}"; do
    docker rm -f "$name" >/dev/null 2>&1 || true
  done
}

wait_for_cache_test_command() {
  local attempts="$1"
  shift
  local i
  for i in $(seq 1 "$attempts"); do
    if "$@"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

register_cache_test_container() {
  cache_test_containers+=("$1")
}

start_valkey() {
  local container_name="gestalt-go-valkey-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-${RANDOM}"
  local port="${GESTALT_TEST_VALKEY_PORT:-56379}"
  docker run -d --rm \
    --name "$container_name" \
    -p "127.0.0.1:${port}:6379" \
    valkey/valkey:8 >/dev/null
  register_cache_test_container "$container_name"
  wait_for_cache_test_command 60 docker exec "$container_name" sh -c 'valkey-cli ping | grep -q PONG' >/dev/null
  export GESTALT_TEST_VALKEY_ADDR="127.0.0.1:${port}"
}

case "$plugin_dir" in
  cache/valkey)
    start_valkey
    trap cleanup_go_cache_test_env EXIT
    ;;
  *)
    ;;
esac
