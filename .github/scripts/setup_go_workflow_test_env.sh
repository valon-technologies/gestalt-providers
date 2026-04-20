#!/usr/bin/env bash

set -euo pipefail

plugin_dir="${1:?plugin directory is required}"

workflow_test_containers=()

cleanup_go_workflow_test_env() {
  local name
  for name in "${workflow_test_containers[@]:-}"; do
    docker rm -f "$name" >/dev/null 2>&1 || true
  done
}

wait_for_workflow_test_command() {
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

register_workflow_test_container() {
  workflow_test_containers+=("$1")
}

wait_for_workflow_test_tcp() {
  local host="$1"
  local port="$2"
  python3 - "$host" "$port" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.settimeout(1)
    sock.connect((host, port))
PY
}

start_temporal_dev_server() {
  local container_name="gestalt-go-temporal-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-${RANDOM}"
  local grpc_port="${GESTALT_TEST_TEMPORAL_PORT:-57233}"
  local ui_port="${GESTALT_TEST_TEMPORAL_UI_PORT:-58233}"
  local namespace="${GESTALT_TEST_TEMPORAL_NAMESPACE:-default}"
  local temporal_image="${GESTALT_TEST_TEMPORAL_IMAGE:-temporalio/temporal:1.30.2}"

  docker run -d --rm \
    --name "$container_name" \
    -p "127.0.0.1:${grpc_port}:7233" \
    -p "127.0.0.1:${ui_port}:8233" \
    "$temporal_image" \
    server start-dev --ip 0.0.0.0 >/dev/null
  register_workflow_test_container "$container_name"

  wait_for_workflow_test_command 90 wait_for_workflow_test_tcp 127.0.0.1 "$grpc_port"

  export GESTALT_TEST_TEMPORAL_PORT="$grpc_port"
  export GESTALT_TEST_TEMPORAL_UI_PORT="$ui_port"
  export GESTALT_TEST_TEMPORAL_NAMESPACE="$namespace"
  export GESTALT_TEST_TEMPORAL_ADDRESS="127.0.0.1:${grpc_port}"
  export GESTALT_TEST_TEMPORAL_HOST_PORT="$GESTALT_TEST_TEMPORAL_ADDRESS"
  export GESTALT_TEST_TEMPORAL_UI_ADDRESS="http://127.0.0.1:${ui_port}"

  export TEMPORAL_ADDRESS="$GESTALT_TEST_TEMPORAL_ADDRESS"
  export TEMPORAL_HOST_PORT="$GESTALT_TEST_TEMPORAL_ADDRESS"
  export TEMPORAL_NAMESPACE="$namespace"
}

case "$plugin_dir" in
  workflow/temporal)
    trap cleanup_go_workflow_test_env EXIT
    start_temporal_dev_server
    ;;
  *)
    ;;
esac
