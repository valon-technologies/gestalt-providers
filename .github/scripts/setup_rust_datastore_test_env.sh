#!/usr/bin/env bash

set -euo pipefail

plugin_dir="${1:?plugin directory is required}"

wait_for_command() {
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

case "$plugin_dir" in
  datastore/postgres)
    container_name="gestalt-postgres-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-${RANDOM}"
    port="${GESTALT_TEST_POSTGRES_PORT:-55432}"
    docker run -d --rm \
      --name "$container_name" \
      -e POSTGRES_PASSWORD=gestalt \
      -e POSTGRES_DB=gestalt_test \
      -p "127.0.0.1:${port}:5432" \
      postgres:16 >/dev/null
    cleanup_rust_datastore_test_env() {
      docker rm -f "$container_name" >/dev/null 2>&1 || true
    }
    trap cleanup_rust_datastore_test_env EXIT
    wait_for_command 60 docker exec "$container_name" pg_isready -U postgres -d gestalt_test >/dev/null
    export GESTALT_TEST_POSTGRES_DSN="postgres://postgres:gestalt@127.0.0.1:${port}/gestalt_test?sslmode=disable"
    ;;
  datastore/mysql)
    container_name="gestalt-mysql-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-${RANDOM}"
    port="${GESTALT_TEST_MYSQL_PORT:-53306}"
    docker run -d --rm \
      --name "$container_name" \
      -e MYSQL_ROOT_PASSWORD=gestalt \
      -e MYSQL_DATABASE=gestalt_test \
      -p "127.0.0.1:${port}:3306" \
      mysql:8.4 \
      --character-set-server=utf8mb4 \
      --collation-server=utf8mb4_unicode_ci >/dev/null
    cleanup_rust_datastore_test_env() {
      docker rm -f "$container_name" >/dev/null 2>&1 || true
    }
    trap cleanup_rust_datastore_test_env EXIT
    wait_for_command 90 docker exec "$container_name" mysqladmin ping -h 127.0.0.1 -uroot -pgestalt --silent >/dev/null
    export GESTALT_TEST_MYSQL_DSN="mysql://root:gestalt@127.0.0.1:${port}/gestalt_test"
    export GESTALT_TEST_MYSQL_VERSION="8.4"
    ;;
  *)
    ;;
esac
