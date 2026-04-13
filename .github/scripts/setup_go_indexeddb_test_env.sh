#!/usr/bin/env bash

set -euo pipefail

plugin_dir="${1:?plugin directory is required}"

containers=()

cleanup_go_indexeddb_test_env() {
  local name
  for name in "${containers[@]:-}"; do
    docker rm -f "$name" >/dev/null 2>&1 || true
  done
}

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

register_container() {
  containers+=("$1")
}

start_postgres() {
  local container_name="gestalt-go-postgres-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-${RANDOM}"
  local port="${GESTALT_TEST_POSTGRES_PORT:-55432}"
  docker run -d --rm \
    --name "$container_name" \
    -e POSTGRES_PASSWORD=gestalt \
    -e POSTGRES_DB=gestalt_test \
    -p "127.0.0.1:${port}:5432" \
    postgres:16 >/dev/null
  register_container "$container_name"
  wait_for_command 60 docker exec "$container_name" pg_isready -U postgres -d gestalt_test >/dev/null
  export GESTALT_TEST_POSTGRES_DSN="postgres://postgres:gestalt@127.0.0.1:${port}/gestalt_test?sslmode=disable"
}

start_mysql() {
  local container_name="gestalt-go-mysql-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-${RANDOM}"
  local port="${GESTALT_TEST_MYSQL_PORT:-53306}"
  docker run -d --rm \
    --name "$container_name" \
    -e MYSQL_ROOT_PASSWORD=gestalt \
    -e MYSQL_DATABASE=gestalt_test \
    -p "127.0.0.1:${port}:3306" \
    mysql:8.4 \
    --character-set-server=utf8mb4 \
    --collation-server=utf8mb4_unicode_ci >/dev/null
  register_container "$container_name"
  wait_for_command 90 docker exec "$container_name" mysqladmin ping -h 127.0.0.1 -uroot -pgestalt --silent >/dev/null
  export GESTALT_TEST_MYSQL_DSN="mysql://root:gestalt@tcp(127.0.0.1:${port})/gestalt_test"
}

start_sqlserver() {
  local container_name="gestalt-go-sqlserver-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-${RANDOM}"
  local port="${GESTALT_TEST_SQLSERVER_PORT:-11433}"
  local password="${GESTALT_TEST_SQLSERVER_PASSWORD:-GestaltPass1!}"
  docker run -d --rm \
    --name "$container_name" \
    -e ACCEPT_EULA=Y \
    -e MSSQL_SA_PASSWORD="$password" \
    -p "127.0.0.1:${port}:1433" \
    mcr.microsoft.com/mssql/server:2022-latest >/dev/null
  register_container "$container_name"
  wait_for_command 120 docker exec "$container_name" /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$password" -Q "SELECT 1" >/dev/null
  export GESTALT_TEST_SQLSERVER_DSN="sqlserver://sa:${password}@127.0.0.1:${port}?database=master&encrypt=disable&TrustServerCertificate=true"
}

start_mongodb() {
  local container_name="gestalt-go-mongodb-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-${RANDOM}"
  local port="${GESTALT_TEST_MONGODB_PORT:-27017}"
  docker run -d --rm \
    --name "$container_name" \
    -p "127.0.0.1:${port}:27017" \
    mongo:8 >/dev/null
  register_container "$container_name"
  wait_for_command 60 docker exec "$container_name" mongosh --quiet --eval "db.runCommand({ ping: 1 })" >/dev/null
  export GESTALT_TEST_MONGODB_URI="mongodb://127.0.0.1:${port}"
}

start_dynamodb_local() {
  local container_name="gestalt-go-dynamodb-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-${RANDOM}"
  local port="${GESTALT_TEST_DYNAMODB_PORT:-8000}"
  docker run -d --rm \
    --name "$container_name" \
    -p "127.0.0.1:${port}:8000" \
    amazon/dynamodb-local:latest \
    -jar DynamoDBLocal.jar -inMemory -sharedDb >/dev/null
  register_container "$container_name"
  wait_for_command 60 curl -sS \
    -H 'Content-Type: application/x-amz-json-1.0' \
    -H 'X-Amz-Target: DynamoDB_20120810.ListTables' \
    -d '{}' \
    "http://127.0.0.1:${port}" >/dev/null
  export GESTALT_TEST_DYNAMODB_ENDPOINT="http://127.0.0.1:${port}"
  export GESTALT_TEST_DYNAMODB_REGION="${GESTALT_TEST_DYNAMODB_REGION:-us-east-1}"
}

trap cleanup_go_indexeddb_test_env EXIT

case "$plugin_dir" in
  indexeddb/relationaldb)
    start_postgres
    start_mysql
    start_sqlserver
    ;;
  indexeddb/mongodb)
    start_mongodb
    ;;
  indexeddb/dynamodb)
    start_dynamodb_local
    ;;
  *)
    ;;
esac
