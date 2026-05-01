#!/usr/bin/env bash
#
# Single dispatcher for spinning up local backing services that provider tests
# need (databases, caches, etc.). Replaces the three legacy setup scripts:
#   setup_go_indexeddb_test_env.sh
#   setup_go_cache_test_env.sh
#   setup_rust_datastore_test_env.sh
#
# Usage (must be sourced so the EXIT trap stays in scope):
#   . .github/scripts/setup_test_env.sh postgres mysql sqlserver
#   . .github/scripts/setup_test_env.sh valkey
#   . .github/scripts/setup_test_env.sh mongodb
#   . .github/scripts/setup_test_env.sh dynamodb
#
# Each service exports a GESTALT_TEST_<SERVICE>_* env var (DSN, addr, etc.) for
# the test process to consume.

set -euo pipefail

if [ "${#}" -eq 0 ]; then
  echo "usage: . setup_test_env.sh <service> [<service>...]" >&2
  return 1 2>/dev/null || exit 1
fi

# Containers we have started so the EXIT trap can clean them up.
__test_env_containers=()

__test_env_cleanup() {
  local name
  for name in "${__test_env_containers[@]:-}"; do
    docker rm -f "$name" >/dev/null 2>&1 || true
  done
}

__test_env_register() {
  __test_env_containers+=("$1")
}

__test_env_wait() {
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

__test_env_container_name() {
  local service="$1"
  echo "gestalt-test-${service}-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}-${RANDOM}"
}

__start_postgres() {
  local container_name port
  container_name="$(__test_env_container_name postgres)"
  port="${GESTALT_TEST_POSTGRES_PORT:-55432}"
  docker run -d --rm \
    --name "$container_name" \
    -e POSTGRES_PASSWORD=gestalt \
    -e POSTGRES_DB=gestalt_test \
    -p "127.0.0.1:${port}:5432" \
    postgres:16 >/dev/null
  __test_env_register "$container_name"
  __test_env_wait 60 docker exec "$container_name" pg_isready -U postgres -d gestalt_test >/dev/null
  export GESTALT_TEST_POSTGRES_DSN="postgres://postgres:gestalt@127.0.0.1:${port}/gestalt_test?sslmode=disable"
}

__start_mysql() {
  local container_name port
  container_name="$(__test_env_container_name mysql)"
  port="${GESTALT_TEST_MYSQL_PORT:-53306}"
  docker run -d --rm \
    --name "$container_name" \
    -e MYSQL_ROOT_PASSWORD=gestalt \
    -e MYSQL_DATABASE=gestalt_test \
    -p "127.0.0.1:${port}:3306" \
    mysql:8.4 \
    --character-set-server=utf8mb4 \
    --collation-server=utf8mb4_unicode_ci >/dev/null
  __test_env_register "$container_name"
  __test_env_wait 90 docker exec "$container_name" mysqladmin ping -h 127.0.0.1 -uroot -pgestalt --silent >/dev/null
  export GESTALT_TEST_MYSQL_DSN="mysql://root:gestalt@tcp(127.0.0.1:${port})/gestalt_test"
  export GESTALT_TEST_MYSQL_VERSION="8.4"
}

__start_sqlserver() {
  local container_name port password
  container_name="$(__test_env_container_name sqlserver)"
  port="${GESTALT_TEST_SQLSERVER_PORT:-11433}"
  password="${GESTALT_TEST_SQLSERVER_PASSWORD:-GestaltPass1!}"
  docker run -d --rm \
    --name "$container_name" \
    -e ACCEPT_EULA=Y \
    -e MSSQL_SA_PASSWORD="$password" \
    -p "127.0.0.1:${port}:1433" \
    mcr.microsoft.com/mssql/server:2022-latest >/dev/null
  __test_env_register "$container_name"
  __test_env_wait 120 docker exec "$container_name" /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$password" -Q "SELECT 1" >/dev/null
  export GESTALT_TEST_SQLSERVER_DSN="sqlserver://sa:${password}@127.0.0.1:${port}?database=master&encrypt=disable&TrustServerCertificate=true"
}

__start_mongodb() {
  local container_name port
  container_name="$(__test_env_container_name mongodb)"
  port="${GESTALT_TEST_MONGODB_PORT:-27017}"
  docker run -d --rm \
    --name "$container_name" \
    -p "127.0.0.1:${port}:27017" \
    mongo:8 --replSet rs0 --bind_ip_all >/dev/null
  __test_env_register "$container_name"
  __test_env_wait 60 docker exec "$container_name" mongosh --quiet --eval "db.runCommand({ ping: 1 })" >/dev/null
  docker exec "$container_name" mongosh --quiet --eval "rs.initiate({_id: 'rs0', members: [{_id: 0, host: '127.0.0.1:27017'}]})" >/dev/null
  __test_env_wait 60 bash -c "docker exec '$container_name' mongosh --quiet --eval 'db.hello().isWritablePrimary' | grep -q true"
  export GESTALT_TEST_MONGODB_URI="mongodb://127.0.0.1:${port}/?replicaSet=rs0"
}

__start_dynamodb() {
  local container_name port
  container_name="$(__test_env_container_name dynamodb)"
  port="${GESTALT_TEST_DYNAMODB_PORT:-8000}"
  docker run -d --rm \
    --name "$container_name" \
    -p "127.0.0.1:${port}:8000" \
    amazon/dynamodb-local:latest \
    -jar DynamoDBLocal.jar -inMemory -sharedDb >/dev/null
  __test_env_register "$container_name"
  __test_env_wait 60 curl -sS \
    -H 'Content-Type: application/x-amz-json-1.0' \
    -H 'X-Amz-Target: DynamoDB_20120810.ListTables' \
    -d '{}' \
    "http://127.0.0.1:${port}" >/dev/null
  export GESTALT_TEST_DYNAMODB_ENDPOINT="http://127.0.0.1:${port}"
  export GESTALT_TEST_DYNAMODB_REGION="${GESTALT_TEST_DYNAMODB_REGION:-us-east-1}"
}

__start_valkey() {
  local container_name port
  container_name="$(__test_env_container_name valkey)"
  port="${GESTALT_TEST_VALKEY_PORT:-56379}"
  docker run -d --rm \
    --name "$container_name" \
    -p "127.0.0.1:${port}:6379" \
    valkey/valkey:8 >/dev/null
  __test_env_register "$container_name"
  __test_env_wait 60 docker exec "$container_name" sh -c 'valkey-cli ping | grep -q PONG' >/dev/null
  export GESTALT_TEST_VALKEY_ADDR="127.0.0.1:${port}"
}

trap __test_env_cleanup EXIT

for service in "$@"; do
  case "$service" in
    postgres)  __start_postgres ;;
    mysql)     __start_mysql ;;
    sqlserver) __start_sqlserver ;;
    mongodb)   __start_mongodb ;;
    dynamodb)  __start_dynamodb ;;
    valkey)    __start_valkey ;;
    *)
      echo "setup_test_env.sh: unknown service '$service'" >&2
      return 1 2>/dev/null || exit 1
      ;;
  esac
done
