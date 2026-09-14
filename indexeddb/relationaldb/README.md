# RelationalDB

IndexedDB provider supporting PostgreSQL, MySQL, SQLite, and SQL Server.

Requires gestaltd built with `GESTALT_PROVIDER_SOCKET` (gestaltd after the plugin→app
provider env rename). The published `0.0.1-alpha.1` release is incompatible with current
gestaltd; use `0.0.1-alpha.2` or newer.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
indexeddb:
  relationaldb:
    source: github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb
    version: ...
    dsn: postgres://...
    schema: plugin_alpha
    connection:
      max_open_conns: 32
      max_idle_conns: 8
      conn_max_lifetime: 30m
      conn_max_idle_time: 5m
      ping_timeout: 5s
      retry_attempts: 2
      retry_backoff: 200ms
```

Optional configuration:

- `table_prefix` or `prefix`: Prepends a string to each object-store table name.
  Defaults to `""`, so stores map directly to the user-supplied table name. On
  SQLite, the same prefix is also used to namespace metadata keys in
  `_gestalt_stores`, which lets multiple provider instances share one database
  while each instance still uses logical store names like `tasks`.
- `schema`: Qualifies provider tables under a database schema. This is
  supported for PostgreSQL, MySQL, and SQL Server. SQLite does not support
  schema qualification.
- `connection`: Optional `database/sql` pool and retry tuning.
  - `max_open_conns`: Maximum open connections. `0` leaves the pool unlimited.
  - `max_idle_conns`: Maximum idle connections. `0` disables idle retention.
  - `conn_max_lifetime`: Maximum lifetime for a pooled connection. Defaults to
    `30m`. Set `0` to disable lifetime-based recycling.
  - `conn_max_idle_time`: Maximum idle time for a pooled connection. Defaults
    to `5m`. Set `0` to disable idle-time recycling.
  - `ping_timeout`: Per-attempt timeout for connectivity checks. Defaults to
    `5s`. Set `0` to use the caller's context without an extra timeout.
  - `retry_attempts`: Additional attempts for retryable connection setup,
    read-path failures, and transient database lock/deadlock contention.
    Defaults to `2`.
  - `retry_backoff`: Base backoff between retry attempts. Defaults to `200ms`.

Provision the physical schema before starting the provider:

```sh
RELATIONALDB_DSN=... go run ./cmd/migrate --schema plugin_alpha
```

Pass the same `--schema` and `--table-prefix` values used in provider
configuration. Provider startup validates the schema without issuing DDL.

### Ordered primary-key upgrade

This version adds a nullable `pk_ord` column and a primary-key scan index to the
physical records table. Provision these with the migration command **before**
deploying the new provider. Existing providers can continue writing while it is
provisioned; they leave new rows' `pk_ord` values null. Use the database's supported
online schema-change procedure for large production tables.

After deploying the provider, run the resumable backfill:

```sh
RELATIONALDB_DSN=... go run ./cmd/migrate --schema plugin_alpha --backfill-primary-keys
```

The backfill reads at most 1,000 keys per SQL result and commits each conditional
update independently. Interrupting it is safe; rerunning processes remaining null
keys. Retire older writers and rerun once more to finish rows they inserted during
the rollout. Readers merge null-key rows with ordered results throughout the
upgrade, so correctness does not depend on backfill completion. The legacy scan
cost disappears once all keys are backfilled.

Object-store ranges and limits are applied in SQL for ordered rows. Each SQL
result contains at most 1,000 rows, including unbounded requests and secondary
index scans. Limited secondary reads sort matching keys before fetching only the
requested payloads; they still inspect all matching index keys. MySQL finishes
equal ordered-key prefix groups before limiting, preserving long-key ordering
without depending on the server's BLOB sort-length setting.

Examples:

```yaml
# PostgreSQL / MySQL / SQL Server: isolate provider state in a schema.
indexeddb:
  relationaldb:
    source: github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb
    version: ...
    dsn: postgres://...
    schema: plugin_alpha
```

```yaml
# SQLite fallback: isolate provider state with table and metadata prefixes.
indexeddb:
  relationaldb:
    source: github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb
    version: ...
    dsn: file:/var/lib/gestalt/plugins.sqlite
    table_prefix: plugin_alpha_
    connection:
      conn_max_lifetime: 15m
      conn_max_idle_time: 2m
      retry_attempts: 1
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider implements the Gestalt IndexedDB storage interface backed by a
relational database. It supports four engines:

- **PostgreSQL**
- **MySQL**
- **SQLite**
- **SQL Server**

Use it when you want to persist Gestalt state in an existing relational database
or need the transactional guarantees and query capabilities of SQL.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
