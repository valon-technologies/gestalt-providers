# RelationalDB

IndexedDB provider supporting PostgreSQL, MySQL, SQLite, and SQL Server.

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
  - `retry_attempts`: Additional attempts for retryable connection setup and
    read-path failures. Defaults to `2`.
  - `retry_backoff`: Base backoff between retry attempts. Defaults to `200ms`.

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
