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
    table_prefix: tenant_
    schema: app
```

Optional configuration:

- `table_prefix` or `prefix`: Prepends a string to each object-store table name.
  Defaults to `""`, so stores map directly to the user-supplied table name.
- `schema` or `namespace`: Qualifies provider tables under a database schema or
  namespace. This is supported for PostgreSQL, MySQL, and SQL Server. SQLite
  does not support schema qualification.

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
