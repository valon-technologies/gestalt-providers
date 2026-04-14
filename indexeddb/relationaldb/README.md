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
- `legacy_table_prefix`: Compatibility hook for migrating legacy prefixed plugin
  tables and metadata. The Gestalt host sets this automatically for plugin
  IndexedDB bindings when it needs to preserve old `plugin_<name>_<store>`
  relationaldb data.

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
