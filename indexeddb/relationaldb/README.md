# RelationalDB

IndexedDB provider supporting PostgreSQL, MySQL, SQLite, and SQL Server.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
indexeddb:
  relationaldb:
    source: github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb
    version: ...
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
