# MongoDB

MongoDB datastore provider.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/indexeddb/mongodb` |
| **Version** | `0.0.1-alpha.2` |
| **Category** | Datastore |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
indexeddb:
  mongodb:
    source: github.com/valon-technologies/gestalt-providers/indexeddb/mongodb
    version: 0.0.1-alpha.2
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider implements the Gestalt IndexedDB storage interface backed by
MongoDB. Use it when you want a document-oriented datastore with flexible
schemas, rich querying, and aggregation pipelines.

Requires a reachable MongoDB instance and valid connection credentials.

Implemented in Go.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
