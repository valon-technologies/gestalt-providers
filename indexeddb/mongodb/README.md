# MongoDB

MongoDB datastore provider.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
server:
  providers:
    indexeddb: main

providers:
  indexeddb:
    main:
      source:
        ref: github.com/valon-technologies/gestalt-providers/indexeddb/mongodb
        version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider implements the Gestalt IndexedDB storage interface backed by
MongoDB. Use it when you want a document-oriented datastore with flexible
schemas, rich querying, and aggregation pipelines.

Requires a reachable MongoDB instance and valid connection credentials.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
