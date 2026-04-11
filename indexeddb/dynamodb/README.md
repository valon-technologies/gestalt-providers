# DynamoDB

Amazon DynamoDB datastore provider.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
indexeddb:
  dynamodb:
    source: github.com/valon-technologies/gestalt-providers/indexeddb/dynamodb
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider implements the Gestalt IndexedDB storage interface backed by
Amazon DynamoDB. Use it when you need a fully managed, serverless key-value
and document datastore with single-digit-millisecond performance at any scale.

Requires valid AWS credentials with access to the target DynamoDB table.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
