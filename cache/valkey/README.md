# Valkey Cache Provider

Valkey-backed cache provider for Gestalt.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
providers:
  cache:
    session:
      source:
        ref: github.com/valon-technologies/gestalt-providers/cache/valkey
        version: 0.0.1-alpha.1
      config:
        addresses:
          - ${VALKEY_ADDR}
        db: 0
        username: ${VALKEY_USERNAME}
        password: secret://valkey-password
        tls: false
        dialTimeout: 5s
        writeTimeout: 5s

plugins:
  search:
    cache:
      - session
```

## Overview

This provider implements Gestalt's portable cache interface on top of a
standalone Valkey deployment.

Supported operations:

- `Get`
- `GetMany`
- `Set`
- `SetMany`
- `Delete`
- `DeleteMany`
- `Touch`

Values are stored as opaque bytes. TTLs are applied with millisecond precision.

## Scope

V1 is intentionally narrow:

- standalone Valkey only
- opaque key/value storage only
- no pub/sub, streams, lists, sets, or hashes
- no key scanning or listing
- no Sentinel or Cluster configuration yet

## SDK Use

Go:

```go
cache, err := gestalt.Cache("session")
if err != nil {
	panic(err)
}
defer cache.Close()

if err := cache.Set(ctx, "user:1", []byte("alpha"), gestalt.CacheSetOptions{TTL: 5 * time.Minute}); err != nil {
	panic(err)
}
```

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
