# HashiCorp Vault

Resolves secrets from HashiCorp Vault KV v2.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
providers:
  secrets:
    source:
      ref: github.com/valon-technologies/gestalt-providers/secrets/vault
      version: ...
    config:
      address: https://vault.example.com
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider resolves secret references in your Gestalt configuration against
a HashiCorp Vault server using the KV v2 secrets engine. At runtime, `gestaltd`
authenticates to Vault and fetches the requested secret paths.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
