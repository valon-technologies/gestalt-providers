# HashiCorp Vault

Resolves secrets from HashiCorp Vault KV v2.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/secrets/vault` |
| **Version** | `0.0.1-alpha.7` |
| **Category** | Secrets |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
secrets:
  vault:
    source: github.com/valon-technologies/gestalt-providers/secrets/vault
    version: 0.0.1-alpha.7
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider resolves secret references in your Gestalt configuration against
a HashiCorp Vault server using the KV v2 secrets engine. At runtime, `gestaltd`
authenticates to Vault and fetches the requested secret paths.

Implemented in Go.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
