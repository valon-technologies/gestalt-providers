# Azure Key Vault

Resolves secrets from Azure Key Vault.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/secrets/azure` |
| **Version** | `0.0.1-alpha.7` |
| **Category** | Secrets |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
secrets:
  azure:
    source: github.com/valon-technologies/gestalt-providers/secrets/azure
    version: 0.0.1-alpha.7
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider resolves secret references in your Gestalt configuration against
Azure Key Vault. At runtime, `gestaltd` fetches the secret values using the
Azure SDK default credential chain (environment variables, managed identity,
Azure CLI, etc.).

Implemented in Go.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
