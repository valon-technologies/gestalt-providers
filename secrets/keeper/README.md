# Keeper Secrets Manager

Resolves secrets from Keeper Secrets Manager.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
secrets:
  keeper:
    source: github.com/valon-technologies/gestalt-providers/secrets/keeper
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider resolves secret references in your Gestalt configuration against
Keeper Secrets Manager. Secrets can be referenced by record UID (using the
configured default field) or by full Keeper notation for specific field access.
Authentication uses a pre-generated KSM configuration.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
