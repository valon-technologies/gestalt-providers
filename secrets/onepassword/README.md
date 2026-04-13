# 1Password

Resolves secrets from 1Password.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
secrets:
  onepassword:
    source: github.com/valon-technologies/gestalt-providers/secrets/onepassword
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider resolves secret references in your Gestalt configuration against
1Password using Service Account authentication. At runtime, `gestaltd` resolves
secrets using the 1Password SDK, constructing `op://` references from the
configured vault and field defaults.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
