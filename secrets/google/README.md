# Google Secret Manager

Resolves secrets from Google Cloud Secret Manager.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/secrets/google` |
| **Version** | `0.0.1-alpha.15` |
| **Category** | Secrets |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
secrets:
  google:
    source: github.com/valon-technologies/gestalt-providers/secrets/google
    version: 0.0.1-alpha.15
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider resolves secret references in your Gestalt configuration against
Google Cloud Secret Manager. At runtime, `gestaltd` fetches the secret values
using Application Default Credentials (ADC) or an explicit service account.

Implemented in Go.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
