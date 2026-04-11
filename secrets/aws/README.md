# AWS Secrets Manager

Resolves secrets from AWS Secrets Manager.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
secrets:
  aws:
    source: github.com/valon-technologies/gestalt-providers/secrets/aws
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider resolves secret references in your Gestalt configuration against
AWS Secrets Manager. At runtime, `gestaltd` fetches the secret values using
the standard AWS credential chain (environment variables, shared credentials
file, IAM role, etc.).

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
