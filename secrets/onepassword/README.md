# 1Password

Resolves secrets from 1Password Connect.

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
a 1Password Connect Server. At runtime, `gestaltd` looks up items by name in
the configured vault and extracts the specified field value. Secret names can
include a field suffix (e.g. `my-item/password`) to override the default field.
Set `host`, `token`, and `vault` in the provider config. The host must use
HTTPS by default; `allowInsecureHttp: true` is only supported for localhost or
loopback development endpoints.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
