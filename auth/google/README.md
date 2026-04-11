# Google

Authenticate users with Google OAuth and validate Google bearer tokens.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/auth/google` |
| **Version** | `0.0.1-alpha.10` |
| **Category** | Auth |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
auth:
  google:
    source: github.com/valon-technologies/gestalt-providers/auth/google
    version: 0.0.1-alpha.10
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider integrates Google as an authentication backend for Gestalt. It
supports Google OAuth 2.0 login flows and validation of Google-issued bearer
tokens, making it suitable for organizations that use Google Workspace for
identity.

Implemented in Go.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
