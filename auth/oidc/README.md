# OIDC

Authenticate users with an OpenID Connect provider.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/auth/oidc` |
| **Version** | `0.0.1-alpha.9` |
| **Category** | Auth |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
auth:
  oidc:
    source: github.com/valon-technologies/gestalt-providers/auth/oidc
    version: 0.0.1-alpha.9
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider integrates any standards-compliant OpenID Connect identity provider
as an authentication backend for Gestalt. Use it with Okta, Auth0, Azure AD,
Keycloak, or any other OIDC-compatible issuer.

Implemented in Go.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
