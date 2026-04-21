# OIDC

Authenticate users with an OpenID Connect provider.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
server:
  providers:
    authentication: oidc
providers:
  authentication:
    oidc:
      source: github.com/valon-technologies/gestalt-providers/auth/oidc
      version: ...
      config:
        issuerUrl: https://login.example.com
        clientId: ${OIDC_CLIENT_ID}
        clientSecret: secret://oidc-client-secret
        allowedDomains:
          - example.com
        scopes:
          - openid
          - email
          - profile
        sessionTtl: 24h
        pkce: true
        displayName: Example SSO
        allowInsecureHttp: false
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Overview

This provider integrates any standards-compliant OpenID Connect identity provider
as an authentication provider for Gestalt. Use it with Okta, Auth0, Azure AD,
Keycloak, or any other OIDC-compatible issuer.

`issuerUrl` must use `https://` by default. Set `allowInsecureHttp: true` only
for local development against loopback issuers such as `http://127.0.0.1:8080`
or `http://localhost:8080`. The same opt-in applies to any endpoints returned by
OIDC discovery, and non-loopback `http://` endpoints are always rejected.

PKCE verifier state is stored server-side with bounded retention. By default,
entries live for `1h` and the cache holds up to `10000` in-flight login attempts.
Tune those values only if your identity provider requires longer user interaction
windows or materially higher concurrent login volume.

## Config Interface

```go
type config struct {
    IssuerURL           string        `yaml:"issuerUrl"`
    ClientID            string        `yaml:"clientId"`
    ClientSecret        string        `yaml:"clientSecret"`
    RedirectURL         string        `yaml:"redirectUrl"`
    AllowedDomains      []string      `yaml:"allowedDomains"`
    Scopes              []string      `yaml:"scopes"`
    SessionTTL          time.Duration `yaml:"sessionTtl"`
    PKCE                bool          `yaml:"pkce"`
    DisplayName         string        `yaml:"displayName"`
    AllowInsecureHTTP   bool          `yaml:"allowInsecureHttp"`
    PKCEVerifierTTL     time.Duration `yaml:"pkceVerifierTtl"`
    PKCEVerifierMaxItems int          `yaml:"pkceVerifierMaxItems"`
}
```

`pkceVerifierTtl` and `pkceVerifierMaxItems` are optional. When set, they must
both be greater than zero.

## Examples

Use the default production-safe cache limits:

```yaml
server:
  providers:
    authentication: oidc
providers:
  authentication:
    oidc:
      source: github.com/valon-technologies/gestalt-providers/auth/oidc
      version: ...
      config:
        issuerUrl: https://login.example.com
        clientId: ${OIDC_CLIENT_ID}
        clientSecret: ${OIDC_CLIENT_SECRET}
        allowedDomains:
          - example.com
        scopes:
          - openid
          - email
          - profile
        sessionTtl: 24h
        pkce: true
        displayName: Example SSO
        allowInsecureHttp: false
```

Tune the PKCE cache for a slower MFA flow or higher login concurrency:

```yaml
server:
  providers:
    authentication: oidc
providers:
  authentication:
    oidc:
      source: github.com/valon-technologies/gestalt-providers/auth/oidc
      version: ...
      config:
        issuerUrl: https://login.example.com
        clientId: ${OIDC_CLIENT_ID}
        clientSecret: ${OIDC_CLIENT_SECRET}
        pkce: true
        pkceVerifierTtl: 90m
        pkceVerifierMaxItems: 20000
```

## Local Development

Use the insecure HTTP escape hatch only when the issuer and discovered endpoints
are bound to loopback for local testing:

```yaml
server:
  providers:
    authentication: oidc
providers:
  authentication:
    oidc:
      source: github.com/valon-technologies/gestalt-providers/auth/oidc
      version: ...
      config:
        issuerUrl: http://127.0.0.1:8080/realms/dev
        clientId: local-dev-client
        clientSecret: local-dev-secret
        pkce: true
        allowInsecureHttp: true
```

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
