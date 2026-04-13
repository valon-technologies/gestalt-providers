# Vercel

Deploy, manage, and monitor Vercel projects and deployments.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  vercel:
    source: github.com/valon-technologies/gestalt-providers/plugins/vercel
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Vercel OpenAPI specification. Exposes
operations for managing projects, deployments, domains, and environment
variables.

Authenticates with Vercel OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
