# Figma

Access files, components, comments, and team projects.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  figma:
    source: github.com/valon-technologies/gestalt-providers/plugins/figma
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Figma REST API OpenAPI specification. Exposes
operations for accessing files, components, comments, team projects, library
assets, dev resources, and webhooks.

Authenticates with Figma OAuth 2.0 (PKCE).

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
