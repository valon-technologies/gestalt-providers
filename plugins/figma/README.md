# Figma

Access files, components, comments, and team projects.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/plugins/figma` |
| **Version** | `0.0.1-alpha.8` |
| **Category** | Plugin |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  figma:
    source: github.com/valon-technologies/gestalt-providers/plugins/figma
    version: 0.0.1-alpha.8
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
