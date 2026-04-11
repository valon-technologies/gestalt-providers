# Hex

Manage Hex projects, runs, and cells.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/plugins/hex` |
| **Version** | `0.0.1-alpha.8` |
| **Category** | Plugin |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  hex:
    source: github.com/valon-technologies/gestalt-providers/plugins/hex
    version: 0.0.1-alpha.8
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Hex OpenAPI specification. Exposes operations
for managing Hex projects, triggering and monitoring runs, and working with
notebook cells.

Authenticates with a manually provided API token.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
