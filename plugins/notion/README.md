# Notion

Current Notion REST operations plus the official Notion MCP tool surface.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/plugins/notion` |
| **Version** | `0.0.1-alpha.9` |
| **Category** | Plugin |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  notion:
    source: github.com/valon-technologies/gestalt-providers/plugins/notion
    version: 0.0.1-alpha.9
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider with both an OpenAPI surface and an
[MCP](https://modelcontextprotocol.io/) surface. The OpenAPI surface exposes
Notion REST API operations, while the MCP surface connects to the official
Notion MCP server for tool-based interactions.

Authenticates with MCP OAuth.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
