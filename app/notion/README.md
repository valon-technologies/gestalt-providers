# Notion

Current Notion REST operations plus the official Notion MCP tool surface.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  notion:
    source: github.com/valon-technologies/gestalt-providers/app/notion
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider with both an OpenAPI surface and an
[MCP](https://modelcontextprotocol.io/) surface. The OpenAPI surface exposes
Notion REST API operations, while the MCP surface connects to the official
Notion MCP server for tool-based interactions.

REST operations authenticate with Notion OAuth. MCP tools authenticate with
Notion MCP OAuth.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  notion:
    source: github.com/valon-technologies/gestalt-providers/app/notion
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `OAuth` uses OAuth 2.0; mode `user`.
- `MCP` uses MCP OAuth; mode `user`.

Managed request headers:

- `Notion-Version: 2026-03-11`

Operation surfaces: OpenAPI, MCP.

Representative operations include:

- `search`
- `pages.create`
- `pages.retrieve`
- `pages.update`
- `pages.properties.retrieve`
- `pages.retrieveMarkdown`
- `pages.updateMarkdown`
- `blocks.retrieve`

- REST operations use the Notion REST OAuth connection and managed `Notion-Version` header; MCP tools use the separate MCP OAuth connection.

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - plugin: notion
        operation: search
```

Example `search` call:

```ts
await invoker.invoke("notion", "search", { query: "Roadmap" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
