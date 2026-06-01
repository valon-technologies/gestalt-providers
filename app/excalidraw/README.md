# Excalidraw+

Gestalt app for [Excalidraw+](https://plus.excalidraw.com) hosted MCP. Agents can read, search, and edit workspace scenes using Excalidraw-aware tools.

## Endpoint

```text
https://api.excalidraw.com/api/v1/mcp
```

## Authentication

MCP uses the same API keys as the [Excalidraw public API](https://plus.excalidraw.com/docs/api/authentication). Create a key in [Excalidraw Plus](https://app.excalidraw.com/account/api-keys) and connect it as a bearer token.

- **Personal keys** — best when an agent should act with your own access.
- **Workspace keys** — better for shared team integrations.

See [Personal vs Workspace MCP/API Keys](https://plus.excalidraw.com/docs/mcp/mcp-api-key-types) for details.

Tool access follows key permissions (read-only keys expose read-only MCP tools).

## Public beta

Excalidraw+ MCP and API are in public beta; tool names and schemas may change.

## Not the OSS MCP

This targets Excalidraw+ (`api.excalidraw.com`). The separate open-source server at [mcp.excalidraw.com](https://mcp.excalidraw.com) is a different product.

## Configuration

```yaml
app:
  excalidraw:
    source: github.com/valon-technologies/gestalt-providers/app/excalidraw
    version: 0.0.1-alpha.1
```

See [Getting Started](https://gestaltd.ai/getting-started) and [Configuration](https://gestaltd.ai/configuration).
