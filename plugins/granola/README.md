# Granola

Read meeting notes, summaries, attendees, and transcripts from Granola, and use
Granola's official MCP server.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  granola:
    source: github.com/valon-technologies/gestalt-providers/plugins/granola
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Dual-surface provider built on Granola's public REST API plus Granola's hosted
MCP endpoint. The REST surface exposes the note list and note detail endpoints,
including cursor-based pagination and optional transcript retrieval.

Authenticates with a Granola API key using HTTP Bearer auth. Generate a key in
the Granola desktop app under Settings > API. Granola documents personal API
keys for Business and Enterprise workspaces, plus enterprise-scoped keys for
workspace admins.

Use the default user connection with a personal key for user-scoped note
access. For automation or shared deployments, connect a workload or managed
identity under its own subject with an enterprise admin key so the plugin can
access Team-space notes across the workspace.

The MCP surface uses Granola's OAuth-protected hosted endpoint at
`https://mcp.granola.ai/mcp`, so MCP clients authenticate in-browser rather
than by reusing the REST API key connection.

Granola only returns notes that already have an AI summary and transcript.

## Documentation

- [Granola API Introduction](https://docs.granola.ai/introduction)
- `https://mcp.granola.ai/mcp`
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
