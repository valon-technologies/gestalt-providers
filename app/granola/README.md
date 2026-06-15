# Granola

Read meeting notes, summaries, attendees, and transcripts from Granola, and use
Granola's official MCP server.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  granola:
    source: github.com/valon-technologies/gestalt-providers/app/granola
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
access. For shared deployments, prefer the identity connection with an
enterprise admin key so the plugin can access Team-space notes across the
workspace.

The MCP surface uses Granola's OAuth-protected hosted endpoint at
`https://mcp.granola.ai/mcp`, so MCP clients authenticate in-browser rather
than by reusing the REST API key connection.

Granola only returns notes that already have an AI summary and transcript.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  granola:
    source: github.com/valon-technologies/gestalt-providers/app/granola
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses bearer token; mode `subject`.
  - Credential fields: `token`.
  - `token`: For user-scoped access, personal keys only expose notes the key owner can access. Create a key in the Granola desktop app under Settings > API.
- `MCP` uses MCP OAuth; mode `subject`.

Operation surfaces: OpenAPI, MCP.

Representative operations include:

- `listNotes`
- `getNote`

- Personal API keys expose only notes the key owner can access; MCP uses Granola OAuth.

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `listNotes` call:

```ts
await app.invoke("granola", "listNotes", { page_size: 10 });
```

Example `getNote` call:

```ts
await app.invoke("granola", "getNote", { id: "note-id", include: "transcript" });
```

## Documentation
- [Granola API Introduction](https://docs.granola.ai/introduction)
- `https://mcp.granola.ai/mcp`
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
