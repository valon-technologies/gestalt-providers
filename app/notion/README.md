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

REST operations authenticate with Notion OAuth (the OpenAPI surface default),
with a manually supplied internal integration secret, or with a personal access
token (PAT) selected via deployment config. MCP tools authenticate with Notion
MCP OAuth.

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

- `OAuth` uses OAuth 2.0; mode `subject`.
- `ApiKey` uses manual credentials; mode `subject`.
  - Credential fields: `token`.
  - `token`: Create an internal integration at [notion.so/profile/integrations](https://www.notion.so/profile/integrations) and copy its secret, then share the relevant pages or databases with the integration
- `PAT` uses bearer auth; mode `subject`.
  - Credential fields: `token`.
  - `token`: Create a PAT in the [Notion Developer portal](https://www.notion.so/developers/tokens) with the Notion API capability; the token acts as you (uses your workspace membership and page permissions, no bot page-sharing), expires one year after creation, and cannot call `users.list` (see [PAT permissions](https://developers.notion.com/guides/get-started/personal-access-tokens#permissions-and-content-access)). Select it via deployment config (`defaultConnection: PAT` or a surface connection override).
- `MCP` uses MCP OAuth; mode `subject`.

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

- REST operations default to the Notion REST OAuth connection and managed `Notion-Version` header; the `ApiKey` connection accepts an internal integration secret as a bearer token instead, and the `PAT` connection accepts a personal access token (opt-in via `defaultConnection: PAT` or a surface connection override). MCP tools use the separate MCP OAuth connection, which has no API key or PAT alternative.

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `search` call:

```ts
await app.invoke("notion", "search", { query: "Roadmap" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
