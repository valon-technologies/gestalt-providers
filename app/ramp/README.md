# Ramp

Manage corporate cards, transactions, and reimbursements.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  ramp:
    source: github.com/valon-technologies/gestalt-providers/app/ramp
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider with both an OpenAPI surface and an
[MCP](https://modelcontextprotocol.io/) surface. The OpenAPI surface exposes
Ramp Developer API operations for managing corporate cards, transactions,
users, departments, reimbursements, receipts, limits, and spend programs. The
MCP surface is configured as a passthrough to Ramp's hosted MCP server at
`https://mcp.ramp.com/mcp`.

The OpenAPI surface authenticates with Ramp OAuth 2.0. The MCP surface
authenticates with Ramp MCP OAuth.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  ramp:
    source: github.com/valon-technologies/gestalt-providers/app/ramp
    version: ...
    config:
      clientId: ${RAMP_CLIENT_ID}
      clientSecret: ${RAMP_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Ramp OAuth client ID.
- `clientSecret` (required): Ramp OAuth client secret.

Connections and authentication:

- `oauth` uses OAuth 2.0; mode `user`.
  - Requested scopes: `transactions:read`, `cards:read`, `cards:write`, `users:read`, `users:write`, `departments:read`, `reimbursements:read`, `receipts:read`, `limits:read`, `spend_programs:read`, `spend_programs:write`.
- `mcp` uses MCP OAuth; mode `user`.

Operation surfaces: OpenAPI, MCP.

Representative operations include:

- `listCards`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - plugin: ramp
        operation: listCards
```

Example `listCards` call:

```ts
await app.invoke("ramp", "listCards", { page_size: 20 });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
