# Ramp

Manage corporate cards, transactions, and reimbursements.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  ramp:
    source: github.com/valon-technologies/gestalt-providers/plugins/ramp
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

The OpenAPI surface authenticates with Ramp OAuth 2.0. The MCP surface is
exposed as a passive passthrough connection.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
