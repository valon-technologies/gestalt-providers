# incident.io

Manage incidents, schedules, users, severities, statuses, and incident.io MCP
tools.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  incident_io:
    source: github.com/valon-technologies/gestalt-providers/plugins/incident_io
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Dual-surface provider built on the incident.io OpenAPI specification plus
incident.io's hosted MCP endpoint. The OpenAPI surface exposes operations for
listing, creating, and editing incidents, listing users and schedules, and
retrieving severities and incident statuses.

The OpenAPI surface authenticates with a manually provided API key. Create one
in incident.io under Settings > API keys.

The MCP surface uses incident.io's hosted MCP endpoint at
`https://mcp.incident.io/mcp`, so MCP clients authenticate through incident.io
MCP OAuth rather than reusing the OpenAPI API key connection.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
- [incident.io Remote MCP server](https://docs.incident.io/ai/remote-mcp)
