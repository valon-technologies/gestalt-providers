# incident.io

Manage incidents, schedules, users, severities, statuses, and incident.io MCP
tools.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  incident_io:
    source: github.com/valon-technologies/gestalt-providers/app/incident_io
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

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  incident_io:
    source: github.com/valon-technologies/gestalt-providers/app/incident_io
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials; mode `subject`.
  - Credential fields: `token`.
  - `token`: Create one in [Settings → API Keys](https://app.incident.io/settings/api-keys)
- `MCP` uses MCP OAuth; mode `subject`.

Operation surfaces: OpenAPI, MCP.

Representative operations include:

- `Incidents.List`
- `Incidents.Show`
- `Users.List`
- `Users.Show`
- `Schedules.List`
- `Schedules.ListScheduleEntries`
- `Severities.List`
- `IncidentStatuses.List`
- `Incidents.Create`
- `Incidents.Edit`

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `Incidents.List` call:

```ts
await app.invoke("incident_io", "Incidents.List", { page_size: 25 });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
- [incident.io Remote MCP server](https://docs.incident.io/ai/remote-mcp)
