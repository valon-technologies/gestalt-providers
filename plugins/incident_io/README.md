# incident.io

Manage incidents, schedules, users, severities, and statuses.

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

Declarative provider built on the incident.io OpenAPI specification. Exposes
operations for listing, creating, and editing incidents, listing users and
schedules, and retrieving severities and incident statuses.

Authenticates with a manually provided API key.

## Connections

The manifest now exposes both a per-user `default` connection and an additive
`identity` connection for shared deployment credentials. Workloads should bind
to `identity`; existing user flows continue to use `default`.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
