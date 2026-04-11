# incident.io

Manage incidents, schedules, users, severities, and statuses.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/plugins/incident_io` |
| **Version** | `0.0.1-alpha.8` |
| **Category** | Plugin |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  incident_io:
    source: github.com/valon-technologies/gestalt-providers/plugins/incident_io
    version: 0.0.1-alpha.8
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the incident.io OpenAPI specification. Exposes
operations for listing, creating, and editing incidents, listing users and
schedules, and retrieving severities and incident statuses.

Authenticates with a manually provided API key.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
