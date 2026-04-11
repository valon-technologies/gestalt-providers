# Datadog

Manage dashboards, monitors, incidents, logs, and RUM.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  datadog:
    source: github.com/valon-technologies/gestalt-providers/plugins/datadog
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on a local OpenAPI specification. Exposes operations
for managing Datadog dashboards, monitors, incidents, log queries, and Real User
Monitoring (RUM) data.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
