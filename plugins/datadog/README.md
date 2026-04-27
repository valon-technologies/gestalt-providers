# Datadog

Manage dashboards, monitors, incidents, logs, RUM, CI Visibility, and Synthetics.

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
for managing Datadog dashboards, monitors, incidents, log queries, Real User
Monitoring (RUM) data, CI Visibility pipeline events, and Synthetic tests.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
