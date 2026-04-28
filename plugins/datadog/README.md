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
for managing Datadog dashboards, monitors, metric metadata, metric tag
configurations, incidents, log queries, Real User Monitoring (RUM) data,
CI Visibility pipeline events, and Synthetic tests.

Metric configuration operations:

```bash
gestalt invoke datadog get_metric_metadata \
  -p metric_name=gestaltd.operation.duration

gestalt invoke datadog update_metric_metadata \
  -p metric_name=gestaltd.operation.duration \
  -p unit=second \
  -p 'description=Measures gestaltd operation invocation duration.'

gestalt invoke datadog create_metric_tag_configuration \
  -p metric_name=gestaltd.operation.duration \
  -p 'data:={"type":"manage_tags","id":"gestaltd.operation.duration","attributes":{"metric_type":"distribution","tags":["env","service","gestalt.provider","gestalt.operation"],"include_percentiles":true}}'

gestalt invoke datadog update_metric_tag_configuration \
  -p metric_name=gestaltd.operation.duration \
  -p 'data:={"type":"manage_tags","id":"gestaltd.operation.duration","attributes":{"tags":["env","service","gestalt.provider","gestalt.operation"],"include_percentiles":true}}'
```

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
