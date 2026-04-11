# BigQuery

Google BigQuery data warehouse.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  bigquery:
    source: github.com/valon-technologies/gestalt-providers/plugins/bigquery
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Source-backed provider implemented in Python. Exposes operations for listing
datasets, tables, and routines, and for retrieving their metadata via the
BigQuery REST API.

Authenticates with Google OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
