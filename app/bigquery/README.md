# BigQuery

Google BigQuery data warehouse.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  bigquery:
    source: github.com/valon-technologies/gestalt-providers/app/bigquery
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Provider with Google BigQuery operations. Exposes operations for running
bounded SQL queries, listing datasets, tables, and routines, and retrieving
metadata via the BigQuery REST API.

Authenticates with Google OAuth 2.0.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  bigquery:
    source: github.com/valon-technologies/gestalt-providers/app/bigquery
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `https://www.googleapis.com/auth/bigquery`.

Operation surfaces: REST.

Representative operations include:

- `query`

- The `query` operation returns a bounded result set with `schema`, `rows`, `total_rows`, and `job_complete`. Use `max_results` to keep agent-visible responses small.

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - plugin: bigquery
        operation: query
```

Example `query` call:

```ts
await invoker.invoke("bigquery", "query", {
  project_id: "analytics-prod",
  query: "select name from `dataset.table` limit 10",
  max_results: 10,
});
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
