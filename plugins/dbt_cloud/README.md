# dbt Cloud

Manage accounts, projects, jobs, and runs.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/plugins/dbt_cloud` |
| **Version** | `0.0.1-alpha.8` |
| **Category** | Plugin |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  dbt_cloud:
    source: github.com/valon-technologies/gestalt-providers/plugins/dbt_cloud
    version: 0.0.1-alpha.8
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the dbt Cloud OpenAPI specification. Exposes
operations for managing dbt Cloud accounts, projects, jobs, and runs.

Authenticates with a manually provided API token.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
