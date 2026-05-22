# dbt Cloud

Manage accounts, projects, jobs, and runs.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  dbt_cloud:
    source: github.com/valon-technologies/gestalt-providers/apps/dbt_cloud
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the dbt Cloud OpenAPI specification. Exposes
operations for managing dbt Cloud accounts, projects, jobs, and runs.

Authenticates with a manually provided API token.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  dbt_cloud:
    source: github.com/valon-technologies/gestalt-providers/apps/dbt_cloud
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials.
  - Credential fields: `token`.
  - `token`: See [Profile settings](https://cloud.getdbt.com/settings/profile#api-tokens) to retrieve your token

Operation surfaces: OpenAPI.

Representative operations include:

- `listAccounts`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: dbt_cloud
        operation: listAccounts
```

Example `listAccounts` call:

```ts
await invoker.invoke("dbt_cloud", "listAccounts", {});
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
