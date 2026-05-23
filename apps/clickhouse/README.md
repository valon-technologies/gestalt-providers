# ClickHouse

Query and manage analytical databases.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  clickhouse:
    source: github.com/valon-technologies/gestalt-providers/plugins/clickhouse
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider that connects to ClickHouse Cloud through its
[MCP](https://modelcontextprotocol.io/) surface. No additional authentication
configuration is required beyond the MCP connection URL.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  clickhouse:
    source: github.com/valon-technologies/gestalt-providers/plugins/clickhouse
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `MCP` uses MCP OAuth; mode `user`.

Operation surfaces: MCP.

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: clickhouse
        operation: <operation-id>
```

Example `MCP tools` call:

Use the hosted ClickHouse MCP surface after completing MCP OAuth.

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
