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

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
