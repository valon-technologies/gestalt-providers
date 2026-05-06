# Linear

Manage issues, projects, and teams.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  linear:
    source: github.com/valon-technologies/gestalt-providers/plugins/linear
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider with both a GraphQL surface and an
[MCP](https://modelcontextprotocol.io/) surface. Exposes Linear's full API for
managing issues, projects, teams, cycles, and more.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  linear:
    source: github.com/valon-technologies/gestalt-providers/plugins/linear
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `OAuth` uses OAuth 2.0; mode `user`.
  - Requested scopes: `read`, `write`.

Operation surfaces: GraphQL, MCP.

Representative operations include:

- `issueCreate`

- GraphQL and hosted MCP surfaces both use the Linear OAuth connection with `read` and `write` scopes.

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: linear
        operation: issueCreate
```

Example `issueCreate` call:

```ts
await invoker.invoke("linear", "issueCreate", { input: { teamId: "team-id", title: "Follow up" } });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
