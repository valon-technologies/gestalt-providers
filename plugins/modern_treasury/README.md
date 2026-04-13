# Modern Treasury

Create payment orders, manage external accounts, and inspect treasury activity in Modern Treasury.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  modern_treasury:
    source: github.com/valon-technologies/gestalt-providers/plugins/modern_treasury
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on a local OpenAPI specification. Exposes operations
for counterparties, external and internal accounts, payment orders, transactions,
returns, incoming payments, balance reports, and routing number validation.
Supports cursor-based pagination.

Authenticates with Organization ID and API key (HTTP Basic).

## Connections

The manifest now exposes both a per-user `default` connection and an additive
`identity` connection for shared deployment credentials. Workloads should bind
to `identity`; existing user flows continue to use `default`.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
