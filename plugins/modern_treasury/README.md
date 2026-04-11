# Modern Treasury

Create payment orders, manage external accounts, and inspect treasury activity in Modern Treasury.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/plugins/modern_treasury` |
| **Version** | `0.0.1-alpha.9` |
| **Category** | Plugin |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  modern_treasury:
    source: github.com/valon-technologies/gestalt-providers/plugins/modern_treasury
    version: 0.0.1-alpha.9
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on a local OpenAPI specification. Exposes operations
for counterparties, external and internal accounts, payment orders, transactions,
returns, incoming payments, balance reports, and routing number validation.
Supports cursor-based pagination.

Authenticates with Organization ID and API key (HTTP Basic).

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
