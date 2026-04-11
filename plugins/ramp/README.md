# Ramp

Manage corporate cards, transactions, and reimbursements.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/plugins/ramp` |
| **Version** | `0.0.1-alpha.8` |
| **Category** | Plugin |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  ramp:
    source: github.com/valon-technologies/gestalt-providers/plugins/ramp
    version: 0.0.1-alpha.8
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Ramp Developer API OpenAPI specification.
Exposes operations for managing corporate cards, transactions, users,
departments, reimbursements, receipts, limits, and spend programs.

Authenticates with Ramp OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
