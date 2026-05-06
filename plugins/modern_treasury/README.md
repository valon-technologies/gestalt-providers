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

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  modern_treasury:
    source: github.com/valon-technologies/gestalt-providers/plugins/modern_treasury
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials; mode `user`.
  - Credential fields: `organization_id`, `api_key`.
  - `organization_id`: Find this in Modern Treasury under Developers > API Keys.
  - `api_key`: Create a key in [Settings > API keys](https://app.moderntreasury.com/settings/api_keys).

Operation surfaces: OpenAPI.

Representative operations include:

- `listPaymentOrders`
- `listCounterparties`
- `getCounterparty`
- `createCounterparty`
- `updateCounterparty`
- `listExternalAccounts`
- `getExternalAccount`
- `createExternalAccount`
- `updateExternalAccount`
- `listInternalAccounts`

- The manual connection maps `organization_id` and `api_key` to Modern Treasury Basic auth.

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: modern_treasury
        operation: listPaymentOrders
```

Example `listPaymentOrders` call:

```ts
await invoker.invoke("modern_treasury", "listPaymentOrders", { per_page: 20 });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
