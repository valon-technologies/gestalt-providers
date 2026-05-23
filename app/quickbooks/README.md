# QuickBooks Online

Manage customers, invoices, bills, payments, accounts, and reports in QuickBooks Online.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  quickbooks:
    source: github.com/valon-technologies/gestalt-providers/app/quickbooks
    version: ...
    config:
      clientId: ${QUICKBOOKS_CLIENT_ID}
      clientSecret: ${QUICKBOOKS_CLIENT_SECRET}
    connections:
      default:
        params:
          host: sandbox-quickbooks.api.intuit.com
          realm_id: "1234567890"
```

Provider config fields:

- `clientId` (required): QuickBooks OAuth client ID.
- `clientSecret` (required): QuickBooks OAuth client secret.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `com.intuit.quickbooks.accounting`.
  - Connection params:
    - `host` (optional): API host. Use sandbox-quickbooks.api.intuit.com for sandbox companies.
    - `realm_id` (required): QuickBooks Company ID (Realm ID). Found in your QuickBooks URL after /app/ or returned during OAuth authorization.

Operation surfaces: OpenAPI.

Representative operations include:

- `query`
- `get_preferences`
- `get_customer`
- `save_customer`
- `get_invoice`
- `save_invoice`
- `send_invoice`
- `get_payment`
- `save_payment`

- Set `realm_id` to the QuickBooks company ID returned during OAuth or visible in the QuickBooks URL.
- Use `host: sandbox-quickbooks.api.intuit.com` for sandbox companies; omit it for the production host default.

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - plugin: quickbooks
        operation: query
```

Example `query` call:

```ts
await invoker.invoke("quickbooks", "query", {
  query: "select * from Customer maxresults 10",
  minorversion: "75",
});
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
