# Google Admin Data Transfer

Transfer data between Google Workspace users via the [Admin SDK Data Transfer API](https://developers.google.com/admin-sdk/data-transfer).

## Authentication

OAuth 2.0 with scope: `admin.datatransfer`

## Operations

- **List application transfers** — `datatransfer.applicationTransfers.list`
- **List transfer applications** — `datatransfer.datatransfer.applications.list`
- **Get a transfer** — `datatransfer.transfers.get`
- **Insert (start) a transfer** — `datatransfer.transfers.insert`
- **List transfers** — `datatransfer.transfers.list`

## OpenAPI Spec

Remote: `https://api.apis.guru/v2/specs/googleapis.com/admin/datatransfer_v1/openapi.json`

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  google_admin_datatransfer:
    source: github.com/valon-technologies/gestalt-providers/plugins/google_admin_datatransfer
    version: ...
    config:
      clientId: ${GOOGLE_ADMIN_DATATRANSFER_CLIENT_ID}
      clientSecret: ${GOOGLE_ADMIN_DATATRANSFER_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Provider configuration value.
- `clientSecret` (required): Provider configuration value.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `https://www.googleapis.com/auth/admin.datatransfer`.

Operation surfaces: OpenAPI.

Representative operations include:

- `datatransfer.applications.list`
- `datatransfer.applicationTransfers.list`
- `datatransfer.datatransfer.applications.list`
- `datatransfer.transfers.get`
- `datatransfer.transfers.insert`
- `datatransfer.transfers.list`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: google_admin_datatransfer
        operation: datatransfer.applications.list
```

Example `datatransfer.applications.list` call:

```ts
await invoker.invoke("google_admin_datatransfer", "datatransfer.applications.list", {});
```
