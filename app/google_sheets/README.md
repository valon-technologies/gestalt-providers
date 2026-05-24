# Google Sheets

Read and update Google Sheets spreadsheets and values.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  google_sheets:
    source: github.com/valon-technologies/gestalt-providers/app/google_sheets
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Google Sheets OpenAPI specification. Exposes
operations for creating and getting spreadsheets, and reading, updating,
appending, batch-getting, batch-updating, and clearing cell values.

Authenticates with Google OAuth 2.0.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  google_sheets:
    source: github.com/valon-technologies/gestalt-providers/app/google_sheets
    version: ...
    config:
      clientId: ${GOOGLE_SHEETS_CLIENT_ID}
      clientSecret: ${GOOGLE_SHEETS_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Google OAuth client ID for Google Sheets.
- `clientSecret` (required): Google OAuth client secret for Google Sheets.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `https://www.googleapis.com/auth/spreadsheets`.

Operation surfaces: OpenAPI.

Representative operations include:

- `values.get`
- `create`
- `get`
- `values.get`
- `values.update`
- `values.append`
- `values.batchGet`
- `values.batchUpdate`
- `values.clear`
- `batchUpdate`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - plugin: google_sheets
        operation: values.get
```

Example `values.get` call:

```ts
await app.invoke("google_sheets", "values.get", { spreadsheetId: "sheet-id", range: "Sheet1!A1:D10" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
