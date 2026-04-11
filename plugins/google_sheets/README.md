# Google Sheets

Read and update Google Sheets spreadsheets and values.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  google_sheets:
    source: github.com/valon-technologies/gestalt-providers/plugins/google_sheets
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Google Sheets OpenAPI specification. Exposes
operations for creating and getting spreadsheets, and reading, updating,
appending, batch-getting, batch-updating, and clearing cell values.

Authenticates with Google OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
