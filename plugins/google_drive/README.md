# Google Drive

Read, create, update, and share files in Google Drive.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/plugins/google_drive` |
| **Version** | `0.0.1-alpha.8` |
| **Category** | Plugin |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  google_drive:
    source: github.com/valon-technologies/gestalt-providers/plugins/google_drive
    version: 0.0.1-alpha.8
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Google Drive OpenAPI specification. Exposes
operations for listing, getting, creating, updating, deleting, copying, and
exporting files, as well as managing permissions.

Authenticates with Google OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
