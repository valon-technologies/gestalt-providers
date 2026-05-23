# Google Drive

Read, create, update, and share files in Google Drive.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  google_drive:
    source: github.com/valon-technologies/gestalt-providers/plugins/google_drive
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Google Drive OpenAPI specification. Exposes
operations for listing, getting, creating, updating, deleting, copying, and
exporting files, as well as managing permissions.

Authenticates with Google OAuth 2.0.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  google_drive:
    source: github.com/valon-technologies/gestalt-providers/plugins/google_drive
    version: ...
    config:
      clientId: ${GOOGLE_DRIVE_CLIENT_ID}
      clientSecret: ${GOOGLE_DRIVE_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Google OAuth client ID for Google Drive.
- `clientSecret` (required): Google OAuth client secret for Google Drive.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `https://www.googleapis.com/auth/drive`.

Operation surfaces: OpenAPI.

Representative operations include:

- `files.list`
- `files.get`
- `files.create`
- `files.update`
- `files.delete`
- `files.copy`
- `files.export`
- `permissions.list`
- `permissions.create`
- `permissions.update`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: google_drive
        operation: files.list
```

Example `files.list` call:

```ts
await invoker.invoke("google_drive", "files.list", { pageSize: 10, q: "mimeType != 'application/vnd.google-apps.folder'" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
