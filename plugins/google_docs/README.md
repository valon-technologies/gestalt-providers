# Google Docs

Read, create, and edit Google Docs documents.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  google_docs:
    source: github.com/valon-technologies/gestalt-providers/plugins/google_docs
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Google Docs OpenAPI specification. Exposes
operations for getting, creating, and batch-updating documents.

Authenticates with Google OAuth 2.0.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  google_docs:
    source: github.com/valon-technologies/gestalt-providers/plugins/google_docs
    version: ...
    config:
      clientId: ${GOOGLE_DOCS_CLIENT_ID}
      clientSecret: ${GOOGLE_DOCS_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Google OAuth client ID for Google Docs.
- `clientSecret` (required): Google OAuth client secret for Google Docs.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `https://www.googleapis.com/auth/documents`.

Operation surfaces: OpenAPI.

Representative operations include:

- `get`
- `get`
- `create`
- `batchUpdate`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: google_docs
        operation: get
```

Example `get` call:

```ts
await invoker.invoke("google_docs", "get", { documentId: "document-id" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
