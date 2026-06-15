# Google Docs

Read, create, and edit Google Docs documents.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  google_docs:
    source: github.com/valon-technologies/gestalt-providers/app/google_docs
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
apps:
  google_docs:
    source: github.com/valon-technologies/gestalt-providers/app/google_docs
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

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `get` call:

```ts
await app.invoke("google_docs", "get", { documentId: "document-id" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
