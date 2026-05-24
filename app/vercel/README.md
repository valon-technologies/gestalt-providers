# Vercel

Deploy, manage, and monitor Vercel projects and deployments, and read or write
Vercel Blob storage.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  vercel:
    source: github.com/valon-technologies/gestalt-providers/app/vercel
    version: ...
    config:
      clientId: ${VERCEL_CLIENT_ID}
      clientSecret: ${VERCEL_CLIENT_SECRET}
      blobReadWriteToken: ${VERCEL_BLOB_READ_WRITE_TOKEN} # optional, only needed for blob.* operations
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Hybrid provider:
- OpenAPI-backed operations for the Vercel platform API
- Executable `blob.*` operations that mirror the Vercel Blob SDK methods

Blob operations currently include:
- `blob.put`
- `blob.get`
- `blob.head`
- `blob.list`
- `blob.delete`
- `blob.copy`

The OpenAPI surface authenticates with Vercel OAuth 2.0. The executable
`blob.*` operations use the configured `blobReadWriteToken`.

Example Blob write:

```yaml
apps:
  roadmapPublisher:
    invokes:
      - plugin: vercel
        operation: blob.put
```

```ts
await invoker.invoke("vercel", "blob.put", {
  pathname: "roadmaps/newrez.json",
  access: "private",
  body: JSON.stringify(payload),
  content_type: "application/json",
  overwrite: true,
});
```

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  vercel:
    source: github.com/valon-technologies/gestalt-providers/app/vercel
    version: ...
    config:
      clientId: ${VERCEL_CLIENT_ID}
      clientSecret: ${VERCEL_CLIENT_SECRET}
      blobReadWriteToken: ${VERCEL_BLOB_READ_WRITE_TOKEN}
```

Provider config fields:

- `clientId` (required): Vercel OAuth client ID.
- `clientSecret` (required): Vercel OAuth client secret.
- `blobReadWriteToken` (optional): Optional Vercel Blob read-write token used by the executable `blob.*` operations.

Connections and authentication:

- `default` uses OAuth 2.0.

Operation surfaces: OpenAPI.

Representative operations include:

- `blob.put`
- `teamMembers.invite`
- `blob.get`
- `blob.head`
- `blob.list`
- `blob.delete`
- `blob.copy`

- `blob.*` operations require `blobReadWriteToken`; platform API operations use Vercel OAuth.

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - plugin: vercel
        operation: blob.put
```

Example `blob.put` call:

```ts
await invoker.invoke("vercel", "blob.put", {
  pathname: "reports/daily.json",
  access: "private",
  body: JSON.stringify(payload),
  content_type: "application/json",
  overwrite: true,
});
```

Example `teamMembers.invite` call:

```ts
await invoker.invoke("vercel", "teamMembers.invite", { team_id: "team_123", email: "teammate@example.com", role: "MEMBER" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
