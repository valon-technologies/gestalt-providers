# Vercel

Deploy, manage, and monitor Vercel projects and deployments, and read or write
Vercel Blob storage.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  vercel:
    source: github.com/valon-technologies/gestalt-providers/plugins/vercel
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
plugins:
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

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
