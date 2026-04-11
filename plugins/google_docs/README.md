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

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
