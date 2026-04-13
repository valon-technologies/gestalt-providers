# Google Forms

Read and manage Google Forms and responses.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  google_forms:
    source: github.com/valon-technologies/gestalt-providers/plugins/google_forms
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Google Forms OpenAPI specification. Exposes
operations for getting, creating, and updating forms, and listing and getting
form responses.

Authenticates with Google OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
