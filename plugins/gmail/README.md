# Gmail

Read, send, and manage Gmail messages, threads, drafts, and labels.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  gmail:
    source: github.com/valon-technologies/gestalt-providers/plugins/gmail
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Source-backed provider implemented in Python with an OpenAPI surface. Exposes
operations for listing, reading, trashing, and modifying messages, managing
labels, reading threads, and retrieving the user's profile.

Authenticates with Google OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
