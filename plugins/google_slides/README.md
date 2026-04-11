# Google Slides

Create and update Google Slides presentations.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  google_slides:
    source: github.com/valon-technologies/gestalt-providers/plugins/google_slides
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Google Slides OpenAPI specification. Exposes
operations for creating and getting presentations, batch-updating slides, and
retrieving page content and thumbnails.

Authenticates with Google OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
