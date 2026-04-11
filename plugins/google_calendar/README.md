# Google Calendar

Read and manage Google calendars and events.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  google_calendar:
    source: github.com/valon-technologies/gestalt-providers/plugins/google_calendar
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Google Calendar OpenAPI specification. Exposes
operations for listing calendars, and listing, creating, updating, deleting, and
quick-adding events.

Authenticates with Google OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
