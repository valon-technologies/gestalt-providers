# Google Calendar

Read and manage Google calendars and events.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/plugins/google_calendar` |
| **Version** | `0.0.1-alpha.10` |
| **Category** | Plugin |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  google_calendar:
    source: github.com/valon-technologies/gestalt-providers/plugins/google_calendar
    version: 0.0.1-alpha.10
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
