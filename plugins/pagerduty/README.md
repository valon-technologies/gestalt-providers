# PagerDuty

Manage incidents, services, and on-call schedules.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  pagerduty:
    source: github.com/valon-technologies/gestalt-providers/plugins/pagerduty
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the PagerDuty OpenAPI specification. Exposes
operations for managing incidents, services, and on-call schedules.

Authenticates with PagerDuty OAuth 2.0 (PKCE).

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
