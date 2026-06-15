# PagerDuty

Manage incidents, services, and on-call schedules.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  pagerduty:
    source: github.com/valon-technologies/gestalt-providers/app/pagerduty
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the PagerDuty OpenAPI specification. Exposes
operations for managing incidents, services, and on-call schedules.

Authenticates with PagerDuty OAuth 2.0 (PKCE).

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  pagerduty:
    source: github.com/valon-technologies/gestalt-providers/app/pagerduty
    version: ...
    config:
      clientId: ${PAGERDUTY_CLIENT_ID}
      clientSecret: ${PAGERDUTY_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): PagerDuty OAuth client ID.
- `clientSecret` (required): PagerDuty OAuth client secret.

Connections and authentication:

- `default` uses OAuth 2.0.

Operation surfaces: OpenAPI.

Representative operations include:

- `listIncidents`

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `listIncidents` call:

```ts
await app.invoke("pagerduty", "listIncidents", { statuses: ["triggered", "acknowledged"], limit: 10 });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
