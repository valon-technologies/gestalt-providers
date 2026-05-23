# Gong

Access the Gong public API v2.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  gong:
    source: github.com/valon-technologies/gestalt-providers/app/gong
    version: ...
```

The default connection uses Gong API key credentials as HTTP Basic auth:

- `access_key_id` maps to the Basic auth username.
- `secret_key` maps to the Basic auth password.

Gong's manual API-key token is `Base64(<accessKey>:<accessKeySecret>)` and is
sent as `Authorization: Basic <token>`. OAuth uses a separate access token and
is sent as `Authorization: Bearer <token>`.

The OpenAPI surface defaults to `https://api.gong.io`. Override
`plugins.gong.surfaces.openapi.baseUrl` when using a tenant-specific Gong API
base URL.

Deployments that use Gong OAuth instead of API-key credentials can override the
default connection auth:

```yaml
apps:
  gong:
    connections:
      default:
        auth:
          type: oauth2
          authorizationUrl: https://app.gong.io/oauth2/authorize
          tokenUrl: https://app.gong.io/oauth2/generate-customer-token
          clientAuth: header
          clientId:
            secret:
              provider: secrets
              name: gong-client-id
          clientSecret:
            secret:
              provider: secrets
              name: gong-client-secret
```

## Capabilities

Declarative OpenAPI-backed provider generated from Gong's public API v2
specification. The provider exposes Gong operations for calls, users, stats,
settings, CRM, library, coaching, call outcomes, and data-privacy workflows.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  gong:
    source: github.com/valon-technologies/gestalt-providers/app/gong
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials; mode `user`.
  - Credential fields: `access_key_id`, `secret_key`.
  - `access_key_id`: Gong API access key ID. In Gong, this is the Basic auth username.
  - `secret_key`: Gong API access key secret. In Gong, this is the Basic auth password.

Operation surfaces: OpenAPI.

Representative operations include:

- `list_calls`
- `listCalls`
- `addCall`
- `addCallRecording`
- `get_call`
- `getPermissionProfile`
- `updatePermissionProfile`
- `createPermissionProfile`
- `updateMeeting`
- `deleteMeeting`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - plugin: gong
        operation: list_calls
```

Example `list_calls` call:

```ts
await invoker.invoke("gong", "list_calls", { fromDateTime: "2026-01-01T00:00:00Z", toDateTime: "2026-01-07T00:00:00Z" });
```

## Documentation
- [Gong API access](https://help.gong.io/docs/receive-access-to-the-api)
- [Gong API capabilities](https://help.gong.io/docs/what-the-gong-api-provides)
- [Gong OAuth setup](https://help.gong.io/docs/create-an-app-for-gong)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
