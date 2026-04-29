# Gong

Retrieve Gong calls, transcripts, users, and data-privacy exports.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  gong:
    source: github.com/valon-technologies/gestalt-providers/plugins/gong
    version: ...
```

The default connection uses Gong API key credentials as HTTP Basic auth:

- `access_key_id` maps to the Basic auth username.
- `secret_key` maps to the Basic auth password.

The OpenAPI surface defaults to `https://api.gong.io`. Override
`plugins.gong.surfaces.openapi.baseUrl` when using a tenant-specific Gong API
base URL.

Deployments that use Gong OAuth instead of API-key credentials can override the
default connection auth:

```yaml
plugins:
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

Declarative OpenAPI-backed provider exposing common Gong public API operations:

- get a single call
- search calls
- retrieve call transcripts
- list users
- retrieve data references for an email address

## Documentation

- [Gong API access](https://help.gong.io/docs/receive-access-to-the-api)
- [Gong API capabilities](https://help.gong.io/docs/what-the-gong-api-provides)
- [Gong OAuth setup](https://help.gong.io/docs/create-an-app-for-gong)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
