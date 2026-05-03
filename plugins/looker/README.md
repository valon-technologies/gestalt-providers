# Looker

Run queries, manage dashboards, looks, folders, users, groups, schedules,
projects, content validation, and instance configuration in Looker.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  looker:
    source: github.com/valon-technologies/gestalt-providers/plugins/looker
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Runtime provider built from Looker's official OpenAPI 3.0 API 4.0
specification, referenced from a pinned
[`looker-open-source/sdk-codegen`](https://github.com/looker-open-source/sdk-codegen)
artifact. The provider uses the spec to generate its operation catalog and to
proxy requests to a connection-scoped Looker instance host. The raw Looker
`/login` and `/logout` endpoints are not exposed as operations because the
provider owns that exchange for connection authentication.

```yaml
plugins:
  looker:
    connections:
      default:
        params:
          host: company.cloud.looker.com
```

## Authentication

Create API credentials for a Looker user or service account in the Looker Admin
console. Connect the provider with the API credential's `client_id` and
`client_secret`; the plugin sends them to `POST /api/4.0/login` in the request
body, caches the short-lived `access_token`, and sends API requests with
Looker's required `Authorization: token <access_token>` header.

```yaml
plugins:
  looker:
    connections:
      default:
        auth:
          type: manual
          credentials:
            - name: client_id
              label: Client ID
            - name: client_secret
              label: Client Secret
```

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
- [Looker API overview](https://cloud.google.com/looker/docs/api-overview)
- [Looker API 4.0 reference](https://cloud.google.com/looker/docs/reference/looker-api/latest)
- [Looker SDK Codegen](https://github.com/looker-open-source/sdk-codegen)
