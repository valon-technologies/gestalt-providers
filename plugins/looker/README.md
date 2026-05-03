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

Declarative provider built from Looker's official OpenAPI 3.0 API 4.0
specification, referenced from a pinned
[`looker-open-source/sdk-codegen`](https://github.com/looker-open-source/sdk-codegen)
artifact. The manifest overrides the spec server URL to use a connection-scoped
Looker instance host:

```yaml
plugins:
  looker:
    connections:
      default:
        params:
          host: company.cloud.looker.com
```

## Authentication

Looker API requests use an authorization header in this exact form:

```text
Authorization: token <access_token>
```

Create API credentials for a Looker user in the Looker Admin console, then call
`POST /api/4.0/login` with the client ID and client secret to obtain the
short-lived `access_token`. Store the full header value, including the `token`
prefix, in the `authorization` credential:

```yaml
plugins:
  looker:
    connections:
      default:
        auth:
          type: manual
          credentials:
            - name: authorization
              label: Authorization header
              description: Full Looker authorization header value, for example `token <access_token>`.
```

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
- [Looker API overview](https://cloud.google.com/looker/docs/api-overview)
- [Looker API 4.0 reference](https://cloud.google.com/looker/docs/reference/looker-api/latest)
- [Looker SDK Codegen](https://github.com/looker-open-source/sdk-codegen)
