# Looker

Run queries, manage dashboards, looks, folders, users, groups, schedules,
projects, content validation, and instance configuration in Looker.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  looker:
    source: github.com/valon-technologies/gestalt-providers/app/looker
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
apps:
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

Create API credentials for a Looker user in the Looker Admin console. Gestalt
exchanges the client ID and client secret against `POST /api/4.0/login`, stores
the returned short-lived `access_token`, and automatically reissues it with the
same client credentials when it expires:

```yaml
apps:
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

The manifest sets Looker's required lowercase `token ` authorization prefix on
API requests.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  looker:
    source: github.com/valon-technologies/gestalt-providers/app/looker
    version: ...
    connections:
      default:
        params:
          host: "..."
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials; mode `user`.
  - Credential fields: `client_id`, `client_secret`.
  - `client_id`: Looker API client ID for a user with API access enabled.
  - `client_secret`: Looker API client secret for the same user.
  - Connection params:
    - `host` (required): Looker API host for your instance, without the scheme (for example, `company.cloud.looker.com` or `looker.example.com`).

Operation surfaces: OpenAPI.

Representative operations include:

- `search_dashboards`
- `create_query_task`
- `query_task_multi_results`
- `query_task`
- `query_task_results`
- `query`
- `query_for_slug`
- `create_query`
- `run_query`
- `run_inline_query`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - plugin: looker
        operation: search_dashboards
```

Example `search_dashboards` call:

```ts
await app.invoke("looker", "search_dashboards", { title: "Revenue" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
- [Looker API overview](https://cloud.google.com/looker/docs/api-overview)
- [Looker API 4.0 reference](https://cloud.google.com/looker/docs/reference/looker-api/latest)
- [Looker SDK Codegen](https://github.com/looker-open-source/sdk-codegen)
