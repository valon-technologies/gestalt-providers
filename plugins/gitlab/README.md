# GitLab

Repository, issue, merge request, and pipeline operations.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  gitlab:
    source: github.com/valon-technologies/gestalt-providers/plugins/gitlab
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider with both an OpenAPI surface and a GraphQL surface. The
OpenAPI surface exposes operations for managing GitLab repositories, issues,
merge requests, and pipelines. The GraphQL surface provides access to GitLab's
GraphQL API.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  gitlab:
    source: github.com/valon-technologies/gestalt-providers/plugins/gitlab
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses OAuth 2.0; mode `user`.

Operation surfaces: OpenAPI, GraphQL.

Representative operations include:

- `getProject`
- `list_projects`
- `get_project`
- `list_merge_requests`
- `create_merge_request`
- `get_merge_request`
- `list_issues`
- `create_issue`
- `get_issue`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: gitlab
        operation: getProject
```

Example `getProject` call:

```ts
await invoker.invoke("gitlab", "getProject", { id: "group%2Fproject" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
