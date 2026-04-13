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

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
