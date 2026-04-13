# GitHub

Repository, issue, pull request, workflow, and code search operations.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  github:
    source: github.com/valon-technologies/gestalt-providers/plugins/github
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider with both an OpenAPI surface and a GraphQL surface. The
OpenAPI surface exposes GitHub's REST API for repositories, issues, pull
requests, workflows, and code search. The GraphQL surface provides access to
GitHub's GraphQL API.

Authenticates with GitHub OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
