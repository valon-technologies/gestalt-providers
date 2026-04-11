# Confluence Cloud

Atlassian Confluence Cloud pages, spaces, and content search.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  confluence:
    source: github.com/valon-technologies/gestalt-providers/plugins/confluence
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative REST provider for Atlassian Confluence Cloud. Exposes operations for
listing and retrieving spaces and pages, searching content with CQL, and
creating and updating pages. The Confluence Cloud site identifier is discovered
automatically during the OAuth flow.

Authenticates with Atlassian OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
