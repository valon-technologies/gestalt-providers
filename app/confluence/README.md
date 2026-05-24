# Confluence Cloud

Atlassian Confluence Cloud pages, spaces, and content search.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  confluence:
    source: github.com/valon-technologies/gestalt-providers/app/confluence
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

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  confluence:
    source: github.com/valon-technologies/gestalt-providers/app/confluence
    version: ...
    connections:
      default:
        params:
          cloud_id: "..."
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `read:me`, `read:space:confluence`, `read:page:confluence`, `search:confluence`, `write:page:confluence`, `offline_access`.
  - Connection params:
    - `cloud_id` (required): Confluence Cloud site identifier (discovered automatically)

Operation surfaces: REST.

Representative operations include:

- `search`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - plugin: confluence
        operation: search
```

Example `search` call:

```ts
await app.invoke("confluence", "search", { cql: "type=page and text~\"roadmap\"", limit: 10 });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
