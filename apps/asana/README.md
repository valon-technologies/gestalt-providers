# Asana

Manage tasks, projects, and workspaces.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  asana:
    source: github.com/valon-technologies/gestalt-providers/apps/asana
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Asana OpenAPI specification. Exposes
operations for managing tasks, projects, workspaces, and teams.

Authenticates with Asana OAuth 2.0.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  asana:
    source: github.com/valon-technologies/gestalt-providers/apps/asana
    version: ...
    config:
      clientId: ${ASANA_CLIENT_ID}
      clientSecret: ${ASANA_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Asana OAuth client ID.
- `clientSecret` (required): Asana OAuth client secret.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `default`.

Operation surfaces: OpenAPI.

Representative operations include:

- `getTasks`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: asana
        operation: getTasks
```

Example `getTasks` call:

```ts
await invoker.invoke("asana", "getTasks", { project: "project-id", limit: 20 });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
