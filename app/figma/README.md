# Figma

Access files, components, comments, and team projects.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  figma:
    source: github.com/valon-technologies/gestalt-providers/app/figma
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Figma REST API OpenAPI specification. Exposes
operations for accessing files, components, comments, team projects, library
assets, dev resources, and webhooks.

Authenticates with Figma OAuth 2.0 (PKCE).

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  figma:
    source: github.com/valon-technologies/gestalt-providers/app/figma
    version: ...
    config:
      clientId: ${FIGMA_CLIENT_ID}
      clientSecret: ${FIGMA_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Figma OAuth client ID.
- `clientSecret` (required): Figma OAuth client secret.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `current_user:read`, `file_content:read`, `file_metadata:read`, `file_versions:read`, `file_comments:read`, `file_comments:write`, `file_dev_resources:read`, `file_dev_resources:write`, `projects:read`, `library_assets:read`, `library_content:read`, `team_library_content:read`, `webhooks:read`, `webhooks:write`.

Operation surfaces: OpenAPI.

Representative operations include:

- `getFile`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - plugin: figma
        operation: getFile
```

Example `getFile` call:

```ts
await invoker.invoke("figma", "getFile", { file_key: "figma-file-key" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
