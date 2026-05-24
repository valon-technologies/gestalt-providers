# Hex

Hex Public API plus CLI-only draft notebook, YAML import/export, cell run, and Context Studio operations.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  hex:
    source: github.com/valon-technologies/gestalt-providers/app/hex
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Hybrid provider built on Hex's published OpenAPI specification plus a small
provider-managed layer for CLI-parity operations. It exposes the full current Hex
Public API surface, including embedding, projects, semantic projects, runs,
groups, users, collections, guides, data connections, and notebook cells.

Additional provider-managed operations:

- `projects.export`: export a project as YAML
- `projects.import`: import or update a project from YAML
- `projects.runDraft`: run the draft notebook version of a project
- `cells.run`: run a cell and its dependencies
- `suggestions.list`: list Context Studio suggestions
- `suggestions.get`: fetch a single Context Studio suggestion
- `contextVersions.create`: create a Context Studio context version
- `contextVersions.update`: update a Context Studio context version
- `contextVersions.publish`: publish a Context Studio context version

Authenticates with a manually provided API token.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  hex:
    source: github.com/valon-technologies/gestalt-providers/app/hex
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials.
  - Credential fields: `token`.
  - `token`: Create a Personal access token in your Hex workspace settings.

Operation surfaces: OpenAPI.

Representative operations include:

- `projects.export`
- `cells.run`
- `runs.cancel`
- `cells.create`
- `collections.create`
- `dataConnections.create`
- `groups.create`
- `presignedUrls.create`
- `projects.create`
- `users.deactivate`

- Create the Hex API token in Hex and provide it to the manual connection as `token`.

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - plugin: hex
        operation: projects.export
```

Example `projects.export` call:

```ts
await invoker.invoke("hex", "projects.export", { project_id: "hex-project-id" });
```

Example `cells.run` call:

```ts
await invoker.invoke("hex", "cells.run", { project_id: "hex-project-id", cell_id: "cell-id" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
