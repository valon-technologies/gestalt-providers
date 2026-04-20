# Hex

Hex Public API plus CLI-only draft notebook, YAML import/export, cell run, and Context Studio operations.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  hex:
    source: github.com/valon-technologies/gestalt-providers/plugins/hex
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Hybrid provider built on Hex's published OpenAPI specification plus a small
source-backed layer for CLI-parity operations. It exposes the full current Hex
Public API surface, including embedding, projects, semantic projects, runs,
groups, users, collections, guides, data connections, and notebook cells.

Additional source-backed operations:

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

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
