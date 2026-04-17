# Hex

Hex Public API plus draft notebook, YAML import/export, and cell run operations.

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

- `project.export`: export a project as YAML
- `project.import`: import or update a project from YAML
- `project.runDraft`: run the draft notebook version of a project
- `cell.run`: run a cell and its dependencies

Authenticates with a manually provided API token.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
