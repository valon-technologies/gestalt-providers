# gestalt-plugins

Standalone repository for Gestalt plugins extracted from `github.com/valon-technologies/gestalt` at commit `dc16a16e1075435f83beb070d21233bc4355d8f6`.

## Layout

- `plugins/<name>` contains the source for each plugin package.
- `sdk/go` contains the Go SDK snapshot used by compiled plugins in this repository.
- Declarative plugins ship from their manifests and support files.
- Compiled plugins (`bigquery`, `gmail`, and `slack`) are built and packaged with `gestaltd plugin release`.

## CI

`Validate Plugins` runs on pushes and pull requests that touch plugin or workflow files. It:

- downloads a pinned `gestaltd` bootstrap asset from this repository's releases
- runs `go test ./...` for compiled plugins
- packages every plugin to verify releaseability

## Releasing

1. Update the target plugin under `plugins/<name>`.
2. Commit the change.
3. Create and push a tag in the format `plugin/<name>/v<version>`.

The release workflow packages the plugin with `gestaltd`, creates a GitHub release using the same tag, and uploads the generated archives from `dist/`.
