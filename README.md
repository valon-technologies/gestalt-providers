# gestalt-plugins

Standalone repository for Gestalt plugins extracted from `github.com/valon-technologies/gestalt` at commit `dc16a16e1075435f83beb070d21233bc4355d8f6`.

## Layout

- `plugins/<name>` contains the source for each plugin package.
- Declarative plugins ship from their manifests and support files.
- Compiled plugins (`bigquery`, `gmail`, and `slack`) are built and packaged with `gestaltd plugin release`.

## CI

`Validate Plugins` runs on pushes and pull requests that touch plugin or workflow files. It:

- downloads a pinned `gestaltd` bootstrap asset from `valon-technologies/gestalt`
- authenticates to GitHub to fetch the private `github.com/valon-technologies/gestalt/sdk/go` module
- runs `go test ./...` for compiled plugins
- packages every plugin to verify releaseability

The repository requires a `PAT_TOKEN` Actions secret with read access to `valon-technologies/gestalt` so compiled plugins can fetch the private Go SDK module and CI can download the pinned `gestaltd` release.

## Releasing

1. Update the target plugin under `plugins/<name>`.
2. Commit the change.
3. Create and push a tag in the format `<name>/v<version>`.

The release workflow packages the plugin with `gestaltd`, creates a GitHub release using the same tag, and uploads the generated archives from `dist/`. CI bootstraps `gestaltd` from the `gestaltd/v<version>` release line in `valon-technologies/gestalt`.
