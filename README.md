# gestalt-plugins

Versioned plugin packages for Gestalt.

## Layout

- `plugins/<name>` contains the source for each plugin package.
- Declarative plugins ship from their manifests and support files.
- Compiled plugins are built and packaged with `gestaltd plugin release`.

## CI

Pushes and pull requests validate every plugin package. Compiled plugins also run `go test ./...`.

## Releasing

Push a tag in the format `<plugin>/v<version>`.

Example:

```sh
git tag slack/v0.1.0
git push origin slack/v0.1.0
```

That triggers the release workflow, packages the plugin, and publishes a GitHub release with the same tag.

In Gestalt config, use:

```yaml
plugin:
  source: github.com/valon-technologies/gestalt-plugins/<plugin>
  version: 0.1.0
```
