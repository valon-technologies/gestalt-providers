# gestalt-providers

Versioned provider packages for Gestalt.

## Layout

- `plugins/<name>` contains the source for each integration plugin package.
- `auth/<name>` is the intended home for auth provider packages.
- `datastore/<name>` is the intended home for datastore provider packages.
- Declarative plugins ship from their manifests and support files.
- Go source plugins use `go.mod` and are built and packaged with `gestaltd plugin release`.
- Python source plugins use `pyproject.toml` and are built and packaged with `gestaltd plugin release`.

## CI

Pushes and pull requests validate every plugin package. Go plugins also run `go test ./...`. Python plugins run `uv sync` before packaging so the selected interpreter environment contains the SDK, PyInstaller, and plugin dependencies.

The workflows fetch `gestaltd` and private SDK sources from `valon-technologies/gestalt`, so this repo needs a `PAT_TOKEN` Actions secret with read access to that repository.

## Python source plugins

Until the Python SDK is published to a package index, Python plugins should
source the SDK directly from the `gestalt` repo with `uv`.

Minimal `pyproject.toml`:

```toml
[project]
name = "my-plugin"
version = "0.0.1-alpha.1"
dependencies = ["gestalt"]

[tool.uv]
package = false

[tool.uv.sources]
gestalt = { git = "https://github.com/valon-technologies/gestalt.git", rev = "<gestalt-commit-sha>", subdirectory = "sdk/python" }

[tool.gestalt]
plugin = "provider"
```

Recommended local flow:

```sh
uv sync
uv run python -c "import gestalt"
gestaltd plugin release --version 0.0.1-alpha.1
```

Pin `rev` to a specific `valon-technologies/gestalt` commit and bump it
intentionally when you want SDK changes.

## Releasing

Push a tag in the format `<kind>/<name>/v<version>`.

Python source plugin releases publish separate `linux/amd64/glibc` and
`linux/amd64/musl` artifacts so `gestaltd` can resolve the correct binary for
glibc and Alpine runtimes.

Example:

```sh
git tag plugins/slack/v0.0.1-alpha.1
git push origin plugins/slack/v0.0.1-alpha.1
```

That triggers the release workflow, packages the plugin, and publishes a GitHub release with the same tag.

In Gestalt config, use:

```yaml
plugins:
  <plugin>:
    provider:
      source:
        ref: github.com/valon-technologies/gestalt-providers/plugins/<plugin>
        version: 0.0.1-alpha.1
```

For top-level auth/datastore providers, use:

```yaml
auth:
  provider:
    source:
      ref: github.com/valon-technologies/gestalt-providers/auth/<provider>
      version: 0.0.1-alpha.1

datastore:
  provider:
    source:
      ref: github.com/valon-technologies/gestalt-providers/datastore/<provider>
      version: 0.0.1-alpha.1
```
