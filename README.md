# gestalt-providers

[![Stability: Alpha](https://img.shields.io/badge/stability-alpha-f4d03f.svg)](https://github.com/valon-technologies/gestalt-providers/issues)

> **Alpha software** -- All provider packages are under active development. Package interfaces and manifest schemas may change between releases. We welcome feedback and bug reports via [GitHub Issues](https://github.com/valon-technologies/gestalt-providers/issues).

Versioned provider packages for Gestalt.

## Layout

- `plugins/<name>` contains the source for each integration plugin package.
- `auth/<name>` contains platform auth provider packages.
- `datastore/<name>` contains datastore provider packages.
- `web/<name>` contains packaged web UI bundles.
- Declarative plugins ship from their manifests and support files.
- Go source plugins use `go.mod` and are built and packaged with `gestaltd provider release`.
- Python source plugins use `pyproject.toml` and are built and packaged with `gestaltd provider release`.
- Rust source plugins use `Cargo.toml` and are built and packaged with `gestaltd provider release`.

## CI

Pushes and pull requests validate every plugin package. Go plugins also run `go test ./...`. Python plugins run `uv sync` before packaging so the selected interpreter environment contains the SDK, PyInstaller, and plugin dependencies.

The workflows fetch `gestaltd` and private SDK sources from `valon-technologies/gestalt`, so this repo needs a `PAT_TOKEN` Actions secret with read access to that repository.

Web bundles can define `release.build` in `manifest.yaml`. The first-party
`web/default` package uses that hook to build assets from
`gestalt/gestaltd/ui` before `gestaltd provider release`, so generated
frontend output does not need to be committed here.

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
gestaltd provider release --version 0.0.1-alpha.1
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

For top-level web UI bundles, use:

```yaml
ui:
  provider:
    source:
      ref: github.com/valon-technologies/gestalt-providers/web/<bundle>
      version: 0.0.1-alpha.1
```

Anonymous mode is a host-side special case, not a published auth provider
package. Omit `auth` entirely or set `auth.provider: none` when you want
platform auth disabled.
