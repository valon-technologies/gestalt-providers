# Gestalt Providers

[![Stability: Alpha](https://img.shields.io/badge/stability-alpha-f4d03f.svg)](https://github.com/valon-technologies/gestalt-providers/issues)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> **Alpha.** All provider packages are under active development. Interfaces and
> manifest schemas may change between releases. Feedback and bug reports welcome
> via [GitHub Issues](https://github.com/valon-technologies/gestalt-providers/issues).

Official provider packages for [Gestalt](https://github.com/valon-technologies/gestalt).
Each provider is independently versioned, tested, and released as a
cross-platform artifact that `gestaltd` resolves at runtime.

## Documentation

- [Getting Started](https://gestaltd.ai/getting-started): run Gestalt in five minutes
- [Configuration](https://gestaltd.ai/configuration): config model, plugin setup, and authentication
- [Provider Development](https://gestaltd.ai/providers): writing plugins, authentication providers, S3 object stores, datastores, and secrets engines
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests): manifest format and schema
- [Releasing](https://gestaltd.ai/providers/releasing): publishing provider packages

## Usage

Use `gestaltd provider search` and `gestaltd provider add` to discover and add
providers without hand-writing source metadata:

```sh
gestaltd provider search slack

gestaltd provider add github.com/valon-technologies/gestalt-providers/plugins/slack \
  --config gestaltd.yaml \
  --name slack \
  --version 0.0.1-alpha.37
```

The lifecycle commands write package sources by default. `provider add`
validates the config and refreshes the lockfile before committing changes.

```yaml
plugins:
  slack:
    source:
      package: github.com/valon-technologies/gestalt-providers/plugins/slack
      version: 0.0.1-alpha.37
```

Use `provider list` to inspect configured providers and lock status, and
`provider upgrade` or `provider remove` to maintain entries:

```sh
gestaltd provider list --config gestaltd.yaml
gestaltd provider upgrade slack --config gestaltd.yaml --version 0.0.1-alpha.38
gestaltd provider remove slack --config gestaltd.yaml --kind plugin
```

See the [Getting Started](https://gestaltd.ai/getting-started) guide and
[Configuration](https://gestaltd.ai/configuration) docs for complete examples.

The repository publishes a generated provider index at `provider-index.yaml`.
Normal package resolution fetches this static YAML file directly; it does not
call the GitHub Releases API while selecting package versions. Release metadata
and archives still live in GitHub Releases and are fetched after a package
version is selected.

Refresh it after manifest changes with:

```sh
python3 .github/scripts/generate_provider_index.py
```

## Developing Providers

### Repository Layout

```
plugins/<name>/       Integration plugin packages (Go, Python)
agent/<name>/         Agent providers (Python, Go)
runtime/<name>/       Hosted runtime backend packages (Go)
auth/<name>/          Authentication providers (Go)
authorization/<name>/ Authorization providers (Go)
external_credentials/<name>/ External credential providers (Go)
s3/<name>/            S3-compatible object-store providers (Go)
indexeddb/<name>/     Datastore providers (Go, Rust)
workflow/<name>/      Workflow providers (Go)
cache/<name>/         Cache providers (Go)
secrets/<name>/       Secrets providers (Go)
ui/<name>/            UI bundles
```

Every provider requires a
[`manifest.yaml`](https://gestaltd.ai/reference/plugin-manifests) that declares
its source, version, display name, and capabilities. Declarative plugins need
only the manifest and optional assets. Source plugins add an implementation in
Go, Python, or Rust. See the
[provider development guide](https://gestaltd.ai/providers) for SDK setup and
writing custom operations.

All providers are built and packaged with `gestaltd provider release`,
including hosted runtime backends under `runtime/`.

## Releasing

Push a tag in the format `<kind>/<name>/v<version>`:

```sh
git tag plugins/slack/v0.0.1-alpha.1
git push origin plugins/slack/v0.0.1-alpha.1
```

Runtime providers use the same release flow:

```sh
git tag runtime/modal/v0.0.1-alpha.2
git push origin runtime/modal/v0.0.1-alpha.2
```

The release workflow packages the provider for all supported platforms and
publishes a GitHub Release with the same tag. See
[Releasing](https://gestaltd.ai/providers/releasing) for details on
cross-platform artifacts and the CI pipeline.

## License

[Apache License 2.0](LICENSE)
