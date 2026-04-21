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

Reference providers in your Gestalt configuration by source and version. See the
[Getting Started](https://gestaltd.ai/getting-started) guide and
[Configuration](https://gestaltd.ai/configuration) docs for examples.

## Developing Providers

### Repository Layout

```
plugins/<name>/       Integration plugin packages (Go, Python)
auth/<name>/          Authentication providers (Go)
authorization/<name>/ Authorization providers (Go)
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

All providers are built and packaged with `gestaltd provider release`.

## Releasing

Push a tag in the format `<kind>/<name>/v<version>`:

```sh
git tag plugins/slack/v0.0.1-alpha.1
git push origin plugins/slack/v0.0.1-alpha.1
```

The release workflow packages the provider for all supported platforms and
publishes a GitHub Release with the same tag. See
[Releasing](https://gestaltd.ai/providers/releasing) for details on
cross-platform artifacts and the CI pipeline.

## License

[Apache License 2.0](LICENSE)
