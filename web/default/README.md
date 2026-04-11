# Default Web UI

Default Gestalt web UI bundle served at /.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/web/default` |
| **Version** | `0.0.1-alpha.9` |
| **Category** | Web UI |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
web:
  default:
    source: github.com/valon-technologies/gestalt-providers/web/default
    version: 0.0.1-alpha.9
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Build

This package publishes the default Gestalt web UI as a `webui` bundle.

`gestaltd provider release` runs the package's `release.build` recipe, which
builds the assets from `gestalt/gestaltd/ui` and materializes `out/` before
packaging.

By default the build looks for a sibling checkout at `../../../gestalt` from
this package directory. Set `GESTALT_CHECKOUT=/path/to/gestalt` if your local
layout is different.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
