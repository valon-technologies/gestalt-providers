# Default UI

Default Gestalt UI bundle served at /.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
providers:
  ui:
    default:
      source: github.com/valon-technologies/gestalt-providers/ui/default
      version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

If you serve this static bundle behind a Content Security Policy, allow
`data:` in `img-src` for provider icons.

## Build

This package publishes the default Gestalt UI as a `ui` bundle.

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
