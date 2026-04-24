# Default UI

Default Gestalt UI bundle served at `/`. The same published artifact also
contains the provider-owned admin shell under `admin/` for `gestaltd`'s
`/admin` surface.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
server:
  admin:
    # Optional. When omitted, gestaltd auto-discovers admin/ from the root UI bundle.
    ui: default

providers:
  ui:
    default:
      source: github.com/valon-technologies/gestalt-providers/ui/default
      path: /
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

If you serve this static bundle behind a Content Security Policy, allow
`data:` in `img-src` for provider icons.

## Build

This package publishes the default Gestalt UI as a `ui` bundle.

`gestaltd provider release` runs the package's `release.build` recipe, which
builds the exported Next app and bundles the static admin shell into `out/`
before packaging.

By default the build looks for a sibling checkout at `../../../gestalt` from
this package directory. Set `GESTALT_CHECKOUT=/path/to/gestalt` if your local
layout is different.

## Documentation

- [Configuration](https://gestaltd.ai/configuration)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
