# Default Web UI

This package publishes the default Gestalt web UI as a `webui` bundle.

`gestaltd provider release` runs the package's `release.build` recipe, which
builds the assets from `gestalt/gestaltd/ui` and materializes `out/` before
packaging.

By default the build looks for a sibling checkout at `../../../gestalt` from
this package directory. Set `GESTALT_CHECKOUT=/path/to/gestalt` if your local
layout is different.
