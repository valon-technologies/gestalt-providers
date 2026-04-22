# Modal Runtime Provider

Runtime provider for running executable Gestalt plugins in
[Modal](https://modal.com/).

This package is a manifest-driven `kind: runtime` provider implemented against
the public Gestalt SDK runtime-provider surface in
`github.com/valon-technologies/gestalt/sdk/go`.

## Configuration

```yaml
runtime:
  providers:
    modal:
      source:
        path: ./runtime/modal/manifest.yaml
      config:
        app: gestalt-runtime
        environment: main
        cpu: 2
        memoryMiB: 4096
        timeout: 10m
        idleTimeout: 2m

plugins:
  github:
    runtime:
      provider: modal
      image: ghcr.io/valon-technologies/github-plugin-runtime:latest
```

`config.app` is required. The runtime also requires
`plugins.<name>.runtime.image` so Modal can create a sandbox from a concrete
runtime image.

## Current Limitations

- no generic host-service tunnels
- relay-backed IndexedDB env bindings are accepted, which is enough for the host-side IndexedDB relay path
- no hostname-based egress proxy support
