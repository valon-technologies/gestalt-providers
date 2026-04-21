# Modal Hosted Runtime Backend

Hosted runtime backend for running executable Gestalt plugins in
[Modal](https://modal.com/).

This backend implements the public hosted-runtime contract from
`github.com/valon-technologies/gestalt/server/pluginruntime` and is consumed by
`gestaltd` when a plugin selects a runtime with `driver: modal`.

## Configuration

```yaml
runtime:
  providers:
    modal:
      driver: modal
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

`config.app` is required. The current backend also requires
`plugins.<name>.runtime.image` so Modal can create a sandbox from a concrete
runtime image.

## Current Limitations

- no host-service tunnels
- no hostname-based egress proxy support
