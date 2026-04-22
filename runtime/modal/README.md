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
- relay-backed public host-service bindings are accepted for IndexedDB, cache, S3, workflow manager, authorization, and plugin invoker
- hostname-based egress is enforced by routing outbound HTTP(S) traffic through the public `gestaltd` proxy and constraining the sandbox with Modal `CIDRAllowlist`
