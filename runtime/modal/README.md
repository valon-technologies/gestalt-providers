# Modal Runtime Provider

Runtime provider for running executable Gestalt plugins and hosted agent
providers in
[Modal](https://modal.com/).

This package is a manifest-driven `kind: runtime` provider implemented against
the public Gestalt SDK runtime-provider surface in
`github.com/valon-technologies/gestalt/sdk/go`.

## Configuration

```yaml
server:
  runtime:
    defaultHostedProvider: modal

runtime:
  providers:
    modal:
      source:
        path: ./runtime/modal/manifest.yaml
      config:
        app: gestalt-runtime
        tokenId:
          secret:
            provider: secrets
            name: modal-token-id
        tokenSecret:
          secret:
            provider: secrets
            name: modal-token-secret
        environment: main
        cpu: 2
        memoryMiB: 4096
        timeout: 10m
        idleTimeout: 2m

plugins:
  github:
    execution:
      mode: hosted
      runtime:
        image: ghcr.io/valon-technologies/github-plugin-runtime:latest
    egress:
      allowedHosts:
        - api.github.com

providers:
  agent:
    simple:
      execution:
        mode: hosted
        runtime:
          image: python:3.14-alpine
          pool:
            minReadyInstances: 1
            maxReadyInstances: 2
            startupTimeout: 5m
            healthCheckInterval: 30s
            restartPolicy: always
            drainTimeout: 2m
```

`config.app` is required. `config.tokenId` and `config.tokenSecret` are
optional, but when one is set the other must also be set. Supplying credentials
in config is the preferred deployment shape because runtime providers run as
child processes and do not automatically inherit arbitrary host environment
variables.

The runtime also requires `execution.runtime.image` so Modal can create a
sandbox from a concrete runtime image. For plugins, set
`plugins.<name>.execution.runtime.image`; for hosted agent providers, set
`providers.agent.<name>.execution.runtime.image`.

## Current Limitations

- no generic host-service tunnels
- relay-backed public host-service bindings are accepted for agent host, IndexedDB, cache, S3, workflow manager, authorization, and plugin invoker
- hostname-based `egress.allowedHosts` is enforced by routing outbound HTTP(S) traffic through the public `gestaltd` proxy and constraining the sandbox with Modal `CIDRAllowlist`
