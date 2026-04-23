# Nebius Runtime Provider

Runtime provider for running executable Gestalt plugins on
[Nebius Compute](https://docs.nebius.com/compute) virtual machines.

This package is a manifest-driven `kind: runtime` provider implemented against
the public Gestalt SDK runtime-provider surface in
`github.com/valon-technologies/gestalt/sdk/go`.

## Configuration

```yaml
runtime:
  providers:
    nebius:
      source:
        path: ./runtime/nebius/manifest.yaml
      config:
        projectID: project-xxxxxxxx
        subnetID: vpcsubnet-xxxxxxxx
        securityGroupIDs:
          - sg-ssh-and-egress
        platform: cpu-d3
        preset: 4vcpu-16gb
        bootDiskImageFamily: ubuntu24.04-driverless
        bootDiskSizeGiB: 30
        username: gestalt

plugins:
  github:
    runtime:
      provider: nebius
      image: ghcr.io/valon-technologies/github-plugin-runtime:latest
```

`config.subnetID` is required. The runtime also requires
`plugins.<name>.runtime.image` so it can pull and run a concrete runtime image
inside the Nebius VM.

## Interface

The runtime provider config currently exposes:

```yaml
projectID: project-xxxxxxxx
endpoint: api.nebius.cloud:443
subnetID: vpcsubnet-xxxxxxxx
securityGroupIDs:
  - sg-ssh-and-egress
platform: cpu-d3
preset: 4vcpu-16gb
serviceAccountID: serviceaccount-xxxxxxxx
bootDiskSizeGiB: 30
bootDiskType: network_ssd
bootDiskImageID: image-xxxxxxxx
bootDiskImageFamily: ubuntu24.04-driverless
bootDiskImageProjectID: project-xxxxxxxx
publicIPStatic: false
username: gestalt
instanceReadyTimeout: 10m
bootstrapTimeout: 10m
pluginReadyTimeout: 30s
cleanupTimeout: 2m
```

Example runtime selection on a plugin:

```yaml
plugins:
  github:
    runtime:
      provider: nebius
      image: ghcr.io/valon-technologies/github-plugin-runtime:latest
```

Authentication uses the Nebius Go SDK:

- If `NEBIUS_IAM_TOKEN` and `config.projectID` are set, the runtime uses that
  IAM token directly.
- Otherwise it falls back to the standard Nebius CLI profile/config flow.

## Execution Model

Each runtime session lazily provisions a Nebius VM, injects an ephemeral SSH key
and a pinned SSH host key through cloud-init, installs Docker, uploads the
plugin bundle over SSH, and launches the requested runtime image with
`docker run --network host`. The hosted plugin binds only on guest loopback and
the host talks to it through a local SSH port forward to the VM.

## Current Limitations

- relay-backed public host-service bindings are accepted for agent host,
  IndexedDB, cache, S3, authorization, workflow manager, and plugin invoker
- outbound egress is not policy-enforced the way the Modal runtime constrains it
- the runtime currently depends on a public IPv4 address because the plugin
  gRPC connection is tunneled over SSH
- runtime images must be pullable from the VM, either anonymously or via
  registry credentials/service-account access already available in the guest
