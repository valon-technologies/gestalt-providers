# Kubernetes Runtime Provider

Runtime provider for running executable Gestalt plugin, agent, and workflow
providers in native Kubernetes Pods.

This is a manifest-driven `kind: runtime` provider implemented against
`github.com/valon-technologies/gestalt/sdk/go`.

## Fit

The provider maps one Gestalt runtime session to one Kubernetes Pod. It starts
the provider process already present in the runtime image with `pods/exec`, then
returns a `tcp://...` dial target through Kubernetes port-forwarding, Pod IP
networking, or Service DNS.

It does not require Agent Sandbox CRDs. The runtime image or PodTemplate must
contain the tools needed to launch the provider process.

## Provider Contract

- `StartSession` copies a native `core/v1` `PodTemplate` when
  `execution.runtime.template` is set.
- `StartSession` builds a Pod from `podDefaults` when
  `execution.runtime.image` is set without a template.
- `StartApp` injects `GESTALT_PROVIDER_SOCKET=/tmp/gestalt/plugin.sock`,
  compatibility `GESTALT_PLUGIN_SOCKET=/tmp/gestalt/plugin.sock`, and
  `GESTALT_APP_NAME`, starts `socat` from `appPort` to the Unix socket, and
  launches the requested command/args/env through `pods/exec`.
- Session state is derived from Pod phase, the Ready condition, the plugin-start
  Lease, and provider-start annotations.
- Hostname egress is advertised only when `hostnameEgress.mode: publicProxy` is
  configured.

The provider does not implement workspace preparation in v1. Hosted agents that
configure `execution.runtime.workspace` need a runtime with workspace support.

## Configuration

Template-backed sessions:

```yaml
server:
  runtime:
    defaultHostedProvider: kubernetes

runtime:
  providers:
    kubernetes:
      source:
        path: ./runtime/kubernetes/manifest.yaml
      config:
        namespace: gestalt-runtime
        container: runtime
        appPort: 50051
        connectionMode: podIP
        sessionReadyTimeout: 3m
        pluginReadyTimeout: 30s
        execTimeout: 2m
        cleanupTimeout: 30s
        gke:
          projectID: gitlab-peach-street
          location: us-east4
          cluster: gestalt-agent-sandbox
          endpoint: private
        hostnameEgress:
          mode: publicProxy

providers:
  agent:
    simple:
      execution:
        mode: hosted
        runtime:
          template: agent-runtime
          pool:
            minReadyInstances: 1
            maxReadyInstances: 2
            startupTimeout: 5m
            healthCheckInterval: 30s
            restartPolicy: never
            drainTimeout: 2m
```

Direct image sessions:

```yaml
runtime:
  providers:
    kubernetes:
      source:
        path: ./runtime/kubernetes/manifest.yaml
      config:
        namespace: gestalt-runtime
        container: runtime
        connectionMode: portForward
        podDefaults:
          serviceAccountName: gestaltd
          runtimeClassName: gvisor
          cpuRequest: 250m
          memoryRequest: 512Mi
          cpuLimit: "1"
          memoryLimit: 1Gi

plugins:
  github:
    execution:
      mode: hosted
      runtime:
        image: us-docker.pkg.dev/my-project/gestalt/github-runtime:latest
```

## PodTemplate Example

The Pod must become ready before the provider process starts. Do not make the
readiness probe depend on `appPort`; `StartApp` performs provider gRPC
readiness checks after the process is launched.

```yaml
apiVersion: v1
kind: PodTemplate
metadata:
  name: agent-runtime
  namespace: gestalt-runtime
template:
  spec:
    runtimeClassName: gvisor
    serviceAccountName: gestaltd
    automountServiceAccountToken: false
    securityContext:
      runAsNonRoot: true
      runAsUser: 65532
    containers:
      - name: runtime
        image: us-docker.pkg.dev/my-project/gestalt/agent-runtime:latest
        command: ["sh", "-c", "sleep 2147483647"]
        ports:
          - name: plugin-grpc
            containerPort: 50051
        readinessProbe:
          exec:
            command: ["sh", "-c", "test -d /tmp && command -v socat >/dev/null"]
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop: ["ALL"]
```

## Runtime Image Requirements

The selected template image or direct image must include:

- POSIX `sh`
- `socat`
- the provider binary/package layout and runtime dependencies needed by the
  hosted provider command

## Connection Modes

`portForward` returns a loopback dial target from Kubernetes port-forwarding.
Use this for local or out-of-cluster `gestaltd` processes with Kubernetes API
access.

`podIP` returns the runtime Pod IP directly. Use this when `gestaltd` runs in a
network that can route to Pod IPs.

`serviceDNS` creates a one-session Service and returns its cluster DNS name.
Use this when `gestaltd` runs inside the same cluster or has Kubernetes service
DNS and networking.

## Egress

`hostnameEgress.mode: publicProxy` advertises hostname egress support and
creates a per-session `NetworkPolicy` that allows DNS plus TCP egress to the
`HTTP_PROXY` / `HTTPS_PROXY` and host-service relay targets injected by
`gestaltd`. This requires a cluster with NetworkPolicy enforcement.

The default is `disabled`, which advertises no runtime hostname egress support.

## RBAC

The configured identity needs namespace-scoped access to:

- `pods`
- `pods/exec`
- `pods/portforward` when `connectionMode: portForward` is used
- `podtemplates`
- `services` when `connectionMode: serviceDNS` is used
- `secrets` when `execution.runtime.imagePullAuth` is used
- `coordination.k8s.io` `leases`
- `networking.k8s.io` `networkpolicies` when hostname egress is enabled
