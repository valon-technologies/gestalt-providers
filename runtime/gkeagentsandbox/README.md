# GKE Agent Sandbox Runtime Provider

Runtime provider for running executable Gestalt plugins and hosted agent
providers in
[GKE Agent Sandbox](https://cloud.google.com/kubernetes-engine/docs/concepts/machine-learning/agent-sandbox)
through the upstream
[kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox)
CRDs and typed Kubernetes clients.

This is a manifest-driven `kind: runtime` provider implemented against
`github.com/valon-technologies/gestalt/sdk/go`.

## Fit

GKE Agent Sandbox is a good fit for the `RuntimeProvider` interface when the
unit of isolation is a Kubernetes sandbox pod. The provider maps one Gestalt
runtime session to one Agent Sandbox `SandboxClaim` or direct `Sandbox`, starts
the app process already present in the runtime image, and returns a
`tcp://...` dial target through either Kubernetes port-forwarding or direct
sandbox service or pod networking for in-cluster `gestaltd` deployments.

It is not a Modal-equivalent hosted process API. The runtime image or template
must contain the tools required to launch the app process.

## Provider Contract

- `StartSession` creates a namespaced `SandboxClaim` when
  `plugins.<name>.execution.runtime.template` or
  `providers.agent.<name>.execution.runtime.template` is set.
- `StartSession` creates a direct `agents.x-k8s.io/v1alpha1` `Sandbox` when no
  template is configured. In this mode `execution.runtime.image` is required on
  the plugin or hosted agent provider.
- Kubernetes resource names include a provider-instance suffix as well as the
  session counter, so a rolling deploy can start replacement runtime sessions
  while the previous `gestaltd` revision is still draining.
- `StartApp` starts the manifest-derived command with
  `GESTALT_PROVIDER_SOCKET=/tmp/gestalt/plugin.sock` and compatibility
  `GESTALT_PLUGIN_SOCKET=/tmp/gestalt/plugin.sock`, bridges that Unix socket to
  `config.appPort` with `socat`, and opens the configured connection back to
  `gestaltd`.
- `BindHostService` accepts relay-backed bindings only. This matches the public
  relay path that `gestaltd` uses for hosted runtimes without direct host
  sockets, including hosted agent bindings such as
  `GESTALT_HOST_SERVICE_SOCKET`.
- Capabilities advertise hostname-based egress when the runtime can enforce
  proxy-only outbound access, and no direct
  host-service sockets.

## Configuration

Claim-backed sessions with a reusable `SandboxTemplate`:

```yaml
server:
  runtime:
    defaultHostedProvider: gkeAgentSandbox

runtime:
  providers:
    gkeAgentSandbox:
      source:
        path: ./runtime/gkeagentsandbox/manifest.yaml
      config:
        namespace: gestalt-runtime
        container: runtime
        appPort: 50051
        connectionMode: portForward
        gke:
          projectID: gitlab-peach-street
          location: us-east4
          cluster: gestalt-agent-sandbox
          endpoint: private
        sandboxReadyTimeout: 3m
        pluginReadyTimeout: 30s
        execTimeout: 2m
        cleanupTimeout: 30s

plugins:
  github:
    execution:
      mode: hosted
      runtime:
        template: gestalt-app-runtime

providers:
  agent:
    simple:
      execution:
        mode: hosted
        runtime:
          template: gestalt-app-runtime
          pool:
            minReadyInstances: 1
            maxReadyInstances: 2
            startupTimeout: 5m
            healthCheckInterval: 30s
            restartPolicy: always
            drainTimeout: 2m
```

Direct `Sandbox` sessions without a pre-created template:

```yaml
server:
  runtime:
    defaultHostedProvider: gkeAgentSandbox

runtime:
  providers:
    gkeAgentSandbox:
      source:
        path: ./runtime/gkeagentsandbox/manifest.yaml
      config:
        namespace: gestalt-runtime
        container: runtime
        connectionMode: portForward
        direct:
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

providers:
  agent:
    simple:
      execution:
        mode: hosted
        runtime:
          image: us-docker.pkg.dev/my-project/gestalt/agent-simple-runtime:latest
          pool:
            minReadyInstances: 1
            maxReadyInstances: 2
            startupTimeout: 5m
            healthCheckInterval: 30s
            restartPolicy: always
            drainTimeout: 2m
```

## Runtime Image Requirements

The selected template image or direct image must include:

- a POSIX shell at `sh`
- `socat` for bridging the SDK Unix socket to the Kubernetes port-forward
- the provider package layout and runtime dependencies needed by the command in
  the plugin manifest

For template mode, keep the container alive and ready before `StartApp`
runs. The provider launches the plugin later with Kubernetes `exec`; it does
not expect the container entrypoint to start the plugin by itself.

## Connection Modes

`connectionMode: portForward` is the default and returns a loopback dial target
from a Kubernetes port-forward. This is suitable for local development and
out-of-cluster `gestaltd` processes with Kubernetes API access.

`connectionMode: serviceDNS` returns the headless sandbox Service DNS name.
Use this when `gestaltd` runs inside the same cluster or otherwise has routed
access to Kubernetes service DNS. This avoids Kubernetes port-forward behavior
on GKE Agent Sandbox pods without pinning the connection to one pod IP.

`connectionMode: podIP` returns the current sandbox pod IP as a transient
transport target. The provider re-reads the SandboxClaim/Sandbox/Pod before
opening this target; do not treat pod IPs as durable route or session identity.
It is mainly a Cloud Run or diagnostic fallback for networks where service DNS
is not available.

## Egress

This provider now claims `RUNTIME_EGRESS_MODE_HOSTNAME` by enforcing
proxy-only outbound access with a per-session Kubernetes `NetworkPolicy`.
When `gestaltd` injects `HTTP_PROXY` / `HTTPS_PROXY`, the provider resolves the
proxy and relay hosts and allows only:

- TCP egress to the proxy / relay endpoints
- DNS on TCP/UDP port `53` to the sandbox resolvers discovered from
  `/etc/resolv.conf`

There is one important template-mode constraint: Kubernetes unions allowed
egress across all matching `NetworkPolicy` objects. Because Agent Sandbox
templates default to a shared managed policy, hostname-based egress is only
enforceable for:

- direct `Sandbox` sessions, or
- `SandboxTemplate` sessions where `spec.networkPolicyManagement` is
  `Unmanaged`

If a template-backed session needs hostname egress and the selected
`SandboxTemplate` uses managed network policy, `StartApp` fails with a clear
precondition error instead of claiming enforcement that the cluster cannot
provide.

## SandboxTemplate Example

```yaml
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxTemplate
metadata:
  name: gestalt-app-runtime
  namespace: gestalt-runtime
spec:
  networkPolicyManagement: Unmanaged
  envVarsInjectionPolicy: Disallowed
  podTemplate:
    spec:
      runtimeClassName: gvisor
      automountServiceAccountToken: false
      containers:
        - name: runtime
          image: us-docker.pkg.dev/my-project/gestalt/app-runtime:latest
          command: ["sh", "-c", "sleep 2147483647"]
          ports:
            - name: plugin-grpc
              containerPort: 50051
          readinessProbe:
            exec:
              command: ["sh", "-c", "test -d /tmp"]
            periodSeconds: 1
          resources:
            requests:
              cpu: 250m
              memory: 512Mi
            limits:
              cpu: "1"
              memory: 1Gi
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
            runAsNonRoot: true
            runAsUser: 65532
            seccompProfile:
              type: RuntimeDefault
      restartPolicy: Never
```

## Kubernetes Access

When `config.gke` is present, the provider discovers the cluster endpoint and
CA bundle through the GKE API and authenticates to Kubernetes with Google
application default credentials. `gke.endpoint` can be `private` or `public`
and defaults to `private`.

Without `config.gke`, the provider uses in-cluster configuration first and
falls back to the default kubeconfig. `config.kubeconfig` and `config.context`
can override that behavior for local development. `config.gke` cannot be
combined with `config.kubeconfig` or `config.context`.

For GKE discovery, the Google service account running `gestaltd` needs IAM
permission for `container.clusters.get`, such as `roles/container.clusterViewer`
on the project or a narrower custom role.

The provider identity needs permissions for:

- `extensions.agents.x-k8s.io/v1alpha1` `sandboxclaims`
- `extensions.agents.x-k8s.io/v1alpha1` `sandboxtemplates` (read-only)
- `agents.x-k8s.io/v1alpha1` `sandboxes`
- core `pods`, including `pods/exec`; `pods/portforward` is required for
  `connectionMode: portForward`
- `networking.k8s.io/v1` `networkpolicies`, including update when hostname
  egress policies are reused

## Verification

The local contract tests exercise both:

- the runtime-provider gRPC surface with a fake sandbox runtime and a real
  local plugin lifecycle gRPC endpoint
- the Kubernetes `NetworkPolicy` contract with fake Kubernetes clients

```sh
go test ./...
```
