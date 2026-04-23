# GKE Agent Sandbox Runtime Provider

Runtime provider for running executable Gestalt plugins in
[GKE Agent Sandbox](https://cloud.google.com/kubernetes-engine/docs/concepts/machine-learning/agent-sandbox)
through the upstream
[kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox)
CRDs and typed Kubernetes clients.

This is a manifest-driven `kind: runtime` provider implemented against
`github.com/valon-technologies/gestalt/sdk/go`.

## Fit

GKE Agent Sandbox is a good fit for the `RuntimeProvider` interface when the
unit of isolation is a Kubernetes sandbox pod. The provider maps one Gestalt
runtime session to one Agent Sandbox `SandboxClaim` or direct `Sandbox`, stages
the executable plugin bundle into that pod, starts the plugin process there,
and returns a host-reachable `tcp://127.0.0.1:<port>` dial target through a
Kubernetes port-forward.

It is not a Modal-equivalent hosted process API. The runtime image or template
must contain the tools required to stage and launch the plugin process.

## Provider Contract

- `StartSession` creates a namespaced `SandboxClaim` when `config.template` or
  `plugins.<name>.runtime.template` is set.
- `StartSession` creates a direct `agents.x-k8s.io/v1alpha1` `Sandbox` when no
  template is configured. In this mode `plugins.<name>.runtime.image` is
  required.
- `StartPlugin` copies `bundle_dir` to `/workspace/plugin`, starts the command
  with `GESTALT_PLUGIN_SOCKET=/tmp/gestalt/plugin.sock`, bridges that Unix
  socket to `config.pluginPort` with `socat`, and opens a Kubernetes
  port-forward back to `gestaltd`.
- `BindHostService` accepts relay-backed bindings only. This matches the public
  relay path that `gestaltd` uses for hosted runtimes without direct host
  sockets.
- Capabilities advertise Linux/amd64 bundle launch, no provider-enforced egress
  policy, and no direct host-service sockets.

## Configuration

Claim-backed sessions with a reusable `SandboxTemplate`:

```yaml
runtime:
  providers:
    gkeAgentSandbox:
      source:
        path: ./runtime/gkeagentsandbox/manifest.yaml
      config:
        namespace: gestalt-runtime
        template: gestalt-plugin-runtime
        container: runtime
        pluginPort: 50051
        sandboxReadyTimeout: 3m
        pluginReadyTimeout: 30s
        execTimeout: 2m
        cleanupTimeout: 30s

plugins:
  github:
    runtime:
      provider: gkeAgentSandbox
```

Direct `Sandbox` sessions without a pre-created template:

```yaml
runtime:
  providers:
    gkeAgentSandbox:
      source:
        path: ./runtime/gkeagentsandbox/manifest.yaml
      config:
        namespace: gestalt-runtime
        container: runtime
        direct:
          runtimeClassName: gvisor
          cpuRequest: 250m
          memoryRequest: 512Mi
          cpuLimit: "1"
          memoryLimit: 1Gi

plugins:
  github:
    runtime:
      provider: gkeAgentSandbox
      image: us-docker.pkg.dev/my-project/gestalt/github-runtime:latest
```

## Runtime Image Requirements

The selected template image or direct image must include:

- a POSIX shell at `sh`
- `tar` for bundle staging
- `socat` for bridging the SDK Unix socket to the Kubernetes port-forward
- the plugin runtime dependencies needed by the command in the plugin manifest

For template mode, keep the container alive and ready before `StartPlugin`
runs. The provider launches the plugin later with Kubernetes `exec`; it does
not expect the container entrypoint to start the plugin by itself.

## Egress

This provider does not yet claim `PLUGIN_RUNTIME_EGRESS_MODE_HOSTNAME` because
the initial implementation does not create a per-session NetworkPolicy or other
enforceable CNI policy. If the cluster/template permits internet access, the
hosted plugin can still make network calls according to cluster policy.

## SandboxTemplate Example

```yaml
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxTemplate
metadata:
  name: gestalt-plugin-runtime
  namespace: gestalt-runtime
spec:
  envVarsInjectionPolicy: Disallowed
  podTemplate:
    spec:
      runtimeClassName: gvisor
      automountServiceAccountToken: false
      containers:
        - name: runtime
          image: us-docker.pkg.dev/my-project/gestalt/plugin-runtime:latest
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

The provider uses in-cluster configuration first and falls back to the default
kubeconfig. `config.kubeconfig` and `config.context` can override that behavior
for local development.

The provider identity needs permissions for:

- `extensions.agents.x-k8s.io/v1alpha1` `sandboxclaims`
- `agents.x-k8s.io/v1alpha1` `sandboxes`
- core `pods`, including `pods/exec` and `pods/portforward`

## Verification

The local contract tests exercise the runtime-provider gRPC surface with a fake
sandbox runtime and a real local plugin lifecycle gRPC endpoint:

```sh
go test ./...
```
