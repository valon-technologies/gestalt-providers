# Simple Agent Provider

`agent/simple` is the first real `kind: agent` provider for Gestalt.

It is intentionally narrow:

- text-in, text-out
- Gestalt plugin operations exposed as tools
- provider-owned IndexedDB persistence
- explicit `CreateSession`, then asynchronous `CreateTurn` / `GetTurn` polling
- direct OpenAI and Anthropic SDK backends
- provider-owned tool discovery over the Gestalt MCP catalog

It does not try to expose every vendor-specific agent feature. The goal is a
small, usable provider that can drive common text-and-tools workflows now.

## Local quickstart

`gestaltd init` builds source-backed Python providers from the provider-local
Python environment. For `agent/simple`, create that environment first:

```sh
cd /path/to/gestalt-providers/agent/simple
uv sync --group dev
```

Set the upstream model credentials you plan to use:

```sh
export OPENAI_API_KEY=...
# or
export ANTHROPIC_API_KEY=...
```

Then point a Gestalt config at both the IndexedDB backend and this provider:

```yaml
apiVersion: gestaltd.config/v4
server:
  public:
    port: 18080
  encryptionKey: local-dev-key
  providers:
    indexeddb: main

providers:
  indexeddb:
    main:
      source: /absolute/path/to/gestalt-providers/indexeddb/relationaldb/manifest.yaml
      config:
        dsn: sqlite:///tmp/gestalt-agent-local.db

  secrets:
    env:
      source: env

  agent:
    simple:
      source: /absolute/path/to/gestalt-providers/agent/simple/manifest.yaml
      default: true
      indexeddb:
        provider: main
        db: simple_agent
      config:
        defaultModel: fast
        aliases:
          fast: openai/gpt-4.1-mini
          deep: anthropic/claude-sonnet-4-20250514
        providerOptions:
          openai:
            reasoning_effort: medium
          anthropic:
            thinking:
              type: adaptive
            output_config:
              effort: medium
        maxSteps: 8
        timeoutSeconds: 120
```

Bring the server up from the `gestalt` repo:

```sh
cd /path/to/gestalt/gestaltd
go run ./cmd/gestaltd init --config ../config.yaml
go run ./cmd/gestaltd serve --locked --config ../config.yaml
```

Then talk to the provider through the CLI:

```sh
gestalt --url http://localhost:18080 agent --provider simple --model fast
```

For a no-auth local server, the CLI does not need `GESTALT_API_KEY`.

Notes:

- Store names are internal and derived from the configured provider name. The simplest local setup is to omit `providers.agent.simple.indexeddb.objectStores` entirely so Gestalt can create the provider-owned IndexedDB stores on demand.
- `CreateTurn` returns after the turn is persisted in `RUNNING`; the provider continues the model/tool loop in the background and callers should use `GetTurn`, `ListTurns`, or `ListTurnEvents` to observe terminal state. Startup recovery is backed by IndexedDB transactions.
- If a restart finds a tool call that may already have executed, the provider marks the turn failed rather than replaying a possible side effect. Tool retries can be added once the host/tool invocation layer guarantees idempotency before the side effect.
- The provider advertises `mcp_catalog`. Each turn initially exposes a small `gestalt_search_tools` function; matching authorized integration tools are loaded lazily through `AgentHost.ListTools` before the model invokes them.

## Telemetry

`agent/simple` consumes the shared `gestalt.telemetry` Python SDK helpers. It
does not configure OpenTelemetry exporters inside the provider. Configure
telemetry once in `gestaltd`, and the host injects standard `OTEL_*` environment
into this provider process before the SDK runtime starts serving.

```yaml
apiVersion: gestaltd.config/v4
server:
  encryptionKey: ${GESTALT_ENCRYPTION_KEY}
  providers:
    telemetry: default

providers:
  telemetry:
    default:
      source: otlp
      config:
        endpoint: otel-collector:4317
        protocol: grpc
        traces:
          samplingRatio: 1.0
        metrics:
          interval: 60s
```

The same pattern applies to any Python provider using `gestalt-sdk`: keep
exporter configuration in `providers.telemetry`, let `gestaltd` inject the OTel
environment, and use SDK helpers around the GenAI work owned by provider code.

## Hosted runtime image

Release tags publish a provider release archive and a matching multi-arch runtime
image. Hosted runtimes can use the image directly instead of receiving a staged
plugin bundle on every sandbox launch:

```yaml
providers:
  agent:
    simple:
      execution:
        mode: hosted
        runtime:
          provider: modal
          image: ghcr.io/valon-technologies/agent-simple-runtime:0.0.1-alpha.22
```

Production deploys should pin the image by digest and provide registry auth as a
Docker config JSON secret when the registry is private:

```yaml
providers:
  agent:
    simple:
      execution:
        mode: hosted
        runtime:
          provider: modal
          image: ghcr.io/valon-technologies/agent-simple-runtime@sha256:...
          imagePullAuth:
            dockerConfigJson:
              secret:
                provider: secrets
                name: agent-runtime-registry-dockerconfigjson
```

The hosted image launch command is derived from the provider manifest
`entrypoint.artifactPath`. For the Simple Agent this resolves to
`./gestalt-plugin-simple`, and runtime providers currently launch without an
explicit working directory. The image therefore exposes both
`/app/gestalt-plugin-simple` and `/gestalt-plugin-simple`; release automation
checks that `./gestalt-plugin-simple` can be executed from `/` for
`linux/amd64` and `linux/arm64`.

## YAML configuration

```yaml
providers:
  agent:
    simple:
      source:
        path: github.com/valon-technologies/gestalt-providers/agent/simple
      default: true
      config:
        defaultModel: fast
        aliases:
          fast: openai/gpt-4.1-mini
          deep: anthropic/claude-sonnet-4-20250514
        providerOptions:
          openai:
            reasoning_effort: medium
          anthropic:
            thinking:
              type: adaptive
            output_config:
              effort: medium
        maxSteps: 8
        timeoutSeconds: 120
        systemPrompt: You are a concise operations assistant.
        openaiApiKey:
          secret:
            provider: secrets
            name: openai-api-key
        anthropicApiKey:
          secret:
            provider: secrets
            name: anthropic-api-key
```

The provider stores canonical session, turn, and turn-event state through the
Gestalt Python SDK `IndexedDB()` binding exposed as `GESTALT_INDEXEDDB_SOCKET`.
IndexedDB object-store names are derived from the provider name so normal
configuration only needs behavior-level settings such as model aliases,
timeouts, and prompts. It does not currently persist or expose canonical
interactions. It also relies on the standard backend environment variables that
the vendor SDKs already know how to read, such as `OPENAI_API_KEY` and
`ANTHROPIC_API_KEY`. You can either set those in the provider environment or
pass `openaiApiKey` / `anthropicApiKey` in provider config; config values are
copied into the corresponding environment variables when the provider starts.

Supported model families today are:

- `openai/<model>`
- `anthropic/<model>`

Other prefixed model IDs are still forwarded through the OpenAI-compatible
path with the full model string preserved. Use config `providerOptions` for
provider-wide defaults and per-turn `provider_options` for request-specific
overrides. Both support top-level generic request options and
`providerOptions.<prefix>` for provider-specific values.

OpenAI-compatible endpoints can use a static `api_key` or OAuth client
credentials. OAuth config is nested under the provider prefix and the returned
`access_token` is sent as the bearer credential:

```yaml
providers:
  agent:
    simple:
      source: https://github.com/valon-technologies/gestalt-providers/releases/download/agent/simple/v0.0.1-alpha.33/provider-release.yaml
      config:
        defaultModel: vertex/kimi-k2-6
        providerOptions:
          vertex:
            base_url: https://aiplatform.googleapis.com/v1/projects/PROJECT_ID/locations/us-west2/endpoints/ENDPOINT_ID
            auth:
              type: oauth_client_credentials
              token_url: https://oauth2.example.com/token
              client_id: ${VERTEX_CLIENT_ID}
              client_secret: ${VERTEX_CLIENT_SECRET}
              scope: https://www.googleapis.com/auth/cloud-platform
```

By default the OAuth token request uses HTTP Basic client authentication. Set
`client_auth: body` if your token service expects `client_id` and
`client_secret` in the form body.

When targeting Anthropic, set `providerOptions.max_tokens` (or
`providerOptions.anthropic.max_tokens`) to control the response budget. If you
omit it, the provider defaults to `1024`.

## JSON request and response

Example agent-manager session request:

```json
{
  "providerName": "simple",
  "model": "fast",
  "clientRef": "ops-briefing"
}
```

Example agent-manager turn request for that session:

```json
{
  "sessionId": "session_01",
  "model": "fast",
  "messages": [
    {
      "role": "user",
      "text": "Summarize the latest open production incidents."
    }
  ],
  "toolRefs": [
    {
      "plugin": "incident_io",
      "operation": "incidents.list"
    }
  ],
  "toolSource": "mcp_catalog",
  "responseSchema": {
    "type": "object",
    "properties": {
      "summary": {"type": "string"}
    },
    "required": ["summary"]
  }
}
```

Example response shape:

```json
{
  "session": {
    "id": "session_01",
    "providerName": "simple",
    "model": "openai/gpt-4.1-mini",
    "state": "AGENT_SESSION_STATE_ACTIVE"
  },
  "turn": {
    "id": "turn_01",
    "sessionId": "session_01",
    "providerName": "simple",
    "model": "openai/gpt-4.1-mini",
    "status": "AGENT_EXECUTION_STATUS_SUCCEEDED",
    "messages": [
      {"role": "user", "text": "Summarize the latest open production incidents."},
      {"role": "assistant", "text": "{\"summary\":\"Two incidents remain open.\"}"}
    ],
    "outputText": "{\"summary\":\"Two incidents remain open.\"}",
    "structuredOutput": {
      "summary": "Two incidents remain open."
    }
  }
}
```

## Notes

- `responseSchema` is validated against the final model response, but the final
  value must still be a JSON object because `AgentTurn.structuredOutput`
  is a protobuf `Struct`.
- `CancelTurn` is cooperative in V1. The provider checks cancellation between
  model calls and tool invocations.
