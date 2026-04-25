# Simple Agent Provider

`agent/simple` is the first real `kind: agent` provider for Gestalt.

It is intentionally narrow:

- text-in, text-out
- Gestalt plugin operations exposed as tools
- provider-owned IndexedDB persistence
- explicit `CreateSession`, then asynchronous `CreateTurn` / `GetTurn` polling
- direct OpenAI and Anthropic SDK backends

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
apiVersion: gestaltd.config/v3
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
- `CreateTurn` returns after the turn is persisted in `RUNNING`; the provider continues the model/tool loop in the background and callers should use `GetTurn`, `ListTurns`, or `ListTurnEvents` to observe terminal state.

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
        maxSteps: 8
        timeoutSeconds: 120
        systemPrompt: You are a concise operations assistant.
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
path with the full model string preserved. Use `providerOptions.<prefix>` for
provider-specific overrides, and `providerOptions.litellm` remains accepted as
a legacy generic override block during migration.

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
      "pluginName": "incident_io",
      "operation": "incidents.list"
    }
  ],
  "toolSource": "AGENT_TOOL_SOURCE_MODE_EXPLICIT",
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
