# Simple Agent Provider

`agent/simple` is the first real `kind: agent` provider for Gestalt.

It is intentionally narrow:

- text-in, text-out
- Gestalt plugin operations exposed as tools
- provider-owned IndexedDB persistence
- explicit `CreateSession` then asynchronous `CreateTurn` / `GetTurn` polling
- direct OpenAI and Anthropic SDK backends

It does not try to expose every vendor-specific agent feature. The goal is a
small, usable provider that can drive common text-and-tools workflows now.

## YAML configuration

```yaml
providers:
  agent:
    simple:
      source:
        path: github.com/valon-technologies/gestalt-providers/agent/simple
      default: true
      config:
        runStore: runs
        idempotencyStore: run_idempotency
        defaultModel: fast
        aliases:
          fast: openai/gpt-4.1-mini
          deep: anthropic/<model>
        maxSteps: 8
        timeoutSeconds: 120
        systemPrompt: You are a concise operations assistant.
        anthropicApiKey:
          secret:
            provider: secrets
            name: anthropic-api-key
```

The provider stores session and turn state through the Gestalt Python SDK `IndexedDB()`
binding exposed as `GESTALT_INDEXEDDB_SOCKET`. It also relies on the standard
backend environment variables that the vendor SDKs already know how to read,
such as `OPENAI_API_KEY` and `ANTHROPIC_API_KEY`. You can either set those in
the provider environment or pass `openaiApiKey` / `anthropicApiKey` in
provider config; config values are copied into the corresponding environment
variables when the provider starts.

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

`CreateSession` returns an `ACTIVE` session. `CreateTurn` returns after the
turn is persisted in `RUNNING`; the provider continues the model/tool loop in
the background and callers should use `GetTurn` or `ListTurns` to observe
terminal status.

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
