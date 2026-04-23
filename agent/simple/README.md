# Simple Agent Provider

`agent/simple` is the first real `kind: agent` provider for Gestalt.

It is intentionally narrow:

- text-in, text-out
- Gestalt plugin operations exposed as tools
- provider-owned IndexedDB persistence
- asynchronous `StartRun` with later `GetRun` polling
- LiteLLM as the backend normalization layer

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
          deep: anthropic/claude-sonnet-4-20250514
        maxSteps: 8
        timeoutSeconds: 120
        systemPrompt: You are a concise operations assistant.
        anthropicApiKey:
          secret:
            provider: secrets
            name: anthropic-api-key
```

The provider stores run state through the Gestalt Python SDK `IndexedDB()`
binding exposed as `GESTALT_INDEXEDDB_SOCKET`. It also relies on the standard
backend environment variables that LiteLLM already knows how to read, such as
`OPENAI_API_KEY` and `ANTHROPIC_API_KEY`. You can either set those in the
provider environment or pass `openaiApiKey` / `anthropicApiKey` in provider
config; config values are copied into the corresponding LiteLLM environment
variables when the provider starts.

`StartRun` returns after the run is persisted in `RUNNING`; the provider
continues the model/tool loop in the background and callers should use `GetRun`
or `ListRuns` to observe terminal status.

## JSON request and response

Example run request through Gestalt's agent manager surface:

```json
{
  "providerName": "simple",
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

## CLI stdin/stdout

```sh
cat <<'JSON' | gestalt --format json agent runs create --request-file -
{
  "providerName": "simple",
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
  "toolSource": "AGENT_TOOL_SOURCE_MODE_EXPLICIT"
}
JSON
```

Example response shape:

```json
{
  "providerName": "simple",
  "run": {
    "id": "run_01",
    "providerName": "simple",
    "model": "openai/gpt-4.1-mini",
    "status": "AGENT_RUN_STATUS_SUCCEEDED",
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

## Golang

```go
package main

import (
	"context"
	"fmt"

	gestalt "github.com/valon-technologies/gestalt/sdk/go"
	proto "github.com/valon-technologies/gestalt/sdk/go/gen/v1"
)

func main() {
	ctx := context.Background()

	manager, err := gestalt.AgentManager(ctx)
	if err != nil {
		panic(err)
	}
	defer manager.Close()

	run, err := manager.Run(ctx, &proto.AgentManagerRunRequest{
		ProviderName: "simple",
		Model:        "fast",
		Messages: []*proto.AgentMessage{
			{Role: "user", Text: "Summarize the latest open PRs."},
		},
		ToolRefs: []*proto.AgentToolRef{
			{PluginName: "github", Operation: "pull_requests.list"},
		},
		ToolSource: proto.AgentToolSourceMode_AGENT_TOOL_SOURCE_MODE_EXPLICIT,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(run.Run.OutputText)
}
```

## TypeScript

```ts
import { AgentManagerClient } from "@valon/gestalt";

const client = new AgentManagerClient();

const run = await client.run({
  providerName: "simple",
  model: "fast",
  messages: [{ role: "user", text: "Summarize the latest open PRs." }],
  toolRefs: [{ pluginName: "github", operation: "pull_requests.list" }],
  toolSource: "AGENT_TOOL_SOURCE_MODE_EXPLICIT",
});

console.log(run.run?.outputText ?? "");
```

## Notes

- `responseSchema` is validated against the final model response, but the final
  value must still be a JSON object because `BoundAgentRun.structured_output`
  is a protobuf `Struct`.
- `CancelRun` is cooperative in V1. The provider checks cancellation between
  model calls and tool invocations.
