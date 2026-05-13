# Codex MCP Agent Provider

`agent/codex` runs Codex through the Codex CLI MCP server. Gestalt does not rank
or search tools in this provider. For `toolSource: mcp_catalog`, the provider
hydrates the exact granted catalog tools with `AgentHost.ListTools`, writes those
tool names into a temporary Codex `mcp_servers.gestalt.enabled_tools` config,
and routes nested Codex MCP tool calls back through `AgentHost.ExecuteTool`.

The first cut is intentionally small:

- in-memory AgentProvider sessions and turns
- one Codex MCP session per turn
- no durable Codex thread store, resume, branching, or fork support
- no provider-side search RPC call
- no interactive approval prompts; `approvalPolicy` must be `never`
- optional Codex surfaces such as apps, multi-agent tools, hooks, skills, and
  web search disabled in the generated per-turn config
- Gestalt tool calls routed through `AgentHost.ExecuteTool`

`enabled_tools` constrains only the Gestalt MCP tools exposed to Codex. Codex may
still use its own built-in behavior within the configured sandbox.

The Gestalt tool bridge runs inside the provider process as a temporary
`127.0.0.1` MCP HTTP endpoint with an unguessable per-turn path. AgentHost socket
credentials and tool grants are not serialized into Codex config, and the
generated Codex shell environment policy excludes `OPENAI_API_KEY` and
`GESTALT_*` from shell commands.

Use `config.openaiApiKey` or `OPENAI_API_KEY` for Codex authentication. The
provider starts Codex with an isolated per-turn `CODEX_HOME`, so it does not read
the user's `codex login` state from the default Codex home.

## Local Usage

```yaml
providers:
  agent:
    codex:
      source: /absolute/path/to/gestalt-providers/agent/codex/manifest.yaml
      default: true
      env:
        OPENAI_API_KEY:
          secret:
            provider: secrets
            name: openai-api-key
      config:
        workingDirectory: /path/to/trusted/workspace
        timeoutSeconds: 300
        approvalPolicy: never
        sandbox: read-only
```

`env` is the provider-level Gestalt environment block, not a field inside
`config`. Gestalt resolves structured secret refs there before launching the
provider process. For backwards compatibility, `env.OPENAI_API_KEY` may also be
a literal or environment-interpolated string such as `${OPENAI_API_KEY:-}`. When
`config.openaiApiKey` is omitted, the provider uses the host-injected
`OPENAI_API_KEY` environment variable for Codex authentication.

Use exact tool refs with the MCP catalog source:

```yaml
agent:
  provider: codex
  toolSource: mcp_catalog
  toolRefs:
    - plugin: linear
      operation: searchIssues
    - plugin: github
      operation: pulls/list
```

`runGrant` is the authorization source of truth. `toolRefs` are required so the
caller's intent is explicit, while `AgentHost.ListTools` decides the actual
tools exposed to Codex for the grant.

`codexCommand` and `codexArgs` can be set when `codex mcp-server` is not on
`PATH`, for example:

```yaml
codexCommand: npx
codexArgs: ["-y", "codex", "mcp-server"]
```

`defaultModel` is optional. When it is empty and the turn/session omits `model`,
the provider omits the Codex `model` argument and lets Codex use its configured
default.
