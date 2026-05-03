# Claude Agent SDK Provider

`agent/claude` runs Claude through the Claude Agent SDK. For
`toolSource: mcp_catalog`, the provider registers an in-process SDK MCP server
named `gestalt` and calls tools through `AgentHost.ExecuteTool`.

The first cut is intentionally small:

- in-memory AgentProvider sessions and turns
- one SDK client per turn
- no durable Claude session store, resume, branching, or fork support
- no provider-side search RPC call
- built-in Claude tools disabled with `tools=[]`
- Gestalt tool calls routed through `AgentHost.ExecuteTool`

## Local Usage

```yaml
providers:
  agent:
    claude:
      source: /absolute/path/to/gestalt-providers/agent/claude/manifest.yaml
      default: true
      config:
        defaultModel: claude-sonnet-4-5-20250929
        workingDirectory: /path/to/trusted/workspace
        timeoutSeconds: 300
        permissionMode: dontAsk
        anthropicApiKey:
          secret: ANTHROPIC_API_KEY
```

Use exact tool refs with the MCP catalog source:

```yaml
agent:
  provider: claude
  toolSource: mcp_catalog
  toolRefs:
    - plugin: linear
      operation: searchIssues
    - plugin: github
      operation: pulls/list
```

For small exact grants, the SDK MCP bridge exposes the granted catalog tools
directly and passes exact allowed tool names like
`mcp__gestalt__github_pulls_list`. For broad grants, it exposes only
`gestalt_catalog_search` and `gestalt_catalog_execute`, so Claude can discover
and run connected-app tools without loading the full catalog into context. The
provider sets `ENABLE_TOOL_SEARCH=false` so those catalog tools are always
visible.

`cliPath` can be set when the Claude CLI executable is not on `PATH`. The SDK
still uses the Claude Code transport internally, but this provider integrates
through the SDK API rather than launching `claude` directly.

Linux release artifacts are built on the same Alpine/musl Python base as
`agent/simple`, and only for `linux/amd64` and `linux/arm64`. The Claude Agent
SDK does not publish a bundled CLI wheel for `linux/arm`.
