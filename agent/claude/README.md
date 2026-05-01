# Claude Agent SDK Provider

`agent/claude` runs Claude through the Claude Agent SDK. Gestalt does not rank
or search tools in this provider. For `toolSource: mcp_catalog`, the provider
hydrates the exact granted catalog tools with `AgentHost.ListTools`, registers
them as an in-process SDK MCP server named `gestalt`, and lets Claude select
from that exact allowed set.

The first cut is intentionally small:

- in-memory AgentProvider sessions and turns
- one SDK client per turn
- no durable Claude session store, resume, branching, or fork support
- no provider-side `SearchTools` call
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

The provider passes the SDK exact allowed tool names like
`mcp__gestalt__github_pulls_list`. The allowed names come from the host
`ListTools` response, not from provider-owned search or ranking.

`cliPath` can be set when the Claude CLI executable is not on `PATH`. The SDK
still uses the Claude Code transport internally, but this provider integrates
through the SDK API rather than launching `claude` directly.

Linux release artifacts are built only for `linux/amd64` and `linux/arm64`.
The Claude Agent SDK does not publish a bundled CLI wheel for `linux/arm`.
