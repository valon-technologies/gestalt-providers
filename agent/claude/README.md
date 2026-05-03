# Claude Agent SDK Provider

`agent/claude` runs Claude through the Claude Agent SDK. Gestalt does not rank
or search tools in this provider. For `toolSource: mcp_catalog`, the provider
registers an in-process SDK MCP server named `gestalt`; Claude Code indexes the
granted MCP catalog with its native tool search and calls tools through
`AgentHost.ExecuteTool`.

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

The provider passes the SDK exact allowed tool names like
`mcp__gestalt__github_pulls_list`. The allowed names come from the host
`ListTools` response, not from provider-owned search or ranking.

The provider sets `ENABLE_TOOL_SEARCH=true` for Claude Code and adds system
prompt guidance for the Gestalt MCP catalog. For broad grants, the SDK MCP
bridge drains the granted catalog into a single MCP `tools/list` response so
Claude Code's native tool search can discover tools beyond the first Gestalt
host page.

`cliPath` can be set when the Claude CLI executable is not on `PATH`. The SDK
still uses the Claude Code transport internally, but this provider integrates
through the SDK API rather than launching `claude` directly.

Linux release artifacts are built on the same Alpine/musl Python base as
`agent/simple`, and only for `linux/amd64` and `linux/arm64`. The Claude Agent
SDK does not publish a bundled CLI wheel for `linux/arm`.
