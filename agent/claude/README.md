# Claude Agent SDK Provider

`agent/claude` runs Claude through the Claude Agent SDK. For
`toolSource: mcp_catalog`, the provider registers an in-process SDK MCP server
named `gestalt` and calls tools through `AgentHost.ExecuteTool`.

The first cut is intentionally small:

- provider-owned IndexedDB persistence for AgentProvider sessions, turns, and turn events
- one SDK client per turn
- no Claude resume, branching, or fork support
- no provider-side search RPC call
- built-in Claude tools disabled with `tools=[]`
- Gestalt tool calls routed through `AgentHost.ExecuteTool`

## Local Usage

```yaml
providers:
  indexeddb:
    main:
      source: /absolute/path/to/gestalt-providers/indexeddb/relationaldb/manifest.yaml
      config:
        dsn: sqlite:///tmp/gestalt-claude-agent.db

  secrets:
    env:
      source: env

  agent:
    claude:
      source: /absolute/path/to/gestalt-providers/agent/claude/manifest.yaml
      default: true
      indexeddb:
        provider: main
        db: claude_agent
      config:
        defaultModel: claude-sonnet-4-5-20250929
        workingDirectory: /path/to/trusted/workspace
        timeoutSeconds: 300
        permissionMode: dontAsk
        anthropicApiKey:
          secret: ANTHROPIC_API_KEY
```

Normal runtime usage requires an agent `indexeddb` binding. The provider gets
the host socket through the Gestalt Python SDK `IndexedDB()` binding
(`GESTALT_INDEXEDDB_SOCKET`) and derives collision-safe object-store names from
the configured provider name. `configure` does not open the socket; if the
binding is missing or unreachable, the first session/turn RPC fails with
`FAILED_PRECONDITION`.

Use exact, plugin-level, or global tool refs with the MCP catalog source:

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
`mcp__gestalt__github_pulls_list`. For broad grants, it drains the AgentHost
tool pages into the SDK MCP `tools/list` response and relies on Claude Code
native tool search. The provider sets `ENABLE_TOOL_SEARCH=auto:5` so those
catalog tools can be discovered without a Gestalt-specific search wrapper.

`cliPath` can be set when the Claude CLI executable is not on `PATH`. The SDK
still uses the Claude Code transport internally, but this provider integrates
through the SDK API rather than launching `claude` directly.

Linux release artifacts are built on the same Alpine/musl Python base as
`agent/simple`, and only for `linux/amd64` and `linux/arm64`. The Claude Agent
SDK does not publish a bundled CLI wheel for `linux/arm`.
