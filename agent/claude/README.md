# Claude Code Agent Provider

`agent/claude` runs the Claude Code CLI as the agent harness. Gestalt tools are
not ranked or searched by this provider. For `toolSource: mcp_catalog`, the
provider starts a small MCP stdio server for the turn and Claude Code uses its
native MCP tool listing and selection behavior.

The first cut is intentionally small:

- in-memory sessions and turns
- one Claude Code subprocess per turn
- no resume, branching, or provider-owned configuration files
- built-in Claude Code tools disabled by default for the subprocess
- Gestalt tool calls routed through `AgentHost.ExecuteTool`

## Local Usage

```yaml
providers:
  agent:
    claude:
      source: /absolute/path/to/gestalt-providers/agent/claude/manifest.yaml
      default: true
      config:
        defaultModel: sonnet
        claudeBinary: claude
        workingDirectory: /path/to/trusted/workspace
        timeoutSeconds: 300
        permissionMode: bypassPermissions
```

Use exact tool refs with the MCP catalog source:

```yaml
agent:
  provider: claude
  toolSource: mcp_catalog
  toolRefs:
    - plugin: linear
      operation: issues
    - plugin: github
      operation: repos/list-for-authenticated-user
```

The provider requires the Claude Code binary to be installed and authenticated in
the environment where the provider process runs. Configuration fails fast when
`claudeBinary` cannot be resolved.
