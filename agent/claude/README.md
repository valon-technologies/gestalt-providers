# Claude Agent SDK Provider

`agent/claude` runs Claude through the Claude Agent SDK. For
`toolSource: mcp_catalog`, the provider registers an in-process SDK MCP server
named `gestalt` and calls tools through `AgentHost.ExecuteTool`.

The first cut is intentionally small:

- provider-owned IndexedDB persistence for AgentProvider sessions, turns, and turn events
- one SDK client per turn
- no Claude resume, branching, or fork support
- no provider-side search RPC call
- Claude Code native MCP Tool Search over `mcp__gestalt__*`
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
      env:
        ANTHROPIC_API_KEY:
          secret:
            provider: secrets
            name: anthropic-api-key
      config:
        defaultModel: claude-sonnet-4-5-20250929
        workingDirectory: /path/to/trusted/workspace
        timeoutSeconds: 300
        permissionMode: dontAsk
        settingSources: []
        disableAutoMemory: true
```

`env` is the provider-level Gestalt environment block, not a field inside
`config`. Gestalt resolves structured secret refs there before launching the
provider process. For backwards compatibility, `env.ANTHROPIC_API_KEY` may also
be a literal or environment-interpolated string such as `${ANTHROPIC_API_KEY:-}`.
When `config.anthropicApiKey` is omitted, the provider uses the host-injected
`ANTHROPIC_API_KEY` environment variable for Claude SDK authentication.

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

## Claude Code Plugins

Claude Code plugins and built-in tool permissions are configured explicitly
in the provider `config`. By default the provider loads no Claude Code plugins,
does not enable skill discovery, passes `setting_sources=[]`, and only allows
the provider-owned Gestalt MCP bridge (`mcp__gestalt__*`). The runner also uses
an isolated per-turn `CLAUDE_CONFIG_DIR`; `disableAutoMemory: true` sets
`CLAUDE_CODE_DISABLE_AUTO_MEMORY=1`.

Local plugin paths must be trusted provider configuration. Session-start hooks
cannot provide plugin paths.

```yaml
providers:
  agent:
    claude:
      config:
        settingSources: []
        disableAutoMemory: true
        skillDiscovery: all
        plugins:
          - /opt/company/claude-plugins/docs
          - /opt/company/claude-plugins/workflows
        allowedTools:
          - Skill
          - Read
          - Bash(git status:*)
```

### Claude Code Config Reference

`settingSources` controls which Claude Code filesystem settings sources are
passed to the SDK. Supported values are `user`, `project`, and `local`.
Omitting the field and setting it to `[]` both mean no user, project, or local
Claude settings are loaded by this provider.

`disableAutoMemory` defaults to `true`. When enabled, the provider sets
`CLAUDE_CODE_DISABLE_AUTO_MEMORY=1` for the Claude SDK turn. This is separate
from `settingSources`; the provider still uses an isolated per-turn
`CLAUDE_CONFIG_DIR`.

`skillDiscovery` controls the SDK `skills` option. Supported values are `none`
and `all`; the default is `none`. `all` asks Claude Code to discover skills, but
the provider only passes `skills: "all"` to the SDK when the resolved turn also
has an active `Skill` permission. Setting `skillDiscovery: all` alone does not
grant the `Skill` tool.

`plugins` is the ordered list of trusted local Claude Code plugin directories to
load on every turn. Each entry must be an absolute path to an existing local
plugin directory containing `.claude-plugin/plugin.json`. Only local skill
plugins are supported. The provider validates the manifest, rejects executable
plugin components such as MCP servers, hooks, commands, agents, shell
entrypoints, and local settings, and passes the canonical plugin path to the
Claude SDK. Skills can be declared in the manifest or in the plugin's default
`skills/` directory.

`allowedTools` controls Claude Code built-in tools in addition to the
provider-owned Gestalt MCP bridge. If omitted, no Claude built-ins are exposed.
It accepts exact built-in names (`Skill`, `Read`, `Write`, `Bash`), specific
skill specifiers such as `Skill(docs:search)`, and conservative Bash prefix
specifiers such as `Bash(git status:*)`. A Bash prefix grants the exact command
or that command followed by a space; shell metacharacters, control characters,
chaining, pipes, redirection, and command substitution are rejected.
`permissionMode: bypassPermissions` cannot be combined with configured
`allowedTools`.

Linux release artifacts are built only for `linux/amd64` and `linux/arm64`.
The Claude Agent SDK does not publish a bundled CLI wheel for `linux/arm`.
