# Claude Agent SDK Provider

`agent/claude` runs Claude through the Claude Agent SDK. For session
`tools.catalog`, gestaltd sends the exact listed catalog tools on
`tools.catalog.tools`; the provider registers those as an in-process SDK MCP
server named `gestalt` and invokes app operations directly through the Gestalt
SDK request context.

The first cut is intentionally small:

- provider-owned IndexedDB persistence for AgentProvider sessions, turns, and turn events
- one SDK client per turn
- no Claude resume, branching, or fork support
- no provider-side search RPC call
- Claude Code native MCP Tool Search over `mcp__gestalt__*`
- Gestalt tool calls routed through direct SDK app invocation

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
        anthropicApiKey:
          secret:
            provider: secrets
            name: anthropic-api-key
        defaultModel: claude-sonnet-4-5-20250929
        workingDirectory: /path/to/trusted/workspace
        timeoutSeconds: 300
        permissionMode: dontAsk
        settingSources: []
        disableAutoMemory: true
```

Gestalt resolves the structured secret ref before launching the provider
process. The provider passes `config.anthropicApiKey` only to the Claude SDK.

Normal runtime usage requires an agent `indexeddb` binding. The provider gets
the host socket through the Gestalt Python SDK `IndexedDB()` binding
(`GESTALT_HOST_SERVICE_SOCKET`) and derives collision-safe object-store names from
the configured provider name. `configure` does not open the socket; if the
binding is missing or unreachable, the first session/turn RPC fails with
`FAILED_PRECONDITION`.

Use exact, plugin-level, or global tool refs with the MCP catalog source:

```yaml
agent:
  provider: claude
  tools:
    catalog:
      refs:
        - plugin: linear
          operation: searchIssues
        - plugin: github
          operation: pulls/list
```

For small exact tool scopes, the SDK MCP bridge exposes the scoped catalog tools
directly and passes exact allowed tool names like
`mcp__gestalt__github_pulls_list`. For broad tool scopes, the denormalized
`tools.catalog.tools` payload is the concrete set exposed to Claude Code's SDK
MCP `tools/list` response. The provider sets `ENABLE_TOOL_SEARCH=auto:5` so
those catalog tools can be discovered without a Gestalt-specific search wrapper.

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

`skills` enables only the listed skills instead of discovering everything.
Entries are bare skill names or `plugin:skill` qualified names; qualified
entries must reference the manifest name of a configured plugin. The skill
component itself is not verified against plugin contents, so a typo'd skill
name silently enables nothing. The list cannot be combined with
`skillDiscovery: all`, and like discovery it only takes effect when the
resolved turn has an active `Skill` permission. Before the list is passed to
the SDK `skills` option it is intersected with the turn's `Skill`
permissions, so a scoped permission such as `Skill(docs:search)` narrows the
advertised skills to the ones the turn may actually invoke. The filter
controls skill visibility, not file access: skill files from loaded plugins
remain readable by any file tools the turn allows.

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
