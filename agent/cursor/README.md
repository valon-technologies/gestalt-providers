# Cursor Agent SDK Provider

This agent provider runs the official Cursor Agent SDK and exposes Gestalt MCP catalog tools through a per-turn localhost MCP server.

The provider is intentionally in-memory. It does not support durable turn resume, branching, forking, structured output, interactions, custom prompts, subagents, plugins, skills, or hooks. Cursor SDK currently does not expose a general switch for disabling built-in local harness tools; the provider limits ambient configuration with `local.settingSources: []`, uses only the Gestalt MCP server in `mcpServers`, isolates per-turn state, and can enable Cursor sandboxing when configured.

Configuration:

- `defaultModel`: Cursor model id to use when the request does not specify one. Defaults to `composer-2`.
- `timeoutSeconds`: maximum live Cursor run duration. Defaults to `300`.
- `systemPrompt`: optional system prompt prepended to every Cursor turn.
- `workingDirectory`: local workspace directory for Cursor. Defaults to the provider process working directory.
- `cursorApiKey`: optional Cursor API key. If omitted, the Cursor SDK reads `CURSOR_API_KEY`.
- `sandboxEnabled`: optional Cursor local sandbox flag.
