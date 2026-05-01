# Agent Providers

Agent providers for [Gestalt](https://github.com/valon-technologies/gestalt).

Each package under `agent/` implements the portable agent provider surface, so
plugins, HTTP callers, and the CLI can run tool-using agent loops against a
concrete backend.

Current packages:

- `claude`: Python agent provider backed by the Claude Agent SDK with Gestalt MCP catalog tools
- `simple`: Python agent provider backed by the OpenAI and Anthropic SDKs with provider-owned IndexedDB state
