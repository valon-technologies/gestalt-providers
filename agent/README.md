# Agent Providers

Agent providers for [Gestalt](https://github.com/valon-technologies/gestalt).

Each package under `agent/` implements the portable agent provider surface, so
plugins, HTTP callers, and the CLI can run tool-using agent loops against a
concrete backend.

Current packages:

- `simple`: Python agent provider backed by LiteLLM and provider-owned IndexedDB state
