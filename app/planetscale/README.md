# PlanetScale Plugin

Gestalt plugin that connects to PlanetScale's official hosted MCP server.

This uses the insights-only MCP endpoint:

```text
https://mcp.pscale.dev/mcp/planetscale-insights-only
```

Each user authenticates through PlanetScale's MCP OAuth flow. The insights-only
endpoint excludes query execution tools while keeping organization, database,
branch, schema, Insights, documentation search, and schema recommendation tools.
