# HTTPBin

HTTP request and response testing service.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/plugins/httpbin` |
| **Version** | `0.0.1-alpha.1` |
| **Category** | Plugin |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  httpbin:
    source: github.com/valon-technologies/gestalt-providers/plugins/httpbin
    version: 0.0.1-alpha.1
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative REST provider with an MCP surface for testing HTTP request and
response behavior. Exposes operations for inspecting headers, IP addresses,
user agents, and echoing arbitrary request data. Useful for quickstart testing
and debugging.

No authentication required.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
