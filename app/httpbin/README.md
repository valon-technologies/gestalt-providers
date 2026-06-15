# HTTPBin

HTTP request and response testing service.

> Warning: development/testing-only plugin. HTTPBin is a public echo service and
> should not be used with production or sensitive data.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  httpbin:
    source: github.com/valon-technologies/gestalt-providers/app/httpbin
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative REST provider with an MCP surface for testing HTTP request and
response behavior. Exposes operations for inspecting headers, IP addresses,
user agents, and echoing arbitrary request data. Useful for quickstart testing
and debugging.

No authentication required.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  httpbin:
    source: github.com/valon-technologies/gestalt-providers/app/httpbin
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses unspecified; mode `none`.

Operation surfaces: REST.

Representative operations include:

- `get`

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `get` call:

```ts
await app.invoke("httpbin", "get", { anything: "value" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
