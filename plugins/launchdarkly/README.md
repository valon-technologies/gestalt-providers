# LaunchDarkly

Manage feature flags and targeting rules.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  launchdarkly:
    source: github.com/valon-technologies/gestalt-providers/plugins/launchdarkly
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the LaunchDarkly OpenAPI specification. Exposes
operations for managing feature flags and targeting rules.

Authenticates with a manually provided API access token.

## Connections

The manifest now exposes both a per-user `default` connection and an additive
`identity` connection for shared deployment credentials. Workloads should bind
to `identity`; existing user flows continue to use `default`.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
