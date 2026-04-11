# LaunchDarkly

Manage feature flags and targeting rules.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/plugins/launchdarkly` |
| **Version** | `0.0.1-alpha.8` |
| **Category** | Plugin |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  launchdarkly:
    source: github.com/valon-technologies/gestalt-providers/plugins/launchdarkly
    version: 0.0.1-alpha.8
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the LaunchDarkly OpenAPI specification. Exposes
operations for managing feature flags and targeting rules.

Authenticates with a manually provided API access token.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
