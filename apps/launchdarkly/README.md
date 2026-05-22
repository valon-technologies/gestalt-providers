# LaunchDarkly

Manage feature flags and targeting rules.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  launchdarkly:
    source: github.com/valon-technologies/gestalt-providers/apps/launchdarkly
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the LaunchDarkly OpenAPI specification. Exposes
operations for managing feature flags and targeting rules.

Authenticates with a manually provided API access token.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  launchdarkly:
    source: github.com/valon-technologies/gestalt-providers/apps/launchdarkly
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials; mode `user`.
  - Credential fields: `token`.
  - `token`: See [Authorization](https://app.launchdarkly.com/settings/authorization) to create an access token

Operation surfaces: OpenAPI.

Representative operations include:

- `getFeatureFlag`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: launchdarkly
        operation: getFeatureFlag
```

Example `getFeatureFlag` call:

```ts
await invoker.invoke("launchdarkly", "getFeatureFlag", { projectKey: "default", featureFlagKey: "example-flag" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
