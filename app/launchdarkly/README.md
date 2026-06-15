# LaunchDarkly

Manage feature flags and targeting rules.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  launchdarkly:
    source: github.com/valon-technologies/gestalt-providers/app/launchdarkly
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
apps:
  launchdarkly:
    source: github.com/valon-technologies/gestalt-providers/app/launchdarkly
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials; mode `subject`.
  - Credential fields: `token`.
  - `token`: See [Authorization](https://app.launchdarkly.com/settings/authorization) to create an access token

Operation surfaces: OpenAPI.

Representative operations include:

- `getFeatureFlag`

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `getFeatureFlag` call:

```ts
await app.invoke("launchdarkly", "getFeatureFlag", { projectKey: "default", featureFlagKey: "example-flag" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
