# Linear

Manage issues, projects, and teams.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  linear:
    source: github.com/valon-technologies/gestalt-providers/app/linear
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider with both a GraphQL surface and an
[MCP](https://modelcontextprotocol.io/) surface. The GraphQL surface exposes an
audited allowlist of Linear operations with explicit response selections for
issues, projects, teams, cycles, comments, labels, and related workflow data.
Those generated GraphQL operations avoid Linear team-access fields that require
private/tented team support. Raw GraphQL passthrough remains a separate surface
permission for callers that need custom queries.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  linear:
    source: github.com/valon-technologies/gestalt-providers/app/linear
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `OAuth` uses OAuth 2.0; mode `subject`.
  - Requested scopes: `read`, `write`.
- `ApiKey` uses manual credentials; mode `subject`.
  - Credential fields: `api_key`.
  - `api_key`: Create one under [Security & access](https://linear.app/settings/account/security) in Linear settings

Operation surfaces: GraphQL, MCP.

Representative operations include:

- `searchIssues`
- `issues`
- `teams`
- `viewer`
- `issueCreate`

- GraphQL and hosted MCP surfaces both default to the Linear OAuth connection with `read` and `write` scopes. The `ApiKey` connection instead sends a personal API key directly in the `Authorization` header, for subjects that prefer a manually issued credential over the OAuth flow.

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `issueCreate` call:

```ts
await app.invoke("linear", "issueCreate", { input: { teamId: "team-id", title: "Follow up" } });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
