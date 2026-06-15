# Jira Cloud

Atlassian Jira Cloud project and issue management.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  jira:
    source: github.com/valon-technologies/gestalt-providers/app/jira
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative REST provider for Atlassian Jira Cloud. Exposes operations for
listing projects, getting and creating issues, searching with JQL, adding
comments, transitioning issue statuses, and managing issue properties. The Jira Cloud site identifier
is discovered automatically during the OAuth flow.

Authenticates with Atlassian OAuth 2.0.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  jira:
    source: github.com/valon-technologies/gestalt-providers/app/jira
    version: ...
    connections:
      default:
        params:
          cloud_id: "..."
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `read:me`, `read:jira-work`, `write:jira-work`, `offline_access`.
  - Connection params:
    - `cloud_id` (required): Jira Cloud site identifier (discovered automatically)

Operation surfaces: REST.

Representative operations include:

- `searchForIssuesUsingJql`

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `searchForIssuesUsingJql` call:

```ts
await app.invoke("jira", "searchForIssuesUsingJql", { jql: "project = ENG order by updated DESC", maxResults: 10 });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
