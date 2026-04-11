# Jira Cloud

Atlassian Jira Cloud project and issue management.

| | |
|---|---|
| **Source** | `github.com/valon-technologies/gestalt-providers/plugins/jira` |
| **Version** | `0.0.1-alpha.8` |
| **Category** | Plugin |

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  jira:
    source: github.com/valon-technologies/gestalt-providers/plugins/jira
    version: 0.0.1-alpha.8
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative REST provider for Atlassian Jira Cloud. Exposes operations for
listing projects, getting and creating issues, searching with JQL, adding
comments, and transitioning issue statuses. The Jira Cloud site identifier
is discovered automatically during the OAuth flow.

Authenticates with Atlassian OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
