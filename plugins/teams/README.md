# Microsoft Teams

Browse Microsoft Teams teams, channels, chats, members, and messages through
Microsoft Graph.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  teams:
    source: github.com/valon-technologies/gestalt-providers/plugins/teams
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative OpenAPI-backed provider for common Microsoft Teams read workflows
via Microsoft Graph. The curated surface covers:

- teams and team memberships
- channels and channel member listings
- channel root posts and threaded replies
- chats, chat members, and chat messages

This first release is intentionally read-only and uses delegated Microsoft
OAuth 2.0 with least-privilege Graph scopes for browsing Teams content. Teams
meetings, calling, and message send/write operations are intentionally excluded
from this plugin for now.

## Documentation

- [Microsoft Graph OpenAPI metadata](https://github.com/microsoftgraph/msgraph-metadata)
- [Microsoft Teams overview in Microsoft Graph](https://learn.microsoft.com/en-us/graph/teams-concept-overview)
- [Use the Microsoft Graph API to work with Microsoft Teams](https://learn.microsoft.com/en-us/graph/api/resources/teams-api-overview?view=graph-rest-1.0)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
