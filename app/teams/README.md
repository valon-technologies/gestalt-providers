# Microsoft Teams

Browse Microsoft Teams teams, channels, chats, members, and messages through
Microsoft Graph.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  teams:
    source: github.com/valon-technologies/gestalt-providers/app/teams
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

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  teams:
    source: github.com/valon-technologies/gestalt-providers/app/teams
    version: ...
    config:
      clientId: ${TEAMS_CLIENT_ID}
      clientSecret: ${TEAMS_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Microsoft Entra OAuth client ID for the Microsoft Teams integration.
- `clientSecret` (required): Microsoft Entra OAuth client secret for the Microsoft Teams integration.

Connections and authentication:

- `default` uses OAuth 2.0; mode `subject`.
  - Requested scopes: `offline_access`, `Team.ReadBasic.All`, `TeamMember.Read.All`, `Channel.ReadBasic.All`, `ChannelMember.Read.All`, `ChannelMessage.Read.All`, `Chat.Read`.

Operation surfaces: OpenAPI.

Representative operations include:

- `list_joined_teams`
- `list_joined_teams`
- `get_team`
- `list_team_members`
- `list_team_channels`
- `get_channel`
- `list_channel_members`
- `list_channel_messages`
- `get_channel_message`

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `list_joined_teams` call:

```ts
await app.invoke("teams", "list_joined_teams", { top: 10 });
```

## Documentation
- [Microsoft Graph OpenAPI metadata](https://github.com/microsoftgraph/msgraph-metadata)
- [Microsoft Teams overview in Microsoft Graph](https://learn.microsoft.com/en-us/graph/teams-concept-overview)
- [Use the Microsoft Graph API to work with Microsoft Teams](https://learn.microsoft.com/en-us/graph/api/resources/teams-api-overview?view=graph-rest-1.0)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
