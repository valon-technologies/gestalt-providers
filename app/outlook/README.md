# Microsoft Outlook

Gestalt app for Microsoft's official Outlook Mail MCP server (Work IQ Mail,
`mcp_MailTools`), part of Microsoft Agent 365. Agents can search, read, draft,
send, and reply to mail in the signed-in user's own mailbox.

## Endpoint

```text
https://agent365.svc.cloud.microsoft/agents/servers/mcp_MailTools
```

The endpoint is tenant-agnostic: the user's organization is taken from their
access token, so one app definition serves users from any Microsoft Entra
tenant.

## Requirements

- A work or school Microsoft Entra tenant enabled for Agent 365 / Frontier.
  Personal Outlook.com accounts are not supported.
- A Microsoft 365 Copilot license for each connecting user.
- The server is in preview; tool names and schemas may change.

## Authentication

Users sign in with delegated OAuth 2.0 against the multi-tenant
`organizations` Microsoft Entra endpoint. Microsoft Entra does not support
dynamic client registration, so the deployment provides one app registration:

1. Register an app in Microsoft Entra with **Supported account types** set to
   *Accounts in any organizational directory (multitenant)*, and add a web
   redirect URI for your Gestalt deployment.
2. Under **API permissions**, add the **Agent 365 Tools** delegated permission
   `McpServers.Mail.All`.
3. Create a client secret and supply the client ID and secret as app config.

Each customer tenant's administrator must consent to the app (or allow user
consent for it) before their users can connect. Every tool acts only on the
connected user's mailbox.

## Configuration

```yaml
apps:
  outlook:
    source: github.com/valon-technologies/gestalt-providers/app/outlook
    version: 0.0.1-alpha.1
    config:
      clientId: <entra-client-id>
      clientSecret: <entra-client-secret>
```

See [Getting Started](https://gestaltd.ai/getting-started) and [Configuration](https://gestaltd.ai/configuration).
