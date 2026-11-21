# Microsoft Entra app registration for Outlook

The Outlook app signs users in with delegated OAuth 2.0 against the
multi-tenant `organizations` Microsoft Entra endpoint. Microsoft Entra does not
support dynamic client registration, so each Gestalt deployment must provide one
app registration and supply its client ID and secret as app config.

This is done in the [Microsoft Entra admin center](https://entra.microsoft.com),
not the Microsoft 365 admin center.

## Prerequisites

- A work or school Microsoft Entra tenant. Personal Outlook.com accounts are not
  supported.
- The tenant is enabled for Agent 365 / Frontier, Microsoft's preview program
  for the Work IQ Mail MCP server. A tenant administrator controls enrollment.
- Each connecting user has a Microsoft 365 Copilot license.
- A role that can create app registrations (for example Application Developer),
  and, for step 4, a role that can grant admin consent (for example Cloud
  Application Administrator or Global Administrator).

Without Agent 365 enrollment and a Copilot license, sign-in may succeed while
calls to the mail endpoint are rejected. Confirm both before debugging Gestalt.

## 1. Register the application

1. Go to **Entra ID > App registrations > New registration**.
2. Name: for example `Gestalt Outlook`.
3. **Supported account types**: *Accounts in any organizational directory
   (Any Microsoft Entra ID tenant - Multitenant)*. This lets users from any
   customer tenant sign in with one registration.
4. **Redirect URI**: platform **Web**, set to your Gestalt OAuth callback URL
   (see [Redirect URIs](#redirect-uris)). You can add more later.
5. Select **Register** and copy the **Application (client) ID**. This is the
   `clientId` app config value.

## 2. Add the API permission

1. Open **API permissions > Add a permission**.
2. Choose **APIs my organization uses** and search for **Agent 365 Tools**.
3. Choose **Delegated permissions** and select `McpServers.Mail.All`.
4. Select **Add permissions**.

If Agent 365 Tools does not appear in the list, the tenant is probably not yet
enrolled in Agent 365 / Frontier. Ask the tenant administrator to confirm.

The app requests these scopes at sign-in (from `manifest.yaml`):

- `offline_access`
- `https://agent365.svc.cloud.microsoft/agents/servers/mcp_MailTools/.default`

## 3. Create a client secret

1. Open **Certificates & secrets > Client secrets > New client secret**.
2. Choose a description and expiry.
3. Copy the secret **Value** immediately; it is not shown again. This is the
   `clientSecret` app config value.

Secrets expire. Record the expiry date and rotate before then by creating a new
secret, updating the deployment, and deleting the old one.

## 4. Grant consent

A tenant administrator must consent to the app, or allow user consent for it,
before users in that tenant can connect. Until then, sign-in is blocked with an
"Need admin approval" screen.

- In your own tenant: **API permissions > Grant admin consent for <tenant>**.
- In a customer tenant: the customer's administrator consents, either through
  the sign-in prompt or an admin consent URL:

  ```text
  https://login.microsoftonline.com/<tenant-id>/adminconsent?client_id=<client-id>
  ```

Every tool acts only on the connected user's own mailbox.

## Redirect URIs

Add one **Web** redirect URI per environment, matching the Gestalt
`server.baseUrl` for that environment. For local development, `http://localhost`
is permitted for web redirects.

Gestalt derives OAuth callback URLs from `server.baseUrl`. If sign-in fails with
`AADSTS50011` (redirect URI mismatch), the error message includes the exact
`redirect_uri` Gestalt sent. Register that value exactly.

## Supply the credentials to Gestalt

Provide the client ID and secret as app config. Prefer environment-backed or
secret-store values over literals:

```yaml
apps:
  outlook:
    source: github.com/valon-technologies/gestalt-providers/app/outlook
    version: 0.0.1-alpha.1
    config:
      clientId: ${OUTLOOK_CLIENT_ID:-}
      clientSecret: ${OUTLOOK_CLIENT_SECRET:-}
```

## Verifying

1. Start Gestalt with the config above and connect Outlook from the UI or CLI.
2. Sign in with a work account from a consented tenant that has a Copilot
   license.
3. List the app's operations and run a read-only one against your mailbox.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `AADSTS50011` | Redirect URI not registered, or differs from `server.baseUrl`. |
| "Need admin approval" | Tenant admin has not consented to the app. |
| `AADSTS700016` / app not found in tenant | Registration is single-tenant; switch to multitenant. |
| `AADSTS7000215` / invalid client secret | Wrong or expired secret; use the secret Value, not the secret ID. |
| Agent 365 Tools missing from API permissions | Tenant not enrolled in Agent 365 / Frontier. |
| Sign-in works but mail calls fail | Missing Copilot license, or tenant not enrolled in Agent 365. |
