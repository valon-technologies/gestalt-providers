# Zendesk

Support tickets, users, organizations, macros, automations, triggers, and SLA policies.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  zendesk:
    source: github.com/valon-technologies/gestalt-providers/app/zendesk
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the official Zendesk Support API OpenAPI
specification (v2). Exposes operations for managing tickets, users,
organizations, groups, views, macros, automations, triggers, SLA policies,
satisfaction ratings, attachments, and more.

The Zendesk subdomain is configured as a connection parameter so the provider
targets the correct Zendesk instance.

## Authentication

Authenticates with the Zendesk API via HTTP Basic Auth. The username is your
Zendesk agent email address and the password is an API token:

1. In Zendesk, go to **Admin Center > Apps and integrations > APIs > Zendesk
   API**.
2. Enable **Token access** and create a new token.
3. Copy the token into the connection credentials.

```yaml
apps:
  zendesk:
    connections:
      default:
        auth:
          type: manual
          credentials:
            - name: email
              label: Email address
              description: The email address of the Zendesk agent or admin account.
            - name: api_token
              label: API Token
              description: Zendesk API token with read and write access.
```

The provider maps **email** to the Basic Auth username and **api_token** to the
Basic Auth password.

## Connection Parameters

The provider requires the Zendesk `subdomain`. This is the first part of your
Zendesk URL: `https://<subdomain>.zendesk.com`.

```yaml
apps:
  zendesk:
    connections:
      default:
        params:
          subdomain: mycompany
```

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  zendesk:
    source: github.com/valon-technologies/gestalt-providers/app/zendesk
    version: ...
    connections:
      default:
        params:
          subdomain: "..."
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials; mode `subject`.
  - Credential fields: `email`, `api_token`.
  - `email`: The email address of a Zendesk agent or admin account.
  - `api_token`: Zendesk API token with read and write access. Create one in Admin Center > Apps and integrations > APIs > Zendesk API.
  - Connection params:
    - `subdomain` (required): Your Zendesk subdomain (e.g. mycompany in https://mycompany.zendesk.com).

Operation surfaces: OpenAPI.

Representative operations include:

- `ListTickets`
- `ListTickets`
- `ShowTicket`
- `CreateTicket`
- `UpdateTicket`
- `DeleteTicket`
- `TicketsShowMany`
- `TicketsCreateMany`
- `TicketsUpdateMany`
- `CountTickets`

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `ListTickets` call:

```ts
await app.invoke("zendesk", "ListTickets", { page: { size: 25 } });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
- [Zendesk Support API Reference](https://developer.zendesk.com/api-reference/ticketing/introduction/)
