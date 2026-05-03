# Zendesk

Support tickets, users, organizations, macros, automations, triggers, and SLA policies.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  zendesk:
    source: github.com/valon-technologies/gestalt-providers/plugins/zendesk
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
plugins:
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
plugins:
  zendesk:
    connections:
      default:
        params:
          subdomain: mycompany
```

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
- [Zendesk Support API Reference](https://developer.zendesk.com/api-reference/ticketing/introduction/)
