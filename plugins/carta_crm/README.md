# Carta CRM

Access Carta CRM public API endpoints for deals, companies, investors,
fundraisings, contacts, notes, attachments, linking, and workspace
configuration.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  carta_crm:
    source: github.com/valon-technologies/gestalt-providers/plugins/carta_crm
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

OpenAPI-backed provider using Carta CRM's official public OpenAPI specification,
vendored locally for stability. The provider exposes the full public CRM
surface, including:

- Deals and deal metadata
- Companies and company attachments
- Investors and investor attachments
- Fundraisings and fundraising attachments
- Contacts and relationship linking endpoints
- Notes and note-linking endpoints
- Organization configuration endpoints

Authenticate with a Carta CRM API key. The plugin passes that key through the
`Authorization` header expected by Carta CRM.

## Documentation

- [Carta CRM OpenAPI Spec](https://docs.carta.com/crm/openapi/public-swagger.json)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
