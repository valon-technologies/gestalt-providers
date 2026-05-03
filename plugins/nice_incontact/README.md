# NICE CXone

Access NICE CXone / NICE inContact public REST APIs for Admin, Agent,
Authentication, Patron, Real-Time Data, Reporting, UserHub, Data Extraction,
Media Playback, Digital Engagement, Business Data, WFM, Recording, Interaction
Analytics, Privacy, and Data Policy workflows.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  nice_incontact:
    source: github.com/valon-technologies/gestalt-providers/plugins/nice_incontact
    version: ...
    config:
      clientId: ...
      clientSecret: ...
```

The OAuth connection uses NICE CXone OIDC:

- Authorization URL: `https://cxone.niceincontact.com/auth/authorize`
- Token URL: `https://cxone.niceincontact.com/auth/token`
- Scope: `openid`

Set the `api_host` connection parameter to the tenant API host returned by NICE
API discovery, for example `api-na1.niceincontact.com`. Do not include the URL
scheme.

ACD-family operations are generated with the current developer portal path
prefix `/inContactAPI/services/v34.0`. Digital Engagement operations are
generated with `/dfo/3.0`. UserHub, Data Extraction, Recording, WFM,
Interaction Analytics, Privacy, and Data Policy operations keep the versioned
paths published in their source documents.

## Capabilities

Declarative provider backed by a committed OpenAPI document generated from the
raw OpenAPI documents served by the NICE developer portal. The committed
`openapi.yaml` vendors the referenced NICE documents into a single standalone
OpenAPI 3.2.0 document with stable operation IDs.

The committed OpenAPI document is size-reduced for provider execution: verbose
descriptions, examples, schema titles, vendor extensions, and repeated operation
security declarations are omitted while operation summaries, parameters,
request bodies, responses, and schemas are retained.

The committed source inventory intentionally uses the current official NICE
developer portal documents instead of the older public `openapis/api-specs`
snapshot, which only covers a subset of the legacy API families and keeps
unresolved external Swagger 2.0 references.

Example operations include:

- `admin_admin_agents_api_docs.get_agents`
- `dataextraction_api_docs.extractdata`
- `digital_api_docs.gettags`
- `global_authentication_api.getcxoneconfig`

Backend access-key/password grant token exchange and automatic tenant discovery
are not implemented by this provider. Use NICE API discovery to determine the
tenant API host, then provide it as `api_host`.

## Maintenance

`sources.lock.json` records the official NICE source documents and checksums
used to build the committed `openapi.yaml`.

## Documentation

- [NICE CXone API Overview](https://developer.niceincontact.com/API)
- [NICE CXone Authentication and Discovery](https://developer.niceincontact.com/Documentation/GettingStarted)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
