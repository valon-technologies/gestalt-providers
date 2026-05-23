# Intercom

Read and update contacts, companies, conversations, and notes.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  intercom:
    source: github.com/valon-technologies/gestalt-providers/plugins/intercom
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Intercom OpenAPI specification. Exposes a
broad set of operations spanning contacts, companies, conversations, tickets,
articles, tags, notes, data attributes, events, segments, and more.

Authenticates with a manually provided API access token.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  intercom:
    source: github.com/valon-technologies/gestalt-providers/plugins/intercom
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials; mode `user`.
  - Credential fields: `token`.
  - `token`: Provide an Intercom API access token.

Operation surfaces: OpenAPI.

Representative operations include:

- `ListContacts`
- `ArchiveContact`
- `BlockContact`
- `CreateContact`
- `DeleteContact`
- `ListAttachedContacts`
- `ListAttachedSegmentsForCompanies`
- `ListContacts`
- `MergeContact`
- `RetrieveACompanyById`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: intercom
        operation: ListContacts
```

Example `ListContacts` call:

```ts
await invoker.invoke("intercom", "ListContacts", { per_page: 25 });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
