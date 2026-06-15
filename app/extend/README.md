# Extend

Document processing and extraction with Extend.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  extend:
    source: github.com/valon-technologies/gestalt-providers/app/extend
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on a local OpenAPI specification. Exposes operations
for intelligent document processing, data extraction, classification, and
editing through the Extend platform.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  extend:
    source: github.com/valon-technologies/gestalt-providers/app/extend
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials; mode `subject`.
  - Provide an Extend API key as a bearer token when creating the connection.
    Create one in Extend Developer Settings.

Managed request headers:

- `x-extend-api-version: 2026-02-09`

Operation surfaces: OpenAPI.

Representative operations include:

- `list_files`
- `upload_file`
- `list_files`
- `get_file`
- `delete_file`
- `parse_file`
- `parse_file_async`
- `get_parse_run`
- `delete_parse_run`

- The provider sends the managed `x-extend-api-version` header declared in the manifest.

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `list_files` call:

```ts
await app.invoke("extend", "list_files", { page_size: 20 });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
