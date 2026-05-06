# Extend

Document processing and extraction with Extend.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  extend:
    source: github.com/valon-technologies/gestalt-providers/plugins/extend
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
plugins:
  extend:
    source: github.com/valon-technologies/gestalt-providers/plugins/extend
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials; mode `user`.
  - Provide an Extend API key as a bearer token when creating the connection.
    Create one in Extend Developer Settings.

Managed request headers:

- `x-extend-api-version: 2026-02-09`

Operation surfaces: OpenAPI.

Representative operations include:

- `listFiles`
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

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: extend
        operation: listFiles
```

Example `listFiles` call:

```ts
await invoker.invoke("extend", "listFiles", { page_size: 20 });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
