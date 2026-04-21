# Microsoft SharePoint

Browse SharePoint sites, document libraries, lists, pages, and file content
through Microsoft Graph.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  sharepoint:
    source: github.com/valon-technologies/gestalt-providers/plugins/sharepoint
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative OpenAPI-backed provider for common SharePoint Online workflows via
Microsoft Graph. The curated surface covers:

- site discovery
- document libraries and drive items
- file content download and overwrite
- SharePoint lists and list items
- site pages

This plugin uses delegated Microsoft OAuth 2.0 and targets the Microsoft Graph
`v1.0` REST surface rather than the legacy SharePoint `/_api/web` endpoints.

## Documentation

- [Microsoft Graph OpenAPI metadata](https://github.com/microsoftgraph/msgraph-metadata)
- [Working with SharePoint sites in Microsoft Graph](https://learn.microsoft.com/graph/api/resources/sharepoint)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
