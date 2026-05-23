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

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  sharepoint:
    source: github.com/valon-technologies/gestalt-providers/plugins/sharepoint
    version: ...
    config:
      clientId: ${SHAREPOINT_CLIENT_ID}
      clientSecret: ${SHAREPOINT_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Microsoft Entra OAuth client ID for the SharePoint integration.
- `clientSecret` (required): Microsoft Entra OAuth client secret for the SharePoint integration.

Connections and authentication:

- `default` uses OAuth 2.0; mode `user`.
  - Requested scopes: `offline_access`, `Sites.ReadWrite.All`, `Files.ReadWrite.All`.

Operation surfaces: OpenAPI.

Representative operations include:

- `list_site_drives`
- `list_sites`
- `get_site`
- `get_site_default_drive`
- `list_site_drives`
- `get_drive_item`
- `list_drive_item_children`
- `download_drive_item_content`
- `overwrite_drive_item_content`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: sharepoint
        operation: list_site_drives
```

Example `list_site_drives` call:

```ts
await invoker.invoke("sharepoint", "list_site_drives", { site_id: "contoso.sharepoint.com,site-id,web-id" });
```

## Documentation
- [Microsoft Graph OpenAPI metadata](https://github.com/microsoftgraph/msgraph-metadata)
- [Working with SharePoint sites in Microsoft Graph](https://learn.microsoft.com/graph/api/resources/sharepoint)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
