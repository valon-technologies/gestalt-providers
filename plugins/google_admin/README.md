# Google Admin

Manage Google Workspace users, groups, organizational units, devices, roles,
resources, and data transfers via the Admin SDK.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  google_admin:
    source: github.com/valon-technologies/gestalt-providers/plugins/google_admin
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

This plugin surfaces Google Admin SDK operations from the Directory and Data
Transfer APIs via a merged OpenAPI spec.

**Directory API operations:**
- **Users**: list, get, create, update, patch, delete, undelete, makeAdmin,
  signOut; manage aliases, photos, ASPS, tokens, verification codes, 2SV
- **Groups**: list, get, create, update, patch, delete; manage aliases and
  members
- **Organizational Units**: list, get, create, update, patch, delete
- **Chrome OS Devices**: list, get, action, update, patch
- **Mobile Devices**: list, get, action, delete
- **Domains & Domain Aliases**: list, get, create, delete
- **Roles & Role Assignments**: list, get, create, update, patch, delete
- **Privileges**: list
- **User Schemas**: list, get, create, update, patch, delete
- **Resources**: manage buildings, calendar resources, and features
- **Customers**: get, update, patch

**Data Transfer API operations:**
- **Applications**: list, get
- **Transfers**: list, get, insert (create)

Authenticates with Google OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
- [Google Admin SDK Directory API](https://developers.google.com/admin-sdk/directory)
- [Google Admin SDK Data Transfer API](https://developers.google.com/admin-sdk/data-transfer)
