# Google Admin Directory

Manage Google Workspace users, groups, organizational units, devices, roles, domains, resources, and schemas via the [Admin SDK Directory API](https://developers.google.com/admin-sdk/directory).

## Authentication

OAuth 2.0 with the following scopes:

- `admin.directory.customer`
- `admin.directory.device.chromeos`
- `admin.directory.device.mobile`
- `admin.directory.device.mobile.action`
- `admin.directory.domain`
- `admin.directory.group`
- `admin.directory.group.member`
- `admin.directory.orgunit`
- `admin.directory.resource.calendar`
- `admin.directory.rolemanagement`
- `admin.directory.user`
- `admin.directory.user.alias`
- `admin.directory.user.security`
- `admin.directory.userschema`
- `admin.chrome.printers`

## Operations

127 Directory API operations covering:

- **Users** — create, read, update, delete, make admin, sign out, undelete, manage photos, aliases, 2SV, verification codes
- **Groups** — create, read, update, delete, manage aliases and members
- **Org Units** — create, read, update, delete
- **Devices** — Chrome OS and mobile devices, actions, commands, move between OUs
- **Chrome Printers & Print Servers** — create, read, update, delete, batch operations, list models
- **Roles & Privileges** — create, read, update, delete roles; list privileges; manage role assignments
- **Domains & Domain Aliases** — create, read, update, delete
- **Calendar Resources** — buildings, calendars, features
- **Schemas** — custom user schemas
- **Tokens & ASPs** — manage authorized apps and application-specific passwords
- **Channels** — stop push notification channels

## OpenAPI Spec

Remote: `https://api.apis.guru/v2/specs/googleapis.com/admin/directory_v1/openapi.json`
