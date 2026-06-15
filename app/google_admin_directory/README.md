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

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  google_admin_directory:
    source: github.com/valon-technologies/gestalt-providers/app/google_admin_directory
    version: ...
    config:
      clientId: ${GOOGLE_ADMIN_DIRECTORY_CLIENT_ID}
      clientSecret: ${GOOGLE_ADMIN_DIRECTORY_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Provider configuration value.
- `clientSecret` (required): Provider configuration value.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `https://www.googleapis.com/auth/admin.directory.customer`, `https://www.googleapis.com/auth/admin.directory.device.chromeos`, `https://www.googleapis.com/auth/admin.directory.device.mobile`, `https://www.googleapis.com/auth/admin.directory.device.mobile.action`, `https://www.googleapis.com/auth/admin.directory.domain`, `https://www.googleapis.com/auth/admin.directory.group`, `https://www.googleapis.com/auth/admin.directory.group.member`, `https://www.googleapis.com/auth/admin.directory.orgunit`, `https://www.googleapis.com/auth/admin.directory.resource.calendar`, `https://www.googleapis.com/auth/admin.directory.rolemanagement`, `https://www.googleapis.com/auth/admin.directory.user`, `https://www.googleapis.com/auth/admin.directory.user.alias`, `https://www.googleapis.com/auth/admin.directory.user.security`, `https://www.googleapis.com/auth/admin.directory.userschema`, `https://www.googleapis.com/auth/admin.chrome.printers`.

Operation surfaces: OpenAPI.

Representative operations include:

- `directory.users.list`
- `admin.channels.stop`
- `admin.customer.devices.chromeos.commands.get`
- `admin.customer.devices.chromeos.issueCommand`
- `admin.customers.chrome.printServers.batchCreatePrintServers`
- `admin.customers.chrome.printServers.batchDeletePrintServers`
- `admin.customers.chrome.printServers.create`
- `admin.customers.chrome.printServers.delete`
- `admin.customers.chrome.printServers.get`
- `admin.customers.chrome.printServers.list`

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `directory.users.list` call:

```ts
await app.invoke("google_admin_directory", "directory.users.list", { customer: "my_customer", maxResults: 10 });
```
