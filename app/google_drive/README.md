# Google Drive

Read, create, update, and share files in Google Drive.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  google_drive:
    source: github.com/valon-technologies/gestalt-providers/app/google_drive
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Google Drive OpenAPI specification. Exposes
operations for listing, getting, creating, updating, deleting, copying, and
exporting files, managing permissions, and full comment/reply CRUD on files.

Authenticates with Google OAuth 2.0.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  google_drive:
    source: github.com/valon-technologies/gestalt-providers/app/google_drive
    version: ...
    config:
      clientId: ${GOOGLE_DRIVE_CLIENT_ID}
      clientSecret: ${GOOGLE_DRIVE_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Google OAuth client ID for Google Drive.
- `clientSecret` (required): Google OAuth client secret for Google Drive.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `https://www.googleapis.com/auth/drive`.

Operation surfaces: OpenAPI.

Representative operations include:

- `files.list`
- `files.get`
- `files.create`
- `files.update`
- `files.delete`
- `files.copy`
- `files.export`
- `permissions.list`
- `permissions.create`
- `permissions.update`
- `permissions.delete`
- `about.get`
- `comments.list`
- `comments.get`
- `comments.create`
- `comments.update`
- `comments.delete`
- `replies.list`
- `replies.get`
- `replies.create`
- `replies.update`
- `replies.delete`

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `files.list` call:

```ts
await app.invoke("google_drive", "files.list", { pageSize: 10, q: "mimeType != 'application/vnd.google-apps.folder'" });
```

### Comments and replies

For all comment and reply operations except `comments.delete` and `replies.delete`, Google requires a `fields` query parameter specifying which fields to return. Omitting `fields` causes the API to return an error.

Comment resources expose anchored passage text as `quotedFileContent` (`{ mimeType, value }`), plus `content`, `author`, `replies`, and `resolved`. To resolve a comment, use `replies.create` with `action: "resolve"` — the `resolved` field on comments is read-only and cannot be set via `comments.update`.

List comments with anchored passage, author, and replies:

```ts
await app.invoke("google_drive", "comments.list", {
  fileId: "FILE_ID",
  fields: "comments(id,content,author,quotedFileContent,replies,resolved),nextPageToken",
  pageSize: 50,
});
```

Get a single comment:

```ts
await app.invoke("google_drive", "comments.get", {
  fileId: "FILE_ID",
  commentId: "COMMENT_ID",
  fields: "id,content,htmlContent,author,quotedFileContent,replies,resolved",
});
```

Create an unanchored comment:

```ts
await app.invoke("google_drive", "comments.create", {
  fileId: "FILE_ID",
  fields: "id,content,htmlContent,author,createdTime",
  content: "Please review this section.",
});
```

Edit comment text:

```ts
await app.invoke("google_drive", "comments.update", {
  fileId: "FILE_ID",
  commentId: "COMMENT_ID",
  fields: "id,content,modifiedTime",
  content: "Updated comment text.",
});
```

Delete a comment (no `fields` required):

```ts
await app.invoke("google_drive", "comments.delete", {
  fileId: "FILE_ID",
  commentId: "COMMENT_ID",
});
```

List replies on a comment:

```ts
await app.invoke("google_drive", "replies.list", {
  fileId: "FILE_ID",
  commentId: "COMMENT_ID",
  fields: "replies(id,content,author,action,createdTime),nextPageToken",
  pageSize: 50,
});
```

Get a single reply:

```ts
await app.invoke("google_drive", "replies.get", {
  fileId: "FILE_ID",
  commentId: "COMMENT_ID",
  replyId: "REPLY_ID",
  fields: "id,content,htmlContent,author,action",
});
```

Create a reply:

```ts
await app.invoke("google_drive", "replies.create", {
  fileId: "FILE_ID",
  commentId: "COMMENT_ID",
  fields: "id,content,author,createdTime",
  content: "Thanks, will update.",
});
```

Resolve a comment via reply (not `comments.update`):

```ts
await app.invoke("google_drive", "replies.create", {
  fileId: "FILE_ID",
  commentId: "COMMENT_ID",
  fields: "id,action,content,comment",
  content: "Addressed in latest revision.",
  action: "resolve",
});
```

Update a reply:

```ts
await app.invoke("google_drive", "replies.update", {
  fileId: "FILE_ID",
  commentId: "COMMENT_ID",
  replyId: "REPLY_ID",
  fields: "id,content,modifiedTime",
  content: "Updated reply text.",
});
```

Delete a reply (no `fields` required):

```ts
await app.invoke("google_drive", "replies.delete", {
  fileId: "FILE_ID",
  commentId: "COMMENT_ID",
  replyId: "REPLY_ID",
});
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
- [Manage comments and replies](https://developers.google.com/drive/api/guides/manage-comments)
