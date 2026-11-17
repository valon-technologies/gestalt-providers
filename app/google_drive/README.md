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

`files.get` and `files.list` accept the optional Google Drive `fields` query
parameter for a partial response. Drive's default file response omits version,
modification time, and download capability. Callers that need those fields must
request them explicitly; for example, use
`id,kind,name,mimeType,modifiedTime,version,trashed,parents,webViewLink,capabilities(canDownload)`
for `files.get`, or prefix the file selection with `nextPageToken,files(...)`
for `files.list`. File access remains governed by the connected Google identity.

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `files.list` call:

```ts
await app.invoke("google_drive", "files.list", { pageSize: 10, q: "mimeType != 'application/vnd.google-apps.folder'" });
```

### File metadata and downloads

`files.get` continues to return file metadata when `alt` is omitted (or set to
`"json"`):

```ts
const metadata = await app.invoke("google_drive", "files.get", {
  fileId: "FILE_ID",
});
```

To download a non-Google Workspace file, set `alt: "media"` and use
`invokeRaw` so the response body is returned as bytes rather than decoded as
JSON:

```ts
const result = await app.invokeRaw({
  app: "google_drive",
  operation: "files.get",
  params: {
    fileId: "FILE_ID",
    alt: "media",
  },
});

const bytes = result.body;
```

The response preserves the upstream status, headers, and content bytes. Google
Workspace documents (Docs, Sheets, and Slides) should be downloaded with
`files.export` and an appropriate `mimeType` instead. Only use
`acknowledgeAbuse: true` after warning the user about the download risk.

### Upload local file bytes

`files.create` is metadata-only. Use `files.generateIds` followed by
`files.beginUpload` for a resumable binary upload. These operations use the
connected caller's existing Drive OAuth connection and request no new scope.

```ts
const { ids } = await app.invoke("google_drive", "files.generateIds", {
  count: 1, space: "drive", type: "files",
});
// Retain this ID in a private upload receipt before starting the session.
const started = await app.invokeRaw({
  app: "google_drive",
  operation: "files.beginUpload",
  params: {
    uploadType: "resumable",
    supportsAllDrives: true,
    id: ids[0],
    name: "Agreement.pdf",
    mimeType: "application/pdf",
    parents: ["AUTHORIZED_FOLDER_ID"],
    "X-Upload-Content-Type": "application/pdf",
    "X-Upload-Content-Length": bytes.byteLength,
  },
});
// started.status must be 200; its empty body is not a JSON file resource.
const location = Object.entries(started.headers)
  .find(([name]) => name.toLowerCase() === "location")?.[1].values[0];
```

The `Location` URI is a secret upload capability, valid for one week. Do not
print it, put it in a shared record, or forward the Gestalt bearer token to it.
Validate that it is the expected HTTPS Google Drive upload endpoint, then PUT
the exact original bytes to the returned session URI. The session URI carries
the upload authorization; Google's documented single-request PUT uses no
additional Authorization header. A completed upload returns the file resource.
Check `files.get` for the retained file ID, size, name, and checksum before
reporting completion or attaching it to another record. A local SHA-256 alone
does not verify server content.

The initial request does not change sharing permissions. Files inherit access
from the destination folder, so the caller must explicitly choose an authorized
folder. This operation creates new files; it does not overwrite existing files
or add revisions to an existing Drive file.

After an uncertain response, reconcile the retained file ID and existing session
before trying again. Do not allocate another file ID or restart a new upload
blindly. Google returns `308 Resume Incomplete` for unfinished sessions; follow
the documented status/Range procedure rather than claiming success. The provider
passes upstream status, headers, and body through `invokeRaw`.

Offline upload contract checks:

```sh
python3 -m unittest discover -s app/google_drive -p 'test_*.py' -v
```

The checks require PyYAML. They verify route preservation and the declarative
upload contract; no production upload is performed by these tests.

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
- [Download and export files](https://developers.google.com/workspace/drive/api/guides/manage-downloads)
- [Upload file data and resume interrupted uploads](https://developers.google.com/workspace/drive/api/guides/manage-uploads)
