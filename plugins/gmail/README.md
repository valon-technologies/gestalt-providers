# Gmail

Read, send, and manage Gmail messages, threads, drafts, and labels.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  gmail:
    source: github.com/valon-technologies/gestalt-providers/plugins/gmail
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Internal Platform Ingestion

Most installations should use the default user OAuth connection. For
provider-owned ingestion jobs, the Gmail provider also supports an internal
platform mailbox pattern. The mailbox's Gmail visibility determines what can be
read, including Google Groups mail delivered to that mailbox.

`mode: platform` and `exposure: internal` are intentionally different controls:
`mode` means the credential is deployer-owned, while `exposure` keeps that
binding out of public caller selection.

### `platformConnection`

Use `platformConnection` when Gestalt owns a platform OAuth credential for a
real Gmail or Workspace mailbox. This is the preferred internal ingestion shape
because Gestalt resolves and refreshes the mailbox token, while the Gmail
provider verifies the token belongs to the configured mailbox and only permits
the configured read operations.

```yaml
connections:
  gmail-platform-mailbox:
    mode: platform
    auth:
      type: oauth2
      grantType: refresh_token
      tokenUrl: https://oauth2.googleapis.com/token
      clientId:
        secret:
          provider: secrets
          name: google-oauth-client-id
      clientSecret:
        secret:
          provider: secrets
          name: google-oauth-client-secret
      refreshToken:
        secret:
          provider: secrets
          name: gmail-platform-mailbox-refresh-token

plugins:
  gmail:
    source: github.com/valon-technologies/gestalt-providers/plugins/gmail
    version: 0.0.1-alpha.16
    config:
      clientId:
        secret:
          provider: secrets
          name: google-oauth-client-id
      clientSecret:
        secret:
          provider: secrets
          name: google-oauth-client-secret
      platformConnection:
        enabled: true
        email: groups-ingest@example.com
        operations:
          - messages.list
          - messages.get
          - messages.attachments.get
          - threads.get
          - labels.list
          - getProfile
    connections:
      platform:
        ref: gmail-platform-mailbox
        exposure: internal
```

Internal callers declare the Gmail operations they may invoke and select the
internal connection at call time:

```yaml
plugins:
  brain:
    invokes:
      - plugin: gmail
        operation: messages.list
      - plugin: gmail
        operation: threads.get
      - plugin: gmail
        operation: messages.attachments.get
```

```ts
await invoker.invoke(
  "gmail",
  "messages.list",
  { q: "to:group@example.com newer_than:7d" },
  { connection: "platform" },
);
```

### `platformIdentity`

Use `platformIdentity` only when the provider must mint its own Google
domain-wide-delegation token from a service account. The service account signs
the assertion, but `subjectEmail` is still the real mailbox being impersonated
and its mailbox or Google Groups access controls what Gmail can return.

```yaml
plugins:
  gmail:
    source: github.com/valon-technologies/gestalt-providers/plugins/gmail
    version: 0.0.1-alpha.16
    config:
      clientId:
        secret:
          provider: secrets
          name: google-oauth-client-id
      clientSecret:
        secret:
          provider: secrets
          name: google-oauth-client-secret
      platformIdentity:
        enabled: true
        subjectEmail: groups-ingest@example.com
        serviceAccountEmail: gmail-ingest@example-project.iam.gserviceaccount.com
        scopes:
          - https://www.googleapis.com/auth/gmail.readonly
        operations:
          - messages.list
          - messages.attachments.get
          - threads.get
```

Callers using `platformIdentity` should declare the Gmail invoke grants with
`credentialMode: none`, because Gestalt should not resolve a user or platform
credential for those calls. `platformConnection` and `platformIdentity` are
mutually exclusive; enable only one.

## Capabilities

Provider with an OpenAPI surface and helper operations. Exposes
Gmail API operations for listing, reading, updating, trashing, sending, and
drafting messages; managing labels; reading threads; and retrieving the user's
profile.

Also includes helper operations for sending a message; creating,
updating, and sending drafts; replying to an existing message; and forwarding a
message without requiring callers to build raw Gmail MIME payloads.

Authenticates with Google OAuth 2.0.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  gmail:
    source: github.com/valon-technologies/gestalt-providers/plugins/gmail
    version: ...
    config:
      clientId: ${GMAIL_CLIENT_ID}
      clientSecret: ${GMAIL_CLIENT_SECRET}
      platformIdentity: ...
      platformConnection: ...
```

Provider config fields:

- `clientId` (required): Google OAuth client ID for Gmail.
- `clientSecret` (required): Google OAuth client secret for Gmail.
- `platformIdentity` (optional): Internal Gmail identity used for provider-managed ingestion.
- `platformConnection` (optional): Internal Gmail platform connection used for provider-managed ingestion.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `https://www.googleapis.com/auth/gmail.modify`, `https://www.googleapis.com/auth/gmail.compose`.
- `platform` uses manual credentials; mode `platform`; exposure `internal`.

Operation surfaces: OpenAPI.

Representative operations include:

- `messages.send`
- `messages.reply`
- `messages.forward`
- `messages.list`
- `messages.get`
- `messages.attachments.get`
- `messages.trash`
- `messages.modify`
- `drafts.list`
- `drafts.get`
- `drafts.delete`
- `drafts.create`
- `drafts.update`
- `drafts.send`

- Platform mailbox configuration is for internal ingestion jobs. Regular user calls should use the default OAuth connection.

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: gmail
        operation: messages.send
```

Example `messages.send` call:

```ts
await invoker.invoke("gmail", "messages.send", {
  to: ["recipient@example.com"],
  subject: "Status update",
  text_body: "The report is ready.",
});
```

Example `messages.reply` call:

```ts
await invoker.invoke("gmail", "messages.reply", {
  message_id: "18c1234567890abc",
  text_body: "Thanks for the context.",
  reply_all: true,
});
```

Create and send a draft when the caller needs review or later delivery:

```ts
const draft = await invoker.invoke("gmail", "drafts.create", {
  to: ["recipient@example.com"],
  subject: "Draft update",
  text_body: "Please review before sending.",
});

await invoker.invoke("gmail", "drafts.send", {
  draft_id: draft.id,
});
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
