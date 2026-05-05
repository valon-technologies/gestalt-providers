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
        email: google-groups-ingest@valon.com
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
  { q: "to:athena-implementation@valon.com newer_than:7d" },
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
        subjectEmail: google-groups-ingest@valon.com
        serviceAccountEmail: valon-tools-gmail-ingest@example-project.iam.gserviceaccount.com
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

Source-backed provider implemented in Python with an OpenAPI surface. Exposes
Gmail API operations for listing, reading, updating, trashing, sending, and
drafting messages; managing labels; reading threads; and retrieving the user's
profile.

Also includes source-backed operations for sending a message; creating,
updating, and sending drafts; replying to an existing message; and forwarding a
message without requiring callers to build raw Gmail MIME payloads.

Authenticates with Google OAuth 2.0.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
