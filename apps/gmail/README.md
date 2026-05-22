# Gmail

Read, send, and manage Gmail messages, threads, drafts, and labels.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  gmail:
    source: github.com/valon-technologies/gestalt-providers/apps/gmail
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

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
    source: github.com/valon-technologies/gestalt-providers/apps/gmail
    version: ...
    config:
      clientId: ${GMAIL_CLIENT_ID}
      clientSecret: ${GMAIL_CLIENT_SECRET}
```

Provider config fields:

- `clientId` (required): Google OAuth client ID for Gmail.
- `clientSecret` (required): Google OAuth client secret for Gmail.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `https://www.googleapis.com/auth/gmail.modify`, `https://www.googleapis.com/auth/gmail.compose`.

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
