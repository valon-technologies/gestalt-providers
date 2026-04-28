# Slack

Read public and private conversations, DMs, and group DMs; send messages; and manage channels.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  slack:
    source: github.com/valon-technologies/gestalt-providers/plugins/slack
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

Slack Events API agent replies use the app's bot token. Configure it as a
provider secret-backed value:

```yaml
plugins:
  slack:
    config:
      bot:
        token:
          secret:
            provider: secrets
            name: slack-bot-token
```

## Capabilities

Source-backed provider implemented in Python with both a REST surface and an
[MCP](https://modelcontextprotocol.io/) surface. Exposes operations for listing
and creating channels, reading message history and threads, sending and
scheduling messages, searching messages, managing reactions, setting channel
topics, inviting users, creating canvases, building thread context, and reading
Slack file or image contents.

Authenticates with Slack OAuth 2.0 (user scope).

The requested scopes cover public channels, private channels, direct messages,
and multi-person direct messages. That matches the provider's current
conversation history, thread, search, and message URL lookup behavior.

The provider also exposes a Slack Events API ingress. Slack owns event
verification, Slack-user subject resolution, bot-token replies, and optional
Slack event routing. Agent selection, prompts, tools, and completion delivery
belong in workflow configuration.

```yaml
plugins:
  slack:
    source: https://github.com/valon-technologies/gestalt-providers/releases/download/plugins/slack/v0.0.1-alpha.N/provider-release.yaml
    authorizationPolicy: platform
    invokes:
      - plugin: slack
        operation: events.reply
        credentialMode: none
      - plugin: slack
        operation: events.setStatus
        credentialMode: none
      - plugin: slack
        operation: events.deleteStatus
        credentialMode: none
      - plugin: slack
        operation: events.addReaction
        credentialMode: none
      - plugin: slack
        operation: events.removeReaction
        credentialMode: none
      - plugin: slack
        operation: events.setAssistantStatus
        credentialMode: none
      - plugin: slack
        operation: events.clearAssistantStatus
        credentialMode: none
      - plugin: slack
        operation: events.setThreadTitle
        credentialMode: none
      - plugin: slack
        operation: events.setSuggestedPrompts
        credentialMode: none
      - plugin: slack
        operation: events.startStream
        credentialMode: none
      - plugin: slack
        operation: events.appendStream
        credentialMode: none
      - plugin: slack
        operation: events.stopStream
        credentialMode: none
      - plugin: slack
        operation: conversations.getThreadContext
      - plugin: slack
        operation: files.get
      - plugin: workplaceHub
        operation: getMe
    config:
      bot:
        token:
          secret:
            provider: secrets
            name: slack-bot-token
      workflow:
        provider: indexeddb
      assistant:
        enabled: true
        status: is checking that...
        loadingMessages:
          - Reading the Slack thread
          - Calling available tools
        iconEmoji: ":hourglass_flowing_sand:"
        suggestedPrompts:
          title: Try next
          prompts:
            - title: Summarize this thread
              message: Summarize this thread and call out open questions.
```

Slack should send Events API requests to `POST /api/v1/slack/event`. The route
is declared in `manifest.yaml` under `spec.http.event`, validates Slack HMAC
signatures with `SLACK_SIGNING_SECRET`, resolves the Slack team/user through the
managed `external_identity` authorization relationship, and publishes a
`com.valon.slack.event` workflow event as the resolved Gestalt subject.

`events.handle`, `events.reply`, `events.setStatus`, `events.deleteStatus`,
`events.addReaction`, `events.removeReaction`, the native assistant helpers,
and the native stream helpers are hidden operations (`visible: false`).
`events.handle` is invoked by the signed Slack webhook binding. It publishes
safe public Slack event fields and keeps the opaque `reply_ref` in workflow
`private_input`. Completion delivery can call `slack.events.reply` with that
`reply_ref` and response text; the provider validates that the ref belongs to
the invoking Gestalt subject before posting to Slack with the configured bot
token. The same `reply_ref` scopes progress statuses, native assistant updates,
streaming replies, suggested prompts, thread titles, and reactions to the source
event channel, so event replies never need raw `chat.postMessage` access.

Example workflow event trigger:

```yaml
workflows:
  eventTriggers:
    slack-agent:
      provider: indexeddb
      match:
        source: slack
        type: com.valon.slack.event
      target:
        agent:
          provider: simple
          model: deep
          prompt: |
            Slack event from {{ trigger.event.data.user_id }}:
            {{ trigger.event.data.text }}
      completion:
        onSuccess:
          bestEffort: true
          plugin:
            name: slack
            operation: events.reply
            input:
              reply_ref: "{{ private.reply_ref }}"
              text: "{{ result.body }}"
        onFailure:
          bestEffort: true
          plugin:
            name: slack
            operation: events.reply
            input:
              reply_ref: "{{ private.reply_ref }}"
              text: "I hit an error: {{ error.message }}"
```

Workflow completion helper examples:

```json
{"reply_ref":"...","text":"I'll check that now."}
```

Call `slack.events.setStatus` without `status_ts` to create a progress message:

```json
{"reply_ref":"...","text":"Checking deployment status..."}
```

Use the returned `status_ts` to update or delete the same status:

```json
{"reply_ref":"...","status_ts":"1712161830.000400","text":"Still checking logs..."}
```

Use `slack.events.addReaction` or `slack.events.removeReaction` to mark the
source message:

```json
{"reply_ref":"...","name":"eyes"}
```

Use `slack.events.setAssistantStatus` for Slack's native assistant
typing/loading indicator. Passing an empty status, or calling
`slack.events.clearAssistantStatus`, clears it:

```json
{
  "reply_ref": "...",
  "status": "is checking deployment status",
  "loading_messages": ["Reading the thread", "Checking deploys"],
  "icon_emoji": ":hourglass_flowing_sand:"
}
```

Use `slack.events.setThreadTitle` and `slack.events.setSuggestedPrompts` for
native assistant thread metadata:

```json
{"reply_ref":"...","title":"Deploy status"}
```

```json
{
  "reply_ref": "...",
  "title": "Try next",
  "prompts": [
    {"title": "Summarize deploys", "message": "Summarize the latest deploy status"}
  ]
}
```

Use `slack.events.startStream`, `slack.events.appendStream`, and
`slack.events.stopStream` for Slack's native streaming response UI:

```json
{"reply_ref":"...","markdown_text":"Starting deploy checks"}
```

```json
{"reply_ref":"...","stream_ts":"1712161831.000500","markdown_text":"Still checking"}
```

```json
{"reply_ref":"...","stream_ts":"1712161831.000500","markdown_text":"Done"}
```

`slack.conversations.getThreadContext` builds a thread-shaped payload with
normalized messages, mentions, participants, and attached Slack file metadata:

```json
{
  "channel": "C0123456789",
  "ts": "1712161829.000300",
  "cursor": "",
  "limit": 15,
  "include_user_info": true,
  "include_file_content": true,
  "max_file_bytes": 200000
}
```

`slack.files.get` accepts either a `file_id` or Slack `url_private` and returns
metadata plus bounded content. Caller-supplied private URLs must be HTTPS Slack
file URLs; authenticated downloads reject redirects to non-Slack hosts. Text
files are returned as UTF-8 text; images and other binary files are returned as
base64:

```json
{"file_id":"F0123456789","include_content":true,"max_bytes":200000}
```

If `agent.routes` is omitted, the provider uses its default behavior:
`app_mention` events and direct-message `message` events publish workflow events.
Plain channel messages are ignored unless a route explicitly opts them in.
For the native Slack assistant experience, enable the app's Agents & AI Apps
features in Slack, add the bot `assistant:write` scope, and subscribe the bot to
`assistant_thread_started`, `assistant_thread_context_changed`, and `message.im`
events in addition to `app_mention`.

To publish only selected Slack channels or event types, add `agent.routes`.
Matching routes attach `agent_route_id` to the public workflow event data; the
workflow target can use that field to choose prompts or behavior.

```yaml
plugins:
  slack:
    source: https://github.com/valon-technologies/gestalt-providers/releases/download/plugins/slack/v0.0.1-alpha.N/provider-release.yaml
    authorizationPolicy: platform
    invokes:
      - plugin: slack
        operation: events.reply
        credentialMode: none
      - plugin: slack
        operation: events.setStatus
        credentialMode: none
      - plugin: slack
        operation: events.setAssistantStatus
        credentialMode: none
      - plugin: slack
        operation: events.clearAssistantStatus
        credentialMode: none
      - plugin: slack
        operation: events.setThreadTitle
        credentialMode: none
      - plugin: slack
        operation: events.setSuggestedPrompts
        credentialMode: none
      - plugin: slack
        operation: events.startStream
        credentialMode: none
      - plugin: slack
        operation: events.appendStream
        credentialMode: none
      - plugin: slack
        operation: events.stopStream
        credentialMode: none
      - plugin: slack
        operation: conversations.getThreadContext
      - plugin: slack
        operation: files.get
      - plugin: workplaceHub
        operation: getMe
      - plugin: deploymentViewer
        operation: status
    config:
      bot:
        token:
          secret:
            provider: secrets
            name: slack-bot-token
      workflow:
        provider: indexeddb
      agent:
        routes:
          - id: workplace-help
            match:
              channels:
                - C0123456789
              eventTypes:
                - app_mention
                - message
          - id: deploy-help
            match:
              channels:
                - C9876543210
              eventTypes:
                - app_mention
```

When `agent.routes` is present, only matching routes publish workflow events. Match
rules support singular or plural forms of `team`, `channel`, `channelType`,
`eventType`, and `user`.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
