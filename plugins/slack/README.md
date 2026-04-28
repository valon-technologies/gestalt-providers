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

The provider also exposes a Slack Events API ingress. For the common case where
one Slack app has one default bot behavior, configure the agent once:

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
        operation: interactions.request
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
      agent:
        provider: simple
        model: deep
        systemPrompt: Use Slack formatting and keep replies concise.
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

Slack event handling always uses the workflow manager for durable per-thread
dispatch:

```yaml
plugins:
  slack:
    config:
      workflow:
        provider: local
      agent:
        provider: simple
        model: deep
```

`events.handle` calls `WorkflowManager.SignalOrStartRun(provider_name=workflow.provider,
workflow_key="slack:${team_id}:${channel_id}:${root_ts}", signal.name="slack.event")`.
The workflow target is an agent target built from the `agent` and `agent.routes`
configuration. The Slack event, `reply_ref`, and generated user prompt are
delivered in the signal payload, so later Slack messages in the same thread
signal the existing keyed run instead of replacing its target or authorization
context.

Slack should send Events API requests to `POST /api/v1/slack/event` and Slack
interactivity requests to `POST /api/v1/slack/interactions`. Both routes are
declared in `manifest.yaml` under `spec.http`, validate Slack HMAC signatures
with `SLACK_SIGNING_SECRET`, and resolve the Slack team/user through the managed
`external_identity` authorization relationship. Workflow-started agent runs use
`toolSource=native_search` with scoped Slack event helper refs plus native tool
search for the resolved Gestalt user.

`events.handle`, `events.reply`, `events.setStatus`, `events.deleteStatus`,
`events.addReaction`, `events.removeReaction`, the native assistant helpers,
the native stream helpers, and the interaction helpers are hidden operations
(`visible: false`).
`events.handle` is invoked by the signed Slack webhook binding. It starts an
or signals a keyed workflow run and passes an opaque `reply_ref` in the signal
payload's user prompt. The agent should call `slack.events.reply` with that
`reply_ref` and response text; the provider validates that the ref belongs to
the invoking Gestalt subject before posting to Slack with the configured bot
token. The same `reply_ref` scopes progress statuses, native assistant updates,
streaming replies, suggested prompts, thread titles, and reactions to the source
event channel, so the agent never needs raw `chat.postMessage` access for event
replies.

Agent-facing event helper examples:

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

Use `slack.interactions.request` to post signed Slack buttons.
When the Slack user clicks a button, the interactivity webhook validates the
signed metadata and calls `WorkflowManager.SignalOrStartRun` with
`signal.name="slack.interaction"` for the same `workflow_key`:

```json
{
  "reply_ref": "...",
  "text": "Approve deployment?",
  "actions": [
    {"id": "approve", "label": "Approve", "value": "approved", "style": "primary"},
    {"id": "reject", "label": "Reject", "value": "rejected", "style": "danger"}
  ]
}
```

Interaction refs are scoped to the Slack user who received the original
`reply_ref`. Broader delegated approval semantics need a separate authorization
model and are intentionally not inferred from button payloads.

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
`app_mention` events and direct-message `message` events start or signal a
workflow run.
Plain channel messages are ignored unless a route explicitly opts them in.
For the native Slack assistant experience, enable the app's Agents & AI Apps
features in Slack, add the bot `assistant:write` scope, and subscribe the bot to
`assistant_thread_started`, `assistant_thread_context_changed`, and `message.im`
events in addition to `app_mention`.

To use different prompts for different Slack channels or event types, add
`agent.routes`:

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
      agent:
        provider: simple
        model: deep
        systemPrompt: Use Slack formatting and keep replies concise.
        routes:
          - id: workplace-help
            match:
              channels:
                - C0123456789
              eventTypes:
                - app_mention
                - message
            agent:
              systemPrompt: Help employees with workplace questions.
          - id: deploy-help
            match:
              channels:
                - C9876543210
              eventTypes:
                - app_mention
            agent:
              systemPrompt: Help engineers inspect deployment status.
```

When `agent.routes` is present, only matching routes start or signal a workflow
run. Match rules support singular or plural forms of `team`, `channel`,
`channelType`, `eventType`, and `user`. Route-level `agent` fields override the
top-level agent settings, `prompt` is accepted as an alias for `systemPrompt`,
and `providerOptions` are merged with route-level values taking precedence.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
