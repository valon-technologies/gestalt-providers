# Slack

Read public and private conversations, DMs, and group DMs; send messages; and manage channels.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  slack:
    source: github.com/valon-technologies/gestalt-providers/app/slack
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

Slack Events API agent replies and declarative REST operations that run as the
bot use the app's bot token. Generic workflow event publishing does not require
a bot token. Configure the token as both a provider secret-backed value for
event helper code and a subject-owned `bot` connection for runtime-only REST
calls when agent replies or bot REST operations are enabled:

```yaml
connections:
  slack-bot:
    mode: subject
    auth:
      type: bearer

apps:
  slack:
    config:
      bot:
        userId: U0123456789
        token:
          secret:
            provider: secrets
            name: slack-bot-token
    connections:
      bot:
        ref: slack-bot
```

Because Gestalt does not perform Slack bot OAuth for this connection, the Slack
app that issues `slack-bot-token` must be granted the bot scopes needed by the
enabled behaviors. Provision the bot token credential onto the service account
or other subject that invokes bot REST operations. The full scope set used by
this provider's bot behaviors is:
`app_mentions:read`, `channels:read`, `channels:history`, `groups:read`,
`groups:history`, `im:read`, `im:history`, `mpim:read`, `mpim:history`,
`im:write`, `mpim:write`, `users:read`, `files:read`, `files:write`, `chat:write`,
`assistant:write`, `reactions:write`, `channels:manage`, `groups:write`, and
`canvases:write`.

## Capabilities

Provider with both a REST surface and an
[MCP](https://modelcontextprotocol.io/) surface. Exposes operations for listing
and creating channels, reading message history and threads, sending and
scheduling messages, opening or resuming DMs, searching messages, managing
reactions, setting channel topics, inviting users, creating canvases, building
thread context, reading Slack file or image contents, and uploading Slack files.

Authenticates user operations with Slack OAuth 2.0 (user scope). Operations with
fixed bot behavior use the subject-owned `bot` bearer connection.
Public REST and MCP callers use the user OAuth connection by default; hidden
selector parameters such as `actor` are runtime-only and are not part of the
public invocation contract.

The requested scopes cover public channels, private channels, direct messages,
and multi-person direct messages. That matches the provider's current
conversation history, thread, search, and message URL lookup behavior.

The provider also exposes a Slack Events API ingress. For the common case where
one Slack app has one default bot behavior, configure the agent once:

```yaml
apps:
  slack:
    source: https://github.com/valon-technologies/gestalt-providers/releases/download/app/slack/v0.0.1-alpha.N/provider-release.yaml
    authorizationPolicy: platform
    invokes:
      - app: slack
        operation: events.reply
        credentialMode: none
      - app: slack
        operation: events.replySessionStarted
        credentialMode: none
      - app: slack
        operation: events.setStatus
        credentialMode: none
      - app: slack
        operation: events.deleteStatus
        credentialMode: none
      - app: slack
        operation: events.addReaction
        credentialMode: none
      - app: slack
        operation: events.removeReaction
        credentialMode: none
      - app: slack
        operation: events.setAssistantStatus
        credentialMode: none
      - app: slack
        operation: events.clearAssistantStatus
        credentialMode: none
      - app: slack
        operation: events.setThreadTitle
        credentialMode: none
      - app: slack
        operation: events.setSuggestedPrompts
        credentialMode: none
      - app: slack
        operation: interactions.request
        credentialMode: none
      - app: slack
        operation: conversations.getThreadContext
      - app: slack
        operation: files.get
    config:
      bot:
        token:
          secret:
            provider: secrets
            name: slack-bot-token
      acknowledgement:
        reaction: eyes
      workflow:
        provider: local
        definitionId: slack-default-agent
      agent:
        threadContext:
          enabled: true
          maxMessages: 200
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
apps:
  slack:
    config:
      workflow:
        provider: local
        definitionId: slack-default-agent
      agent:
        threadContext:
          enabled: true
          maxMessages: 200
```

When `acknowledgement.reaction` is configured, `events.handle` adds that
reaction to the source Slack message after the workflow provider accepts the
event. Emoji names may be written with or without colons, and Slack's
`already_reacted` response is treated as idempotent success.

`events.handle` calls `req.workflows().signal_or_start_run(WorkflowSignalOrStartRun(...))`
with `provider_name=workflow.provider`,
`workflow_key="slack:${team_id}:${channel_id}:${root_ts}"`, and
`signal.name="slack.event"`. The request references a global workflow
definition with `workflow.definitionId`. The Slack event, `reply_ref`, and
generated user prompt are delivered in the signal payload, so later Slack
messages in the same thread signal the existing keyed run instead of replacing
its workflow definition or authorization context. The global workflow should
call Slack helper operations such as `events.reply`,
`events.replySessionStarted`, status, reactions, and interactions explicitly.
If the workflow handoff fails, `events.handle` returns an error so Slack can
retry the callback. Once the workflow provider accepts the event, workflow state,
signal idempotency, retries, and reply delivery are owned by the workflow
provider.

For workflow-dispatched `message` and `app_mention` events that include
`thread_ts`, `events.handle` fetches one bounded page of thread replies with the
configured bot token before signaling the workflow. On success, the signal
payload includes `slack.thread_context` and the generated `user_prompt` includes
a prefetched thread context section. If Slack returns an API/client error, the
workflow still receives the event with `slack.thread_context_error` and the
thread-context helper remains available as a fallback tool.

The default prefetch limit is 200 messages and `agent.threadContext.maxMessages`
is clamped to Slack's 1-1000 page-size range. Some Slack apps have lower
`conversations.replies` limits; set a lower `maxMessages` or disable prefetch
with `agent.threadContext.enabled: false` for those deployments. Prefetch can
also be tuned with `includeUserInfo`, `includeBots`, `includeFiles`,
`includeFileContent`, `includeImageData`, and `maxFileBytes`.

Route-level `agent.routes[].agent.threadContext` overrides the global thread
context settings for matching events. If omitted, the route inherits the global
settings. If `enabled: false` is set on the route, prefetch is disabled for that
route even when it is enabled globally.

Slack should send Events API requests to `POST /api/v1/slack/event` and Slack
interactivity requests to `POST /api/v1/slack/interactions`. Both routes are
declared in `manifest.yaml` under `spec.http`, validate Slack HMAC signatures
with `SLACK_SIGNING_SECRET`, and by default resolve the Slack team/user through
the managed `external_identity` authorization relationship. Matching
bot-selected agent routes with `runAs.subject` can instead resolve to the
configured service account before external identity lookup.

`events.handle`, `events.reply`, `events.setStatus`, `events.deleteStatus`,
`events.addReaction`, `events.removeReaction`, the native assistant helpers,
and the interaction helpers are hidden operations (`visible: false`).
`events.handle` is invoked by the signed Slack webhook binding. It starts an
or signals a keyed workflow run and passes an opaque `reply_ref` in the signal
payload. The agent returns the final Slack message body as its final assistant
answer; the workflow runtime invokes `events.reply` using the configured output
delivery binding. The provider validates that the ref belongs to the invoking
Gestalt subject before posting to Slack with the configured bot token. The same
`reply_ref` scopes progress statuses, native assistant updates, suggested
prompts, thread titles, reactions, and interactions to the source event channel,
so the agent never needs raw `chat.postMessage` access for event replies.

Runtime reply app input:

```json
{"reply_ref":"...","text":"I'll check that now."}
```

To publish Slack Events API callbacks directly into Gestalt workflow events,
configure `events.publish.routes`. Publish routes are independent from
`agent.routes`; they do not require a linked Slack user or the Slack bot token:

```yaml
apps:
  slack:
    source: https://github.com/valon-technologies/gestalt-providers/releases/download/app/slack/v0.0.1-alpha.N/provider-release.yaml
    authorizationPolicy: platform
    config:
      events:
        publish:
          routes:
            - id: message-events
              workflow:
                provider: local
              workflowEventType: slack.event.received
              source: slack
              subject: route:message-events
              match:
                eventTypes:
                  - message
                subtypes: []
                channelIds:
                  - C0123456789
```

`workflow.provider` selects the Gestalt workflow provider that should receive
the published event; if omitted, Gestalt publishes through the workflow
manager's default behavior. `workflowEventType` defaults to `slack.event.received`,
`source` defaults to `slack`, and `subject` defaults to `route:<routeId>`.
Workflow triggers can match that subject exactly with
`workflows.eventTriggers.match.subject`. Match rules support `eventTypes`,
`subtypes`, `teamIds`, `channelIds`, `channelTypes`, `userIds`, `botIds`, and
`includeBotEvents`. If `subtypes` is omitted, no subtype filter is applied. If
`subtypes: []` is configured, only Slack events without a subtype match. Bot
events are excluded unless `includeBotEvents: true` is set or `botIds` narrows
the route to specific bots.

Published workflow event data includes `routeId`, normalized Slack callback
fields, and the raw Slack payload. Event IDs prefer Slack's `event_id` as
`slack:<event_id>`; when Slack omits it, the provider uses a deterministic ID
from the route, team, event type, subtype, channel, timestamps, and user or bot.

Agent-facing event helper examples:

Call `slack.events.setStatus` without `status_ts` to create a progress message:

```json
{"reply_ref":"...","text":"Checking status..."}
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
  "status": "is checking status",
  "loading_messages": ["Reading the thread", "Checking status"],
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
    {"title": "Summarize this", "message": "Summarize the latest status"}
  ]
}
```

Use `slack.interactions.request` to post signed Slack buttons.
When the Slack user clicks a button, the interactivity webhook validates the
signed metadata and calls `req.workflows().signal_or_start_run(...)` with
`signal.name="slack.interaction"` for the same `workflow_key`:

```json
{
  "reply_ref": "...",
  "text": "Approve this operation?",
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

`slack.files.upload` uploads bytes to Slack and shares the file in a channel or
thread. It uses Slack's external upload flow, not Slack's deprecated
`files.upload` endpoint. Use `content_base64` for PDFs, other binary files, and
UTF-8 text files:

```json
{
  "channel": "C0123456789",
  "thread_ts": "1712161829.000300",
  "filename": "report.pdf",
  "content_base64": "JVBERi0xLjQK...",
  "content_type": "application/pdf",
  "initial_comment": "Attached report"
}
```

Public `files.upload` messages include the same visible Gestalt attribution
footer as `chat.postMessage`. Upload content is capped at 20 MiB per request.
Slack upload operations are not idempotent; retrying a completed request can
create duplicate files.

If `agent.routes` is omitted, the provider uses its default behavior:
`app_mention`, native assistant thread events, direct-message `message` events,
and non-DM `message` events addressed to the bot start or signal a workflow run.
Non-DM `message` events are addressed when they include native assistant thread
context or mention the configured bot user ID. Slack `authorizations` entries
with `is_bot: true` are also honored as bot user IDs on the webhook path.
For the native Slack assistant experience, enable the app's Agents & AI Apps
features in Slack, add the bot `assistant:write` scope, and subscribe the bot to
`assistant_thread_started`, `assistant_thread_context_changed`, and `message.im`
events in addition to `app_mention`.

To use different workflows, assistants, acknowledgement reactions, or thread
context behavior for different Slack channels or event types, add
`agent.routes`:

```yaml
apps:
  slack:
    source: https://github.com/valon-technologies/gestalt-providers/releases/download/app/slack/v0.0.1-alpha.N/provider-release.yaml
    authorizationPolicy: platform
    invokes:
      - app: slack
        operation: events.reply
        credentialMode: none
      - app: slack
        operation: events.replySessionStarted
        credentialMode: none
      - app: slack
        operation: events.setStatus
        credentialMode: none
      - app: slack
        operation: events.deleteStatus
        credentialMode: none
      - app: slack
        operation: events.addReaction
        credentialMode: none
      - app: slack
        operation: events.removeReaction
        credentialMode: none
      - app: slack
        operation: events.setAssistantStatus
        credentialMode: none
      - app: slack
        operation: events.clearAssistantStatus
        credentialMode: none
      - app: slack
        operation: events.setThreadTitle
        credentialMode: none
      - app: slack
        operation: events.setSuggestedPrompts
        credentialMode: none
      - app: slack
        operation: conversations.getThreadContext
      - app: slack
        operation: files.get
      - app: slack
        operation: files.upload
      - app: slack
        operation: events.uploadFile
        credentialMode: none
      - app: slack
        operation: interactions.request
        credentialMode: none
    config:
      bot:
        userId: U0123456789
        token:
          secret:
            provider: secrets
            name: slack-bot-token
      workflow:
        provider: local
        definitionId: slack-default-agent
      agent:
        threadContext:
          maxMessages: 200
        routes:
          - id: team-help-new-messages
            match:
              channels:
                - C0123456789
              eventTypes:
                - message.channels
              thread: root
            workflow:
              definitionId: slack-team-help-new-message
            agent:
              acknowledgement:
                reaction: eyes
          - id: team-help-mentions
            match:
              channels:
                - C0123456789
              eventTypes:
                - app_mention
              thread: any
            workflow:
              definitionId: slack-team-help-mention
          - id: operations-help
            match:
              channels:
                - C9876543210
              eventTypes:
                - app_mention
                - assistant_thread_started
                - assistant_thread_context_changed
            workflow:
              provider: operations
              definitionId: slack-operations-help
            agent:
              assistant:
                enabled: true
                status: is checking status...
                suggestedPrompts:
                  title: Try next
                  prompts:
                    - title: Summarize status
                      message: Summarize the latest service status.
              acknowledgement:
                enabled: false
              threadContext:
                maxMessages: 50
                includeFiles: false
```

Route settings inherit from the top-level provider configuration when omitted.
Set `enabled: false` on route `agent.assistant`, `agent.acknowledgement`, or
`agent.threadContext` to explicitly disable an inherited global setting. Route
`workflow.provider` overrides the global `workflow.provider`, and
`workflow.definitionId` overrides the global `workflow.definitionId`, for both
Slack events and Slack interaction button callbacks generated from that route.
The Slack provider selects the route, signs `reply_ref`, prefetches configured
thread context, and signals the workflow; workflow behavior and Slack replies
live in the global workflow definition. Signed
interaction callbacks include the route ID; if a non-empty signed route ID no
longer exists in the provider configuration, the provider rejects the callback
instead of silently falling back to global behavior. The global workflow should
call Slack helper operations explicitly when it needs to post back to Slack:

```yaml
agent:
  routes:
    - id: alert-triage
      match:
        channels:
          - C1234567890
        eventTypes:
          - message.channels
        thread: root
      workflow:
        provider: local
        definitionId: slack-alert-triage
        keyTemplate: slack:${team_id}:${channel_id}:${reply_thread_ts}:${route_id}
```

`workflow.keyTemplate` is optional. When omitted, the provider uses the default
Slack thread workflow key. Supported template fields are `team_id`,
`channel_id`, `message_ts`, `thread_ts`, `reply_thread_ts`, `event_id`, and
`route_id`.
Route `runAs.subject` can name a managed service-account subject, such as
`service_account:slack-bot`; matching Slack events then resolve to that subject
instead of requiring the Slack bot user to have a linked external identity. This
is only allowed for trusted bot-originated routes selected with `botIds`. Signed
Slack interaction callbacks generated from that route use `runAs.subject` only
when the signed ref subject ID already equals the configured runAs subject.
Route `runAs` changes the Slack invocation subject for the workflow signal.

When a route uses `match.eventTypes`, include
`assistant_thread_started` and `assistant_thread_context_changed` on routes that
should handle native Slack assistant suggested prompts. For native assistant
thread events, `match.channels` matches the active Slack context channel when
Slack includes it; assistant API calls still use Slack's assistant thread
channel internally.

When `agent.routes` is present, only matching routes start or signal a workflow
run. Match rules support singular or plural forms of `team`, `channel`,
`channelType`, `eventType`, `subtype`, and `user`, plus scalar `thread`.
`match.eventTypes` accepts
Slack Events API subscription literals: `app_mention`, `message.channels`,
`message.groups`, `message.im`, `message.mpim`, `message.app_home`,
`assistant_thread_started`, and `assistant_thread_context_changed`. Values must
match Slack's literals exactly.

Slack delivers `message.*` subscriptions as payloads whose inner event type is
`message`, so `match.eventTypes: [message]` matches the payload type only. It
does not opt a non-DM route into every plain channel message. A non-DM `message`
event that does not mention the bot or include assistant context starts an agent
only when the selected route explicitly matches the corresponding `message.*`
Slack event literal. Configure `eventTypes: [message.channels]` for a public
channel where incoming channel messages should trigger the agent. By default
this also matches normal thread replies. Slack does not provide a top-level-only
subscription; configure `thread: root` to match only messages with no
`thread_ts` or with `thread_ts` equal to the message `ts`, and configure a
separate `app_mention` route with `thread: any` or `thread: reply` when the
agent should still answer explicit mentions inside threads. `thread: reply`
matches messages whose `thread_ts` is present and differs from the message
`ts`; omitted `thread` behaves as `thread: any`. Message routes can also
set `subtypes`: omitted means all non-ignored subtypes can match, `subtypes: []`
means only normal messages with no subtype match, and a non-empty list matches
those Slack message subtypes. Bot-originated message events are ignored unless
the route sets `botIds` to the allowed Slack bot IDs, or sets
`includeBotEvents: true`; use `botIds` when only a specific alerting bot should
trigger the agent. Edit, delete, and `message_replied` message events remain
ignored before route matching.

Route-level `agent` fields only control Slack-facing behavior: native assistant
settings, acknowledgements, and thread-context prefetch.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  slack:
    source: github.com/valon-technologies/gestalt-providers/app/slack
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `channels:read`, `channels:history`, `groups:read`, `groups:history`, `im:read`, `im:history`, `im:write`, `mpim:read`, `mpim:history`, `mpim:write`, `search:read`, `users:read`, `users:read.email`, `files:read`, `files:write`, `chat:write`, `reactions:write`, `channels:write`, `groups:write`, `canvases:write`.
  - Existing Slack OAuth installations may need to reconnect or reauthorize before file upload operations can run.
- `bot` uses a bearer token provisioned as a subject-owned credential.

Operation surfaces: REST.

Representative operations include:

- `conversations.getThreadContext`
- `events.startStream`
- `events.handle`
- `interactions.handle`
- `interactions.request`
- `events.reply`
- `events.setStatus`
- `events.deleteStatus`
- `events.addReaction`
- `events.removeReaction`
- `events.uploadFile`
- `files.upload`

- Event helper operations use opaque `reply_ref` values from Slack workflow events; agents should not handle raw Slack bot tokens.

## Usage Examples

Grant another app or workflow permission to invoke this app before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - app: slack
        operation: conversations.getThreadContext
      - app: slack
        operation: files.upload
      - app: slack
        operation: events.uploadFile
        credentialMode: none
```

Example `conversations.getThreadContext` call:

```ts
await app.invoke("slack", "conversations.getThreadContext", {
  url: "https://workspace.slack.com/archives/C0123456789/p1712161829000300",
  include_user_info: true,
  include_file_content: false,
});
```

Example `events.startStream` call:

```ts
await app.invoke("slack", "events.startStream", { reply_ref: "...", markdown_text: "Working on it..." });
```

Example `files.upload` call:

```ts
await app.invoke("slack", "files.upload", {
  channel: "C0123456789",
  thread_ts: "1712161829.000300",
  filename: "report.pdf",
  content_base64: "JVBERi0xLjQK...",
  content_type: "application/pdf",
  initial_comment: "Attached report",
});
```

Example `events.uploadFile` call:

```ts
await app.invoke("slack", "events.uploadFile", {
  reply_ref: "...",
  filename: "report.pdf",
  content_base64: "JVBERi0xLjQK...",
  content_type: "application/pdf",
  initial_comment: "Attached report",
});
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/app-manifests)
