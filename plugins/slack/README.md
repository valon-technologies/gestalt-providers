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

Slack Events API agent replies and declarative REST operations that run as the
bot use the app's bot token. Generic workflow event publishing does not require
a bot token. Configure the token as both a provider secret-backed value for
event helper code and an internal deployment-owned `bot` connection for
runtime-only REST calls when agent replies or bot REST operations are enabled:

```yaml
plugins:
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
        mode: platform
        exposure: internal
        auth:
          type: bearer
          token:
            secret:
              provider: secrets
              name: slack-bot-token
```

Because Gestalt does not perform Slack bot OAuth for this connection, the Slack
app that issues `slack-bot-token` must be granted the bot scopes needed by the
enabled behaviors. The full scope set used by this provider's bot behaviors is:
`app_mentions:read`, `channels:read`, `channels:history`, `groups:read`,
`groups:history`, `im:read`, `im:history`, `mpim:read`, `mpim:history`,
`im:write`, `mpim:write`, `users:read`, `files:read`, `chat:write`,
`assistant:write`, `reactions:write`, `channels:manage`, `groups:write`, and
`canvases:write`.

## Capabilities

Source-backed provider implemented in Python with both a REST surface and an
[MCP](https://modelcontextprotocol.io/) surface. Exposes operations for listing
and creating channels, reading message history and threads, sending and
scheduling messages, opening or resuming DMs, searching messages, managing
reactions, setting channel topics, inviting users, creating canvases, building
thread context, and reading Slack file or image contents.

Authenticates user operations with Slack OAuth 2.0 (user scope). Operations with
fixed bot behavior use the internal deployment-owned `bot` bearer connection.
Public REST and MCP callers use the user OAuth connection by default; hidden
selector parameters such as `actor` are runtime-only and are not part of the
public invocation contract.

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
        operation: interactions.request
        credentialMode: none
      - plugin: slack
        operation: conversations.getThreadContext
      - plugin: slack
        operation: files.get
      - plugin: companyDirectory
        operation: lookupProfile
    config:
      bot:
        token:
          secret:
            provider: secrets
            name: slack-bot-token
      acknowledgement:
        reaction: eyes
      agent:
        provider: simple
        model: deep
        systemPrompt: Use Slack formatting and keep replies concise.
        threadContext:
          enabled: true
          maxMessages: 200
        tools:
          - plugin: companyDirectory
            operation: lookupProfile
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
        threadContext:
          enabled: true
          maxMessages: 200
```

When `acknowledgement.reaction` is configured, `events.handle` adds that
reaction to the source Slack message after the workflow provider accepts the
event. Emoji names may be written with or without colons, and Slack's
`already_reacted` response is treated as idempotent success.

`events.handle` calls `WorkflowManager.SignalOrStartRun(provider_name=workflow.provider,
workflow_key="slack:${team_id}:${channel_id}:${root_ts}", signal.name="slack.event")`.
The workflow target is an agent target built from the `agent` and `agent.routes`
configuration. The Slack event, `reply_ref`, and generated user prompt are
delivered in the signal payload, so later Slack messages in the same thread
signal the existing keyed run instead of replacing its target or authorization
context. The target also sets `output_delivery` so the workflow runtime delivers
the agent's final assistant answer through `events.reply` with `text` sourced
from the agent output and `reply_ref` sourced from the current signal payload.
If the workflow handoff fails, `events.handle` returns an error so Slack can
retry the callback. Once the workflow provider accepts the event, workflow state,
signal idempotency, retries, and output delivery are owned by the workflow
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
with `SLACK_SIGNING_SECRET`, and resolve the Slack team/user through the managed
`external_identity` authorization relationship. Workflow-started agent runs use
exact Slack event helper refs plus the exact `agent.tools` refs configured for
the resolved Gestalt user.

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

Runtime output-delivery input:

```json
{"reply_ref":"...","text":"I'll check that now."}
```

To publish Slack Events API callbacks directly into Gestalt workflow events,
configure `events.publish.routes`. Publish routes are independent from
`agent.routes`; they do not require a linked Slack user or the Slack bot token:

```yaml
plugins:
  slack:
    source: https://github.com/valon-technologies/gestalt-providers/releases/download/plugins/slack/v0.0.1-alpha.N/provider-release.yaml
    authorizationPolicy: platform
    config:
      events:
        publish:
          routes:
            - id: message-events
              workflowProvider: local
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

`workflowProvider` selects the Gestalt workflow provider that should receive the
published event; if omitted, the route uses the top-level `workflow.provider`
when configured, otherwise Gestalt publishes through the workflow manager's
default behavior. `workflowEventType` defaults to `slack.event.received`,
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
signed metadata and calls `WorkflowManager.SignalOrStartRun` with
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

To use different workflows, assistants, prompts, tools, acknowledgement
reactions, or thread context behavior for different Slack channels or event
types, add `agent.routes`:

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
        operation: conversations.getThreadContext
      - plugin: slack
        operation: files.get
      - plugin: slack
        operation: interactions.request
        credentialMode: none
      - plugin: companyDirectory
        operation: lookupProfile
      - plugin: statusPage
        operation: getStatus
    config:
      bot:
        userId: U0123456789
        token:
          secret:
            provider: secrets
            name: slack-bot-token
      agent:
        provider: simple
        model: deep
        systemPrompt: Use Slack formatting and keep replies concise.
        toolSets:
          directory:
            - plugin: companyDirectory
              operation: lookupProfile
          status:
            - plugin: statusPage
              operation: getStatus
        toolSetRefs:
          - directory
        tools:
          - plugin: tickets
            operation: search
        routes:
          - id: team-help
            match:
              channels:
                - C0123456789
              eventTypes:
                - app_mention
                - message
            agent:
              systemPrompt: Help with team questions.
              acknowledgement:
                reaction: eyes
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
              toolSetRefs:
                - status
              systemPrompt: Help engineers inspect service status.
```

Route settings inherit from the top-level provider configuration when omitted.
Set `enabled: false` on route `agent.assistant`, `agent.acknowledgement`, or
`agent.threadContext` to explicitly disable an inherited global setting. Route
`workflow.provider` overrides the global `workflow.provider` for both Slack
events and Slack interaction button callbacks generated from that route. Signed
interaction callbacks include the route ID; if a non-empty signed route ID no
longer exists in the provider configuration, the provider rejects the callback
instead of silently falling back to global behavior.

Tool sets are named groups under `agent.toolSets`. The workflow agent target
expands tool references in this order and deduplicates by exact
plugin/operation/connection/instance with first reference winning:

1. top-level `agent.toolSetRefs`
2. top-level `agent.tools`
3. route `agent.toolSetRefs`
4. route `agent.tools`
5. Slack helper operations

Tool references must name exact plugin and operation IDs. Wildcards and
runtime policy fields such as `credentialMode`, `runAs`, and host-invoke input
bindings are rejected in provider configuration; configure runtime policy in the
deployment's invoke policy instead.

When a route uses `match.eventTypes`, include
`assistant_thread_started` and `assistant_thread_context_changed` on routes that
should handle native Slack assistant suggested prompts. For native assistant
thread events, `match.channels` matches the active Slack context channel when
Slack includes it; assistant API calls still use Slack's assistant thread
channel internally.

When `agent.routes` is present, only matching routes start or signal a workflow
run. Match rules support singular or plural forms of `team`, `channel`,
`channelType`, `eventType`, and `user`. Route-level `agent` fields override or
extend the top-level agent settings, `prompt` is accepted as an alias for
`systemPrompt`, and `modelOptions` are merged with route-level values taking
precedence. Matching routes still require non-DM `message` events to be
addressed to the bot; routes do not opt into every plain channel message.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
