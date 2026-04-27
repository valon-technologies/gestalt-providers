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

## Capabilities

Source-backed provider implemented in Python with both a REST surface and an
[MCP](https://modelcontextprotocol.io/) surface. Exposes operations for listing
and creating channels, reading message history and threads, sending and
scheduling messages, searching messages, managing reactions, setting channel
topics, inviting users, and creating canvases.

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
        operation: chat.postMessage
      - plugin: workplaceHub
        operation: getMe
    config:
      agent:
        provider: simple
        model: deep
        systemPrompt: Use Slack formatting and keep replies concise.
```

Slack should send Events API requests to `POST /api/v1/slack/event`. The route
is declared in `manifest.yaml` under `spec.http.event`, validates Slack HMAC
signatures with `SLACK_SIGNING_SECRET`, resolves the Slack team/user through the
managed `external_identity` authorization relationship, and starts a Gestalt
agent run with `toolSource=INHERIT_INVOKES`.

If `agent.routes` is omitted, the provider uses its default behavior:
`app_mention` events and direct-message `message` events start an agent run.
Plain channel messages are ignored unless a route explicitly opts them in.

To use different prompts for different Slack channels or event types, add
`agent.routes`:

```yaml
plugins:
  slack:
    source: https://github.com/valon-technologies/gestalt-providers/releases/download/plugins/slack/v0.0.1-alpha.N/provider-release.yaml
    authorizationPolicy: platform
    invokes:
      - plugin: slack
        operation: chat.postMessage
      - plugin: workplaceHub
        operation: getMe
      - plugin: deploymentViewer
        operation: status
    config:
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

When `agent.routes` is present, only matching routes start an agent run. Match
rules support singular or plural forms of `team`, `channel`, `channelType`,
`eventType`, and `user`. Route-level `agent` fields override the top-level
agent settings, `prompt` is accepted as an alias for `systemPrompt`, and
`providerOptions` are merged with route-level values taking precedence.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
