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

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
