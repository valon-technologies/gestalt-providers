# Twilio

Send and receive SMS, voice calls, and messaging operations.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  twilio:
    source: github.com/valon-technologies/gestalt-providers/plugins/twilio
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Twilio OpenAPI specification. Exposes
operations for sending and receiving SMS messages, making voice calls, and
managing messaging resources.

Authenticates with Twilio Account SID and Auth Token (HTTP Basic).

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
