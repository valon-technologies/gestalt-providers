# Twilio

Send and receive SMS, voice calls, and messaging operations.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  twilio:
    source: github.com/valon-technologies/gestalt-providers/apps/twilio
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the Twilio OpenAPI specification. Exposes
operations for sending and receiving SMS messages, making voice calls, and
managing messaging resources.

Authenticates with Twilio Account SID and Auth Token (HTTP Basic).

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  twilio:
    source: github.com/valon-technologies/gestalt-providers/apps/twilio
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials; mode `user`.
  - Credential fields: `account_sid`, `auth_token`.
  - `account_sid`: Find this on your Twilio Console dashboard.
  - `auth_token`: Find this on your Twilio Console dashboard.

Operation surfaces: OpenAPI.

Representative operations include:

- `createMessage`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: twilio
        operation: createMessage
```

Example `createMessage` call:

```ts
await invoker.invoke("twilio", "createMessage", { To: "+15551234567", From: "+15557654321", Body: "Hello from Gestalt" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
