# Slack v2

Minimal Slack webhook app.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  slack_v2:
    source: github.com/valon-technologies/gestalt-providers/app/slack_v2
    version: ...
```

## Capabilities

Source app with one operation, `HandleSlackEvent`, exposed to HTTP at
`POST /api/v1/slack_v2/events`. The operation returns `hello world`.
