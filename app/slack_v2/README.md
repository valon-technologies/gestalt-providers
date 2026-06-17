# Slack v2

Minimal Slack webhook app.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
authorization:
  models:
    default:
      resourceTypes:
        slack_v2:
          defaultAccessPolicy: allow
          relations:
            caller:
              subjectTypes: [subject]

apps:
  slack_v2:
    source: github.com/valon-technologies/gestalt-providers/app/slack_v2
    version: ...
```

The `defaultAccessPolicy: allow` entry lets unauthenticated HTTP webhook
requests (subject `system:http_binding:slack_v2:events`) invoke
`handle_slack_event` without explicit authorization relationships. Gestalt
still requires at least one relation on the resource type even when the
default policy is allow; the relation is not consulted in that case.

## Capabilities

- `register_slack_event` — register Slack bot credentials and workflow event routing in IndexedDB (keyed by `app_id`). Set `workflow_event_subject` to the subject that workflow activations should match.
- `get_workflow_event_subject_for_app` — look up the registered workflow event subject for a Slack `app_id`.
- `handle_slack_event` — exposed to HTTP at `POST /api/v1/slack_v2/events`. Resolves the Slack `api_app_id` to a registered workflow event subject, then publishes a `slack_v2.event.received` workflow event.
