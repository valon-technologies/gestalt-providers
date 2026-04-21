# Temporal Workflow Provider

This provider implements the `workflow` base primitive on top of a Temporal
namespace. Workflow runs are Temporal workflow executions, cron schedules are
durable Temporal workflows that emit run workflows, and event-trigger
definitions are durable Temporal workflows that `PublishEvent` matches against.

## Configuration

```yaml
providers:
  workflow:
    temporal:
      source: github.com/valon-technologies/gestalt-providers/workflow/temporal
      config:
        hostPort: "127.0.0.1:7233"
        namespace: "default"
        taskQueue: "gestalt-workflow-temporal"
        activityTimeout: 2m
        runTimeout: 15m
```

## Runtime Requirements

- A reachable Temporal frontend at `hostPort`
- `GESTALT_WORKFLOW_HOST_SOCKET` must point at the workflow host socket

## v1 Behavior

- manual runs are Temporal workflow executions
- idempotent manual runs re-adopt the deterministic existing run workflow
- schedule and event-trigger IDs remain globally unique within a provider
- schedule definitions stay open and emit child run workflows on cron boundaries
- event publication matches open trigger definitions and starts one run per match
