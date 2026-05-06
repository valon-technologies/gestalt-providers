# Temporal Workflow Provider

This provider implements the `workflow` base primitive using Temporal Cloud for
workflow execution and schedule dispatch, with provider metadata persisted in
the host IndexedDB service.

## Configuration

```yaml
apiVersion: gestaltd.config/v5

providers:
  workflow:
    temporal:
      source: https://github.com/valon-technologies/gestalt-providers/releases/download/workflow/temporal/v0.0.1-alpha.6/provider-release.yaml
      indexeddb:
        provider: main-db
      config:
        hostPort: acme.a1b2c.tmprl.cloud:7233
        namespace: acme.a1b2c
        apiKey: ${TEMPORAL_CLOUD_API_KEY}
        taskQueue: gestalt-workflow
        scopeID: prod-main
        identity: gestalt-workflow-prod
        workflowRunTimeout: 5m
        workflowTaskTimeout: 10s
        activityStartToCloseTimeout: 5m
        scheduleCatchupWindow: 1m
        indexShardCount: 64
```

`scopeID` is required and is part of the Temporal workflow IDs used by this
provider. Reuse the same `scopeID` only for the same logical Gestalt workflow
environment. `indexShardCount` is retained for compatibility with legacy
Temporal index workflows; new provider metadata reads and writes use IndexedDB.

## Runtime Requirements

- Gestalt host support for `ProviderLifecycle.StartProvider`
- `GESTALT_INDEXEDDB_SOCKET` must point at an IndexedDB provider socket
- `GESTALT_WORKFLOW_HOST_SOCKET` must point at the workflow host socket
- A Temporal Cloud namespace reachable at `hostPort`
- A Temporal Cloud API key with permission to start workflows, update
  workflows, manage schedules, and run workers on `taskQueue`

Workers are registered when the host calls `ProviderLifecycle.StartProvider` or
when an execution RPC reaches the provider during startup reconciliation.
Metadata-only reads do not start the Temporal worker.

## v1 Behavior

- Temporal Cloud API-key authentication
- Temporal workflows invoke the Gestalt workflow host through activities
- native Temporal schedules for cron dispatch with skip overlap policy
- pending-only cancellation
- `SignalOrStartRun` uses Temporal update-with-start to append signals to the
  active workflow-key run or start a new one
- provider state is persisted in IndexedDB object stores; legacy sharded
  Temporal index workflows remain registered only for compatibility with older
  executions
- public run IDs are opaque handles containing Temporal workflow and run IDs
- completed run state is read from IndexedDB, not closed workflow queries
- event trigger, execution reference, workflow-key, and idempotency lookups are
  stored in IndexedDB
- event-trigger runs can create execution references for the publishing subject
  before the target operation is invoked
