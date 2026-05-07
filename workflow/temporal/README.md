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
      source: https://github.com/valon-technologies/gestalt-providers/releases/download/workflow/temporal/v0.0.1-alpha.8/provider-release.yaml
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
environment. `indexShardCount` controls the number of Temporal-owned shards
used for run projections and owner idempotency ledgers.

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

## Runtime Behavior

- Temporal Cloud API-key authentication
- Temporal V3 run workflows invoke the Gestalt workflow host through activities
- native Temporal schedules for cron dispatch with skip overlap policy
- keyed `SignalOrStartRun` routes through durable Temporal lane workflows; the
  active run, signal acknowledgements, and workflow-key ownership are workflow
  state
- unkeyed `SignalOrStartRun` idempotency is owned by durable Temporal owner
  ledger workflows
- public run IDs are opaque V3 handles that identify the run workflow and, for
  keyed runs, the owning lane workflow
- run state is projected to Temporal run-index workflows for listing and lookup
- IndexedDB stores schedule, event-trigger, and execution-reference metadata;
  run, workflow-key, and idempotency tables are not used
- event-trigger runs can create execution references for the publishing subject
  before the target operation is invoked
