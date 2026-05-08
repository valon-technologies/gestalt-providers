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
        versioning:
          enabled: true
          deploymentName: prod-main
          buildID: ${CLOUD_RUN_REVISION}
          defaultVersioningBehavior: autoUpgrade
          promotion:
            mode: current
            timeout: 30s
            allowReplaceCurrent: true
```

`scopeID` is required and is part of the Temporal workflow IDs used by this
provider. Reuse the same `scopeID` only for the same logical Gestalt workflow
environment. `indexShardCount` controls the number of Temporal-owned shards
used for run projections and owner idempotency ledgers.

`versioning` is optional. When omitted or disabled, workers poll the task queue
as unversioned workers. When enabled, the provider starts Temporal Worker
Deployment Versioning with `DeploymentOptions.UseVersioning`, the configured
`deploymentName`, the resolved build ID, and `autoUpgrade` workflow behavior.
Use either `buildID` or `buildIDEnv`; if `buildIDEnv` is used, the environment
variable must be present in the provider process environment. Hosted provider
processes may not inherit every environment variable from the parent runtime, so
config interpolation into `buildID` is usually the safer deployment interface.

`promotion.mode: current` updates the Temporal worker deployment current version
before the provider runs startup workflow maintenance. This is the mode required
when a deploy contains incompatible workflow or activity behavior, such as new
provider operations that old workers cannot execute. `allowReplaceCurrent` must
be set when replacing an existing current version. `promotion.mode: ramping`
maps to Temporal's ramping version APIs, but it is only safe when old and new
worker versions are intentionally kept alive and compatible during the ramp.
`promotion.mode: none` starts a versioned worker without changing Temporal
routing, for externally managed deployments.

## Runtime Requirements

- Gestalt host support for `ProviderLifecycle.StartProvider`
- `GESTALT_INDEXEDDB_SOCKET` must point at an IndexedDB provider socket
- `GESTALT_WORKFLOW_HOST_SOCKET` must point at the workflow host socket
- A Temporal Cloud namespace reachable at `hostPort`
- A Temporal Cloud API key with permission to start workflows, update
  workflows, manage schedules, run workers on `taskQueue`, and manage Worker
  Deployments when `versioning.promotion.mode` is `current` or `ramping`

Workers are registered when the host calls `ProviderLifecycle.StartProvider` or
when an execution RPC reaches the provider during startup reconciliation.
Metadata-only reads do not start the Temporal worker.

When promotion is enabled and promotion fails, provider startup fails and the
newly started worker is stopped. This keeps Gestalt readiness closed instead of
serving HTTP while Temporal routing is still pointing at an incompatible worker
set. Promotion only affects tasks routed after Temporal accepts the deployment
update; already-polled tasks cannot be recalled by the provider.

## Runtime Behavior

- Temporal Cloud API-key authentication
- Temporal V4 run workflows invoke the Gestalt workflow host through activities
  and project run state into IndexedDB for new unkeyed, scheduled, and event
  runs; V3 remains registered for existing handles and keyed lane compatibility
- native Temporal schedules for cron dispatch with skip overlap policy
- keyed `SignalOrStartRun` routes through durable Temporal lane workflows; the
  active run, signal acknowledgements, and workflow-key ownership are workflow
  state
- unkeyed `StartRun` idempotency for new V4 runs is stored in IndexedDB;
  signal and keyed-run idempotency still use durable Temporal owner ledger
  workflows
- public run IDs are opaque V3 handles that identify the run workflow and, for
  keyed runs, the owning lane workflow
- legacy V3 run state is still projected to Temporal run-index workflows for
  compatibility during the migration window
- IndexedDB stores schedule, event-trigger, execution-reference, V4 run
  projection, and V4 unkeyed-start idempotency metadata; workflow-key and
  signal idempotency ownership still use the legacy Temporal lane/ledger path
- event-trigger runs can create execution references for the publishing subject
  before the target operation is invoked
