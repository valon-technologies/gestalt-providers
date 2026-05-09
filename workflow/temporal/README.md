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
      source: https://github.com/valon-technologies/gestalt-providers/releases/download/workflow/temporal/v0.0.1-alpha.24/provider-release.yaml
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
        versioning:
          enabled: true
          deploymentName: prod-main
          buildID: ${CLOUD_RUN_REVISION}
          defaultVersioningBehavior: autoUpgrade
```

`scopeID` is required and is part of the Temporal workflow IDs and IndexedDB
state records used by this provider. Reuse the same `scopeID` only for the same
logical Gestalt workflow environment.

`versioning` is optional. When omitted or disabled, workers poll the task queue
as unversioned workers. When enabled, the provider starts Temporal Worker
Deployment Versioning with `DeploymentOptions.UseVersioning`, the configured
`deploymentName`, the resolved build ID, and `autoUpgrade` workflow behavior.
Use either `buildID` or `buildIDEnv`; if `buildIDEnv` is used, the environment
variable must be present in the provider process environment. Hosted provider
processes may not inherit every environment variable from the parent runtime, so
config interpolation into `buildID` is usually the safer deployment interface.
The provider does not update Temporal Worker Deployment routing during startup;
deploy pipelines must promote or ramp worker deployment versions after the new
worker version is deployed and polling.

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
- Temporal V4 run workflows invoke the Gestalt workflow host through activities
  and project run state into IndexedDB for unkeyed, keyed, scheduled, and event
  runs
- native Temporal schedules for cron dispatch with skip overlap policy;
  IndexedDB schedule records are the metadata source for schedule listing
- keyed `StartRun` and `SignalOrStartRun` route directly to claim-gated V4 run
  workflows and store workflow-key ownership in IndexedDB
- the first `SignalOrStartRun` signal is delivered with Temporal
  Update-with-Start, using a deterministic update ID derived from the workflow
  signal idempotency key
- unkeyed and keyed `StartRun` idempotency and workflow signal idempotency are
  stored in IndexedDB; owner-scoped signal idempotency keys coalesce duplicate
  payloads while explicit signal IDs remain strict
- public run IDs are opaque V4 handles that identify the run workflow and
  Temporal run ID
- `ListRuns` reads IndexedDB run projections only
- IndexedDB stores schedule, event-trigger, execution-reference, V4 run
  projection, V4 start idempotency, V4 signal idempotency, and workflow-key
  ownership metadata
- event-trigger runs can create execution references for the publishing subject
  before the target operation is invoked
