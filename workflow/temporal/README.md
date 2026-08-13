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
        workflowRunTimeout: 5m
        activityStartToCloseTimeout: 5m
        scheduleCatchupWindow: 1m
        versioning:
          deploymentName: prod-main
          buildID: ${CLOUD_RUN_REVISION}
```

`scopeID` is required and is part of the Temporal workflow IDs and IndexedDB
state records used by this provider. Reuse the same `scopeID` only for the same
logical Gestalt workflow environment.

`versioning.deploymentName` and `versioning.buildID` are required. The provider
always starts workers with Temporal Worker Deployment Versioning,
`DeploymentOptions.UseVersioning`, and Temporal auto-upgrade workflow behavior.
Deployment configs should interpolate runtime revision variables into `buildID`
before provider startup. The provider does not update Temporal Worker Deployment
routing during startup; deploy pipelines must promote or ramp worker deployment
versions after the new worker version is deployed and polling.

## Runtime Requirements

- Gestalt host support for `ProviderLifecycle.StartProvider`
- `GESTALT_HOST_SERVICE_SOCKET` must point at the unified host-service socket
- Named IndexedDB selection happens through SDK-attached
  `x-gestalt-host-binding` metadata
- A Temporal Cloud namespace reachable at `hostPort`
- A Temporal Cloud API key with permission to start workflows, update
  workflows, manage Temporal schedules for schedule activations, and run
  workers on `taskQueue`

Workers are registered when the host calls `ProviderLifecycle.StartProvider` or
when an execution RPC reaches the provider during startup reconciliation.
Metadata-only reads do not start the Temporal worker.

## Runtime Behavior

- Temporal Cloud API-key authentication
- `TemporalRun` workflows invoke Gestalt workflow services through activities
  and store run authority in Temporal workflow state
- `ApplyDefinition` stores durable workflow definitions, compiled activations,
  and definition generations atomically
- native Temporal schedules for cron dispatch with skip overlap policy;
  activation metadata is stored on the workflow definition while Temporal
  schedule records are internal dispatch cursors
- keyed `StartRun` and `SignalOrStartRun` use deterministic Temporal workflow
  IDs for workflow-key ownership
- the first `SignalOrStartRun` signal is delivered with Temporal
  Update-with-Start, using a deterministic update ID derived from the workflow
  signal idempotency key
- unkeyed and keyed `StartRun` idempotency and workflow signal idempotency are
  stored in IndexedDB; owner-scoped signal idempotency keys coalesce duplicate
  payloads while explicit signal IDs remain strict
- public run IDs are opaque `temporal-run` handles that identify the run
  workflow and Temporal run ID
- `GetRun`, `GetRunEvents`, and `GetRunOutput` read authoritative run state
  from the Temporal workflow query or completed workflow result; `ListRuns`
  queries Temporal Visibility and builds summaries from search attributes plus
  memo (`gestaltOwnerKey`, `gestaltListSummary` for list-safe trigger metadata
  and startedAt — not full event payloads) without per-run worker round-trips;
  on the first page only, attaches visibility aggregates (not `len(runs)`):
  - when that page has no continuation token, `total_count` is the number of
    visibility hits on the page (including executions that lack listable memo).
    CountWorkflow is not used. Grouped Runs is one complete ListWorkflow per
    definition; those groups do not stampede Temporal.
  - when the first page is truncated, `total_count` comes from CountWorkflow
    with the same filter as the page (including status and `definition_id`)
  - `status_counts` is the provider + `target_app` histogram with the list
    status filter cleared (tab/facet totals). On a complete unfiltered page it
    is counted from that page. Truncated pages and status-filtered lists still
    CountWorkflow. Omitted on definition-scoped lists (grouped Runs has no tabs)
  - either aggregate is omitted when its count RPC fails ("unknown", not zero);
    continuation pages omit both so paging stays a single ListWorkflow call
  - CountWorkflow retries `ResourceExhausted` and `Unavailable` up to 3 attempts;
    each backend caps in-flight count RPCs at 4 and releases the slot before
    retry wait so backoff does not occupy the limiter
- IndexedDB stores workflow definitions and request idempotency records only;
  Temporal owns run listing, current run state, schedule cursors, and
  workflow-key ownership
- event activation runs use the delivering subject as `created_by` when it is
  provided
