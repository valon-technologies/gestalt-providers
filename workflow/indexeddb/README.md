# IndexedDB Workflow Provider

This provider implements the `workflow` base primitive using IndexedDB for
persistence and Gestalt's unified host-service socket for app callbacks.

## Configuration

```yaml
apiVersion: gestaltd.config/v5

providers:
  indexeddb:
    main-db:
      source: https://github.com/valon-technologies/gestalt-providers/releases/download/indexeddb/relationaldb/v0.0.1-alpha.21/provider-release.yaml
      config:
        dsn: "file:/var/lib/gestalt/workflow.db?_pragma=journal_mode(WAL)"

  workflow:
    local:
      source: https://github.com/valon-technologies/gestalt-providers/releases/download/workflow/indexeddb/v0.0.1-alpha.46/provider-release.yaml
      indexeddb:
        provider: main-db
      config:
        pollInterval: 1s
        workerCount: 4
        runClaimTTL: 10m
        runClaimRenewEvery: 3m20s
```

`pollInterval` controls how often workers scan for due cron schedules
and pending runs. `workerCount` controls how many local poll workers this
provider starts after lifecycle start. `runClaimTTL` controls how long another
provider instance must wait before recovering a run claim that stopped
renewing; live workers renew claims every `runClaimRenewEvery`, which defaults
to one third of `runClaimTTL`.

Poll workers start only when the host calls
`ProviderLifecycle.StartProvider`, after agents, authorization, app providers,
and workflow host services are ready.

## Runtime Requirements

- Gestalt host support for `ProviderLifecycle.StartProvider`
- `GESTALT_HOST_SERVICE_SOCKET` must point at the unified host-service socket
- Named IndexedDB selection happens through SDK-attached
  `x-gestalt-host-binding` metadata

## v1 Behavior

- single-process worker execution
- pending-only cancellation
- startup recovers stale `running` runs without blocking provider readiness
- missed cron ticks collapse to one run
- `PublishEvent` enqueues runs for matching event triggers; the local
  preferred wake is an optimization, and fallback dispatch durably prioritizes
  app event ingress ahead of generic agent backlog when the wake is lost
- `SignalOrStartRun` keeps one active run per workflow key and appends durable
  signal records for same-run re-invocation; keyed continuations are also
  prioritized ahead of generic agent backlog
- agent tool reference validation happens in the workflow host; this provider
  only validates the runnable agent fields needed for storage and dispatch
