# IndexedDB Workflow Provider

This provider implements the `workflow` base primitive using the IndexedDB
primitive for persistence and the workflow host socket for plugin callbacks.

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
      source: https://github.com/valon-technologies/gestalt-providers/releases/download/workflow/indexeddb/v0.0.1-alpha.42/provider-release.yaml
      indexeddb:
        provider: main-db
      config:
        pollInterval: 1s
```

`pollInterval` controls how often workers scan for due cron schedules
and pending runs.

Poll workers start only when the host calls
`ProviderLifecycle.StartProvider`, after agents, authorization, plugin
providers, and workflow host services are ready.

## Runtime Requirements

- Gestalt host support for `ProviderLifecycle.StartProvider`
- `GESTALT_INDEXEDDB_SOCKET` must point at an IndexedDB provider socket
- `GESTALT_WORKFLOW_HOST_SOCKET` must point at the workflow host socket

## v1 Behavior

- single-process worker execution
- pending-only cancellation
- startup recovers stale `running` runs without blocking provider readiness
- missed cron ticks collapse to one run
- `PublishEvent` enqueues runs for matching event triggers
- `SignalOrStartRun` keeps one active run per workflow key and appends durable
  signal records for same-run re-invocation
- agent tool reference validation happens in the workflow host; this provider
  only validates the runnable agent fields needed for storage and dispatch
