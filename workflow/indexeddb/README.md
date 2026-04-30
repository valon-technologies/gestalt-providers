# IndexedDB Workflow Provider

This provider implements the `workflow` base primitive using the IndexedDB
primitive for persistence and the workflow host socket for plugin callbacks.

## Configuration

```yaml
apiVersion: gestaltd.config/v4

providers:
  indexeddb:
    main-db:
      source: https://github.com/valon-technologies/gestalt-providers/releases/download/indexeddb/relationaldb/v0.0.1-alpha.16/provider-release.yaml
      config:
        dsn: "file:/var/lib/gestalt/workflow.db?_pragma=journal_mode(WAL)"

  workflow:
    local:
      source: https://github.com/valon-technologies/gestalt-providers/releases/download/workflow/indexeddb/v0.0.1-alpha.28/provider-release.yaml
      indexeddb:
        provider: main-db
      config:
        pollInterval: 1s
```

`pollInterval` controls how often the single worker scans for due cron schedules
and pending runs.

The poll worker starts only when the host calls
`ProviderLifecycle.StartProvider`, after agents, authorization, plugin
providers, and workflow host services are ready.

## Runtime Requirements

- Gestalt host support for `ProviderLifecycle.StartProvider`
- `GESTALT_INDEXEDDB_SOCKET` must point at an IndexedDB provider socket
- `GESTALT_WORKFLOW_HOST_SOCKET` must point at the workflow host socket

## v1 Behavior

- single-process, single-worker execution
- pending-only cancellation
- startup retries stale `running` agent-target runs and marks other stale
  `running` runs as `failed`
- missed cron ticks collapse to one run
- `PublishEvent` enqueues runs for matching event triggers
- `SignalOrStartRun` keeps one active run per workflow key and appends durable
  signal records for same-run re-invocation
- agent tool reference validation happens in the workflow host; this provider
  only validates the runnable agent fields needed for storage and dispatch
