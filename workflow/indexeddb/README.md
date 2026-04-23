# IndexedDB Workflow Provider

This provider implements the `workflow` base primitive using the IndexedDB
primitive for persistence and the workflow host socket for plugin callbacks.

## Configuration

```yaml
providers:
  indexeddb:
    workflow_state:
      source:
        path: github.com/valon-technologies/gestalt-providers/indexeddb/relationaldb
      config:
        dsn: "file:/var/lib/gestalt/workflow.db?_pragma=journal_mode(WAL)"

  workflow:
    local:
      source: github.com/valon-technologies/gestalt-providers/workflow/indexeddb
      indexeddb:
        provider: workflow_state
        db: workflow
        objectStores:
          - schedules
          - event_triggers
          - runs
          - idempotency
          - execution_refs
      config:
        pollInterval: 1s
```

`pollInterval` controls how often the single worker scans for due cron schedules
and pending runs.

## Runtime Requirements

- `GESTALT_INDEXEDDB_SOCKET` must point at an IndexedDB provider socket
- `GESTALT_WORKFLOW_HOST_SOCKET` must point at the workflow host socket

## Persisted Stores

- `schedules`
- `event_triggers`
- `runs`
- `idempotency`
- `execution_refs`

## v1 Behavior

- single-process, single-worker execution
- pending-only cancellation
- startup marks stale `running` runs as `failed`
- missed cron ticks collapse to one run
- `PublishEvent` enqueues runs for matching event triggers
