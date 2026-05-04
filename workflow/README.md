# Workflow Providers

Workflow providers for [Gestalt](https://github.com/valon-technologies/gestalt).

Each package under `workflow/` implements the portable workflow provider
surface, so plugins can start runs, manage cron schedules, and publish events
against a concrete backend.

Current packages:

- `indexeddb`: single-process workflow provider backed by IndexedDB object stores
- `temporal`: Temporal Cloud-backed workflow provider using Temporal workflows,
  schedules, and sharded provider index workflows
