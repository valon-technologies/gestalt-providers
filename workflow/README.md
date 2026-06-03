# Workflow Providers

Workflow providers for [Gestalt](https://github.com/valon-technologies/gestalt).

Each package under `workflow/` implements the portable workflow provider
surface, so plugins can apply definitions, start or signal runs, deliver
events, and read run projections against a concrete backend.

Current packages:

- `indexeddb`: single-process workflow provider backed by IndexedDB object stores
- `temporal`: Temporal Cloud-backed workflow provider using Temporal workflows,
  schedule activation dispatch, and sharded provider index workflows
