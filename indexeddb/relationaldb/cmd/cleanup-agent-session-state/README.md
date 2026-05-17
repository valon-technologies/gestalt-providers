# cleanup-agent-session-state

Deletes legacy `gestaltd` core agent-session state from a relational IndexedDB
database after provider-owned session visibility is deployed.

The command is dry-run by default. It refuses to start unless the operator
confirms the expected production project, Cloud SQL instance, secret, and
schema. It also reads `_gestalt_stores` before configuring the relationaldb
provider so an empty or wrong database does not look like a safe no-op.

## Dry Run

```sh
cd indexeddb/relationaldb

export GESTALT_INDEXEDDB_DSN="$(
  gcloud secrets versions access latest \
    --project=gitlab-peach-street \
    --secret=gestalt-mysql-dsn-east4
)"

go run ./cmd/cleanup-agent-session-state \
  --project gitlab-peach-street \
  --instance terra-east4 \
  --secret gestalt-mysql-dsn-east4 \
  --schema gestaltd
```

Use the reported `agent_session_relationships`,
`agent_session_route_records`, and `agent_turn_route_records` values to compute
the exact `--max-delete` value for execution. Empty legacy route stores still
count as cleanup work, but they add `0` to `--max-delete`.

## Execute

Before running with `--execute`:

- complete a Cloud SQL backup or export and keep its operation id;
- verify provider-owned Slack shared-session visibility is deployed;
- verify old `gestaltd` revisions that can write legacy route/authz state are
  drained.

```sh
cd indexeddb/relationaldb

go run ./cmd/cleanup-agent-session-state \
  --project gitlab-peach-street \
  --instance terra-east4 \
  --secret gestalt-mysql-dsn-east4 \
  --schema gestaltd \
  --execute \
  --max-delete <exact dry-run relationship plus route record count> \
  --backup-confirmation <backup-or-export-operation-id> \
  --provider-visibility-deployed-ref <deployed-provider-ref> \
  --old-gestaltd-drained
```

The command deletes only:

- `relationships` rows whose `by_resource` prefix is `agent_session`;
- the `agent_session_routes` object store;
- the `agent_turn_routes` object store.
