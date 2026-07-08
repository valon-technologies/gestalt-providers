from __future__ import annotations

from gestalt.migrations import MigrationRunOptions, SchemaDeclaration, SchemaRevision, StoreDeclaration


def agent_store_names(run_store: str, idempotency_store: str) -> list[str]:
    return [
        run_store,
        f"{run_store}_events",
        f"{run_store}_sessions",
        f"{run_store}_session_projections",
        f"{run_store}_turn_projections",
        f"{idempotency_store}_sessions",
        f"{idempotency_store}_turns",
    ]


def build_agent_init_revision(
    *,
    revision_id: str,
    run_store: str,
    idempotency_store: str,
) -> SchemaRevision:
    return SchemaRevision(
        id=revision_id,
        schema=SchemaDeclaration(
            stores=[StoreDeclaration(name=name) for name in agent_store_names(run_store, idempotency_store)]
        ),
    )


def build_agent_migration_options(
    *,
    revision_id: str,
    run_store: str,
    idempotency_store: str,
    db_binding: str | None = None,
) -> MigrationRunOptions:
    return MigrationRunOptions(
        revisions=[
            build_agent_init_revision(
                revision_id=revision_id,
                run_store=run_store,
                idempotency_store=idempotency_store,
            )
        ],
        db_binding=db_binding,
    )
