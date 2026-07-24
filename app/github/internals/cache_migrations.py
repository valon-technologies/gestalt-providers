from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import gestalt
from gestalt.migrations import (
    ColumnSchema,
    MigrationRunOptions,
    SchemaDeclaration,
    SchemaRevision,
    StoreDeclaration,
)

CACHE_MIGRATION_LEDGER_STORE_NAME = "_github_cache_migrations"
CACHE_INIT_REVISION_ID = "app/github/0001_cache"

RESPONSES_STORE_NAME = "github_cache_responses"
GENERATIONS_STORE_NAME = "github_cache_generations"
ENTITIES_STORE_NAME = "github_cache_entities"
RECONCILE_STORE_NAME = "github_cache_reconcile"

RESPONSES_BY_EXPIRY_INDEX = "by_scope_repository_expires_at"
RESPONSES_BY_REPOSITORY_INDEX = "by_scope_repository_operation"
ENTITIES_BY_UPDATED_AT_INDEX = "by_scope_repository_type_updated_at"
RECONCILE_BY_LEASE_INDEX = "by_scope_repository_lease_until"


def cache_migration_options(
    config: Mapping[str, Any],
) -> MigrationRunOptions | None:
    if not _cache_enabled(config):
        return None
    return MigrationRunOptions(
        revisions=cache_revisions(),
        ledger_store=CACHE_MIGRATION_LEDGER_STORE_NAME,
    )


def cache_revisions() -> list[SchemaRevision]:
    return [
        SchemaRevision(
            id=CACHE_INIT_REVISION_ID,
            schema=SchemaDeclaration(
                stores=[
                    _store(
                        RESPONSES_STORE_NAME,
                        gestalt.IndexSchema(
                            name=RESPONSES_BY_EXPIRY_INDEX,
                            key_path=["scope", "repository", "expires_at"],
                        ),
                        gestalt.IndexSchema(
                            name=RESPONSES_BY_REPOSITORY_INDEX,
                            key_path=["scope", "repository", "operation"],
                        ),
                    ),
                    _store(GENERATIONS_STORE_NAME),
                    _store(
                        ENTITIES_STORE_NAME,
                        gestalt.IndexSchema(
                            name=ENTITIES_BY_UPDATED_AT_INDEX,
                            key_path=[
                                "scope",
                                "repository",
                                "entity_type",
                                "updated_at",
                            ],
                        ),
                    ),
                    _store(
                        RECONCILE_STORE_NAME,
                        gestalt.IndexSchema(
                            name=RECONCILE_BY_LEASE_INDEX,
                            key_path=["scope", "repository", "lease_until"],
                        ),
                    ),
                ]
            ),
        )
    ]


def _store(name: str, *indexes: gestalt.IndexSchema) -> StoreDeclaration:
    return StoreDeclaration(
        name=name,
        columns=[ColumnSchema(name="id", primary_key=True, not_null=True)],
        indexes=list(indexes),
    )


def _cache_enabled(config: Mapping[str, Any]) -> bool:
    for key in ("cacheEnabled", "cache_enabled"):
        if key not in config:
            continue
        value = config.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on"}:
                return True
            if normalized in {"0", "false", "no", "off"}:
                return False
        raise ValueError(f"{key} must be a boolean")
    return False
