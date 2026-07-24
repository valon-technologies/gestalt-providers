from __future__ import annotations

import unittest
from copy import deepcopy
from typing import Any
from unittest import mock

import gestalt

from internals import cache_store
from internals.cache_migrations import (
    CACHE_MIGRATION_LEDGER_STORE_NAME,
    ENTITIES_BY_UPDATED_AT_INDEX,
    ENTITIES_STORE_NAME,
    GENERATIONS_STORE_NAME,
    RECONCILE_STORE_NAME,
    RESPONSES_BY_EXPIRY_INDEX,
    RESPONSES_STORE_NAME,
    cache_migration_options,
)
from internals.config import github_config_from_mapping


class FakeIndex:
    def __init__(
        self,
        records: dict[str, dict[str, Any]],
        key_path: list[str],
    ) -> None:
        self.records = records
        self.key_path = key_path

    def get_all(
        self, query: Any = None, *, count: int | None = None
    ) -> list[dict[str, Any]]:
        matched = []
        for record in self.records.values():
            if not all(key in record for key in self.key_path):
                continue
            key = [record[field] for field in self.key_path]
            if _matches(key, query):
                matched.append((key, deepcopy(record)))
        matched.sort(key=lambda item: item[0])
        records = [record for _, record in matched]
        return records if count is None else records[:count]


class FakeStore:
    def __init__(
        self,
        database: FakeIndexedDB,
        name: str,
    ) -> None:
        self.database = database
        self.name = name

    def get(self, record_id: str) -> dict[str, Any]:
        try:
            return deepcopy(self.database.records[self.name][record_id])
        except KeyError:
            raise gestalt.NotFoundError("record not found") from None

    def get_all(
        self, query: Any = None, *, count: int | None = None
    ) -> list[dict[str, Any]]:
        matched = [
            deepcopy(record)
            for record_id, record in sorted(self.database.records[self.name].items())
            if _matches(record_id, query)
        ]
        return matched if count is None else matched[:count]

    def put(self, record: dict[str, Any]) -> None:
        self.database.records[self.name][str(record["id"])] = deepcopy(record)

    def delete(self, record_id: str) -> None:
        self.database.records[self.name].pop(record_id, None)

    def index(self, name: str) -> FakeIndex:
        return FakeIndex(
            self.database.records[self.name],
            self.database.indexes[(self.name, name)],
        )


class FakeTransaction:
    def __init__(self, database: FakeIndexedDB) -> None:
        self.database = database

    def __enter__(self) -> FakeTransaction:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def object_store(self, name: str) -> FakeStore:
        return self.database.object_store(name)


class FakeIndexedDB:
    def __init__(self) -> None:
        self.records = {
            RESPONSES_STORE_NAME: {},
            GENERATIONS_STORE_NAME: {},
            ENTITIES_STORE_NAME: {},
            RECONCILE_STORE_NAME: {},
        }
        self.indexes = {
            (RESPONSES_STORE_NAME, RESPONSES_BY_EXPIRY_INDEX): [
                "scope",
                "repository",
                "expires_at",
            ],
            (ENTITIES_STORE_NAME, ENTITIES_BY_UPDATED_AT_INDEX): [
                "scope",
                "repository",
                "entity_type",
                "updated_at",
            ],
        }
        self.closed = False

    def object_store(self, name: str) -> FakeStore:
        return FakeStore(self, name)

    def transaction(
        self, _stores: list[str], _mode: str
    ) -> FakeTransaction:
        return FakeTransaction(self)

    def close(self) -> None:
        self.closed = True


class CacheStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        cache_store.close_cache()
        self.database = FakeIndexedDB()
        indexeddb_patch = mock.patch.object(
            cache_store.gestalt,
            "IndexedDB",
            return_value=self.database,
        )
        indexeddb_patch.start()
        self.addCleanup(indexeddb_patch.stop)
        self.addCleanup(cache_store.close_cache)

    def test_migrations_are_default_off_and_declare_all_stores(self) -> None:
        self.assertIsNone(cache_migration_options({}))

        options = cache_migration_options({"cacheEnabled": True})

        self.assertIsNotNone(options)
        assert options is not None
        self.assertEqual(options.ledger_store, CACHE_MIGRATION_LEDGER_STORE_NAME)
        stores = options.revisions[0].schema.stores or []
        self.assertEqual(
            {store.name for store in stores},
            {
                RESPONSES_STORE_NAME,
                GENERATIONS_STORE_NAME,
                ENTITIES_STORE_NAME,
                RECONCILE_STORE_NAME,
            },
        )

    def test_config_defaults_and_bounds_cache_settings(self) -> None:
        default = github_config_from_mapping({})
        configured = github_config_from_mapping(
            {"cacheEnabled": True, "cacheTtlSeconds": 120},
            provider_name="github-prod",
        )

        self.assertFalse(default.cache_enabled)
        self.assertEqual(default.cache_ttl_seconds, 60.0)
        self.assertTrue(configured.cache_enabled)
        self.assertEqual(configured.cache_ttl_seconds, 120.0)
        self.assertEqual(configured.provider_name, "github-prod")
        with self.assertRaisesRegex(ValueError, "between 1 and 3600"):
            github_config_from_mapping({"cacheTtlSeconds": 0})

    def test_response_round_trip_requires_current_generation_and_ttl(self) -> None:
        request = {"kind": "rest", "path": "/repos/acme/widgets/pulls/7"}
        generation = cache_store.get_generation(
            "scope", "acme/widgets", "pull_request"
        )
        stored = cache_store.put_cached_response_if_generation(
            "scope",
            "acme/widgets",
            "bot.getPullRequest",
            request,
            "pull_request",
            {"id": 9_007_199_254_740_993},
            expected_generation=generation,
            ttl_seconds=60,
            now=100,
        )

        self.assertTrue(stored)
        cached = cache_store.get_cached_response(
            "scope",
            "acme/widgets",
            "bot.getPullRequest",
            request,
            "pull_request",
            now=159,
        )
        self.assertIsNotNone(cached)
        assert cached is not None
        self.assertEqual(cached.body["id"], 9_007_199_254_740_993)
        self.assertIsNone(
            cache_store.get_cached_response(
                "scope",
                "acme/widgets",
                "bot.getPullRequest",
                request,
                "pull_request",
                now=160,
            )
        )

    def test_generation_invalidates_and_prevents_stale_write(self) -> None:
        request = {"kind": "rest", "path": "/repos/acme/widgets/pulls/7"}
        cache_store.increment_generations(
            "scope", "acme/widgets", {"pull_request"}, now=20
        )

        self.assertFalse(
            cache_store.put_cached_response_if_generation(
                "scope",
                "acme/widgets",
                "bot.getPullRequest",
                request,
                "pull_request",
                {"number": 7},
                expected_generation=0,
                ttl_seconds=60,
                now=21,
            )
        )

    def test_entity_writes_are_monotonic_and_range_query_is_scoped(self) -> None:
        self.assertTrue(
            cache_store.put_entity_if_newer(
                "scope",
                "acme/widgets",
                "pull_request",
                "7",
                {"number": 7},
                updated_at="2026-07-24T12:00:00Z",
                observed_at=10,
            )
        )
        self.assertFalse(
            cache_store.put_entity_if_newer(
                "scope",
                "acme/widgets",
                "pull_request",
                "7",
                {"number": 7, "state": "old"},
                updated_at="2026-07-24T11:00:00Z",
                observed_at=11,
            )
        )
        cache_store.put_entity_if_newer(
            "scope",
            "other/widgets",
            "pull_request",
            "8",
            {"number": 8},
            updated_at="2026-07-25T12:00:00Z",
            observed_at=12,
        )

        entities = cache_store.query_entities_updated_since(
            "scope",
            "acme/widgets",
            "pull_request",
            "2026-07-24T00:00:00Z",
        )

        self.assertEqual([entity.entity_id for entity in entities], ["7"])
        self.assertEqual(entities[0].payload, {"number": 7})

    def test_reconcile_lease_is_exclusive_and_token_owned(self) -> None:
        self.assertTrue(
            cache_store.claim_reconcile_lease(
                "scope", "acme/widgets", "first", now=10, lease_seconds=30
            )
        )
        self.assertFalse(
            cache_store.claim_reconcile_lease(
                "scope", "acme/widgets", "second", now=20, lease_seconds=30
            )
        )
        self.assertFalse(
            cache_store.release_reconcile_lease(
                "scope", "acme/widgets", "second", now=21
            )
        )
        self.assertTrue(
            cache_store.release_reconcile_lease(
                "scope", "acme/widgets", "first", now=21
            )
        )

    def test_prune_enforces_retention_and_repository_limit(self) -> None:
        for index in range(4):
            request = {"path": f"/repos/acme/widgets/pulls/{index + 1}"}
            cache_store.put_cached_response_if_generation(
                "scope",
                "acme/widgets",
                "bot.getPullRequest",
                request,
                "pull_request",
                {"number": index + 1},
                expected_generation=0,
                ttl_seconds=60,
                now=float(index + 1),
            )

        deleted = cache_store.prune_responses(
            "scope",
            now=10,
            retention_seconds=100,
            max_per_repository=2,
            max_total=10,
        )

        self.assertEqual(deleted, 2)
        self.assertEqual(len(self.database.records[RESPONSES_STORE_NAME]), 2)


def _matches(key: Any, query: Any) -> bool:
    if query is None:
        return True
    if not isinstance(query, gestalt.KeyRange):
        return key == query
    if query.lower is not None:
        lower = list(query.lower) if isinstance(query.lower, list) else query.lower
        if key < lower or (query.lower_open and key == lower):
            return False
    if query.upper is not None:
        upper = list(query.upper) if isinstance(query.upper, list) else query.upper
        if key > upper or (query.upper_open and key == upper):
            return False
    return True


if __name__ == "__main__":
    unittest.main()
