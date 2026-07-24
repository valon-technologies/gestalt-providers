from __future__ import annotations

import unittest
from copy import deepcopy
from typing import Any
from unittest import mock

import gestalt

from internals import cache_store


class FakeIndex:
    def __init__(self, records: dict[str, dict[str, Any]], key_path: list[str]) -> None:
        self._records = records
        self._key_path = key_path

    def get_all(
        self, query: Any = None, *, count: int | None = None
    ) -> list[dict[str, Any]]:
        matches: list[tuple[list[Any], dict[str, Any]]] = []
        for record in self._records.values():
            if not all(field in record for field in self._key_path):
                continue
            key = [record[field] for field in self._key_path]
            if query is not None and not _key_in_range(key, query):
                continue
            matches.append((key, deepcopy(record)))
        matches.sort(key=lambda item: item[0])
        records = [record for _, record in matches]
        return records if count is None else records[:count]


class FakeObjectStore:
    def __init__(self, database: FakeIndexedDB, name: str) -> None:
        self._database = database
        self._name = name

    def get(self, record_id: str) -> dict[str, Any]:
        try:
            return deepcopy(self._database.records[self._name][record_id])
        except KeyError:
            raise gestalt.NotFoundError("record not found") from None

    def put(self, record: dict[str, Any]) -> None:
        self._database.records[self._name][str(record["id"])] = deepcopy(record)

    def delete(self, record_id: str) -> None:
        try:
            del self._database.records[self._name][record_id]
        except KeyError:
            raise gestalt.NotFoundError("record not found") from None

    def index(self, name: str) -> FakeIndex:
        schema = self._database.schemas[self._name]
        for index in schema.indexes:
            if index.name == name:
                return FakeIndex(self._database.records[self._name], index.key_path)
        raise gestalt.NotFoundError("index not found")


class FakeIndexedDB:
    def __init__(self) -> None:
        self.records: dict[str, dict[str, dict[str, Any]]] = {}
        self.schemas: dict[str, gestalt.ObjectStoreSchema] = {}
        self.create_attempts: list[str] = []

    def create_object_store(
        self,
        name: str,
        schema: gestalt.ObjectStoreSchema | None = None,
    ) -> FakeObjectStore:
        self.create_attempts.append(name)
        if name in self.records:
            raise gestalt.AlreadyExistsError("object store already exists")
        self.records[name] = {}
        self.schemas[name] = schema or gestalt.ObjectStoreSchema()
        return self.object_store(name)

    def object_store(self, name: str) -> FakeObjectStore:
        return FakeObjectStore(self, name)


class CacheStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        cache_store._reset_for_tests()
        self.database = FakeIndexedDB()
        indexeddb_patch = mock.patch.object(
            cache_store.gestalt,
            "IndexedDB",
            return_value=self.database,
        )
        indexeddb_patch.start()
        self.addCleanup(indexeddb_patch.stop)
        self.addCleanup(cache_store._reset_for_tests)

    def test_initialization_is_idempotent_and_creates_compound_index(self) -> None:
        self.assertIsNone(cache_store.get_pull_request("Acme/Widgets", 1))
        cache_store._reset_for_tests()
        self.assertIsNone(cache_store.get_pull_request("Acme/Widgets", 1))

        expected_stores = [
            cache_store.PULL_REQUESTS_STORE_NAME,
            cache_store.CHECK_RUNS_STORE_NAME,
            cache_store.WORKFLOW_RUNS_STORE_NAME,
            cache_store.DEPLOYMENTS_STORE_NAME,
            cache_store.DEPLOYMENT_STATUSES_STORE_NAME,
            cache_store.ISSUE_COMMENTS_STORE_NAME,
        ]
        self.assertEqual(set(self.database.records), set(expected_stores))
        self.assertEqual(
            self.database.create_attempts,
            expected_stores * 2,
        )
        pull_request_schema = self.database.schemas[
            cache_store.PULL_REQUESTS_STORE_NAME
        ]
        self.assertEqual(len(pull_request_schema.indexes), 1)
        self.assertEqual(
            pull_request_schema.indexes[0].name,
            cache_store.PULL_REQUEST_UPDATED_INDEX_NAME,
        )
        self.assertEqual(
            pull_request_schema.indexes[0].key_path,
            ["repository", "updated_at"],
        )

    def test_point_lookup_helpers_round_trip_each_entity_shape(self) -> None:
        cases = [
            (
                lambda: cache_store.put_pull_request(
                    "Acme/Widgets",
                    7,
                    {"number": 7, "updated_at": "2026-07-24T12:00:00Z"},
                    fetched_at=10.0,
                    source="webhook",
                ),
                lambda: cache_store.get_pull_request("acme/widgets", 7),
                {"number": 7, "updated_at": "2026-07-24T12:00:00Z"},
            ),
            (
                lambda: cache_store.put_check_run(
                    "acme/widgets",
                    8,
                    {"id": 8},
                    fetched_at=11.0,
                ),
                lambda: cache_store.get_check_run("acme/widgets", 8),
                {"id": 8},
            ),
            (
                lambda: cache_store.put_check_runs_for_ref(
                    "acme/widgets",
                    "abc123",
                    {"check_runs": [{"id": 8}]},
                    fetched_at=12.0,
                ),
                lambda: cache_store.get_check_runs_for_ref(
                    "acme/widgets", "abc123"
                ),
                {"check_runs": [{"id": 8}]},
            ),
            (
                lambda: cache_store.put_workflow_run(
                    "acme/widgets",
                    9,
                    {"id": 9},
                    fetched_at=13.0,
                ),
                lambda: cache_store.get_workflow_run("acme/widgets", 9),
                {"id": 9},
            ),
            (
                lambda: cache_store.put_deployment(
                    "acme/widgets",
                    10,
                    {"id": 10},
                    fetched_at=14.0,
                ),
                lambda: cache_store.get_deployment("acme/widgets", 10),
                {"id": 10},
            ),
            (
                lambda: cache_store.put_deployment_statuses(
                    "acme/widgets",
                    10,
                    [{"id": 11}],
                    fetched_at=15.0,
                ),
                lambda: cache_store.get_deployment_statuses(
                    "acme/widgets", 10
                ),
                [{"id": 11}],
            ),
            (
                lambda: cache_store.put_issue_comments(
                    "acme/widgets",
                    12,
                    [{"id": 13}],
                    fetched_at=16.0,
                ),
                lambda: cache_store.get_issue_comments("acme/widgets", 12),
                [{"id": 13}],
            ),
        ]

        for put, get, expected_data in cases:
            with self.subTest(expected_data=expected_data):
                put()
                record = get()
                self.assertIsNotNone(record)
                assert record is not None
                self.assertEqual(record["data"], expected_data)

    def test_query_pull_requests_updated_since_uses_repository_window(self) -> None:
        for repo, number, updated_at in [
            ("acme/widgets", 1, "2026-07-23T23:59:59Z"),
            ("acme/widgets", 2, "2026-07-24T00:00:00Z"),
            ("acme/widgets", 3, "2026-07-25T00:00:00Z"),
            ("other/widgets", 4, "2026-07-25T00:00:00Z"),
        ]:
            cache_store.put_pull_request(
                repo,
                number,
                {"number": number, "updated_at": updated_at},
                fetched_at=100.0 + number,
            )

        records = cache_store.query_pull_requests_updated_since(
            "ACME/Widgets", "2026-07-24T00:00:00Z"
        )

        self.assertEqual(
            [record["data"]["number"] for record in records],
            [2, 3],
        )

    def test_is_fresh_treats_ttl_boundary_and_future_timestamp_as_stale(
        self,
    ) -> None:
        with mock.patch.object(cache_store.time, "time", return_value=100.0):
            self.assertTrue(
                cache_store.is_fresh({"fetched_at": 90.001}, ttl_seconds=10.0)
            )
            self.assertFalse(
                cache_store.is_fresh({"fetched_at": 90.0}, ttl_seconds=10.0)
            )
            self.assertFalse(
                cache_store.is_fresh({"fetched_at": 100.001}, ttl_seconds=10.0)
            )
            self.assertFalse(
                cache_store.is_fresh({"fetched_at": 100.0}, ttl_seconds=0.0)
            )

    def test_invalidate_removes_record_and_is_idempotent(self) -> None:
        cache_store.put_pull_request(
            "acme/widgets",
            7,
            {"number": 7, "updated_at": "2026-07-24T12:00:00Z"},
            fetched_at=10.0,
        )
        key = cache_store.pull_request_key("acme/widgets", 7)

        cache_store.invalidate(cache_store.PULL_REQUESTS_STORE_NAME, key)
        cache_store.invalidate(cache_store.PULL_REQUESTS_STORE_NAME, key)

        self.assertIsNone(cache_store.get_pull_request("acme/widgets", 7))


def _key_in_range(key: list[Any], query: Any) -> bool:
    if query.lower is not None:
        if key < list(query.lower) or (query.lower_open and key == list(query.lower)):
            return False
    if query.upper is not None:
        if key > list(query.upper) or (query.upper_open and key == list(query.upper)):
            return False
    return True


if __name__ == "__main__":
    unittest.main()
