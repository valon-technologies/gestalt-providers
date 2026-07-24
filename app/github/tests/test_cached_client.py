from __future__ import annotations

import threading
import time
import unittest
from collections.abc import Mapping, Sequence
from typing import Any
from unittest import mock

from internals import cache_store
from internals.cached_client import CachingGitHubClient
from internals.client import GitHubPermissions, JsonObject, JsonPayload


class FakeGitHubClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.response: Any = {"value": 1}
        self.delay = 0.0

    def installation_token(
        self,
        installation_id: int,
        *,
        repositories: Sequence[str] | None = None,
        permissions: GitHubPermissions | None = None,
    ) -> str:
        self.calls.append(("TOKEN", str(installation_id)))
        return "token"

    def repository_installation_id(self, owner: str, repo: str) -> int:
        self.calls.append(("INSTALLATION", f"{owner}/{repo}"))
        return 99

    def github_json(
        self,
        method: str,
        path: str,
        token: str | None,
        payload: JsonPayload | None = None,
    ) -> JsonObject:
        _ = token, payload
        self.calls.append((method, path))
        if self.delay:
            time.sleep(self.delay)
        return self.response

    def github_json_value(
        self,
        method: str,
        path: str,
        token: str | None,
        payload: JsonPayload | None = None,
    ) -> Any:
        return self.github_json(method, path, token, payload)

    def graphql_json(
        self,
        query: str,
        token: str | None,
        variables: JsonPayload | None = None,
    ) -> JsonObject:
        _ = token, variables
        self.calls.append(("GRAPHQL", query))
        return self.response

    def repository_default_branch(self, token: str, owner: str, repo: str) -> str:
        return "main"

    def repository_installation(self, owner: str, repo: str) -> JsonObject:
        return {"id": 99}

    def app_installations(
        self, *, per_page: int = 100, page: int = 1
    ) -> list[JsonObject]:
        return []

    def installation_repositories(
        self, access_token: str, *, per_page: int = 100, page: int = 1
    ) -> list[JsonObject]:
        return []

    def get_branch_ref(
        self, token: str, owner: str, repo: str, branch: str
    ) -> JsonObject | None:
        return None

    def require_branch_ref(
        self, token: str, owner: str, repo: str, branch: str, field_name: str
    ) -> JsonObject:
        return {}

    def object_sha(self, ref: Mapping[str, Any], name: str) -> str:
        return ""

    def bot_identity_or_none(self) -> Any:
        return None

    def user_identity_by_id(self, user_id: str) -> Any:
        return None

    def commit_url(self, owner: str, repo: str, sha: str) -> str:
        return ""

    def workflow_job_logs(
        self, token: str, owner: str, repo: str, job_id: int
    ) -> str:
        return ""


class InMemoryCache:
    def __init__(self) -> None:
        self.responses: dict[str, cache_store.CachedResponse] = {}
        self.generation = 0
        self.incremented: list[set[str]] = []

    def get(
        self,
        scope: str,
        repository: str,
        operation: str,
        request: dict[str, Any],
        domain: str,
    ) -> tuple[cache_store.CachedResponse | None, str]:
        cached = self.responses.get(cache_store.response_id(scope, operation, request))
        return cached, "hit" if cached is not None else "miss"

    def put(
        self,
        scope: str,
        repository: str,
        operation: str,
        request: dict[str, Any],
        domain: str,
        body: Any,
        *,
        expected_generation: int,
        ttl_seconds: float,
    ) -> bool:
        record_id = cache_store.response_id(scope, operation, request)
        self.responses[record_id] = cache_store.CachedResponse(
            id=record_id,
            operation=operation,
            repository=repository,
            domain=domain,
            request=request,
            body=body,
            generation=expected_generation,
            fetched_at=1,
            expires_at=1 + ttl_seconds,
        )
        return True

    def get_generation(self, scope: str, repository: str, domain: str) -> int:
        return self.generation

    def increment(
        self, scope: str, repository: str, domains: set[str]
    ) -> dict[str, int]:
        self.generation += 1
        self.incremented.append(domains)
        return {domain: self.generation for domain in domains}


class CachingGitHubClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.inner = FakeGitHubClient()
        self.cache = InMemoryCache()
        patches = [
            mock.patch.object(
                cache_store, "lookup_cached_response", side_effect=self.cache.get
            ),
            mock.patch.object(
                cache_store,
                "put_cached_response_if_generation",
                side_effect=self.cache.put,
            ),
            mock.patch.object(
                cache_store, "get_generation", side_effect=self.cache.get_generation
            ),
            mock.patch.object(
                cache_store,
                "increment_generations",
                side_effect=self.cache.increment,
            ),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.client = CachingGitHubClient(
            self.inner,
            provider_name="github",
            app_id="123",
            api_base_url="https://api.github.com",
            ttl_seconds=60,
            search_pull_requests_query="query SearchPullRequests",
        )
        self.client.repository_installation_id("acme", "widgets")
        self.client.installation_token(99, repositories=["widgets"])

    def test_repeated_read_uses_exact_cached_response(self) -> None:
        path = "/repos/acme/widgets/pulls/7"

        first = self.client.github_json("GET", path, "token")
        second = self.client.github_json("GET", path, "token")

        self.assertEqual(first, second)
        self.assertEqual(self.inner.calls.count(("GET", path)), 1)

    def test_query_parameters_produce_distinct_list_entries(self) -> None:
        first_path = "/repos/acme/widgets/commits/main/check-runs?page=1"
        second_path = "/repos/acme/widgets/commits/main/check-runs?page=2"

        self.client.github_json("GET", first_path, "token")
        self.client.github_json("GET", second_path, "token")

        self.assertEqual(self.inner.calls.count(("GET", first_path)), 1)
        self.assertEqual(self.inner.calls.count(("GET", second_path)), 1)
        self.assertEqual(len(self.cache.responses), 2)

    def test_search_cache_key_includes_all_variables(self) -> None:
        query = "query SearchPullRequests"

        self.client.graphql_json(
            query, "token", {"query": "repo:acme/widgets", "first": 30}
        )
        self.client.graphql_json(
            query,
            "token",
            {"query": "repo:acme/widgets", "first": 30, "after": "cursor"},
        )

        self.assertEqual(self.inner.calls.count(("GRAPHQL", query)), 2)
        self.assertEqual(len(self.cache.responses), 2)

    def test_unknown_read_bypasses_cache(self) -> None:
        path = "/repos/acme/widgets/contents/README.md"

        self.client.github_json("GET", path, "token")
        self.client.github_json("GET", path, "token")

        self.assertEqual(self.inner.calls.count(("GET", path)), 2)
        self.assertEqual(self.cache.responses, {})

    def test_successful_mutation_invalidates_domain(self) -> None:
        self.client.github_json(
            "PATCH", "/repos/acme/widgets/pulls/7", "token", {"state": "closed"}
        )

        self.assertEqual(self.cache.incremented, [{"pull_request"}])

    def test_cache_read_error_falls_back_to_live(self) -> None:
        with mock.patch.object(
            cache_store, "lookup_cached_response", side_effect=RuntimeError("offline")
        ):
            result = self.client.github_json(
                "GET", "/repos/acme/widgets/pulls/7", "token"
            )

        self.assertEqual(result, {"value": 1})

    def test_concurrent_misses_are_single_flight(self) -> None:
        self.inner.delay = 0.05
        path = "/repos/acme/widgets/pulls/7"
        results: list[JsonObject] = []
        threads = [
            threading.Thread(
                target=lambda: results.append(
                    self.client.github_json("GET", path, "token")
                )
            )
            for _ in range(2)
        ]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(results, [{"value": 1}, {"value": 1}])
        self.assertEqual(self.inner.calls.count(("GET", path)), 1)


if __name__ == "__main__":
    unittest.main()
