from __future__ import annotations

import logging
import re
import threading
from collections.abc import Callable, Mapping, Sequence
from typing import Any, TypeVar

from . import cache_store
from .client import (
    GitHubAPIClient,
    GitHubBotIdentity,
    GitHubPermissions,
    GitHubUserIdentity,
    JsonObject,
    JsonPayload,
)

logger = logging.getLogger(__name__)
T = TypeVar("T")

_KEY_LOCKS: dict[str, threading.Lock] = {}
_KEY_LOCKS_GUARD = threading.Lock()


class CachingGitHubClient:
    def __init__(
        self,
        inner: GitHubAPIClient,
        *,
        provider_name: str,
        app_id: str,
        api_base_url: str,
        ttl_seconds: float,
        search_pull_requests_query: str,
    ) -> None:
        self.inner = inner
        self.provider_name = provider_name
        self.app_id = app_id
        self.api_base_url = api_base_url
        self.ttl_seconds = ttl_seconds
        self.search_pull_requests_query = search_pull_requests_query
        self._installation_id = 0
        self._repository = ""

    def installation_token(
        self,
        installation_id: int,
        *,
        repositories: Sequence[str] | None = None,
        permissions: GitHubPermissions | None = None,
    ) -> str:
        token = self.inner.installation_token(
            installation_id,
            repositories=repositories,
            permissions=permissions,
        )
        self._installation_id = installation_id
        return token

    def repository_installation_id(self, owner: str, repo: str) -> int:
        installation_id = self.inner.repository_installation_id(owner, repo)
        self._installation_id = installation_id
        self._repository = f"{owner}/{repo}".casefold()
        return installation_id

    def github_json(
        self,
        method: str,
        path: str,
        token: str | None,
        payload: JsonPayload | None = None,
    ) -> JsonObject:
        policy = _rest_policy(method, path)
        call = lambda: self.inner.github_json(method, path, token, payload)
        if policy is None:
            result = call()
            self._invalidate_after_mutation(method, path)
            return result
        return self._read_through(policy, _rest_request(method, path), call)

    def github_json_value(
        self,
        method: str,
        path: str,
        token: str | None,
        payload: JsonPayload | None = None,
    ) -> Any:
        policy = _rest_policy(method, path)
        call = lambda: self.inner.github_json_value(method, path, token, payload)
        if policy is None:
            result = call()
            self._invalidate_after_mutation(method, path)
            return result
        return self._read_through(policy, _rest_request(method, path), call)

    def graphql_json(
        self,
        query: str,
        token: str | None,
        variables: JsonPayload | None = None,
    ) -> JsonObject:
        call = lambda: self.inner.graphql_json(query, token, variables)
        if query != self.search_pull_requests_query:
            result = call()
            if query.lstrip().startswith("mutation"):
                self._increment_domains({"pull_request"})
            return result
        policy = CachePolicy("bot.searchPullRequests", "pull_request")
        request = {
            "kind": "graphql",
            "query": query,
            "variables": dict(variables or {}),
        }
        return self._read_through(policy, request, call)

    def repository_default_branch(self, token: str, owner: str, repo: str) -> str:
        return self.inner.repository_default_branch(token, owner, repo)

    def repository_installation(self, owner: str, repo: str) -> JsonObject:
        return self.inner.repository_installation(owner, repo)

    def app_installations(
        self, *, per_page: int = 100, page: int = 1
    ) -> list[JsonObject]:
        return self.inner.app_installations(per_page=per_page, page=page)

    def installation_repositories(
        self, access_token: str, *, per_page: int = 100, page: int = 1
    ) -> list[JsonObject]:
        return self.inner.installation_repositories(
            access_token, per_page=per_page, page=page
        )

    def get_branch_ref(
        self, token: str, owner: str, repo: str, branch: str
    ) -> JsonObject | None:
        return self.inner.get_branch_ref(token, owner, repo, branch)

    def require_branch_ref(
        self, token: str, owner: str, repo: str, branch: str, field_name: str
    ) -> JsonObject:
        return self.inner.require_branch_ref(token, owner, repo, branch, field_name)

    def object_sha(self, ref: Mapping[str, Any], name: str) -> str:
        return self.inner.object_sha(ref, name)

    def bot_identity_or_none(self) -> GitHubBotIdentity | None:
        return self.inner.bot_identity_or_none()

    def user_identity_by_id(self, user_id: str) -> GitHubUserIdentity | None:
        return self.inner.user_identity_by_id(user_id)

    def commit_url(self, owner: str, repo: str, sha: str) -> str:
        return self.inner.commit_url(owner, repo, sha)

    def workflow_job_logs(
        self, token: str, owner: str, repo: str, job_id: int
    ) -> str:
        return self.inner.workflow_job_logs(token, owner, repo, job_id)

    def _read_through(
        self,
        policy: CachePolicy,
        request: dict[str, Any],
        live_call: Callable[[], T],
    ) -> T:
        repository = self._repository
        scope = self._scope()
        if not repository or not scope:
            return live_call()
        key = cache_store.response_id(scope, policy.operation, request)
        lock = _key_lock(key)
        with lock:
            try:
                cached = cache_store.get_cached_response(
                    scope,
                    repository,
                    policy.operation,
                    request,
                    policy.domain,
                )
                if cached is not None:
                    _cache_log("hit", policy, repository)
                    return cached.body
                generation = cache_store.get_generation(
                    scope, repository, policy.domain
                )
                _cache_log("miss", policy, repository)
            except Exception:
                logger.exception(
                    "GitHub cache read failed",
                    extra={
                        "github_cache_outcome": "error",
                        "github_cache_operation": policy.operation,
                        "github_cache_repository": repository,
                    },
                )
                return live_call()
            result = live_call()
            try:
                cache_store.put_cached_response_if_generation(
                    scope,
                    repository,
                    policy.operation,
                    request,
                    policy.domain,
                    result,
                    expected_generation=generation,
                    ttl_seconds=self.ttl_seconds,
                )
            except Exception:
                logger.exception(
                    "GitHub cache write failed",
                    extra={
                        "github_cache_outcome": "error",
                        "github_cache_operation": policy.operation,
                        "github_cache_repository": repository,
                    },
                )
            return result

    def _scope(self) -> str:
        if self._installation_id <= 0:
            return ""
        return (
            f"v1:{self.provider_name}:{self.api_base_url.casefold()}:"
            f"{self.app_id}:{self._installation_id}"
        )

    def _invalidate_after_mutation(self, method: str, path: str) -> None:
        if method.upper() == "GET":
            return
        domains = _mutation_domains(path)
        if domains:
            self._increment_domains(domains)

    def _increment_domains(self, domains: set[str]) -> None:
        scope = self._scope()
        if not scope or not self._repository:
            return
        try:
            cache_store.increment_generations(
                scope, self._repository, domains
            )
            logger.info(
                "Invalidated GitHub cache generations",
                extra={
                    "github_cache_outcome": "invalidate",
                    "github_cache_repository": self._repository,
                    "github_cache_domains": sorted(domains),
                },
            )
        except Exception:
            logger.exception(
                "GitHub cache invalidation failed",
                extra={
                    "github_cache_outcome": "error",
                    "github_cache_repository": self._repository,
                },
            )


class CachePolicy:
    __slots__ = ("operation", "domain")

    def __init__(self, operation: str, domain: str) -> None:
        self.operation = operation
        self.domain = domain


_REST_READ_POLICIES: tuple[tuple[re.Pattern[str], CachePolicy], ...] = (
    (
        re.compile(r"^/repos/[^/]+/[^/]+/pulls/\d+$"),
        CachePolicy("bot.getPullRequest", "pull_request"),
    ),
    (
        re.compile(r"^/repos/[^/]+/[^/]+/check-runs/\d+$"),
        CachePolicy("bot.getCheckRun", "check_run"),
    ),
    (
        re.compile(r"^/repos/[^/]+/[^/]+/actions/runs/\d+$"),
        CachePolicy("bot.getWorkflowRun", "workflow_run"),
    ),
    (
        re.compile(r"^/repos/[^/]+/[^/]+/check-suites/\d+/check-runs(?:\?.*)?$"),
        CachePolicy("bot.listCheckSuiteCheckRuns", "check_run"),
    ),
    (
        re.compile(r"^/repos/[^/]+/[^/]+/commits/.+/check-runs(?:\?.*)?$"),
        CachePolicy("bot.listCommitCheckRuns", "check_run"),
    ),
    (
        re.compile(
            r"^/repos/[^/]+/[^/]+/actions/(?:runs|workflows/[^/]+/runs)(?:\?.*)?$"
        ),
        CachePolicy("bot.listWorkflowRuns", "workflow_run"),
    ),
    (
        re.compile(r"^/repos/[^/]+/[^/]+/issues/\d+/comments(?:\?.*)?$"),
        CachePolicy("bot.listIssueComments", "issue_comment"),
    ),
    (
        re.compile(r"^/repos/[^/]+/[^/]+/commits/.+/pulls(?:\?.*)?$"),
        CachePolicy("bot.listPullRequestsForCommit", "pull_request"),
    ),
    (
        re.compile(r"^/repos/[^/]+/[^/]+/compare/.+\.\.\..+$"),
        CachePolicy("bot.compareRefs", "pull_request"),
    ),
)


def _rest_policy(method: str, path: str) -> CachePolicy | None:
    if method.upper() != "GET":
        return None
    normalized_path = path.split("#", 1)[0]
    for pattern, policy in _REST_READ_POLICIES:
        if pattern.fullmatch(normalized_path):
            return policy
    return None


def _rest_request(method: str, path: str) -> dict[str, Any]:
    return {"kind": "rest", "method": method.upper(), "path": path}


def _mutation_domains(path: str) -> set[str]:
    path = path.split("?", 1)[0]
    if "/check-runs" in path:
        return {"check_run"}
    if re.search(r"/issues/\d+/comments(?:/|$)", path):
        return {"issue_comment", "pull_request"}
    if "/pulls" in path or re.search(r"/issues/\d+/(?:labels|assignees)", path):
        return {"pull_request"}
    if re.search(r"/issues/\d+$", path):
        return {"pull_request"}
    return set()


def _key_lock(key: str) -> threading.Lock:
    with _KEY_LOCKS_GUARD:
        return _KEY_LOCKS.setdefault(key, threading.Lock())


def _cache_log(outcome: str, policy: CachePolicy, repository: str) -> None:
    logger.info(
        "GitHub cache lookup",
        extra={
            "github_cache_outcome": outcome,
            "github_cache_operation": policy.operation,
            "github_cache_repository": repository,
        },
    )
