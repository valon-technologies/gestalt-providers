from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from typing import Any

import gestalt

from . import cache_store
from .client import DEFAULT_GITHUB_CLIENT, GitHubAPIClient
from .config import get_github_config
from .errors import GitHubAPIError
from .operations import require_slug, scoped_installation_id


@dataclass(frozen=True, slots=True)
class ReconcileReport:
    repository: str
    checked: int = 0
    drifted: int = 0
    refreshed: int = 0
    deleted: int = 0
    failed: int = 0
    pruned: int = 0
    lease_acquired: bool = False
    disabled: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def reconcile_cache(
    owner: str,
    repo: str,
    max_entries: int,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> ReconcileReport:
    config = get_github_config()
    repository = f"{require_slug(owner, 'owner')}/{require_slug(repo, 'repo')}"
    if not config.cache_enabled:
        return ReconcileReport(repository=repository, disabled=True)
    limit = max(1, min(int(max_entries), 25))
    github = client or DEFAULT_GITHUB_CLIENT
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    scope = cache_store.cache_scope(
        config.provider_name,
        config.api_base_url,
        config.graphql_base_url,
        config.app_id,
        installation_id,
    )
    lease_token = uuid.uuid4().hex
    if not cache_store.claim_reconcile_lease(
        scope, repository, lease_token, lease_seconds=900
    ):
        return ReconcileReport(repository=repository)
    checked = drifted = refreshed = deleted = failed = 0
    try:
        records = cache_store.list_expired_responses(
            scope, repository, limit=limit
        )
        for record in records:
            checked += 1
            generation = cache_store.get_generation(
                scope, repository, record.domain
            )
            try:
                live = _replay_response(
                    github,
                    installation_id,
                    repo,
                    record.request,
                )
            except GitHubAPIError as error:
                if error.status == 404:
                    cache_store.delete_cached_response(record.id)
                    deleted += 1
                else:
                    failed += 1
                continue
            except Exception:
                failed += 1
                continue
            if _canonical_json(live) != _canonical_json(record.body):
                drifted += 1
            if cache_store.put_cached_response_if_generation(
                scope,
                repository,
                record.operation,
                record.request,
                record.domain,
                live,
                expected_generation=generation,
                ttl_seconds=config.cache_ttl_seconds,
            ):
                refreshed += 1
        pruned = cache_store.prune_responses(scope)
        pruned += cache_store.prune_entities(scope)
    finally:
        cache_store.release_reconcile_lease(scope, repository, lease_token)
    return ReconcileReport(
        repository=repository,
        checked=checked,
        drifted=drifted,
        refreshed=refreshed,
        deleted=deleted,
        failed=failed,
        pruned=pruned,
        lease_acquired=True,
    )


def _replay_response(
    github: GitHubAPIClient,
    installation_id: int,
    repo: str,
    request: dict[str, Any],
) -> Any:
    permissions = request.get("permissions")
    normalized_permissions = (
        {
            str(key): str(value)
            for key, value in permissions.items()
            if isinstance(key, str) and isinstance(value, str)
        }
        if isinstance(permissions, dict)
        else {}
    )
    token = github.installation_token(
        installation_id,
        repositories=[repo],
        permissions=normalized_permissions,
    )
    kind = request.get("kind")
    if kind == "graphql":
        query = request.get("query")
        variables = request.get("variables")
        if not isinstance(query, str) or not isinstance(variables, dict):
            raise ValueError("invalid cached GraphQL request descriptor")
        return github.graphql_json(query, token, variables)
    if kind != "rest":
        raise ValueError("invalid cached request kind")
    method = request.get("method")
    path = request.get("path")
    if not isinstance(method, str) or not isinstance(path, str):
        raise ValueError("invalid cached REST request descriptor")
    if request.get("response_kind") == "value":
        return github.github_json_value(method, path, token)
    return github.github_json(method, path, token)


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
