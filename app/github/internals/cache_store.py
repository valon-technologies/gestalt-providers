from __future__ import annotations

import math
import threading
import time
from copy import deepcopy
from typing import Any, Literal, Mapping, TypedDict

import gestalt

PULL_REQUESTS_STORE_NAME = "github_cache_pull_requests"
CHECK_RUNS_STORE_NAME = "github_cache_check_runs"
WORKFLOW_RUNS_STORE_NAME = "github_cache_workflow_runs"
DEPLOYMENTS_STORE_NAME = "github_cache_deployments"
DEPLOYMENT_STATUSES_STORE_NAME = "github_cache_deployment_statuses"
ISSUE_COMMENTS_STORE_NAME = "github_cache_issue_comments"

PULL_REQUEST_UPDATED_INDEX_NAME = "by_repository_updated_at"
_MAX_INDEX_STRING = "\uffff"

CacheSource = Literal["live", "webhook"]


class CachedRecord(TypedDict):
    data: Any
    fetched_at: float
    source: CacheSource


_client: Any | None = None
_init_lock = threading.Lock()
_initialized = False


def get_pull_request(repo: str, number: int) -> CachedRecord | None:
    return _get_record(PULL_REQUESTS_STORE_NAME, pull_request_key(repo, number))


def put_pull_request(
    repo: str,
    number: int,
    data: dict[str, Any],
    *,
    fetched_at: float,
    source: CacheSource = "live",
) -> None:
    updated_at = data.get("updated_at") or data.get("updatedAt")
    _put_record(
        PULL_REQUESTS_STORE_NAME,
        pull_request_key(repo, number),
        data,
        fetched_at=fetched_at,
        source=source,
        index_fields={
            "repository": _normalize_repo(repo),
            "updated_at": updated_at if isinstance(updated_at, str) else "",
        },
    )


def query_pull_requests_updated_since(
    repo: str, since_iso: str
) -> list[CachedRecord]:
    normalized_since = since_iso.strip()
    if not normalized_since:
        raise ValueError("since_iso must not be empty")
    repository = _normalize_repo(repo)
    query = gestalt.bound(
        [repository, normalized_since],
        [repository, _MAX_INDEX_STRING],
    )
    records = (
        _object_store(PULL_REQUESTS_STORE_NAME)
        .index(PULL_REQUEST_UPDATED_INDEX_NAME)
        .get_all(query=query)
    )
    return [
        cached
        for record in records
        if (cached := _cached_record(record)) is not None
    ]


def get_check_run(repo: str, check_run_id: int) -> CachedRecord | None:
    return _get_record(CHECK_RUNS_STORE_NAME, check_run_key(repo, check_run_id))


def put_check_run(
    repo: str,
    check_run_id: int,
    data: dict[str, Any],
    *,
    fetched_at: float,
    source: CacheSource = "live",
) -> None:
    _put_record(
        CHECK_RUNS_STORE_NAME,
        check_run_key(repo, check_run_id),
        data,
        fetched_at=fetched_at,
        source=source,
    )


def get_check_runs_for_ref(repo: str, ref: str) -> CachedRecord | None:
    return _get_record(CHECK_RUNS_STORE_NAME, check_runs_for_ref_key(repo, ref))


def put_check_runs_for_ref(
    repo: str,
    ref: str,
    data: Any,
    *,
    fetched_at: float,
    source: CacheSource = "live",
) -> None:
    _put_record(
        CHECK_RUNS_STORE_NAME,
        check_runs_for_ref_key(repo, ref),
        data,
        fetched_at=fetched_at,
        source=source,
    )


def get_workflow_run(repo: str, run_id: int) -> CachedRecord | None:
    return _get_record(WORKFLOW_RUNS_STORE_NAME, workflow_run_key(repo, run_id))


def put_workflow_run(
    repo: str,
    run_id: int,
    data: dict[str, Any],
    *,
    fetched_at: float,
    source: CacheSource = "live",
) -> None:
    _put_record(
        WORKFLOW_RUNS_STORE_NAME,
        workflow_run_key(repo, run_id),
        data,
        fetched_at=fetched_at,
        source=source,
    )


def get_deployment(repo: str, deployment_id: int) -> CachedRecord | None:
    return _get_record(DEPLOYMENTS_STORE_NAME, deployment_key(repo, deployment_id))


def put_deployment(
    repo: str,
    deployment_id: int,
    data: dict[str, Any],
    *,
    fetched_at: float,
    source: CacheSource = "live",
) -> None:
    _put_record(
        DEPLOYMENTS_STORE_NAME,
        deployment_key(repo, deployment_id),
        data,
        fetched_at=fetched_at,
        source=source,
    )


def get_deployment_statuses(
    repo: str, deployment_id: int
) -> CachedRecord | None:
    return _get_record(
        DEPLOYMENT_STATUSES_STORE_NAME,
        deployment_statuses_key(repo, deployment_id),
    )


def put_deployment_statuses(
    repo: str,
    deployment_id: int,
    data: Any,
    *,
    fetched_at: float,
    source: CacheSource = "live",
) -> None:
    _put_record(
        DEPLOYMENT_STATUSES_STORE_NAME,
        deployment_statuses_key(repo, deployment_id),
        data,
        fetched_at=fetched_at,
        source=source,
    )


def get_issue_comments(repo: str, issue_number: int) -> CachedRecord | None:
    return _get_record(
        ISSUE_COMMENTS_STORE_NAME,
        issue_comments_key(repo, issue_number),
    )


def put_issue_comments(
    repo: str,
    issue_number: int,
    data: Any,
    *,
    fetched_at: float,
    source: CacheSource = "live",
) -> None:
    _put_record(
        ISSUE_COMMENTS_STORE_NAME,
        issue_comments_key(repo, issue_number),
        data,
        fetched_at=fetched_at,
        source=source,
    )


def is_fresh(record: Mapping[str, Any], ttl_seconds: float) -> bool:
    fetched_at = record.get("fetched_at")
    if (
        isinstance(fetched_at, bool)
        or not isinstance(fetched_at, (int, float))
        or not math.isfinite(float(fetched_at))
        or not math.isfinite(ttl_seconds)
        or ttl_seconds <= 0
    ):
        return False
    age = time.time() - float(fetched_at)
    return 0 <= age < ttl_seconds


def invalidate(store_name: str, key: str) -> None:
    try:
        _object_store(store_name).delete(key)
    except gestalt.NotFoundError:
        pass


def pull_request_key(repo: str, number: int) -> str:
    return f"{_normalize_repo(repo)}#{_positive_int(number, 'number')}"


def check_run_key(repo: str, check_run_id: int) -> str:
    normalized_id = _positive_int(check_run_id, "check_run_id")
    return f"{_normalize_repo(repo)}#check-run:{normalized_id}"


def check_runs_for_ref_key(repo: str, ref: str) -> str:
    return f"{_normalize_repo(repo)}#check-runs-for-ref:{_nonempty(ref, 'ref')}"


def workflow_run_key(repo: str, run_id: int) -> str:
    normalized_id = _positive_int(run_id, "run_id")
    return f"{_normalize_repo(repo)}#workflow-run:{normalized_id}"


def deployment_key(repo: str, deployment_id: int) -> str:
    normalized_id = _positive_int(deployment_id, "deployment_id")
    return f"{_normalize_repo(repo)}#deployment:{normalized_id}"


def deployment_statuses_key(repo: str, deployment_id: int) -> str:
    return (
        f"{_normalize_repo(repo)}#deployment-statuses:"
        f"{_positive_int(deployment_id, 'deployment_id')}"
    )


def issue_comments_key(repo: str, issue_number: int) -> str:
    return (
        f"{_normalize_repo(repo)}#issue-comments:"
        f"{_positive_int(issue_number, 'issue_number')}"
    )


def _ensure_initialized() -> None:
    global _client, _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        client = gestalt.IndexedDB()
        stores = (
            (
                PULL_REQUESTS_STORE_NAME,
                gestalt.ObjectStoreSchema(
                    indexes=[
                        gestalt.IndexSchema(
                            name=PULL_REQUEST_UPDATED_INDEX_NAME,
                            key_path=["repository", "updated_at"],
                        )
                    ]
                ),
            ),
            (CHECK_RUNS_STORE_NAME, None),
            (WORKFLOW_RUNS_STORE_NAME, None),
            (DEPLOYMENTS_STORE_NAME, None),
            (DEPLOYMENT_STATUSES_STORE_NAME, None),
            (ISSUE_COMMENTS_STORE_NAME, None),
        )
        for object_store_name, schema in stores:
            try:
                client.create_object_store(object_store_name, schema)
            except gestalt.AlreadyExistsError:
                pass
        _client = client
        _initialized = True


def _object_store(name: str) -> Any:
    _ensure_initialized()
    assert _client is not None
    return _client.object_store(name)


def _get_record(store_name: str, key: str) -> CachedRecord | None:
    try:
        record = _object_store(store_name).get(key)
    except gestalt.NotFoundError:
        return None
    return _cached_record(record)


def _put_record(
    store_name: str,
    key: str,
    data: Any,
    *,
    fetched_at: float,
    source: CacheSource,
    index_fields: Mapping[str, Any] | None = None,
) -> None:
    if isinstance(fetched_at, bool) or not math.isfinite(fetched_at):
        raise ValueError("fetched_at must be a finite epoch timestamp")
    if source not in ("live", "webhook"):
        raise ValueError("source must be 'live' or 'webhook'")
    record = {
        "id": key,
        "data": deepcopy(data),
        "fetched_at": float(fetched_at),
        "source": source,
    }
    if index_fields is not None:
        record.update(index_fields)
    _object_store(store_name).put(record)


def _cached_record(record: object) -> CachedRecord | None:
    if not isinstance(record, Mapping):
        return None
    fetched_at = record.get("fetched_at")
    source = record.get("source")
    if (
        isinstance(fetched_at, bool)
        or not isinstance(fetched_at, (int, float))
        or not math.isfinite(float(fetched_at))
        or source not in ("live", "webhook")
        or "data" not in record
    ):
        return None
    return {
        "data": deepcopy(record["data"]),
        "fetched_at": float(fetched_at),
        "source": source,
    }


def _normalize_repo(repo: str) -> str:
    return _nonempty(repo, "repo").casefold()


def _nonempty(value: str, name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _positive_int(value: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _reset_for_tests() -> None:
    global _client, _initialized
    with _init_lock:
        _client = None
        _initialized = False
