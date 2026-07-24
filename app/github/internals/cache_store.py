from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass
from typing import Any, cast

import gestalt

from .cache_migrations import (
    ENTITIES_BY_UPDATED_AT_INDEX,
    ENTITIES_STORE_NAME,
    GENERATIONS_STORE_NAME,
    RECONCILE_STORE_NAME,
    RESPONSES_BY_EXPIRY_INDEX,
    RESPONSES_STORE_NAME,
)

DEFAULT_RETENTION_SECONDS = 24 * 60 * 60
DEFAULT_MAX_RESPONSES_PER_REPOSITORY = 5_000
DEFAULT_MAX_RESPONSES_TOTAL = 50_000
DEFAULT_MAX_ENTITIES_TOTAL = 50_000
_MAX_INDEX_STRING = "\uffff"


@dataclass(frozen=True, slots=True)
class CachedResponse:
    id: str
    operation: str
    repository: str
    domain: str
    request: dict[str, Any]
    body: Any
    generation: int
    fetched_at: float
    expires_at: float


@dataclass(frozen=True, slots=True)
class CachedEntity:
    entity_type: str
    entity_id: str
    repository: str
    updated_at: str
    observed_at: float
    deleted: bool
    payload: dict[str, Any]


_client: Any | None = None
_client_lock = threading.Lock()


def cache_scope(
    provider_name: str,
    api_base_url: str,
    graphql_base_url: str,
    app_id: str,
    installation_id: int,
) -> str:
    if installation_id <= 0:
        raise ValueError("installation_id must be positive")
    return (
        f"v1:{_nonempty(provider_name, 'provider_name')}:"
        f"{_nonempty(api_base_url, 'api_base_url').rstrip('/')}:"
        f"{_nonempty(graphql_base_url, 'graphql_base_url').rstrip('/')}:"
        f"{_nonempty(app_id, 'app_id')}:{installation_id}"
    )


def response_id(
    scope: str,
    repository: str,
    operation: str,
    request: dict[str, Any],
) -> str:
    digest = hashlib.sha256(_json_bytes(request)).hexdigest()
    return (
        f"{_nonempty(scope, 'scope')}:{_normalize_repository(repository)}:"
        f"{_nonempty(operation, 'operation')}:{digest}"
    )


def get_generation(scope: str, repository: str, domain: str) -> int:
    record_id = generation_id(scope, repository, domain)
    record = _optional_record(_store(GENERATIONS_STORE_NAME), record_id)
    return _record_generation(record)


def increment_generations(
    scope: str,
    repository: str,
    domains: set[str],
    *,
    now: float | None = None,
) -> dict[str, int]:
    if not domains:
        return {}
    observed_at = _now(now)
    db = _database()
    updated: dict[str, int] = {}
    with db.transaction([GENERATIONS_STORE_NAME], "readwrite") as transaction:
        store = transaction.object_store(GENERATIONS_STORE_NAME)
        for domain in sorted(domains):
            record_id = generation_id(scope, repository, domain)
            current = _record_generation(_optional_transaction_record(store, record_id))
            generation = current + 1
            store.put(
                {
                    "id": record_id,
                    "scope": scope,
                    "repository": _normalize_repository(repository),
                    "domain": domain,
                    "generation": generation,
                    "updated_at": observed_at,
                }
            )
            updated[domain] = generation
    return updated


def generation_id(scope: str, repository: str, domain: str) -> str:
    return (
        f"{_nonempty(scope, 'scope')}:{_normalize_repository(repository)}:"
        f"{_nonempty(domain, 'domain')}"
    )


def get_cached_response(
    scope: str,
    repository: str,
    operation: str,
    request: dict[str, Any],
    domain: str,
    *,
    now: float | None = None,
) -> CachedResponse | None:
    cached, _ = lookup_cached_response(
        scope,
        repository,
        operation,
        request,
        domain,
        now=now,
    )
    return cached


def lookup_cached_response(
    scope: str,
    repository: str,
    operation: str,
    request: dict[str, Any],
    domain: str,
    *,
    now: float | None = None,
) -> tuple[CachedResponse | None, str]:
    current_time = _now(now)
    record_id = response_id(scope, repository, operation, request)
    db = _database()
    with db.transaction(
        [RESPONSES_STORE_NAME, GENERATIONS_STORE_NAME], "readonly"
    ) as transaction:
        response_record = _optional_transaction_record(
            transaction.object_store(RESPONSES_STORE_NAME), record_id
        )
        if response_record is None:
            return None, "miss"
        generation_record = _optional_transaction_record(
            transaction.object_store(GENERATIONS_STORE_NAME),
            generation_id(scope, repository, domain),
        )
        current_generation = _record_generation(generation_record)
    cached = _response_from_record(response_record)
    if cached is None:
        return None, "miss"
    if cached.generation != current_generation or cached.expires_at <= current_time:
        return None, "stale"
    return cached, "hit"


def put_cached_response_if_generation(
    scope: str,
    repository: str,
    operation: str,
    request: dict[str, Any],
    domain: str,
    body: Any,
    *,
    expected_generation: int,
    ttl_seconds: float,
    now: float | None = None,
) -> bool:
    fetched_at = _now(now)
    if ttl_seconds <= 0:
        return False
    record_id = response_id(scope, repository, operation, request)
    normalized_repository = _normalize_repository(repository)
    db = _database()
    with db.transaction(
        [RESPONSES_STORE_NAME, GENERATIONS_STORE_NAME], "readwrite"
    ) as transaction:
        generation_record = _optional_transaction_record(
            transaction.object_store(GENERATIONS_STORE_NAME),
            generation_id(scope, repository, domain),
        )
        if _record_generation(generation_record) != expected_generation:
            return False
        transaction.object_store(RESPONSES_STORE_NAME).put(
            {
                "id": record_id,
                "scope": scope,
                "repository": normalized_repository,
                "operation": operation,
                "domain": domain,
                "generation": expected_generation,
                "request_json": _json_bytes(request),
                "body_json": _json_bytes(body),
                "fetched_at": fetched_at,
                "expires_at": fetched_at + ttl_seconds,
            }
        )
    return True


def delete_cached_response(record_id: str) -> None:
    try:
        _store(RESPONSES_STORE_NAME).delete(record_id)
    except gestalt.NotFoundError:
        pass


def list_expired_responses(
    scope: str,
    repository: str,
    *,
    now: float | None = None,
    limit: int = 25,
) -> list[CachedResponse]:
    if limit <= 0:
        return []
    query = gestalt.bound(
        [_nonempty(scope, "scope"), _normalize_repository(repository), 0.0],
        [_nonempty(scope, "scope"), _normalize_repository(repository), _now(now)],
    )
    records = (
        _store(RESPONSES_STORE_NAME)
        .index(RESPONSES_BY_EXPIRY_INDEX)
        .get_all(query=query, count=limit)
    )
    return [
        cached
        for record in records
        if (cached := _response_from_record(record)) is not None
    ]


def put_entity_if_newer(
    scope: str,
    repository: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any],
    *,
    updated_at: str,
    deleted: bool = False,
    observed_at: float | None = None,
) -> bool:
    updated, _ = put_entity_and_increment(
        scope,
        repository,
        entity_type,
        entity_id,
        payload,
        updated_at=updated_at,
        deleted=deleted,
        domains=set(),
        observed_at=observed_at,
    )
    return updated


def put_entity_and_increment(
    scope: str,
    repository: str,
    entity_type: str,
    entity_id: str,
    payload: dict[str, Any],
    *,
    updated_at: str,
    deleted: bool,
    domains: set[str],
    observed_at: float | None = None,
) -> tuple[bool, dict[str, int]]:
    normalized_repository = _normalize_repository(repository)
    record_id = cache_entity_id(scope, repository, entity_type, entity_id)
    observed = _now(observed_at)
    db = _database()
    generations: dict[str, int] = {}
    with db.transaction(
        [ENTITIES_STORE_NAME, GENERATIONS_STORE_NAME], "readwrite"
    ) as transaction:
        store = transaction.object_store(ENTITIES_STORE_NAME)
        current = _optional_transaction_record(store, record_id)
        if current is not None:
            current_version = str(current.get("updated_at") or "")
            if current_version > updated_at:
                return False, {}
            if current_version == updated_at:
                current_deleted = bool(current.get("deleted"))
                if current_deleted or not deleted:
                    return False, {}
        store.put(
            {
                "id": record_id,
                "scope": scope,
                "repository": normalized_repository,
                "entity_type": entity_type,
                "entity_id": entity_id,
                "updated_at": updated_at,
                "observed_at": observed,
                "deleted": deleted,
                "payload_json": _json_bytes(payload),
            }
        )
        generation_store = transaction.object_store(GENERATIONS_STORE_NAME)
        for domain in sorted(domains):
            generation_record_id = generation_id(scope, repository, domain)
            generation_record = _optional_transaction_record(
                generation_store, generation_record_id
            )
            generation = _record_generation(generation_record) + 1
            generation_store.put(
                {
                    "id": generation_record_id,
                    "scope": scope,
                    "repository": normalized_repository,
                    "domain": domain,
                    "generation": generation,
                    "updated_at": observed,
                }
            )
            generations[domain] = generation
    return True, generations


def get_entity(
    scope: str, repository: str, entity_type: str, entity_id: str
) -> CachedEntity | None:
    record = _optional_record(
        _store(ENTITIES_STORE_NAME),
        cache_entity_id(scope, repository, entity_type, entity_id),
    )
    return _entity_from_record(record)


def query_entities_updated_since(
    scope: str,
    repository: str,
    entity_type: str,
    updated_at: str,
) -> list[CachedEntity]:
    lower = [scope, _normalize_repository(repository), entity_type, updated_at]
    upper = [
        scope,
        _normalize_repository(repository),
        entity_type,
        _MAX_INDEX_STRING,
    ]
    records = (
        _store(ENTITIES_STORE_NAME)
        .index(ENTITIES_BY_UPDATED_AT_INDEX)
        .get_all(query=gestalt.bound(lower, upper))
    )
    return [
        entity
        for record in records
        if (entity := _entity_from_record(record)) is not None
    ]


def cache_entity_id(
    scope: str, repository: str, entity_type: str, entity_id: str
) -> str:
    return (
        f"{_nonempty(scope, 'scope')}:{_normalize_repository(repository)}:"
        f"{_nonempty(entity_type, 'entity_type')}:{_nonempty(entity_id, 'entity_id')}"
    )


def claim_reconcile_lease(
    scope: str,
    repository: str,
    token: str,
    *,
    now: float | None = None,
    lease_seconds: float = 300.0,
) -> bool:
    current_time = _now(now)
    record_id = reconcile_id(scope, repository)
    db = _database()
    with db.transaction([RECONCILE_STORE_NAME], "readwrite") as transaction:
        store = transaction.object_store(RECONCILE_STORE_NAME)
        current = _optional_transaction_record(store, record_id)
        if current is not None and _number(current.get("lease_until")) > current_time:
            return False
        store.put(
            {
                "id": record_id,
                "scope": scope,
                "repository": _normalize_repository(repository),
                "lease_token": _nonempty(token, "token"),
                "lease_until": current_time + lease_seconds,
                "updated_at": current_time,
            }
        )
    return True


def release_reconcile_lease(
    scope: str, repository: str, token: str, *, now: float | None = None
) -> bool:
    record_id = reconcile_id(scope, repository)
    db = _database()
    with db.transaction([RECONCILE_STORE_NAME], "readwrite") as transaction:
        store = transaction.object_store(RECONCILE_STORE_NAME)
        current = _optional_transaction_record(store, record_id)
        if current is None or current.get("lease_token") != token:
            return False
        current["lease_token"] = ""
        current["lease_until"] = 0.0
        current["updated_at"] = _now(now)
        store.put(current)
    return True


def reconcile_id(scope: str, repository: str) -> str:
    return f"{_nonempty(scope, 'scope')}:{_normalize_repository(repository)}"


def prune_responses(
    scope: str,
    *,
    repository: str | None = None,
    now: float | None = None,
    retention_seconds: float = DEFAULT_RETENTION_SECONDS,
    max_per_repository: int = DEFAULT_MAX_RESPONSES_PER_REPOSITORY,
    max_total: int = DEFAULT_MAX_RESPONSES_TOTAL,
) -> int:
    current_time = _now(now)
    records = _store(RESPONSES_STORE_NAME).get_all()
    scoped = [
        record
        for record in records
        if record.get("scope") == scope
        and (
            repository is None
            or record.get("repository") == _normalize_repository(repository)
        )
    ]
    delete_ids = {
        str(record.get("id") or "")
        for record in scoped
        if _number(record.get("fetched_at")) <= current_time - retention_seconds
    }
    by_repository: dict[str, list[dict[str, Any]]] = {}
    for record in scoped:
        by_repository.setdefault(str(record.get("repository") or ""), []).append(record)
    for repo_records in by_repository.values():
        _mark_oldest_over_limit(repo_records, max_per_repository, delete_ids)
    if repository is None:
        _mark_oldest_over_limit(scoped, max_total, delete_ids)
    for record_id in delete_ids:
        if record_id:
            delete_cached_response(record_id)
    return len([record_id for record_id in delete_ids if record_id])


def prune_entities(
    scope: str,
    *,
    now: float | None = None,
    retention_seconds: float = DEFAULT_RETENTION_SECONDS,
    max_total: int = DEFAULT_MAX_ENTITIES_TOTAL,
) -> int:
    current_time = _now(now)
    store = _store(ENTITIES_STORE_NAME)
    records = [
        record
        for record in store.get_all()
        if record.get("scope") == scope
    ]
    delete_ids = {
        str(record.get("id") or "")
        for record in records
        if _number(record.get("observed_at")) <= current_time - retention_seconds
    }
    retained = [
        record
        for record in records
        if str(record.get("id") or "") not in delete_ids
    ]
    retained.sort(
        key=lambda record: _number(record.get("observed_at")),
        reverse=True,
    )
    for record in retained[max(0, max_total) :]:
        delete_ids.add(str(record.get("id") or ""))
    for record_id in delete_ids:
        if record_id:
            try:
                store.delete(record_id)
            except gestalt.NotFoundError:
                pass
    return len([record_id for record_id in delete_ids if record_id])


def close_cache() -> None:
    global _client
    with _client_lock:
        client = _client
        _client = None
    if client is not None:
        client.close()


def _database() -> Any:
    global _client
    if _client is not None:
        return _client
    with _client_lock:
        if _client is None:
            _client = gestalt.IndexedDB()
        return _client


def _store(name: str) -> Any:
    return _database().object_store(name)


def _optional_record(store: Any, record_id: str) -> dict[str, Any] | None:
    try:
        return store.get(record_id)
    except gestalt.NotFoundError:
        return None


def _optional_transaction_record(
    store: Any, record_id: str
) -> dict[str, Any] | None:
    records = store.get_all(gestalt.only(record_id), count=1)
    return records[0] if records else None


def _record_generation(record: dict[str, Any] | None) -> int:
    if record is None:
        return 0
    value = record.get("generation")
    return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0


def _response_from_record(record: object) -> CachedResponse | None:
    if not isinstance(record, dict):
        return None
    record_data = cast(dict[str, Any], record)
    try:
        return CachedResponse(
            id=str(record_data["id"]),
            operation=str(record_data["operation"]),
            repository=str(record_data["repository"]),
            domain=str(record_data["domain"]),
            request=_json_value(record_data["request_json"], expected=dict),
            body=_json_value(record_data["body_json"]),
            generation=int(record_data["generation"]),
            fetched_at=float(record_data["fetched_at"]),
            expires_at=float(record_data["expires_at"]),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def _entity_from_record(record: object) -> CachedEntity | None:
    if not isinstance(record, dict):
        return None
    record_data = cast(dict[str, Any], record)
    try:
        return CachedEntity(
            entity_type=str(record_data["entity_type"]),
            entity_id=str(record_data["entity_id"]),
            repository=str(record_data["repository"]),
            updated_at=str(record_data["updated_at"]),
            observed_at=float(record_data["observed_at"]),
            deleted=bool(record_data["deleted"]),
            payload=_json_value(record_data["payload_json"], expected=dict),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError):
        return None


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _json_value(value: object, *, expected: type | None = None) -> Any:
    if not isinstance(value, (bytes, bytearray)):
        raise TypeError("cached JSON must be bytes")
    decoded = json.loads(bytes(value).decode("utf-8"))
    if expected is not None and not isinstance(decoded, expected):
        raise TypeError(f"cached JSON must decode to {expected.__name__}")
    return decoded


def _normalize_repository(repository: str) -> str:
    return _nonempty(repository, "repository").casefold()


def _nonempty(value: str, name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _now(value: float | None) -> float:
    current = time.time() if value is None else float(value)
    if not current >= 0:
        raise ValueError("timestamp must be non-negative")
    return current


def _number(value: object) -> float:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _mark_oldest_over_limit(
    records: list[dict[str, Any]], limit: int, delete_ids: set[str]
) -> None:
    if limit < 0:
        limit = 0
    retained = [
        record for record in records if str(record.get("id") or "") not in delete_ids
    ]
    retained.sort(key=lambda record: _number(record.get("fetched_at")), reverse=True)
    for record in retained[limit:]:
        delete_ids.add(str(record.get("id") or ""))
