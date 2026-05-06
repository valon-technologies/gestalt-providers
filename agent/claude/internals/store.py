from __future__ import annotations

import base64
import copy
import os
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any

import gestalt
import grpc

TERMINAL_STATUSES = {
    gestalt.AGENT_EXECUTION_STATUS_SUCCEEDED,
    gestalt.AGENT_EXECUTION_STATUS_FAILED,
    gestalt.AGENT_EXECUTION_STATUS_CANCELED,
}

BUSY_RETRY_INITIAL_DELAY_SECONDS = 0.02
BUSY_RETRY_MAX_DELAY_SECONDS = 0.25
PROJECTION_SCHEMA_VERSION = 1
PROJECTION_SEP = "\x1f"
PROJECTION_RANGE_SUFFIX = "\x7f"
MAX_INVERTED_SORT_MICROS = 99_999_999_999_999_999_999
TURN_IDEMPOTENCY_SEP = "\x1f"
RANGE_SUFFIX = "\U0010ffff"


class StoreConflictError(ValueError):
    pass


class StoreUnavailableError(RuntimeError):
    pass


@dataclass(slots=True)
class StoredSession:
    session_id: str
    idempotency_key: str
    provider_name: str
    model: str
    client_ref: str
    state: int
    metadata: dict[str, Any]
    prepared_workspace: dict[str, str] | None
    created_by: dict[str, str]
    created_at: datetime
    updated_at: datetime
    last_turn_at: datetime | None = None


@dataclass(slots=True)
class StoredTurn:
    turn_id: str
    session_id: str
    idempotency_key: str
    provider_name: str
    model: str
    status: int
    messages: list[dict[str, Any]]
    output_text: str
    status_message: str
    created_by: dict[str, str]
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    execution_ref: str


@dataclass(slots=True)
class StoredTurnEvent:
    event_id: str
    turn_id: str
    seq: int
    event_type: str
    source: str
    visibility: str
    data: dict[str, Any]
    created_at: datetime


class IndexedDBRunStore:
    def __init__(self, *, run_store: str, idempotency_store: str) -> None:
        self._client: Any | None = None
        self._turns = _LazyObjectStore(self, run_store)
        self._events = _LazyObjectStore(self, f"{run_store}_events")
        self._sessions = _LazyObjectStore(self, f"{run_store}_sessions")
        self._session_projections = _LazyObjectStore(self, f"{run_store}_session_projections")
        self._turn_projections = _LazyObjectStore(self, f"{run_store}_turn_projections")
        self._session_idempotency = _LazyObjectStore(self, f"{idempotency_store}_sessions")
        self._turn_idempotency = _LazyObjectStore(self, f"{idempotency_store}_turns")
        self._run_store_name = run_store
        self._event_store_name = f"{run_store}_events"
        self._session_store_name = f"{run_store}_sessions"
        self._session_projection_store_name = f"{run_store}_session_projections"
        self._turn_projection_store_name = f"{run_store}_turn_projections"
        self._session_idempotency_store_name = f"{idempotency_store}_sessions"
        self._turn_idempotency_store_name = f"{idempotency_store}_turns"
        self._initialize_lock = threading.RLock()
        self._mutation_lock = threading.Lock()
        self._initialized = False
        self._closed = False

    def initialize(self) -> None:
        if self._initialized:
            return
        with self._initialize_lock:
            if self._initialized:
                return
            try:
                self._ensure_client()
            except Exception:
                self._close_client()
                raise
            self._initialized = True

    def close(self) -> None:
        with self._initialize_lock:
            self._closed = True
            self._close_client()

    def create_session(
        self,
        *,
        session_id: str,
        idempotency_key: str,
        provider_name: str,
        model: str,
        client_ref: str,
        metadata: dict[str, Any],
        prepared_workspace: dict[str, str] | None,
        created_by: dict[str, str],
    ) -> tuple[StoredSession, bool]:
        session_id = session_id.strip()
        if not session_id:
            raise ValueError("session_id is required")

        def create(stores: dict[str, Any]) -> tuple[StoredSession, bool]:
            sessions = stores[self._session_store_name]
            idempotency = stores[self._session_idempotency_store_name]
            session_projections = stores[self._session_projection_store_name]

            existing = _record_to_session(_get_optional_record(sessions, session_id))
            if existing is not None:
                _replace_session_projections(session_projections, None, existing)
                return existing, False

            if idempotency_key:
                existing = _session_for_idempotency_key_from_stores(idempotency, sessions, idempotency_key)
                if existing is not None:
                    _replace_session_projections(session_projections, None, existing)
                    return existing, False

            now = _utcnow()
            session = StoredSession(
                session_id=session_id,
                idempotency_key=idempotency_key,
                provider_name=provider_name,
                model=model,
                client_ref=client_ref,
                state=gestalt.AGENT_SESSION_STATE_ACTIVE,
                metadata=copy.deepcopy(metadata),
                prepared_workspace=copy.deepcopy(prepared_workspace) if prepared_workspace is not None else None,
                created_by=_coerce_string_dict(created_by),
                created_at=now,
                updated_at=now,
            )
            if idempotency_key:
                idempotency.add(
                    {"id": idempotency_key, "session_id": session_id, "provider_name": provider_name, "created_at": now}
                )
            sessions.add(_session_to_record(session))
            _replace_session_projections(session_projections, None, session)
            return session, True

        with self._mutation_lock:
            try:
                session, created = self._with_transaction(
                    [
                        self._session_store_name,
                        self._session_idempotency_store_name,
                        self._session_projection_store_name,
                    ],
                    create,
                )
            except gestalt.AlreadyExistsError:
                existing = self._session_after_create_conflict(session_id=session_id, idempotency_key=idempotency_key)
                if existing is None:
                    raise
                session, created = existing, False
                self._repair_session_projection(session)
            return copy.deepcopy(session), created

    def get_session(self, session_id: str) -> StoredSession | None:
        return _record_to_session(_get_optional_record(self._sessions, session_id.strip()))

    def get_session_by_idempotency_key(self, idempotency_key: str) -> StoredSession | None:
        idempotency_key = idempotency_key.strip()
        if not idempotency_key:
            return None
        return _session_for_idempotency_key_from_stores(self._session_idempotency, self._sessions, idempotency_key)

    def list_sessions(
        self,
        *,
        session_ids: list[str] | None = None,
        subject_id: str = "",
        state: int = 0,
        limit: int = 0,
        summary_only: bool = False,
    ) -> list[StoredSession]:
        requested_ids = _normalized_unique_ids(session_ids)
        if requested_ids:
            sessions = [self.get_session(session_id) for session_id in requested_ids]
        elif summary_only or limit > 0:
            projected = self._list_session_projections(subject_id=subject_id, state=state, limit=limit)
            if summary_only:
                return copy.deepcopy(projected)
            sessions = [self.get_session(session.session_id) for session in projected]
        else:
            sessions = [_record_to_session(record) for record in self._sessions.iter_records()]
        filtered = [session for session in sessions if session is not None]

        subject_id = subject_id.strip()
        if subject_id:
            filtered = [
                session
                for session in filtered
                if str(session.created_by.get("subject_id", "") or "").strip() == subject_id
            ]
        if state:
            filtered = [session for session in filtered if session.state == state]
        filtered = sorted(filtered, key=_session_projection_order_key)
        if limit > 0:
            filtered = filtered[:limit]
        return copy.deepcopy(filtered)

    def update_session(
        self, *, session_id: str, client_ref: str = "", state: int = 0, metadata: dict[str, Any] | None = None
    ) -> StoredSession | None:
        session_id = session_id.strip()

        def update(stores: dict[str, Any]) -> StoredSession | None:
            sessions = stores[self._session_store_name]
            session_projections = stores[self._session_projection_store_name]
            session = _record_to_session(_get_optional_record(sessions, session_id))
            if session is None:
                return None
            previous = replace(session)
            if client_ref:
                session.client_ref = client_ref
            if state:
                session.state = state
            if metadata is not None:
                session.metadata = copy.deepcopy(metadata)
            session.updated_at = _utcnow()
            sessions.put(_session_to_record(session))
            _replace_session_projections(session_projections, previous, session)
            return session

        with self._mutation_lock:
            session = self._with_transaction([self._session_store_name, self._session_projection_store_name], update)
            return copy.deepcopy(session) if session is not None else None

    def begin_turn(
        self,
        *,
        turn_id: str,
        session_id: str,
        idempotency_key: str,
        provider_name: str,
        model: str,
        messages: list[dict[str, Any]],
        created_by: dict[str, str],
        execution_ref: str,
    ) -> tuple[StoredTurn, bool]:
        turn_id = turn_id.strip()
        session_id = session_id.strip()
        if not turn_id:
            raise ValueError("turn_id is required")
        if not session_id:
            raise ValueError("session_id is required")

        def begin(stores: dict[str, Any]) -> tuple[StoredTurn, bool]:
            turns = stores[self._run_store_name]
            idempotency = stores[self._turn_idempotency_store_name]
            sessions = stores[self._session_store_name]
            session_projections = stores[self._session_projection_store_name]
            turn_projections = stores[self._turn_projection_store_name]
            events = stores[self._event_store_name]

            existing = _record_to_turn(_get_optional_record(turns, turn_id))
            if existing is not None:
                if existing.session_id != session_id:
                    raise StoreConflictError(f"turn_id {turn_id!r} already exists for another session")
                _replace_turn_projections(turn_projections, None, existing)
                return existing, False

            if idempotency_key:
                existing = _turn_for_idempotency_key_from_stores(
                    idempotency, turns, session_id=session_id, idempotency_key=idempotency_key
                )
                if existing is not None:
                    _replace_turn_projections(turn_projections, None, existing)
                    return existing, False

            now = _utcnow()
            _touch_session_for_turn_in_store(sessions, session_id, now, session_projections)
            turn = StoredTurn(
                turn_id=turn_id,
                session_id=session_id,
                idempotency_key=idempotency_key,
                provider_name=provider_name,
                model=model,
                status=gestalt.AGENT_EXECUTION_STATUS_RUNNING,
                messages=copy.deepcopy(messages),
                output_text="",
                status_message="",
                created_by=_coerce_string_dict(created_by),
                created_at=now,
                started_at=now,
                completed_at=None,
                execution_ref=execution_ref,
            )
            if idempotency_key:
                idempotency.add(
                    {
                        "id": _turn_idempotency_record_id(session_id=session_id, idempotency_key=idempotency_key),
                        "session_id": session_id,
                        "idempotency_key": idempotency_key,
                        "turn_id": turn_id,
                        "provider_name": provider_name,
                        "created_at": now,
                    }
                )
            turns.add(_turn_to_record(turn))
            _replace_turn_projections(turn_projections, None, turn)
            self._append_turn_event_locked(
                turn_id=turn_id,
                event_type="turn.started",
                source=provider_name,
                data={"model": model},
                events_store=events,
            )
            return turn, True

        with self._mutation_lock:
            try:
                turn, created = self._with_transaction(
                    [
                        self._run_store_name,
                        self._turn_idempotency_store_name,
                        self._session_store_name,
                        self._session_projection_store_name,
                        self._turn_projection_store_name,
                        self._event_store_name,
                    ],
                    begin,
                )
            except gestalt.AlreadyExistsError:
                existing = self._turn_after_begin_conflict(
                    turn_id=turn_id, session_id=session_id, idempotency_key=idempotency_key
                )
                if existing is None:
                    raise
                turn, created = existing, False
                self._repair_turn_projection(turn)
            return copy.deepcopy(turn), created

    def get_turn(self, turn_id: str) -> StoredTurn | None:
        return _record_to_turn(_get_optional_record(self._turns, turn_id.strip()))

    def list_turns(
        self,
        *,
        session_id: str,
        turn_ids: list[str] | None = None,
        subject_id: str = "",
        status: int = 0,
        limit: int = 0,
        summary_only: bool = False,
    ) -> list[StoredTurn]:
        requested_ids = _normalized_unique_ids(turn_ids)
        if requested_ids:
            turns = [self.get_turn(turn_id) for turn_id in requested_ids]
        else:
            session_id = session_id.strip()
            if not session_id:
                return []
            if summary_only or limit > 0:
                projected = self._list_turn_projections(
                    session_id=session_id, subject_id=subject_id, status=status, limit=limit
                )
                if summary_only:
                    return copy.deepcopy(projected)
                turns = [self.get_turn(turn.turn_id) for turn in projected]
            else:
                turns = [_record_to_turn(record) for record in self._turns.iter_records()]
        filtered = [turn for turn in turns if turn is not None]
        if not requested_ids:
            filtered = [turn for turn in filtered if turn.session_id == session_id]

        subject_id = subject_id.strip()
        if subject_id:
            filtered = [turn for turn in filtered if str(turn.created_by.get("subject_id", "") or "") == subject_id]
        if status:
            filtered = [turn for turn in filtered if turn.status == status]
        filtered = sorted(filtered, key=_turn_projection_order_key)
        if limit > 0:
            filtered = filtered[:limit]
        return copy.deepcopy(filtered)

    def complete_turn(self, *, turn_id: str, output_text: str) -> StoredTurn | None:
        def complete(stores: dict[str, Any]) -> StoredTurn | None:
            turns = stores[self._run_store_name]
            turn_projections = stores[self._turn_projection_store_name]
            events = stores[self._event_store_name]
            turn = _record_to_turn(_get_optional_record(turns, turn_id.strip()))
            if turn is None:
                return None
            if turn.status in TERMINAL_STATUSES:
                return turn
            previous = replace(turn)
            turn.status = gestalt.AGENT_EXECUTION_STATUS_SUCCEEDED
            turn.output_text = output_text
            turn.status_message = ""
            turn.completed_at = _utcnow()
            turns.put(_turn_to_record(turn))
            _replace_turn_projections(turn_projections, previous, turn)
            self._append_turn_event_locked(
                turn_id=turn.turn_id,
                event_type="assistant.message",
                source=turn.provider_name,
                data={"text": output_text},
                events_store=events,
            )
            self._append_turn_event_locked(
                turn_id=turn.turn_id,
                event_type="turn.completed",
                source=turn.provider_name,
                data={"status": "succeeded"},
                events_store=events,
            )
            return turn

        with self._mutation_lock:
            turn = self._with_transaction(
                [self._run_store_name, self._turn_projection_store_name, self._event_store_name], complete
            )
            return copy.deepcopy(turn) if turn is not None else None

    def fail_turn(self, *, turn_id: str, message: str) -> StoredTurn | None:
        def fail(stores: dict[str, Any]) -> StoredTurn | None:
            turns = stores[self._run_store_name]
            turn_projections = stores[self._turn_projection_store_name]
            events = stores[self._event_store_name]
            turn = _record_to_turn(_get_optional_record(turns, turn_id.strip()))
            if turn is None:
                return None
            if turn.status in TERMINAL_STATUSES:
                return turn
            previous = replace(turn)
            turn.status = gestalt.AGENT_EXECUTION_STATUS_FAILED
            turn.status_message = message
            turn.completed_at = _utcnow()
            turns.put(_turn_to_record(turn))
            _replace_turn_projections(turn_projections, previous, turn)
            self._append_turn_event_locked(
                turn_id=turn.turn_id,
                event_type="turn.failed",
                source=turn.provider_name,
                data={"error": message},
                events_store=events,
            )
            return turn

        with self._mutation_lock:
            turn = self._with_transaction(
                [self._run_store_name, self._turn_projection_store_name, self._event_store_name], fail
            )
            return copy.deepcopy(turn) if turn is not None else None

    def cancel_turn(self, *, turn_id: str, reason: str) -> StoredTurn | None:
        def cancel(stores: dict[str, Any]) -> StoredTurn | None:
            turns = stores[self._run_store_name]
            turn_projections = stores[self._turn_projection_store_name]
            events = stores[self._event_store_name]
            turn = _record_to_turn(_get_optional_record(turns, turn_id.strip()))
            if turn is None:
                return None
            if turn.status not in TERMINAL_STATUSES:
                previous = replace(turn)
                turn.status = gestalt.AGENT_EXECUTION_STATUS_CANCELED
                turn.status_message = reason
                turn.completed_at = _utcnow()
                turns.put(_turn_to_record(turn))
                _replace_turn_projections(turn_projections, previous, turn)
                self._append_turn_event_locked(
                    turn_id=turn.turn_id,
                    event_type="turn.canceled",
                    source=turn.provider_name,
                    data={"reason": reason},
                    events_store=events,
                )
            return turn

        with self._mutation_lock:
            turn = self._with_transaction(
                [self._run_store_name, self._turn_projection_store_name, self._event_store_name], cancel
            )
            return copy.deepcopy(turn) if turn is not None else None

    def list_turn_events(self, *, turn_id: str, after_seq: int = 0, limit: int = 0) -> list[StoredTurnEvent]:
        events = self._persisted_turn_events(turn_id.strip())
        events.sort(key=lambda event: (event.seq, event.event_id))
        filtered = [event for event in events if event.seq > after_seq]
        if limit > 0:
            filtered = filtered[:limit]
        return copy.deepcopy(filtered)

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        with self._initialize_lock:
            if self._client is not None:
                return self._client
            if self._closed:
                raise RuntimeError("agent run store is closed")
            env_name = gestalt.indexeddb_socket_env()
            if not os.environ.get(env_name, ""):
                raise StoreUnavailableError(_indexeddb_unavailable_message(f"{env_name} is not set"))
            try:
                self._client = gestalt.IndexedDB()
            except RuntimeError as exc:
                raise StoreUnavailableError(_indexeddb_unavailable_message(str(exc))) from exc
            return self._client

    def _object_store(self, name: str) -> Any:
        self.initialize()
        client = self._ensure_client()
        return _RetryingObjectStore(client.object_store(name))

    def _with_transaction(self, store_names: list[str], operation: Any) -> Any:
        self.initialize()
        client = self._ensure_client()

        def run_transaction() -> Any:
            with client.transaction(store_names, "readwrite") as tx:
                stores = {name: tx.object_store(name) for name in store_names}
                return operation(stores)

        return _call_with_busy_retry(run_transaction)

    def _list_session_projections(
        self, *, subject_id: str = "", state: int = 0, limit: int = 0
    ) -> list[StoredSession]:
        prefix = _session_projection_prefix(subject_id=subject_id, state=state)
        records = self._session_projections.iter_records(_prefix_key_range(prefix), limit=limit, require_cursor=True)
        return [
            session for session in (_record_to_session_projection(record) for record in records) if session is not None
        ]

    def _list_turn_projections(
        self, *, session_id: str, subject_id: str = "", status: int = 0, limit: int = 0
    ) -> list[StoredTurn]:
        prefix = _turn_projection_prefix(session_id=session_id, subject_id=subject_id, status=status)
        records = self._turn_projections.iter_records(_prefix_key_range(prefix), limit=limit, require_cursor=True)
        return [turn for turn in (_record_to_turn_projection(record) for record in records) if turn is not None]

    def _repair_session_projection(self, session: StoredSession) -> None:
        def repair(stores: dict[str, Any]) -> None:
            _replace_session_projections(stores[self._session_projection_store_name], None, session)

        self._with_transaction([self._session_projection_store_name], repair)

    def _repair_turn_projection(self, turn: StoredTurn) -> None:
        def repair(stores: dict[str, Any]) -> None:
            _replace_turn_projections(stores[self._turn_projection_store_name], None, turn)

        self._with_transaction([self._turn_projection_store_name], repair)

    def _close_client(self) -> None:
        if self._client is None:
            return
        try:
            self._client.close()
        finally:
            self._client = None
            self._initialized = False

    def _persisted_turn_events(self, turn_id: str) -> list[StoredTurnEvent]:
        events = [_record_to_turn_event(record) for record in self._events.iter_records(_turn_event_key_range(turn_id))]
        return [event for event in events if event is not None and event.turn_id == turn_id]

    def _session_after_create_conflict(self, *, session_id: str, idempotency_key: str) -> StoredSession | None:
        existing = self.get_session(session_id)
        if existing is not None:
            return existing
        if not idempotency_key:
            return None
        return _session_for_idempotency_key_from_stores(self._session_idempotency, self._sessions, idempotency_key)

    def _turn_after_begin_conflict(self, *, turn_id: str, session_id: str, idempotency_key: str) -> StoredTurn | None:
        existing = self.get_turn(turn_id)
        if existing is not None:
            if existing.session_id != session_id:
                raise StoreConflictError(f"turn_id {turn_id!r} already exists for another session")
            return existing
        if not idempotency_key:
            return None
        existing = _turn_for_idempotency_key_from_stores(
            self._turn_idempotency, self._turns, session_id=session_id, idempotency_key=idempotency_key
        )
        if existing is not None and existing.session_id != session_id:
            raise StoreConflictError(f"idempotency_key {idempotency_key!r} already exists for another session")
        return existing

    def _next_turn_event_seq(self, turn_id: str) -> int:
        existing = self._persisted_turn_events(turn_id)
        if not existing:
            return 1
        return max(event.seq for event in existing) + 1

    def _append_turn_event_locked(
        self,
        *,
        turn_id: str,
        event_type: str,
        source: str,
        visibility: str = "external",
        data: dict[str, Any] | None = None,
        events_store: Any | None = None,
    ) -> StoredTurnEvent:
        store = events_store or self._events
        while True:
            seq = (
                self._next_turn_event_seq(turn_id)
                if events_store is None
                else _next_turn_event_seq_from_store(store, turn_id)
            )
            event = StoredTurnEvent(
                event_id=f"{turn_id}:{seq}",
                turn_id=turn_id,
                seq=seq,
                event_type=event_type.strip(),
                source=source.strip(),
                visibility=visibility.strip() or "external",
                data=copy.deepcopy(data) if isinstance(data, dict) else {},
                created_at=_utcnow(),
            )
            try:
                store.add(_turn_event_to_record(event))
            except gestalt.AlreadyExistsError:
                continue
            return event


class _RetryingObjectStore:
    def __init__(self, store: Any) -> None:
        self._store = store

    def add(self, record: dict[str, Any]) -> None:
        _call_with_busy_retry(lambda: self._store.add(record))

    def get(self, record_id: str) -> dict[str, Any]:
        return _call_with_busy_retry(lambda: self._store.get(record_id))

    def put(self, record: dict[str, Any]) -> None:
        _call_with_busy_retry(lambda: self._store.put(record))

    def delete(self, record_id: str) -> None:
        _call_with_busy_retry(lambda: self._store.delete(record_id))

    def get_all(self, key_range: Any | None = None) -> list[dict[str, Any]]:
        return _call_with_busy_retry(lambda: self._store.get_all(key_range))

    def iter_records(
        self, key_range: Any | None = None, *, limit: int = 0, require_cursor: bool = False
    ) -> Iterator[dict[str, Any]]:
        open_cursor = getattr(self._store, "open_cursor", None)
        if open_cursor is None:
            if require_cursor:
                raise RuntimeError("indexeddb cursor support is required for bounded list projections")
            records = self.get_all(key_range)
            if limit > 0:
                records = records[:limit]
            yield from records
            return

        cursor = _call_with_busy_retry(lambda: open_cursor(key_range))
        try:
            yielded = 0
            while limit <= 0 or yielded < limit:
                if not cursor.continue_():
                    break
                yielded += 1
                yield copy.deepcopy(cursor.value)
        finally:
            cursor.close()


class _LazyObjectStore:
    def __init__(self, owner: IndexedDBRunStore, name: str) -> None:
        self._owner = owner
        self._name = name

    def add(self, record: dict[str, Any]) -> None:
        self._resolve().add(record)

    def get(self, record_id: str) -> dict[str, Any]:
        return self._resolve().get(record_id)

    def put(self, record: dict[str, Any]) -> None:
        self._resolve().put(record)

    def delete(self, record_id: str) -> None:
        self._resolve().delete(record_id)

    def get_all(self, key_range: Any | None = None) -> list[dict[str, Any]]:
        return self._resolve().get_all(key_range)

    def iter_records(
        self, key_range: Any | None = None, *, limit: int = 0, require_cursor: bool = False
    ) -> Iterator[dict[str, Any]]:
        return self._resolve().iter_records(key_range, limit=limit, require_cursor=require_cursor)

    def _resolve(self) -> _RetryingObjectStore:
        return self._owner._object_store(self._name)


def _session_to_record(session: StoredSession) -> dict[str, Any]:
    return {
        "id": session.session_id,
        "idempotency_key": session.idempotency_key,
        "provider_name": session.provider_name,
        "model": session.model,
        "client_ref": session.client_ref,
        "state": session.state,
        "metadata": copy.deepcopy(session.metadata),
        "prepared_workspace": copy.deepcopy(session.prepared_workspace),
        "created_by": dict(session.created_by),
        "created_at": session.created_at,
        "updated_at": session.updated_at,
        "last_turn_at": session.last_turn_at,
    }


def _record_to_session(record: dict[str, Any] | None) -> StoredSession | None:
    if record is None:
        return None
    return StoredSession(
        session_id=str(record.get("id") or ""),
        idempotency_key=str(record.get("idempotency_key") or ""),
        provider_name=str(record.get("provider_name") or ""),
        model=str(record.get("model") or ""),
        client_ref=str(record.get("client_ref") or ""),
        state=int(record.get("state") or gestalt.AGENT_SESSION_STATE_UNSPECIFIED),
        metadata=_coerce_optional_dict(record.get("metadata")) or {},
        prepared_workspace=_coerce_optional_string_dict(record.get("prepared_workspace")),
        created_by=_coerce_string_dict(record.get("created_by")),
        created_at=_coerce_required_datetime(record.get("created_at")),
        updated_at=_coerce_required_datetime(record.get("updated_at")),
        last_turn_at=_coerce_datetime(record.get("last_turn_at")),
    )


def _turn_to_record(turn: StoredTurn) -> dict[str, Any]:
    return {
        "id": turn.turn_id,
        "session_id": turn.session_id,
        "idempotency_key": turn.idempotency_key,
        "provider_name": turn.provider_name,
        "model": turn.model,
        "status": turn.status,
        "messages": copy.deepcopy(turn.messages),
        "output_text": turn.output_text,
        "status_message": turn.status_message,
        "created_by": dict(turn.created_by),
        "created_at": turn.created_at,
        "started_at": turn.started_at,
        "completed_at": turn.completed_at,
        "execution_ref": turn.execution_ref,
    }


def _record_to_turn(record: dict[str, Any] | None) -> StoredTurn | None:
    if record is None:
        return None
    return StoredTurn(
        turn_id=str(record.get("id") or ""),
        session_id=str(record.get("session_id") or ""),
        idempotency_key=str(record.get("idempotency_key") or ""),
        provider_name=str(record.get("provider_name") or ""),
        model=str(record.get("model") or ""),
        status=int(record.get("status") or gestalt.AGENT_EXECUTION_STATUS_UNSPECIFIED),
        messages=_coerce_messages(record.get("messages")),
        output_text=str(record.get("output_text") or ""),
        status_message=str(record.get("status_message") or ""),
        created_by=_coerce_string_dict(record.get("created_by")),
        created_at=_coerce_required_datetime(record.get("created_at")),
        started_at=_coerce_datetime(record.get("started_at")),
        completed_at=_coerce_datetime(record.get("completed_at")),
        execution_ref=str(record.get("execution_ref") or ""),
    )


def _replace_session_projections(store: Any, old: StoredSession | None, new: StoredSession) -> None:
    for key in set(_session_projection_keys(old) if old is not None else []):
        try:
            store.delete(key)
        except gestalt.NotFoundError:
            pass
    for key in _session_projection_keys(new):
        store.put(_session_projection_to_record(key, new))


def _replace_turn_projections(store: Any, old: StoredTurn | None, new: StoredTurn) -> None:
    for key in set(_turn_projection_keys(old) if old is not None else []):
        try:
            store.delete(key)
        except gestalt.NotFoundError:
            pass
    for key in _turn_projection_keys(new):
        store.put(_turn_projection_to_record(key, new))


def _session_projection_to_record(record_id: str, session: StoredSession) -> dict[str, Any]:
    return {
        "id": record_id,
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "session_id": session.session_id,
        "idempotency_key": session.idempotency_key,
        "provider_name": session.provider_name,
        "model": session.model,
        "client_ref": session.client_ref,
        "state": session.state,
        "created_by": copy.deepcopy(session.created_by),
        "created_at": session.created_at,
        "updated_at": session.updated_at,
        "last_turn_at": session.last_turn_at,
    }


def _record_to_session_projection(record: dict[str, Any] | None) -> StoredSession | None:
    if record is None:
        return None
    session_id = str(record.get("session_id") or "").strip()
    if not session_id:
        return None
    return StoredSession(
        session_id=session_id,
        idempotency_key=str(record.get("idempotency_key") or ""),
        provider_name=str(record.get("provider_name") or ""),
        model=str(record.get("model") or ""),
        client_ref=str(record.get("client_ref") or ""),
        state=int(record.get("state") or gestalt.AGENT_SESSION_STATE_UNSPECIFIED),
        metadata={},
        prepared_workspace=None,
        created_by=_coerce_string_dict(record.get("created_by")),
        created_at=_coerce_required_datetime(record.get("created_at")),
        updated_at=_coerce_required_datetime(record.get("updated_at")),
        last_turn_at=_coerce_datetime(record.get("last_turn_at")),
    )


def _turn_projection_to_record(record_id: str, turn: StoredTurn) -> dict[str, Any]:
    return {
        "id": record_id,
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "turn_id": turn.turn_id,
        "session_id": turn.session_id,
        "idempotency_key": turn.idempotency_key,
        "provider_name": turn.provider_name,
        "model": turn.model,
        "status": turn.status,
        "status_message": turn.status_message,
        "created_by": copy.deepcopy(turn.created_by),
        "created_at": turn.created_at,
        "started_at": turn.started_at,
        "completed_at": turn.completed_at,
        "execution_ref": turn.execution_ref,
    }


def _record_to_turn_projection(record: dict[str, Any] | None) -> StoredTurn | None:
    if record is None:
        return None
    turn_id = str(record.get("turn_id") or "").strip()
    if not turn_id:
        return None
    return StoredTurn(
        turn_id=turn_id,
        session_id=str(record.get("session_id") or ""),
        idempotency_key=str(record.get("idempotency_key") or ""),
        provider_name=str(record.get("provider_name") or ""),
        model=str(record.get("model") or ""),
        status=int(record.get("status") or gestalt.AGENT_EXECUTION_STATUS_UNSPECIFIED),
        messages=[],
        output_text="",
        status_message=str(record.get("status_message") or ""),
        created_by=_coerce_string_dict(record.get("created_by")),
        created_at=_coerce_required_datetime(record.get("created_at")),
        started_at=_coerce_datetime(record.get("started_at")),
        completed_at=_coerce_datetime(record.get("completed_at")),
        execution_ref=str(record.get("execution_ref") or ""),
    )


def _session_projection_keys(session: StoredSession) -> list[str]:
    sort_key = _projection_sort_key(_session_sort_time(session))
    session_id = _projection_value(session.session_id)
    state = str(session.state)
    keys = [
        _projection_key("session", "all", sort_key, session_id),
        _projection_key("session", "state", state, sort_key, session_id),
    ]
    subject_id = _subject_id_from_actor(session.created_by)
    if subject_id:
        subject = _projection_value(subject_id)
        keys.append(_projection_key("session", "subject", subject, "all", sort_key, session_id))
        keys.append(_projection_key("session", "subject", subject, "state", state, sort_key, session_id))
    return keys


def _turn_projection_keys(turn: StoredTurn) -> list[str]:
    sort_key = _projection_sort_key(turn.created_at)
    session = _projection_value(turn.session_id)
    turn_id = _projection_value(turn.turn_id)
    status = str(turn.status)
    keys = [
        _projection_key("turn", "session", session, "all", sort_key, turn_id),
        _projection_key("turn", "session", session, "status", status, sort_key, turn_id),
    ]
    subject_id = _subject_id_from_actor(turn.created_by)
    if subject_id:
        subject = _projection_value(subject_id)
        keys.append(_projection_key("turn", "session", session, "subject", subject, "all", sort_key, turn_id))
        keys.append(_projection_key("turn", "session", session, "subject", subject, "status", status, sort_key, turn_id))
    return keys


def _session_projection_prefix(*, subject_id: str = "", state: int = 0) -> str:
    subject_id = subject_id.strip()
    if subject_id and state:
        return _projection_prefix("session", "subject", _projection_value(subject_id), "state", str(state))
    if subject_id:
        return _projection_prefix("session", "subject", _projection_value(subject_id), "all")
    if state:
        return _projection_prefix("session", "state", str(state))
    return _projection_prefix("session", "all")


def _turn_projection_prefix(*, session_id: str, subject_id: str = "", status: int = 0) -> str:
    session = _projection_value(session_id.strip())
    subject_id = subject_id.strip()
    if subject_id and status:
        return _projection_prefix("turn", "session", session, "subject", _projection_value(subject_id), "status", str(status))
    if subject_id:
        return _projection_prefix("turn", "session", session, "subject", _projection_value(subject_id), "all")
    if status:
        return _projection_prefix("turn", "session", session, "status", str(status))
    return _projection_prefix("turn", "session", session, "all")


def _session_projection_order_key(session: StoredSession) -> tuple[str, str]:
    return _projection_sort_key(_session_sort_time(session)), _projection_value(session.session_id)


def _turn_projection_order_key(turn: StoredTurn) -> tuple[str, str]:
    return _projection_sort_key(turn.created_at), _projection_value(turn.turn_id)


def _session_sort_time(session: StoredSession) -> datetime:
    if session.last_turn_at is not None:
        return session.last_turn_at
    return session.updated_at or session.created_at


def _projection_sort_key(value: datetime) -> str:
    normalized = value.astimezone(UTC)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    delta = normalized - epoch
    micros = (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds
    inverted = MAX_INVERTED_SORT_MICROS - max(0, micros)
    return f"{inverted:020d}"


def _projection_value(value: str) -> str:
    raw = str(value or "").strip().encode()
    encoded = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    return encoded or "-"


def _projection_key(*parts: str) -> str:
    return PROJECTION_SEP.join(parts)


def _projection_prefix(*parts: str) -> str:
    return _projection_key(*parts) + PROJECTION_SEP


def _prefix_key_range(prefix: str) -> Any:
    return gestalt.KeyRange(lower=prefix, upper=f"{prefix}{PROJECTION_RANGE_SUFFIX}")


def _subject_id_from_actor(actor: dict[str, str]) -> str:
    return str(actor.get("subject_id", "") or "").strip()


def _turn_event_to_record(event: StoredTurnEvent) -> dict[str, Any]:
    return {
        "id": event.event_id,
        "turn_id": event.turn_id,
        "seq": event.seq,
        "type": event.event_type,
        "source": event.source,
        "visibility": event.visibility,
        "data": copy.deepcopy(event.data),
        "created_at": event.created_at,
    }


def _record_to_turn_event(record: dict[str, Any] | None) -> StoredTurnEvent | None:
    if record is None:
        return None
    return StoredTurnEvent(
        event_id=str(record.get("id") or ""),
        turn_id=str(record.get("turn_id") or ""),
        seq=int(record.get("seq") or 0),
        event_type=str(record.get("type") or ""),
        source=str(record.get("source") or ""),
        visibility=str(record.get("visibility") or ""),
        data=_coerce_optional_dict(record.get("data")) or {},
        created_at=_coerce_required_datetime(record.get("created_at")),
    )


def _get_optional_record(store: Any, record_id: str) -> dict[str, Any] | None:
    record_id = record_id.strip()
    if not record_id:
        return None
    if isinstance(store, (_LazyObjectStore, _RetryingObjectStore)):
        try:
            return store.get(record_id)
        except gestalt.NotFoundError:
            return None
    # Transaction-scoped get() closes the transaction stream on NOT_FOUND, so use
    # a bounded exact-key range where missing records are expected.
    records = store.get_all(gestalt.KeyRange(lower=record_id, upper=record_id))
    for record in records:
        if str(record.get("id") or "") == record_id:
            return record
    return None


def _session_for_idempotency_key_from_stores(
    idempotency_store: Any, session_store: Any, idempotency_key: str
) -> StoredSession | None:
    record = _get_optional_record(idempotency_store, idempotency_key)
    if record is None:
        return None
    session_id = str(record.get("session_id") or "").strip()
    if not session_id:
        return None
    return _record_to_session(_get_optional_record(session_store, session_id))


def _turn_for_idempotency_key_from_stores(
    idempotency_store: Any, turn_store: Any, *, session_id: str, idempotency_key: str
) -> StoredTurn | None:
    record = _get_optional_record(
        idempotency_store, _turn_idempotency_record_id(session_id=session_id, idempotency_key=idempotency_key)
    )
    if record is None:
        return None
    turn_id = str(record.get("turn_id") or "").strip()
    if not turn_id:
        return None
    return _record_to_turn(_get_optional_record(turn_store, turn_id))


def _touch_session_for_turn_in_store(
    store: Any, session_id: str, now: datetime, projection_store: Any | None = None
) -> None:
    session = _record_to_session(_get_optional_record(store, session_id))
    if session is None:
        return
    previous = replace(session)
    session.last_turn_at = now
    session.updated_at = now
    store.put(_session_to_record(session))
    if projection_store is not None:
        _replace_session_projections(projection_store, previous, session)


def _next_turn_event_seq_from_store(store: Any, turn_id: str) -> int:
    existing = _persisted_turn_events_from_store(store, turn_id)
    if not existing:
        return 1
    return max(event.seq for event in existing) + 1


def _persisted_turn_events_from_store(store: Any, turn_id: str) -> list[StoredTurnEvent]:
    events = [_record_to_turn_event(record) for record in store.get_all(_turn_event_key_range(turn_id))]
    return [event for event in events if event is not None and event.turn_id == turn_id]


def _turn_event_key_range(turn_id: str) -> Any:
    prefix = f"{turn_id}:"
    return gestalt.KeyRange(lower=prefix, upper=f"{prefix}{RANGE_SUFFIX}")


def _turn_idempotency_record_id(*, session_id: str, idempotency_key: str) -> str:
    return f"{session_id}{TURN_IDEMPOTENCY_SEP}{idempotency_key}"


def _normalized_unique_ids(raw_ids: list[str] | None) -> list[str]:
    if raw_ids is None:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for raw_id in raw_ids:
        value = str(raw_id or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _coerce_messages(raw_value: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_value, list):
        return []
    messages: list[dict[str, Any]] = []
    for item in raw_value:
        if isinstance(item, dict):
            messages.append(copy.deepcopy(item))
    return messages


def _coerce_string_dict(raw_value: Any) -> dict[str, str]:
    if not isinstance(raw_value, dict):
        return {}
    return {str(key): str(value or "") for key, value in raw_value.items()}


def _coerce_optional_string_dict(raw_value: Any) -> dict[str, str] | None:
    if raw_value is None:
        return None
    if not isinstance(raw_value, dict):
        raise ValueError("stored value must be an object")
    return {str(key): str(value or "") for key, value in raw_value.items()}


def _coerce_optional_dict(raw_value: Any) -> dict[str, Any] | None:
    if raw_value is None:
        return None
    if not isinstance(raw_value, dict):
        raise ValueError("stored value must be an object")
    return copy.deepcopy(raw_value)


def _coerce_datetime(raw_value: Any) -> datetime | None:
    if raw_value is None or raw_value == "":
        return None
    if isinstance(raw_value, datetime):
        return raw_value.astimezone(UTC)
    return datetime.fromisoformat(str(raw_value)).astimezone(UTC)


def _coerce_required_datetime(raw_value: Any) -> datetime:
    parsed = _coerce_datetime(raw_value)
    if parsed is None:
        raise ValueError("expected a timestamp value")
    return parsed


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


def _call_with_busy_retry(operation: Any) -> Any:
    delay = BUSY_RETRY_INITIAL_DELAY_SECONDS
    while True:
        try:
            return operation()
        except (grpc.RpcError, gestalt.TransactionError) as exc:
            if _is_busy_error(exc):
                time.sleep(delay)
                delay = min(delay * 2, BUSY_RETRY_MAX_DELAY_SECONDS)
                continue
            if _is_unavailable_error(exc):
                raise StoreUnavailableError(_indexeddb_unavailable_message(_error_details(exc))) from exc
            raise


def _is_busy_error(exc: BaseException) -> bool:
    code_fn = getattr(exc, "code", None)
    code = code_fn() if callable(code_fn) else None
    details = _error_details(exc).lower()
    if code is not None and code != grpc.StatusCode.INTERNAL:
        return False
    return "database is locked" in details or "sqlite_busy" in details or "sql_busy" in details


def _is_unavailable_error(exc: BaseException) -> bool:
    code_fn = getattr(exc, "code", None)
    code = code_fn() if callable(code_fn) else None
    if code == grpc.StatusCode.UNAVAILABLE:
        return True
    details = _error_details(exc).lower()
    return "failed to connect" in details or "connection refused" in details or "no such file" in details


def _error_details(exc: BaseException) -> str:
    details_fn = getattr(exc, "details", None)
    if callable(details_fn):
        return str(details_fn() or exc)
    return str(exc)


def _indexeddb_unavailable_message(details: str) -> str:
    env_name = gestalt.indexeddb_socket_env()
    detail = details.strip()
    suffix = f": {detail}" if detail else ""
    return f"agent/claude requires an IndexedDB host socket binding via {env_name}{suffix}"
