from __future__ import annotations

import copy
import os
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
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
        self._session_idempotency = _LazyObjectStore(self, f"{idempotency_store}_sessions")
        self._turn_idempotency = _LazyObjectStore(self, f"{idempotency_store}_turns")
        self._run_store_name = run_store
        self._event_store_name = f"{run_store}_events"
        self._session_store_name = f"{run_store}_sessions"
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
            client = self._ensure_client()
            try:
                for name in (
                    self._run_store_name,
                    self._event_store_name,
                    self._session_store_name,
                    self._session_idempotency_store_name,
                    self._turn_idempotency_store_name,
                ):
                    try:
                        _call_with_busy_retry(lambda name=name: client.create_object_store(name))
                    except gestalt.AlreadyExistsError:
                        pass
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
        created_by: dict[str, str],
    ) -> tuple[StoredSession, bool]:
        session_id = session_id.strip()
        if not session_id:
            raise ValueError("session_id is required")

        def create(stores: dict[str, Any]) -> tuple[StoredSession, bool]:
            sessions = stores[self._session_store_name]
            idempotency = stores[self._session_idempotency_store_name]

            existing = _record_to_session(_get_optional_record(sessions, session_id))
            if existing is not None:
                return existing, False

            if idempotency_key:
                existing = _session_for_idempotency_key_from_stores(idempotency, sessions, idempotency_key)
                if existing is not None:
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
                created_by=_coerce_string_dict(created_by),
                created_at=now,
                updated_at=now,
            )
            if idempotency_key:
                idempotency.add(
                    {"id": idempotency_key, "session_id": session_id, "provider_name": provider_name, "created_at": now}
                )
            sessions.add(_session_to_record(session))
            return session, True

        with self._mutation_lock:
            try:
                session, created = self._with_transaction(
                    [self._session_store_name, self._session_idempotency_store_name], create
                )
            except gestalt.AlreadyExistsError:
                existing = self._session_after_create_conflict(session_id=session_id, idempotency_key=idempotency_key)
                if existing is None:
                    raise
                session, created = existing, False
            return copy.deepcopy(session), created

    def get_session(self, session_id: str) -> StoredSession | None:
        return _record_to_session(_get_optional_record(self._sessions, session_id.strip()))

    def list_sessions(
        self, *, session_ids: list[str] | None = None, subject_id: str = "", state: int = 0, limit: int = 0
    ) -> list[StoredSession]:
        requested_ids = _normalized_unique_ids(session_ids)
        if requested_ids:
            sessions = [self.get_session(session_id) for session_id in requested_ids]
            filtered = [session for session in sessions if session is not None]
        else:
            filtered = [_record_to_session(record) for record in self._sessions.iter_records()]
            filtered = [session for session in filtered if session is not None]

        subject_id = subject_id.strip()
        if subject_id:
            filtered = [
                session
                for session in filtered
                if str(session.created_by.get("subject_id", "") or "").strip() == subject_id
            ]
        if state:
            filtered = [session for session in filtered if session.state == state]
        filtered = sorted(filtered, key=lambda session: session.last_turn_at or session.updated_at, reverse=True)
        if limit > 0:
            filtered = filtered[:limit]
        return copy.deepcopy(filtered)

    def update_session(
        self, *, session_id: str, client_ref: str = "", state: int = 0, metadata: dict[str, Any] | None = None
    ) -> StoredSession | None:
        session_id = session_id.strip()

        def update(stores: dict[str, Any]) -> StoredSession | None:
            sessions = stores[self._session_store_name]
            session = _record_to_session(_get_optional_record(sessions, session_id))
            if session is None:
                return None
            if client_ref:
                session.client_ref = client_ref
            if state:
                session.state = state
            if metadata is not None:
                session.metadata = copy.deepcopy(metadata)
            session.updated_at = _utcnow()
            sessions.put(_session_to_record(session))
            return session

        with self._mutation_lock:
            session = self._with_transaction([self._session_store_name], update)
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
            events = stores[self._event_store_name]

            existing = _record_to_turn(_get_optional_record(turns, turn_id))
            if existing is not None:
                if existing.session_id != session_id:
                    raise StoreConflictError(f"turn_id {turn_id!r} already exists for another session")
                return existing, False

            if idempotency_key:
                existing = _turn_for_idempotency_key_from_stores(
                    idempotency, turns, session_id=session_id, idempotency_key=idempotency_key
                )
                if existing is not None:
                    return existing, False

            now = _utcnow()
            _touch_session_for_turn_in_store(sessions, session_id, now)
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
    ) -> list[StoredTurn]:
        requested_ids = _normalized_unique_ids(turn_ids)
        if requested_ids:
            turns = [self.get_turn(turn_id) for turn_id in requested_ids]
            filtered = [turn for turn in turns if turn is not None]
        else:
            session_id = session_id.strip()
            if not session_id:
                return []
            filtered = [_record_to_turn(record) for record in self._turns.iter_records()]
            filtered = [turn for turn in filtered if turn is not None and turn.session_id == session_id]

        subject_id = subject_id.strip()
        if subject_id:
            filtered = [turn for turn in filtered if str(turn.created_by.get("subject_id", "") or "") == subject_id]
        if status:
            filtered = [turn for turn in filtered if turn.status == status]
        filtered = sorted(filtered, key=lambda turn: turn.created_at, reverse=True)
        if limit > 0:
            filtered = filtered[:limit]
        return copy.deepcopy(filtered)

    def complete_turn(self, *, turn_id: str, output_text: str) -> StoredTurn | None:
        def complete(stores: dict[str, Any]) -> StoredTurn | None:
            turns = stores[self._run_store_name]
            events = stores[self._event_store_name]
            turn = _record_to_turn(_get_optional_record(turns, turn_id.strip()))
            if turn is None:
                return None
            if turn.status in TERMINAL_STATUSES:
                return turn
            turn.status = gestalt.AGENT_EXECUTION_STATUS_SUCCEEDED
            turn.output_text = output_text
            turn.status_message = ""
            turn.completed_at = _utcnow()
            turns.put(_turn_to_record(turn))
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
            turn = self._with_transaction([self._run_store_name, self._event_store_name], complete)
            return copy.deepcopy(turn) if turn is not None else None

    def fail_turn(self, *, turn_id: str, message: str) -> StoredTurn | None:
        def fail(stores: dict[str, Any]) -> StoredTurn | None:
            turns = stores[self._run_store_name]
            events = stores[self._event_store_name]
            turn = _record_to_turn(_get_optional_record(turns, turn_id.strip()))
            if turn is None:
                return None
            if turn.status in TERMINAL_STATUSES:
                return turn
            turn.status = gestalt.AGENT_EXECUTION_STATUS_FAILED
            turn.status_message = message
            turn.completed_at = _utcnow()
            turns.put(_turn_to_record(turn))
            self._append_turn_event_locked(
                turn_id=turn.turn_id,
                event_type="turn.failed",
                source=turn.provider_name,
                data={"error": message},
                events_store=events,
            )
            return turn

        with self._mutation_lock:
            turn = self._with_transaction([self._run_store_name, self._event_store_name], fail)
            return copy.deepcopy(turn) if turn is not None else None

    def cancel_turn(self, *, turn_id: str, reason: str) -> StoredTurn | None:
        def cancel(stores: dict[str, Any]) -> StoredTurn | None:
            turns = stores[self._run_store_name]
            events = stores[self._event_store_name]
            turn = _record_to_turn(_get_optional_record(turns, turn_id.strip()))
            if turn is None:
                return None
            if turn.status not in TERMINAL_STATUSES:
                turn.status = gestalt.AGENT_EXECUTION_STATUS_CANCELED
                turn.status_message = reason
                turn.completed_at = _utcnow()
                turns.put(_turn_to_record(turn))
                self._append_turn_event_locked(
                    turn_id=turn.turn_id,
                    event_type="turn.canceled",
                    source=turn.provider_name,
                    data={"reason": reason},
                    events_store=events,
                )
            return turn

        with self._mutation_lock:
            turn = self._with_transaction([self._run_store_name, self._event_store_name], cancel)
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

    def iter_records(self, key_range: Any | None = None) -> Iterator[dict[str, Any]]:
        records = self.get_all(key_range)
        yield from records


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

    def iter_records(self, key_range: Any | None = None) -> Iterator[dict[str, Any]]:
        return self._resolve().iter_records(key_range)

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


def _touch_session_for_turn_in_store(store: Any, session_id: str, now: datetime) -> None:
    session = _record_to_session(_get_optional_record(store, session_id))
    if session is None:
        return
    session.last_turn_at = now
    session.updated_at = now
    store.put(_session_to_record(session))


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
