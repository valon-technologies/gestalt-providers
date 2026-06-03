from __future__ import annotations

import copy
import os
import threading
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import gestalt

from .store_projection import (
    _prefix_key_range,
    _record_to_session_projection,
    _record_to_turn_projection,
    _replace_session_projections,
    _replace_turn_projections,
    _session_projection_order_key,
    _session_projection_prefix,
    _turn_projection_order_key,
    _turn_projection_prefix,
)
from .store_records import (
    SESSION_VISIBILITY_COMPANY,
    TERMINAL_STATUSES,
    StoredSession,
    StoredTurn,
    StoredTurnEvent,
    _coerce_string_dict,
    _record_to_session,
    _record_to_turn,
    _record_to_turn_event,
    _session_to_record,
    _turn_event_to_record,
    _turn_to_record,
    session_readable_by,
    session_visibility_for_create,
)
from .store_retry import StoreUnavailableError, _call_with_busy_retry, _indexeddb_unavailable_message

TURN_IDEMPOTENCY_SEP = "\x1f"
RANGE_SUFFIX = "\U0010ffff"


class StoreConflictError(ValueError):
    pass


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
                client = self._ensure_client()
                for name in (
                    self._run_store_name,
                    self._event_store_name,
                    self._session_store_name,
                    self._session_projection_store_name,
                    self._turn_projection_store_name,
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
        prepared_workspace: dict[str, str] | None,
        created_by_subject_id: str,
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
                created_by_subject_id=created_by_subject_id.strip(),
                visibility=session_visibility_for_create(metadata, created_by_subject_id),
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
        if not subject_id:
            filtered = []
        else:
            filtered = [session for session in filtered if session_readable_by(session, subject_id)]
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
            updated = replace(
                session,
                client_ref=client_ref or session.client_ref,
                state=state or session.state,
                metadata=copy.deepcopy(metadata) if metadata is not None else copy.deepcopy(session.metadata),
                updated_at=_utcnow(),
            )
            sessions.put(_session_to_record(updated))
            _replace_session_projections(session_projections, session, updated)
            return updated

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
        created_by_subject_id: str,
        execution_ref: str,
    ) -> tuple[StoredTurn, bool]:
        turn_id, session_id = _validated_turn_scope(turn_id=turn_id, session_id=session_id)

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
                    lambda stores: self._begin_turn_in_stores(
                        stores,
                        turn_id=turn_id,
                        session_id=session_id,
                        idempotency_key=idempotency_key,
                        provider_name=provider_name,
                        model=model,
                        messages=messages,
                        created_by_subject_id=created_by_subject_id.strip(),
                        execution_ref=execution_ref,
                    ),
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

    def _begin_turn_in_stores(
        self,
        stores: dict[str, Any],
        *,
        turn_id: str,
        session_id: str,
        idempotency_key: str,
        provider_name: str,
        model: str,
        messages: list[dict[str, Any]],
        created_by_subject_id: str,
        execution_ref: str,
    ) -> tuple[StoredTurn, bool]:
        turns = stores[self._run_store_name]
        turn_projections = stores[self._turn_projection_store_name]
        existing = _existing_turn_for_begin(
            turns,
            stores[self._turn_idempotency_store_name],
            turn_id=turn_id,
            session_id=session_id,
            idempotency_key=idempotency_key,
        )
        if existing is not None:
            _replace_turn_projections(turn_projections, None, existing)
            return existing, False

        now = _utcnow()
        _touch_session_for_turn_in_store(
            stores[self._session_store_name], session_id, now, stores[self._session_projection_store_name]
        )
        turn = _new_running_turn(
            turn_id=turn_id,
            session_id=session_id,
            idempotency_key=idempotency_key,
            provider_name=provider_name,
            model=model,
            messages=messages,
            created_by_subject_id=created_by_subject_id.strip(),
            execution_ref=execution_ref,
            now=now,
        )
        _add_turn_idempotency_record(stores[self._turn_idempotency_store_name], turn)
        turns.add(_turn_to_record(turn))
        _replace_turn_projections(turn_projections, None, turn)
        self._append_turn_event_locked(
            turn_id=turn_id,
            event_type="turn.started",
            source=provider_name,
            data={"model": model},
            events_store=stores[self._event_store_name],
        )
        return turn, True

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
            filtered = [
                turn
                for turn in filtered
                if turn.created_by_subject_id.strip() == subject_id.strip()
            ]
        if status:
            filtered = [turn for turn in filtered if turn.status == status]
        filtered = sorted(filtered, key=_turn_projection_order_key)
        if limit > 0:
            filtered = filtered[:limit]
        return copy.deepcopy(filtered)

    def complete_turn(self, *, turn_id: str, output: gestalt.AgentTurnOutput) -> StoredTurn | None:
        def complete(stores: dict[str, Any]) -> StoredTurn | None:
            turns = stores[self._run_store_name]
            turn_projections = stores[self._turn_projection_store_name]
            events = stores[self._event_store_name]
            turn = _record_to_turn(_get_optional_record(turns, turn_id.strip()))
            if turn is None:
                return None
            if turn.status in TERMINAL_STATUSES:
                return turn
            completed = replace(
                turn,
                status=gestalt.AGENT_EXECUTION_STATUS_SUCCEEDED,
                output=copy.deepcopy(output),
                status_message="",
                completed_at=_utcnow(),
            )
            turns.put(_turn_to_record(completed))
            _replace_turn_projections(turn_projections, turn, completed)
            self._append_turn_event_locked(
                turn_id=completed.turn_id,
                event_type="assistant.message",
                source=completed.provider_name,
                data=_assistant_message_event_data(output),
                events_store=events,
            )
            self._append_turn_event_locked(
                turn_id=completed.turn_id,
                event_type="turn.completed",
                source=completed.provider_name,
                data={"status": "succeeded"},
                events_store=events,
            )
            return completed

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
            failed = replace(
                turn, status=gestalt.AGENT_EXECUTION_STATUS_FAILED, status_message=message, completed_at=_utcnow()
            )
            turns.put(_turn_to_record(failed))
            _replace_turn_projections(turn_projections, turn, failed)
            self._append_turn_event_locked(
                turn_id=failed.turn_id,
                event_type="turn.failed",
                source=failed.provider_name,
                data={"error": message},
                events_store=events,
            )
            return failed

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
                canceled = replace(
                    turn, status=gestalt.AGENT_EXECUTION_STATUS_CANCELED, status_message=reason, completed_at=_utcnow()
                )
                turns.put(_turn_to_record(canceled))
                _replace_turn_projections(turn_projections, turn, canceled)
                self._append_turn_event_locked(
                    turn_id=canceled.turn_id,
                    event_type="turn.canceled",
                    source=canceled.provider_name,
                    data={"reason": reason},
                    events_store=events,
                )
                return canceled
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
            env_name = gestalt.ENV_HOST_SERVICE_SOCKET
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

    def _with_transaction(self, store_names: list[str], operation: Callable[[dict[str, Any]], Any]) -> Any:
        self.initialize()
        client = self._ensure_client()

        def run_transaction() -> Any:
            with client.transaction(store_names, "readwrite") as tx:
                stores = {name: tx.object_store(name) for name in store_names}
                return operation(stores)

        return _call_with_busy_retry(run_transaction)

    def _list_session_projections(self, *, subject_id: str = "", state: int = 0, limit: int = 0) -> list[StoredSession]:
        subject_id = subject_id.strip()
        prefixes = [_session_projection_prefix(subject_id=subject_id, state=state)]
        if subject_id:
            prefixes.append(_session_projection_prefix(visibility=SESSION_VISIBILITY_COMPANY, state=state))
        by_id: dict[str, StoredSession] = {}
        cursor_limit = limit if limit > 0 else 0
        for prefix in prefixes:
            records = self._session_projections.iter_records(
                _prefix_key_range(prefix), limit=cursor_limit, require_cursor=True
            )
            for session in (_record_to_session_projection(record) for record in records):
                if session is not None:
                    by_id[session.session_id] = session
        sessions = sorted(by_id.values(), key=_session_projection_order_key)
        if limit > 0:
            sessions = sessions[:limit]
        return sessions

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


def _existing_turn_for_begin(
    turn_store: Any, idempotency_store: Any, *, turn_id: str, session_id: str, idempotency_key: str
) -> StoredTurn | None:
    existing = _record_to_turn(_get_optional_record(turn_store, turn_id))
    if existing is not None:
        if existing.session_id != session_id:
            raise StoreConflictError(f"turn_id {turn_id!r} already exists for another session")
        return existing
    if not idempotency_key:
        return None
    return _turn_for_idempotency_key_from_stores(
        idempotency_store, turn_store, session_id=session_id, idempotency_key=idempotency_key
    )


def _validated_turn_scope(*, turn_id: str, session_id: str) -> tuple[str, str]:
    turn_id = turn_id.strip()
    session_id = session_id.strip()
    if not turn_id:
        raise ValueError("turn_id is required")
    if not session_id:
        raise ValueError("session_id is required")
    return turn_id, session_id


def _new_running_turn(
    *,
    turn_id: str,
    session_id: str,
    idempotency_key: str,
    provider_name: str,
    model: str,
    messages: list[dict[str, Any]],
    created_by_subject_id: str,
    execution_ref: str,
    now: datetime,
) -> StoredTurn:
    return StoredTurn(
        turn_id=turn_id,
        session_id=session_id,
        idempotency_key=idempotency_key,
        provider_name=provider_name,
        model=model,
        status=gestalt.AGENT_EXECUTION_STATUS_RUNNING,
        messages=copy.deepcopy(messages),
        output=None,
        status_message="",
        created_by_subject_id=created_by_subject_id.strip(),
        created_at=now,
        started_at=now,
        completed_at=None,
        execution_ref=execution_ref,
    )


def _assistant_message_event_data(output: gestalt.AgentTurnOutput) -> dict[str, Any]:
    if output.text is not None:
        return {"text": str(output.text or "")}
    structured = output.structured
    if structured is None:
        raise ValueError("completed turn output must include text or structured")
    data: dict[str, Any] = {"text": str(structured.text or "")}
    if structured.value is not None:
        data["value"] = copy.deepcopy(structured.value)
    return data


def _add_turn_idempotency_record(idempotency_store: Any, turn: StoredTurn) -> None:
    if not turn.idempotency_key:
        return
    idempotency_store.add(
        {
            "id": _turn_idempotency_record_id(session_id=turn.session_id, idempotency_key=turn.idempotency_key),
            "session_id": turn.session_id,
            "idempotency_key": turn.idempotency_key,
            "turn_id": turn.turn_id,
            "provider_name": turn.provider_name,
            "created_at": turn.created_at,
        }
    )


def _touch_session_for_turn_in_store(
    store: Any, session_id: str, now: datetime, projection_store: Any | None = None
) -> None:
    session = _record_to_session(_get_optional_record(store, session_id))
    if session is None:
        return
    updated = replace(session, last_turn_at=now, updated_at=now)
    store.put(_session_to_record(updated))
    if projection_store is not None:
        _replace_session_projections(projection_store, session, updated)


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


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)
