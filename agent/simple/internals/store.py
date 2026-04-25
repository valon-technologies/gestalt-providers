import copy
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import gestalt
import grpc

from .agent_proto_compat import agent_pb2

TERMINAL_STATUSES = {
    agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED,
    agent_pb2.AGENT_EXECUTION_STATUS_FAILED,
    agent_pb2.AGENT_EXECUTION_STATUS_CANCELED,
}

BUSY_RETRY_INITIAL_DELAY_SECONDS = 0.02
BUSY_RETRY_MAX_DELAY_SECONDS = 0.25


@dataclass(slots=True)
class StoredRun:
    run_id: str
    idempotency_key: str
    provider_name: str
    model: str
    status: int
    messages: list[dict[str, Any]]
    output_text: str
    structured_output: dict[str, Any] | None
    status_message: str
    session_ref: str
    created_by: dict[str, str]
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    execution_ref: str
    cancel_reason: str


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
    last_turn_at: datetime | None


class SimpleRunStore:
    def __init__(self, *, run_store: str, idempotency_store: str) -> None:
        self._client = gestalt.IndexedDB()
        self._runs = _RetryingObjectStore(self._client.object_store(run_store))
        self._idempotency = _RetryingObjectStore(
            self._client.object_store(idempotency_store)
        )
        self._events = _RetryingObjectStore(
            self._client.object_store(f"{run_store}_events")
        )
        self._sessions = _RetryingObjectStore(
            self._client.object_store(f"{run_store}_sessions")
        )
        self._session_idempotency = _RetryingObjectStore(
            self._client.object_store(f"{idempotency_store}_sessions")
        )
        self._run_store_name = run_store
        self._idempotency_store_name = idempotency_store
        self._event_store_name = f"{run_store}_events"
        self._session_store_name = f"{run_store}_sessions"
        self._session_idempotency_store_name = f"{idempotency_store}_sessions"
        self._mutation_lock = threading.Lock()

    def initialize(self) -> None:
        for name in (
            self._run_store_name,
            self._idempotency_store_name,
            self._event_store_name,
            self._session_store_name,
            self._session_idempotency_store_name,
        ):
            try:
                _call_with_busy_retry(
                    lambda name=name: self._client.create_object_store(name)
                )
            except gestalt.AlreadyExistsError:
                pass

    def close(self) -> None:
        self._client.close()

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
        existing = self.get_session(session_id)
        if existing is not None:
            return existing, False

        if idempotency_key:
            existing = self._session_for_idempotency_key(idempotency_key)
            if existing is not None:
                return existing, False

        now = _utcnow()
        session = StoredSession(
            session_id=session_id,
            idempotency_key=idempotency_key,
            provider_name=provider_name,
            model=model,
            client_ref=client_ref,
            state=agent_pb2.AGENT_SESSION_STATE_ACTIVE,
            metadata=metadata,
            created_by=created_by,
            created_at=now,
            updated_at=now,
            last_turn_at=None,
        )

        claimed_idempotency = False
        if idempotency_key:
            try:
                self._session_idempotency.add(
                    {
                        "id": idempotency_key,
                        "session_id": session_id,
                        "provider_name": provider_name,
                        "created_at": now,
                    }
                )
                claimed_idempotency = True
            except gestalt.AlreadyExistsError:
                existing = self._session_for_idempotency_key(idempotency_key)
                if existing is not None:
                    return existing, False
                raise

        try:
            self._sessions.add(_session_to_record(session))
        except gestalt.AlreadyExistsError:
            if claimed_idempotency:
                self._delete_session_idempotency(idempotency_key)
            existing = self.get_session(session_id)
            if existing is not None:
                return existing, False
            raise
        except Exception:
            if claimed_idempotency:
                self._delete_session_idempotency(idempotency_key)
            raise

        return session, True

    def get_session(self, session_id: str) -> StoredSession | None:
        try:
            return _record_to_session(self._sessions.get(session_id))
        except gestalt.NotFoundError:
            return None

    def list_sessions(self) -> list[StoredSession]:
        sessions = [
            _record_to_session(record) for record in self._sessions.get_all()
        ]
        sessions = [session for session in sessions if session is not None]
        return sorted(
            sessions,
            key=lambda session: (
                session.last_turn_at or session.updated_at,
                session.session_id,
            ),
            reverse=True,
        )

    def update_session(
        self,
        *,
        session_id: str,
        client_ref: str,
        state: int,
        metadata: dict[str, Any] | None,
    ) -> StoredSession | None:
        session = self.get_session(session_id)
        if session is None:
            return None
        if client_ref:
            session.client_ref = client_ref
        if state:
            session.state = state
        if metadata is not None:
            session.metadata = metadata
        session.updated_at = _utcnow()
        self._sessions.put(_session_to_record(session))
        return self.get_session(session_id)

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
    ) -> tuple[StoredRun, bool]:
        return self.begin_run(
            run_id=turn_id,
            idempotency_key=idempotency_key,
            provider_name=provider_name,
            model=model,
            messages=messages,
            session_ref=session_id,
            created_by=created_by,
            execution_ref=execution_ref,
        )

    def get_turn(self, turn_id: str) -> StoredRun | None:
        return self.get_run(turn_id)

    def list_turns(self, session_id: str) -> list[StoredRun]:
        return [
            run for run in self.list_runs() if run.session_ref == session_id
        ]

    def cancel_turn(self, turn_id: str, reason: str) -> StoredRun | None:
        with self._mutation_lock:
            run = self.get_run(turn_id)
            if run is None:
                return None
            if run.status in TERMINAL_STATUSES:
                return run
            cancel_reason = reason.strip() or "canceled"
            run.status = agent_pb2.AGENT_EXECUTION_STATUS_CANCELED
            run.status_message = cancel_reason
            run.cancel_reason = cancel_reason
            run.completed_at = _utcnow()
            self._runs.put(_run_to_record(run))
            return run

    def append_turn_event(
        self,
        *,
        turn_id: str,
        event_type: str,
        source: str,
        visibility: str = "private",
        data: dict[str, Any] | None = None,
    ) -> StoredTurnEvent:
        with self._mutation_lock:
            return self._append_turn_event_locked(
                turn_id=turn_id,
                event_type=event_type,
                source=source,
                visibility=visibility,
                data=data,
            )

    def list_turn_events(
        self, *, turn_id: str, after_seq: int = 0, limit: int = 0
    ) -> list[StoredTurnEvent]:
        events = [
            _record_to_turn_event(record) for record in self._events.get_all()
        ]
        all_events = [
            event
            for event in events
            if event is not None and event.turn_id == turn_id
        ]
        all_events.sort(key=lambda event: (event.seq, event.event_id))
        filtered = [event for event in all_events if event.seq > after_seq]
        filtered.extend(
            self._synthetic_terminal_events(
                turn_id=turn_id,
                after_seq=after_seq,
                start_seq=max((event.seq for event in all_events), default=0),
            )
        )
        if limit > 0:
            return filtered[:limit]
        return filtered

    def mark_turn_succeeded(
        self,
        *,
        turn_id: str,
        messages: list[dict[str, Any]],
        output_text: str,
        structured_output: dict[str, Any] | None,
    ) -> StoredRun:
        with self._mutation_lock:
            current = self._require_run(turn_id)
            if current.status in TERMINAL_STATUSES:
                return current
            current.status = agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED
            current.messages = messages
            current.output_text = output_text
            current.structured_output = structured_output
            current.status_message = ""
            current.completed_at = _utcnow()
            self._runs.put(_run_to_record(current))
            return current

    def mark_turn_failed(
        self,
        *,
        turn_id: str,
        messages: list[dict[str, Any]],
        status_message: str,
    ) -> StoredRun:
        with self._mutation_lock:
            current = self._require_run(turn_id)
            if current.status in TERMINAL_STATUSES:
                return current
            current.status = agent_pb2.AGENT_EXECUTION_STATUS_FAILED
            current.messages = messages
            current.status_message = status_message.strip()
            current.completed_at = _utcnow()
            self._runs.put(_run_to_record(current))
            return current

    def begin_run(
        self,
        *,
        run_id: str,
        idempotency_key: str,
        provider_name: str,
        model: str,
        messages: list[dict[str, Any]],
        session_ref: str,
        created_by: dict[str, str],
        execution_ref: str,
    ) -> tuple[StoredRun, bool]:
        existing = self.get_run(run_id)
        if existing is not None:
            _ensure_matching_session(existing, session_ref, conflict_key="turn_id", conflict_value=run_id)
            return existing, False

        if idempotency_key:
            existing = self._run_for_idempotency_key(idempotency_key)
        if existing is not None:
            _ensure_matching_session(
                existing,
                session_ref,
                conflict_key="idempotency_key",
                conflict_value=idempotency_key,
            )
            return existing, False

        now = _utcnow()
        self._touch_session_for_turn(session_ref, now)
        run = StoredRun(
            run_id=run_id,
            idempotency_key=idempotency_key,
            provider_name=provider_name,
            model=model,
            status=agent_pb2.AGENT_EXECUTION_STATUS_RUNNING,
            messages=messages,
            output_text="",
            structured_output=None,
            status_message="",
            session_ref=session_ref,
            created_by=created_by,
            created_at=now,
            started_at=now,
            completed_at=None,
            execution_ref=execution_ref,
            cancel_reason="",
        )

        claimed_idempotency = False
        if idempotency_key:
            try:
                self._idempotency.add(
                    {
                        "id": idempotency_key,
                        "run_id": run_id,
                        "provider_name": provider_name,
                        "created_at": now,
                    }
                )
                claimed_idempotency = True
            except gestalt.AlreadyExistsError:
                existing = self._run_for_idempotency_key(idempotency_key)
                if existing is not None:
                    return existing, False
                raise

        try:
            self._runs.add(_run_to_record(run))
        except gestalt.AlreadyExistsError:
            if claimed_idempotency:
                self._delete_idempotency(idempotency_key)
            existing = self.get_run(run_id)
            if existing is not None:
                return existing, False
            raise
        except Exception:
            if claimed_idempotency:
                self._delete_idempotency(idempotency_key)
            raise

        return run, True

    def get_run(self, run_id: str) -> StoredRun | None:
        try:
            return _record_to_run(self._runs.get(run_id))
        except gestalt.NotFoundError:
            return None

    def list_runs(self) -> list[StoredRun]:
        runs = [_record_to_run(record) for record in self._runs.get_all()]
        runs = [run for run in runs if run is not None]
        return sorted(runs, key=lambda run: (run.created_at, run.run_id), reverse=True)

    def _require_run(self, run_id: str) -> StoredRun:
        run = self.get_run(run_id)
        if run is None:
            raise RuntimeError(f"agent run {run_id!r} was not found")
        return run

    def _run_for_idempotency_key(self, idempotency_key: str) -> StoredRun | None:
        try:
            record = self._idempotency.get(idempotency_key)
        except gestalt.NotFoundError:
            return None
        run_id = str(record.get("run_id") or "").strip()
        if not run_id:
            return None
        return self.get_run(run_id)

    def _next_turn_event_seq(self, turn_id: str) -> int:
        existing = self.list_turn_events(turn_id=turn_id)
        if not existing:
            return 1
        return max(event.seq for event in existing) + 1

    def _synthetic_terminal_events(
        self, *, turn_id: str, after_seq: int, start_seq: int
    ) -> list[StoredTurnEvent]:
        turn = self.get_turn(turn_id)
        if turn is None or turn.status not in TERMINAL_STATUSES:
            return []

        created_at = turn.completed_at or turn.started_at or turn.created_at
        source = turn.provider_name
        events: list[StoredTurnEvent] = []
        next_seq = start_seq + 1

        if turn.status == agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED:
            events.append(
                StoredTurnEvent(
                    event_id=f"{turn_id}:synthetic:assistant.completed",
                    turn_id=turn_id,
                    seq=next_seq,
                    event_type="assistant.completed",
                    source=source,
                    visibility="private",
                    data={"text": turn.output_text},
                    created_at=created_at,
                )
            )
            next_seq += 1
            events.append(
                StoredTurnEvent(
                    event_id=f"{turn_id}:synthetic:turn.completed",
                    turn_id=turn_id,
                    seq=next_seq,
                    event_type="turn.completed",
                    source=source,
                    visibility="private",
                    data={"status": "succeeded"},
                    created_at=created_at,
                )
            )
        elif turn.status == agent_pb2.AGENT_EXECUTION_STATUS_FAILED:
            events.append(
                StoredTurnEvent(
                    event_id=f"{turn_id}:synthetic:turn.failed",
                    turn_id=turn_id,
                    seq=next_seq,
                    event_type="turn.failed",
                    source=source,
                    visibility="private",
                    data={"status_message": turn.status_message},
                    created_at=created_at,
                )
            )
        elif turn.status == agent_pb2.AGENT_EXECUTION_STATUS_CANCELED:
            events.append(
                StoredTurnEvent(
                    event_id=f"{turn_id}:synthetic:turn.canceled",
                    turn_id=turn_id,
                    seq=next_seq,
                    event_type="turn.canceled",
                    source=source,
                    visibility="private",
                    data={"reason": turn.cancel_reason or turn.status_message or "canceled"},
                    created_at=created_at,
                )
            )

        return [event for event in events if event.seq > after_seq]

    def _append_turn_event_locked(
        self,
        *,
        turn_id: str,
        event_type: str,
        source: str,
        visibility: str = "private",
        data: dict[str, Any] | None = None,
    ) -> StoredTurnEvent:
        while True:
            seq = self._next_turn_event_seq(turn_id)
            event = StoredTurnEvent(
                event_id=f"{turn_id}:{seq}",
                turn_id=turn_id,
                seq=seq,
                event_type=event_type.strip(),
                source=source.strip(),
                visibility=visibility.strip() or "private",
                data=copy.deepcopy(data) if isinstance(data, dict) else {},
                created_at=_utcnow(),
            )
            try:
                self._events.add(_turn_event_to_record(event))
            except gestalt.AlreadyExistsError:
                continue
            return event

    def _delete_idempotency(self, idempotency_key: str) -> None:
        if not idempotency_key:
            return
        try:
            self._idempotency.delete(idempotency_key)
        except gestalt.NotFoundError:
            pass

    def _session_for_idempotency_key(
        self, idempotency_key: str
    ) -> StoredSession | None:
        try:
            record = self._session_idempotency.get(idempotency_key)
        except gestalt.NotFoundError:
            return None
        session_id = str(record.get("session_id") or "").strip()
        if not session_id:
            return None
        return self.get_session(session_id)

    def _delete_session_idempotency(self, idempotency_key: str) -> None:
        if not idempotency_key:
            return
        try:
            self._session_idempotency.delete(idempotency_key)
        except gestalt.NotFoundError:
            pass

    def _touch_session_for_turn(self, session_id: str, now: datetime) -> None:
        if not session_id:
            return
        session = self.get_session(session_id)
        if session is None:
            return
        session.last_turn_at = now
        session.updated_at = now
        self._sessions.put(_session_to_record(session))


def _run_to_record(run: StoredRun) -> dict[str, Any]:
    return {
        "id": run.run_id,
        "idempotency_key": run.idempotency_key,
        "provider_name": run.provider_name,
        "model": run.model,
        "status": run.status,
        "messages": run.messages,
        "output_text": run.output_text,
        "structured_output": run.structured_output,
        "status_message": run.status_message,
        "session_ref": run.session_ref,
        "created_by": run.created_by,
        "created_at": run.created_at,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "execution_ref": run.execution_ref,
        "cancel_reason": run.cancel_reason,
    }


def _turn_event_to_record(event: StoredTurnEvent) -> dict[str, Any]:
    return {
        "id": event.event_id,
        "turn_id": event.turn_id,
        "seq": event.seq,
        "type": event.event_type,
        "source": event.source,
        "visibility": event.visibility,
        "data": event.data,
        "created_at": event.created_at,
    }


def _record_to_run(record: dict[str, Any] | None) -> StoredRun | None:
    if record is None:
        return None
    return StoredRun(
        run_id=str(record.get("id") or ""),
        idempotency_key=str(record.get("idempotency_key") or ""),
        provider_name=str(record.get("provider_name") or ""),
        model=str(record.get("model") or ""),
        status=int(record.get("status") or agent_pb2.AGENT_EXECUTION_STATUS_UNSPECIFIED),
        messages=_coerce_messages(record.get("messages")),
        output_text=str(record.get("output_text") or ""),
        structured_output=_coerce_optional_dict(record.get("structured_output")),
        status_message=str(record.get("status_message") or ""),
        session_ref=str(record.get("session_ref") or ""),
        created_by=_coerce_string_dict(record.get("created_by")),
        created_at=_coerce_required_datetime(record.get("created_at")),
        started_at=_coerce_datetime(record.get("started_at")),
        completed_at=_coerce_datetime(record.get("completed_at")),
        execution_ref=str(record.get("execution_ref") or ""),
        cancel_reason=str(record.get("cancel_reason") or ""),
    )


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


def _session_to_record(session: StoredSession) -> dict[str, Any]:
    return {
        "id": session.session_id,
        "idempotency_key": session.idempotency_key,
        "provider_name": session.provider_name,
        "model": session.model,
        "client_ref": session.client_ref,
        "state": session.state,
        "metadata": session.metadata,
        "created_by": session.created_by,
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
        state=int(
            record.get("state") or agent_pb2.AGENT_SESSION_STATE_UNSPECIFIED
        ),
        metadata=_coerce_optional_dict(record.get("metadata")) or {},
        created_by=_coerce_string_dict(record.get("created_by")),
        created_at=_coerce_required_datetime(record.get("created_at")),
        updated_at=_coerce_required_datetime(record.get("updated_at")),
        last_turn_at=_coerce_datetime(record.get("last_turn_at")),
    )


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
        raise ValueError("stored structured_output must be an object")
    return raw_value


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


def _ensure_matching_session(
    run: StoredRun, session_id: str, *, conflict_key: str, conflict_value: str
) -> None:
    if run.session_ref == session_id:
        return
    raise ValueError(
        f"{conflict_key} {conflict_value!r} already belongs to session {run.session_ref!r}"
    )


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


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

    def get_all(self) -> list[dict[str, Any]]:
        return _call_with_busy_retry(self._store.get_all)


def _call_with_busy_retry(operation: Any) -> Any:
    delay = BUSY_RETRY_INITIAL_DELAY_SECONDS
    while True:
        try:
            return operation()
        except grpc.RpcError as exc:
            if not _is_busy_error(exc):
                raise
            time.sleep(delay)
            delay = min(delay * 2, BUSY_RETRY_MAX_DELAY_SECONDS)


def _is_busy_error(exc: grpc.RpcError) -> bool:
    code_fn = getattr(exc, "code", None)
    details_fn = getattr(exc, "details", None)
    code = code_fn() if callable(code_fn) else None
    details = str(details_fn() or "").lower() if callable(details_fn) else ""
    return code == grpc.StatusCode.INTERNAL and (
        "database is locked" in details
        or "sqlite_busy" in details
        or "sql_busy" in details
    )
