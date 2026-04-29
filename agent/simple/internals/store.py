import copy
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import gestalt
import grpc

from gestalt.gen.v1 import agent_pb2 as _agent_pb2

agent_pb2: Any = cast(Any, _agent_pb2)

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
    resume_seed: dict[str, Any] | None


@dataclass(slots=True)
class StoredTurnCheckpoint:
    turn_id: str
    schema_version: int
    provider_name: str
    session_id: str
    model: str
    phase: str
    messages: list[dict[str, Any]]
    conversation: list[dict[str, Any]]
    response_schema: dict[str, Any]
    provider_options: dict[str, Any]
    tool_specs: list[dict[str, Any]]
    function_name_to_tool_id: dict[str, str]
    loaded_tool_ids: list[str]
    slack_reply_ref: str
    step_index: int
    pending_tool_call: dict[str, Any] | None
    repaired_arguments: dict[str, Any] | None
    attempt: int
    lease_owner: str
    lease_expires_at: datetime | None
    updated_at: datetime


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
        self._client: Any | None = None
        self._runs = _LazyObjectStore(self, run_store)
        self._idempotency = _LazyObjectStore(self, idempotency_store)
        self._events = _LazyObjectStore(self, f"{run_store}_events")
        self._checkpoints = _LazyObjectStore(self, f"{run_store}_checkpoints")
        self._tool_results = _LazyObjectStore(self, f"{run_store}_tool_results")
        self._sessions = _LazyObjectStore(self, f"{run_store}_sessions")
        self._session_idempotency = _LazyObjectStore(
            self, f"{idempotency_store}_sessions"
        )
        self._run_store_name = run_store
        self._idempotency_store_name = idempotency_store
        self._event_store_name = f"{run_store}_events"
        self._checkpoint_store_name = f"{run_store}_checkpoints"
        self._tool_result_store_name = f"{run_store}_tool_results"
        self._session_store_name = f"{run_store}_sessions"
        self._session_idempotency_store_name = f"{idempotency_store}_sessions"
        self._initialize_lock = threading.RLock()
        self._initialized = False
        self._closed = False
        self._mutation_lock = threading.Lock()

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
                    self._idempotency_store_name,
                    self._event_store_name,
                    self._checkpoint_store_name,
                    self._tool_result_store_name,
                    self._session_store_name,
                    self._session_idempotency_store_name,
                ):
                    try:
                        _call_with_busy_retry(
                            lambda name=name: client.create_object_store(name)
                        )
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

    def _ensure_client(self) -> Any:
        if self._client is not None:
            return self._client
        with self._initialize_lock:
            if self._client is not None:
                return self._client
            if self._closed:
                raise RuntimeError("agent run store is closed")
            self._client = gestalt.IndexedDB()
            return self._client

    def _object_store(self, name: str) -> Any:
        self.initialize()
        client = self._ensure_client()
        return _RetryingObjectStore(client.object_store(name))

    def _close_client(self) -> None:
        if self._client is None:
            return
        try:
            self._client.close()
        finally:
            self._client = None
            self._initialized = False

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
        resume_seed: dict[str, Any] | None = None,
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
            resume_seed=resume_seed,
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
        all_events = self._persisted_turn_events(turn_id)
        all_events.sort(key=lambda event: (event.seq, event.event_id))
        filtered = [event for event in all_events if event.seq > after_seq]
        filtered.extend(
            self._synthetic_terminal_events(
                turn_id=turn_id,
                after_seq=after_seq,
                start_seq=max((event.seq for event in all_events), default=0),
                skip_event_types={event.event_type for event in all_events},
            )
        )
        filtered.sort(key=lambda event: (event.seq, event.event_id))
        if limit > 0:
            return filtered[:limit]
        return filtered

    def append_turn_event_once(
        self,
        *,
        event_key: str,
        turn_id: str,
        event_type: str,
        source: str,
        visibility: str = "private",
        data: dict[str, Any] | None = None,
    ) -> StoredTurnEvent:
        event_key = event_key.strip()
        if not event_key:
            return self.append_turn_event(
                turn_id=turn_id, event_type=event_type, source=source, visibility=visibility, data=data
            )
        with self._mutation_lock:
            try:
                existing = _record_to_turn_event(self._events.get(event_key))
            except gestalt.NotFoundError:
                existing = None
            if existing is not None:
                return existing
            return self._append_turn_event_locked(
                turn_id=turn_id,
                event_type=event_type,
                source=source,
                visibility=visibility,
                data=data,
                event_id=event_key,
            )

    def put_turn_checkpoint(self, checkpoint: StoredTurnCheckpoint) -> None:
        self._checkpoints.put(_turn_checkpoint_to_record(checkpoint))

    def get_turn_checkpoint(self, turn_id: str) -> StoredTurnCheckpoint | None:
        try:
            return _record_to_turn_checkpoint(self._checkpoints.get(turn_id))
        except gestalt.NotFoundError:
            return None

    def ensure_turn_checkpoint_from_seed(self, run: StoredRun) -> StoredTurnCheckpoint | None:
        checkpoint = self.get_turn_checkpoint(run.run_id)
        if checkpoint is not None:
            return checkpoint
        seed = run.resume_seed
        if not isinstance(seed, dict):
            return None
        now = _utcnow()
        checkpoint = StoredTurnCheckpoint(
            turn_id=run.run_id,
            schema_version=int(seed.get("schema_version") or 1),
            provider_name=run.provider_name,
            session_id=run.session_ref,
            model=run.model,
            phase=str(seed.get("phase") or "model_next"),
            messages=_coerce_messages(seed.get("messages")) or list(run.messages),
            conversation=_coerce_messages(seed.get("conversation")),
            response_schema=_coerce_optional_dict(seed.get("response_schema")) or {},
            provider_options=_coerce_optional_dict(seed.get("provider_options")) or {},
            tool_specs=_coerce_messages(seed.get("tool_specs")),
            function_name_to_tool_id=_coerce_string_dict(seed.get("function_name_to_tool_id")),
            loaded_tool_ids=_coerce_string_list(seed.get("loaded_tool_ids")),
            slack_reply_ref=str(seed.get("slack_reply_ref") or ""),
            step_index=int(seed.get("step_index") or 0),
            pending_tool_call=_coerce_optional_dict(seed.get("pending_tool_call")),
            repaired_arguments=_coerce_optional_dict(seed.get("repaired_arguments")),
            attempt=0,
            lease_owner="",
            lease_expires_at=None,
            updated_at=now,
        )
        self.put_turn_checkpoint(checkpoint)
        return checkpoint

    def list_recoverable_turn_ids(self, *, limit: int = 0) -> list[str]:
        runs = [
            run
            for run in self.list_runs()
            if run.status not in TERMINAL_STATUSES
        ]
        runs.sort(key=lambda run: (run.started_at or run.created_at, run.run_id))
        turn_ids = [run.run_id for run in runs]
        if limit > 0:
            return turn_ids[:limit]
        return turn_ids

    def put_tool_result(self, *, turn_id: str, tool_call_id: str, result: dict[str, Any]) -> None:
        record = copy.deepcopy(result)
        record["id"] = _tool_result_record_id(turn_id=turn_id, tool_call_id=tool_call_id)
        record["turn_id"] = turn_id
        record["tool_call_id"] = tool_call_id
        record["updated_at"] = _utcnow()
        self._tool_results.put(record)

    def get_tool_result(self, *, turn_id: str, tool_call_id: str) -> dict[str, Any] | None:
        try:
            return self._tool_results.get(_tool_result_record_id(turn_id=turn_id, tool_call_id=tool_call_id))
        except gestalt.NotFoundError:
            return None

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
        resume_seed: dict[str, Any] | None = None,
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
            resume_seed=copy.deepcopy(resume_seed) if isinstance(resume_seed, dict) else None,
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
        existing = self._persisted_turn_events(turn_id)
        if not existing:
            return 1
        return max(event.seq for event in existing) + 1

    def _persisted_turn_events(self, turn_id: str) -> list[StoredTurnEvent]:
        events = [
            _record_to_turn_event(record) for record in self._events.get_all()
        ]
        return [
            event
            for event in events
            if event is not None and event.turn_id == turn_id
        ]

    def _synthetic_terminal_events(
        self, *, turn_id: str, after_seq: int, start_seq: int, skip_event_types: set[str] | None = None
    ) -> list[StoredTurnEvent]:
        turn = self.get_turn(turn_id)
        if turn is None or turn.status not in TERMINAL_STATUSES:
            return []

        created_at = turn.completed_at or turn.started_at or turn.created_at
        source = turn.provider_name
        events: list[StoredTurnEvent] = []
        next_seq = start_seq + 1
        skipped = skip_event_types or set()

        def append_synthetic(event_type: str, data: dict[str, Any]) -> None:
            nonlocal next_seq
            if event_type in skipped:
                return
            events.append(
                StoredTurnEvent(
                    event_id=f"{turn_id}:synthetic:{event_type}",
                    turn_id=turn_id,
                    seq=next_seq,
                    event_type=event_type,
                    source=source,
                    visibility="private",
                    data=data,
                    created_at=created_at,
                )
            )
            next_seq += 1

        if turn.status == agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED:
            append_synthetic("assistant.completed", {"text": turn.output_text})
            append_synthetic("turn.completed", {"status": "succeeded"})
        elif turn.status == agent_pb2.AGENT_EXECUTION_STATUS_FAILED:
            append_synthetic("turn.failed", {"status_message": turn.status_message})
        elif turn.status == agent_pb2.AGENT_EXECUTION_STATUS_CANCELED:
            append_synthetic("turn.canceled", {"reason": turn.cancel_reason or turn.status_message or "canceled"})

        return [event for event in events if event.seq > after_seq]

    def _append_turn_event_locked(
        self,
        *,
        turn_id: str,
        event_type: str,
        source: str,
        visibility: str = "private",
        data: dict[str, Any] | None = None,
        event_id: str = "",
    ) -> StoredTurnEvent:
        while True:
            seq = self._next_turn_event_seq(turn_id)
            event = StoredTurnEvent(
                event_id=event_id.strip() or f"{turn_id}:{seq}",
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
                if event_id:
                    existing = _record_to_turn_event(self._events.get(event_id))
                    if existing is not None:
                        return existing
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


def _turn_checkpoint_to_record(checkpoint: StoredTurnCheckpoint) -> dict[str, Any]:
    return {
        "id": checkpoint.turn_id,
        "schema_version": checkpoint.schema_version,
        "provider_name": checkpoint.provider_name,
        "session_id": checkpoint.session_id,
        "model": checkpoint.model,
        "phase": checkpoint.phase,
        "messages": copy.deepcopy(checkpoint.messages),
        "conversation": copy.deepcopy(checkpoint.conversation),
        "response_schema": copy.deepcopy(checkpoint.response_schema),
        "provider_options": copy.deepcopy(checkpoint.provider_options),
        "tool_specs": copy.deepcopy(checkpoint.tool_specs),
        "function_name_to_tool_id": dict(checkpoint.function_name_to_tool_id),
        "loaded_tool_ids": list(checkpoint.loaded_tool_ids),
        "slack_reply_ref": checkpoint.slack_reply_ref,
        "step_index": checkpoint.step_index,
        "pending_tool_call": copy.deepcopy(checkpoint.pending_tool_call),
        "repaired_arguments": copy.deepcopy(checkpoint.repaired_arguments),
        "attempt": checkpoint.attempt,
        "lease_owner": checkpoint.lease_owner,
        "lease_expires_at": checkpoint.lease_expires_at,
        "updated_at": checkpoint.updated_at,
    }


def _record_to_turn_checkpoint(record: dict[str, Any] | None) -> StoredTurnCheckpoint | None:
    if record is None:
        return None
    return StoredTurnCheckpoint(
        turn_id=str(record.get("id") or ""),
        schema_version=int(record.get("schema_version") or 1),
        provider_name=str(record.get("provider_name") or ""),
        session_id=str(record.get("session_id") or ""),
        model=str(record.get("model") or ""),
        phase=str(record.get("phase") or "model_next"),
        messages=_coerce_messages(record.get("messages")),
        conversation=_coerce_messages(record.get("conversation")),
        response_schema=_coerce_optional_dict(record.get("response_schema")) or {},
        provider_options=_coerce_optional_dict(record.get("provider_options")) or {},
        tool_specs=_coerce_messages(record.get("tool_specs")),
        function_name_to_tool_id=_coerce_string_dict(record.get("function_name_to_tool_id")),
        loaded_tool_ids=_coerce_string_list(record.get("loaded_tool_ids")),
        slack_reply_ref=str(record.get("slack_reply_ref") or ""),
        step_index=int(record.get("step_index") or 0),
        pending_tool_call=_coerce_optional_dict(record.get("pending_tool_call")),
        repaired_arguments=_coerce_optional_dict(record.get("repaired_arguments")),
        attempt=int(record.get("attempt") or 0),
        lease_owner=str(record.get("lease_owner") or ""),
        lease_expires_at=_coerce_datetime(record.get("lease_expires_at")),
        updated_at=_coerce_required_datetime(record.get("updated_at")),
    )


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
        "resume_seed": copy.deepcopy(run.resume_seed) if isinstance(run.resume_seed, dict) else None,
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
        resume_seed=_coerce_optional_dict(record.get("resume_seed")),
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


def _coerce_string_list(raw_value: Any) -> list[str]:
    if not isinstance(raw_value, list):
        return []
    return [str(item) for item in raw_value if str(item or "").strip()]


def _coerce_optional_dict(raw_value: Any) -> dict[str, Any] | None:
    if raw_value is None:
        return None
    if not isinstance(raw_value, dict):
        raise ValueError("stored structured_output must be an object")
    return raw_value


def _tool_result_record_id(*, turn_id: str, tool_call_id: str) -> str:
    return f"{turn_id}:{tool_call_id}"


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


class _LazyObjectStore:
    def __init__(self, owner: SimpleRunStore, name: str) -> None:
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

    def get_all(self) -> list[dict[str, Any]]:
        return self._resolve().get_all()

    def _resolve(self) -> _RetryingObjectStore:
        return self._owner._object_store(self._name)


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
