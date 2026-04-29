import copy
import threading
import time
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
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
    tool_grant: str
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
        self._events = _LazyObjectStore(self, f"{run_store}_events")
        self._checkpoints = _LazyObjectStore(self, f"{run_store}_checkpoints")
        self._tool_results = _LazyObjectStore(self, f"{run_store}_tool_results")
        self._sessions = _LazyObjectStore(self, f"{run_store}_sessions")
        self._session_idempotency = _LazyObjectStore(self, f"{idempotency_store}_sessions")
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
                    {"id": idempotency_key, "session_id": session_id, "provider_name": provider_name, "created_at": now}
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

    def list_sessions(
        self,
        *,
        session_ids: list[str] | None = None,
        subject_id: str = "",
        state: int = 0,
        limit: int = 0,
    ) -> list[StoredSession]:
        requested_ids = _normalized_unique_ids(session_ids)
        if requested_ids:
            sessions = [self.get_session(session_id) for session_id in requested_ids]
        else:
            sessions = [
                _record_to_session(record) for record in self._sessions.get_all()
            ]
        sessions = [session for session in sessions if session is not None]
        subject_id = subject_id.strip()
        if subject_id:
            sessions = [
                session
                for session in sessions
                if str(session.created_by.get("subject_id", "") or "").strip() == subject_id
            ]
        if state:
            sessions = [session for session in sessions if session.state == state]
        sessions = sorted(
            sessions,
            key=lambda session: (
                session.last_turn_at or session.updated_at,
                session.session_id,
            ),
            reverse=True,
        )
        if limit > 0:
            sessions = sessions[:limit]
        return sessions

    def update_session(
        self, *, session_id: str, client_ref: str, state: int, metadata: dict[str, Any] | None
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
        start_event_source: str = "",
        start_event_data: dict[str, Any] | None = None,
    ) -> tuple[StoredRun, bool]:
        with self._mutation_lock:

            def begin(stores: dict[str, Any]) -> tuple[StoredRun, bool]:
                runs = stores[self._run_store_name]
                idempotency = stores[self._idempotency_store_name]
                sessions = stores[self._session_store_name]
                checkpoints = stores[self._checkpoint_store_name]
                events = stores[self._event_store_name]

                existing = _get_run_from_store(runs, turn_id)
                if existing is not None:
                    _ensure_matching_session(existing, session_id, conflict_key="turn_id", conflict_value=turn_id)
                    return existing, False

                if idempotency_key:
                    existing = _run_for_idempotency_key_from_stores(idempotency, runs, idempotency_key)
                    if existing is not None:
                        _ensure_matching_session(
                            existing, session_id, conflict_key="idempotency_key", conflict_value=idempotency_key
                        )
                        return existing, False

                now = _utcnow()
                _touch_session_for_turn_in_store(sessions, session_id, now)
                run = StoredRun(
                    run_id=turn_id,
                    idempotency_key=idempotency_key,
                    provider_name=provider_name,
                    model=model,
                    status=agent_pb2.AGENT_EXECUTION_STATUS_RUNNING,
                    messages=copy.deepcopy(messages),
                    output_text="",
                    structured_output=None,
                    status_message="",
                    session_ref=session_id,
                    created_by=copy.deepcopy(created_by),
                    created_at=now,
                    started_at=now,
                    completed_at=None,
                    execution_ref=execution_ref,
                    cancel_reason="",
                    resume_seed=copy.deepcopy(resume_seed) if isinstance(resume_seed, dict) else None,
                )
                if idempotency_key:
                    idempotency.add(
                        {"id": idempotency_key, "run_id": turn_id, "provider_name": provider_name, "created_at": now}
                    )
                runs.add(_run_to_record(run))
                if isinstance(resume_seed, dict):
                    checkpoints.add(_turn_checkpoint_to_record(_checkpoint_from_seed(run, resume_seed, now=now)))
                if start_event_source:
                    self._append_turn_event_locked(
                        turn_id=turn_id,
                        event_type="turn.started",
                        source=start_event_source,
                        data=start_event_data,
                        event_id=f"{turn_id}:turn.started",
                        events_store=events,
                    )
                return run, True

            return self._with_transaction(
                [
                    self._run_store_name,
                    self._idempotency_store_name,
                    self._session_store_name,
                    self._checkpoint_store_name,
                    self._event_store_name,
                ],
                begin,
            )

    def get_turn(self, turn_id: str) -> StoredRun | None:
        return self.get_run(turn_id)

    def list_turns(
        self,
        session_id: str,
        *,
        turn_ids: list[str] | None = None,
        subject_id: str = "",
        status: int = 0,
        limit: int = 0,
    ) -> list[StoredRun]:
        requested_ids = _normalized_unique_ids(turn_ids)
        if requested_ids:
            runs = [self.get_run(turn_id) for turn_id in requested_ids]
            turns = [run for run in runs if run is not None]
        else:
            if not session_id.strip():
                return []
            turns = self.list_runs()
        session_id = session_id.strip()
        if session_id:
            turns = [run for run in turns if run.session_ref == session_id]
        subject_id = subject_id.strip()
        if subject_id:
            turns = [run for run in turns if str(run.created_by.get("subject_id", "") or "").strip() == subject_id]
        if status:
            turns = [run for run in turns if run.status == status]
        turns = sorted(turns, key=lambda run: (run.created_at, run.run_id), reverse=True)
        if limit > 0:
            turns = turns[:limit]
        return turns

    def cancel_turn(self, turn_id: str, reason: str) -> StoredRun | None:
        with self._mutation_lock:
            def cancel(stores: dict[str, Any]) -> StoredRun | None:
                runs = stores[self._run_store_name]
                run = _get_run_from_store(runs, turn_id)
                if run is None:
                    return None
                if run.status in TERMINAL_STATUSES:
                    return run
                cancel_reason = reason.strip() or "canceled"
                run.status = agent_pb2.AGENT_EXECUTION_STATUS_CANCELED
                run.status_message = cancel_reason
                run.cancel_reason = cancel_reason
                run.completed_at = _utcnow()
                runs.put(_run_to_record(run))
                return run

            return self._with_transaction([self._run_store_name], cancel)

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
                turn_id=turn_id, event_type=event_type, source=source, visibility=visibility, data=data
            )

    def list_turn_events(self, *, turn_id: str, after_seq: int = 0, limit: int = 0) -> list[StoredTurnEvent]:
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

            def append_once(stores: dict[str, Any]) -> StoredTurnEvent:
                events = stores[self._event_store_name]
                existing = _record_to_turn_event(_get_optional_record(events, event_key))
                if existing is not None:
                    return existing
                return self._append_turn_event_locked(
                    turn_id=turn_id,
                    event_type=event_type,
                    source=source,
                    visibility=visibility,
                    data=data,
                    event_id=event_key,
                    events_store=events,
                )

            return self._with_transaction([self._event_store_name], append_once)

    def put_turn_checkpoint(self, checkpoint: StoredTurnCheckpoint, *, lease_owner: str = "") -> None:
        lease_owner = lease_owner.strip()

        def put_checkpoint(stores: dict[str, Any]) -> None:
            checkpoints = stores[self._checkpoint_store_name]
            replacement = checkpoint
            existing = _record_to_turn_checkpoint(_get_optional_record(checkpoints, checkpoint.turn_id))
            if lease_owner:
                _require_checkpoint_lease(existing, owner=lease_owner)
            if existing is not None and lease_owner:
                replacement = replace(
                    replacement, lease_owner=existing.lease_owner, lease_expires_at=existing.lease_expires_at
                )
            if (
                existing is not None
                and replacement.lease_owner
                and existing.lease_owner
                and existing.lease_owner != replacement.lease_owner
            ):
                raise RuntimeError("turn checkpoint lease owner mismatch")
            if existing is not None and not replacement.lease_owner and existing.lease_owner:
                replacement = _checkpoint_with_existing_lease(replacement, existing)
            checkpoints.put(_turn_checkpoint_to_record(replacement))

        self._with_transaction([self._checkpoint_store_name], put_checkpoint)

    def get_turn_checkpoint(self, turn_id: str) -> StoredTurnCheckpoint | None:
        try:
            return _record_to_turn_checkpoint(self._checkpoints.get(turn_id))
        except gestalt.NotFoundError:
            return None

    def ensure_turn_checkpoint_from_seed(self, run: StoredRun) -> StoredTurnCheckpoint | None:
        seed = run.resume_seed
        if not isinstance(seed, dict):
            return None

        def ensure_checkpoint(stores: dict[str, Any]) -> StoredTurnCheckpoint:
            checkpoints = stores[self._checkpoint_store_name]
            existing = _record_to_turn_checkpoint(_get_optional_record(checkpoints, run.run_id))
            if existing is not None:
                return existing
            checkpoint = _checkpoint_from_seed(run, seed)
            checkpoints.add(_turn_checkpoint_to_record(checkpoint))
            return checkpoint

        return self._with_transaction([self._checkpoint_store_name], ensure_checkpoint)

    def claim_turn_lease(self, turn_id: str, *, owner: str, lease_seconds: float, now: datetime | None = None) -> bool:
        owner = owner.strip()
        if not owner:
            return False
        now = now or _utcnow()

        def claim(stores: dict[str, Any]) -> bool:
            checkpoints = stores[self._checkpoint_store_name]
            checkpoint = _record_to_turn_checkpoint(_get_optional_record(checkpoints, turn_id))
            if checkpoint is None:
                return False
            expires_at = checkpoint.lease_expires_at
            if (
                checkpoint.lease_owner
                and checkpoint.lease_owner != owner
                and expires_at is not None
                and expires_at > now
            ):
                return False
            checkpoint.lease_owner = owner
            checkpoint.lease_expires_at = _lease_expiration(now=now, lease_seconds=lease_seconds)
            checkpoint.updated_at = now
            checkpoint.attempt += 1
            checkpoints.put(_turn_checkpoint_to_record(checkpoint))
            return True

        return bool(self._with_transaction([self._checkpoint_store_name], claim))

    def renew_turn_lease(self, turn_id: str, *, owner: str, lease_seconds: float, now: datetime | None = None) -> bool:
        owner = owner.strip()
        if not owner:
            return False
        now = now or _utcnow()

        def renew(stores: dict[str, Any]) -> bool:
            checkpoints = stores[self._checkpoint_store_name]
            checkpoint = _record_to_turn_checkpoint(_get_optional_record(checkpoints, turn_id))
            if checkpoint is None or checkpoint.lease_owner != owner:
                return False
            checkpoint.lease_expires_at = _lease_expiration(now=now, lease_seconds=lease_seconds)
            checkpoint.updated_at = now
            checkpoints.put(_turn_checkpoint_to_record(checkpoint))
            return True

        return bool(self._with_transaction([self._checkpoint_store_name], renew))

    def release_turn_lease(self, turn_id: str, *, owner: str) -> None:
        owner = owner.strip()
        if not owner:
            return

        def release(stores: dict[str, Any]) -> None:
            checkpoints = stores[self._checkpoint_store_name]
            checkpoint = _record_to_turn_checkpoint(_get_optional_record(checkpoints, turn_id))
            if checkpoint is None or checkpoint.lease_owner != owner:
                return
            checkpoint.lease_owner = ""
            checkpoint.lease_expires_at = None
            checkpoint.updated_at = _utcnow()
            checkpoints.put(_turn_checkpoint_to_record(checkpoint))

        self._with_transaction([self._checkpoint_store_name], release)

    def list_recoverable_turn_ids(self, *, limit: int = 0) -> list[str]:
        runs = [run for run in self.list_runs() if run.status not in TERMINAL_STATUSES]
        runs.sort(key=lambda run: (run.started_at or run.created_at, run.run_id))
        turn_ids = [run.run_id for run in runs]
        if limit > 0:
            return turn_ids[:limit]
        return turn_ids

    def put_tool_result(
        self, *, turn_id: str, tool_call_id: str, result: dict[str, Any], lease_owner: str = ""
    ) -> None:
        record = copy.deepcopy(result)
        record["id"] = _tool_result_record_id(turn_id=turn_id, tool_call_id=tool_call_id)
        record["turn_id"] = turn_id
        record["tool_call_id"] = tool_call_id
        record["updated_at"] = _utcnow()
        lease_owner = lease_owner.strip()
        if lease_owner:
            def put_result(stores: dict[str, Any]) -> None:
                _require_checkpoint_lease_from_store(
                    stores[self._checkpoint_store_name], turn_id=turn_id, owner=lease_owner
                )
                stores[self._tool_result_store_name].put(record)

            self._with_transaction([self._checkpoint_store_name, self._tool_result_store_name], put_result)
            return
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
        lease_owner: str = "",
    ) -> StoredRun:
        with self._mutation_lock:

            def mark_succeeded(stores: dict[str, Any]) -> StoredRun:
                runs = stores[self._run_store_name]
                current = _get_run_from_store(runs, turn_id)
                if current is None:
                    raise RuntimeError(f"agent run {turn_id!r} was not found")
                if lease_owner:
                    _require_checkpoint_lease_from_store(
                        stores[self._checkpoint_store_name], turn_id=turn_id, owner=lease_owner
                    )
                if current.status in TERMINAL_STATUSES:
                    return current
                current.status = agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED
                current.messages = messages
                current.output_text = output_text
                current.structured_output = structured_output
                current.status_message = ""
                current.completed_at = _utcnow()
                runs.put(_run_to_record(current))
                return current

            return self._with_transaction([self._run_store_name, self._checkpoint_store_name], mark_succeeded)

    def mark_turn_failed(
        self, *, turn_id: str, messages: list[dict[str, Any]], status_message: str, lease_owner: str = ""
    ) -> StoredRun:
        with self._mutation_lock:

            def mark_failed(stores: dict[str, Any]) -> StoredRun:
                runs = stores[self._run_store_name]
                current = _get_run_from_store(runs, turn_id)
                if current is None:
                    raise RuntimeError(f"agent run {turn_id!r} was not found")
                if lease_owner:
                    _require_checkpoint_lease_from_store(
                        stores[self._checkpoint_store_name], turn_id=turn_id, owner=lease_owner
                    )
                if current.status in TERMINAL_STATUSES:
                    return current
                current.status = agent_pb2.AGENT_EXECUTION_STATUS_FAILED
                current.messages = messages
                current.status_message = status_message.strip()
                current.completed_at = _utcnow()
                runs.put(_run_to_record(current))
                return current

            return self._with_transaction([self._run_store_name, self._checkpoint_store_name], mark_failed)

    def get_run(self, run_id: str) -> StoredRun | None:
        try:
            return _record_to_run(self._runs.get(run_id))
        except gestalt.NotFoundError:
            return None

    def list_runs(self) -> list[StoredRun]:
        runs = [_record_to_run(record) for record in self._runs.get_all()]
        runs = [run for run in runs if run is not None]
        return sorted(runs, key=lambda run: (run.created_at, run.run_id), reverse=True)

    def _next_turn_event_seq(self, turn_id: str) -> int:
        existing = self._persisted_turn_events(turn_id)
        if not existing:
            return 1
        return max(event.seq for event in existing) + 1

    def _persisted_turn_events(self, turn_id: str) -> list[StoredTurnEvent]:
        events = [_record_to_turn_event(record) for record in self._events.get_all()]
        return [event for event in events if event is not None and event.turn_id == turn_id]

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
                store.add(_turn_event_to_record(event))
            except gestalt.AlreadyExistsError:
                if event_id:
                    existing = _record_to_turn_event(store.get(event_id))
                    if existing is not None:
                        return existing
                continue
            return event

    def _session_for_idempotency_key(self, idempotency_key: str) -> StoredSession | None:
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
        "tool_grant": checkpoint.tool_grant,
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


def _checkpoint_from_seed(run: StoredRun, seed: dict[str, Any], *, now: datetime | None = None) -> StoredTurnCheckpoint:
    now = now or _utcnow()
    return StoredTurnCheckpoint(
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
        tool_grant=str(seed.get("tool_grant") or ""),
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


def _checkpoint_with_existing_lease(
    checkpoint: StoredTurnCheckpoint, existing: StoredTurnCheckpoint
) -> StoredTurnCheckpoint:
    return replace(checkpoint, lease_owner=existing.lease_owner, lease_expires_at=existing.lease_expires_at)


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
        tool_grant=str(record.get("tool_grant") or ""),
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
        state=int(record.get("state") or agent_pb2.AGENT_SESSION_STATE_UNSPECIFIED),
        metadata=_coerce_optional_dict(record.get("metadata")) or {},
        created_by=_coerce_string_dict(record.get("created_by")),
        created_at=_coerce_required_datetime(record.get("created_at")),
        updated_at=_coerce_required_datetime(record.get("updated_at")),
        last_turn_at=_coerce_datetime(record.get("last_turn_at")),
    )


def _get_run_from_store(store: Any, run_id: str) -> StoredRun | None:
    return _record_to_run(_get_optional_record(store, run_id))


def _run_for_idempotency_key_from_stores(
    idempotency_store: Any, run_store: Any, idempotency_key: str
) -> StoredRun | None:
    record = _get_optional_record(idempotency_store, idempotency_key)
    if record is None:
        return None
    run_id = str(record.get("run_id") or "").strip()
    if not run_id:
        return None
    return _get_run_from_store(run_store, run_id)


def _touch_session_for_turn_in_store(store: Any, session_id: str, now: datetime) -> None:
    if not session_id:
        return
    session = _record_to_session(_get_optional_record(store, session_id))
    if session is None:
        return
    session.last_turn_at = now
    session.updated_at = now
    store.put(_session_to_record(session))


def _require_checkpoint_lease_from_store(store: Any, *, turn_id: str, owner: str) -> None:
    checkpoint = _record_to_turn_checkpoint(_get_optional_record(store, turn_id))
    _require_checkpoint_lease(checkpoint, owner=owner)


def _require_checkpoint_lease(
    checkpoint: StoredTurnCheckpoint | None, *, owner: str, now: datetime | None = None
) -> None:
    if checkpoint is None:
        raise RuntimeError("turn checkpoint was not found")
    if checkpoint.lease_owner != owner:
        raise RuntimeError("turn checkpoint lease owner mismatch")
    expires_at = checkpoint.lease_expires_at
    if expires_at is None or expires_at <= (now or _utcnow()):
        raise RuntimeError("turn checkpoint lease expired")


def _get_optional_record(store: Any, record_id: str) -> dict[str, Any] | None:
    records = store.get_all(gestalt.KeyRange(lower=record_id, upper=record_id))
    for record in records:
        if str(record.get("id") or "") == record_id:
            return record
    return None


def _persisted_turn_events_from_store(store: Any, turn_id: str) -> list[StoredTurnEvent]:
    events = [_record_to_turn_event(record) for record in store.get_all()]
    return [event for event in events if event is not None and event.turn_id == turn_id]


def _next_turn_event_seq_from_store(store: Any, turn_id: str) -> int:
    existing = _persisted_turn_events_from_store(store, turn_id)
    if not existing:
        return 1
    return max(event.seq for event in existing) + 1


def _coerce_messages(raw_value: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_value, list):
        return []
    messages: list[dict[str, Any]] = []
    for item in raw_value:
        if isinstance(item, dict):
            messages.append(copy.deepcopy(item))
    return messages


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


def _lease_expiration(*, now: datetime, lease_seconds: float) -> datetime:
    return now + timedelta(seconds=max(lease_seconds, 1.0))


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


def _ensure_matching_session(run: StoredRun, session_id: str, *, conflict_key: str, conflict_value: str) -> None:
    if run.session_ref == session_id:
        return
    raise ValueError(f"{conflict_key} {conflict_value!r} already belongs to session {run.session_ref!r}")


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

    def get_all(self, key_range: Any | None = None) -> list[dict[str, Any]]:
        return _call_with_busy_retry(lambda: self._store.get_all(key_range))


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

    def get_all(self, key_range: Any | None = None) -> list[dict[str, Any]]:
        return self._resolve().get_all(key_range)

    def _resolve(self) -> _RetryingObjectStore:
        return self._owner._object_store(self._name)


def _call_with_busy_retry(operation: Any) -> Any:
    delay = BUSY_RETRY_INITIAL_DELAY_SECONDS
    while True:
        try:
            return operation()
        except (grpc.RpcError, gestalt.TransactionError) as exc:
            if not _is_busy_error(exc):
                raise
            time.sleep(delay)
            delay = min(delay * 2, BUSY_RETRY_MAX_DELAY_SECONDS)


def _is_busy_error(exc: BaseException) -> bool:
    code_fn = getattr(exc, "code", None)
    details_fn = getattr(exc, "details", None)
    code = code_fn() if callable(code_fn) else None
    details = str(details_fn() or exc).lower() if callable(details_fn) else str(exc).lower()
    if code is not None and code != grpc.StatusCode.INTERNAL:
        return False
    return "database is locked" in details or "sqlite_busy" in details or "sql_busy" in details
