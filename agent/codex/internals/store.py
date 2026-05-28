from __future__ import annotations

import copy
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import gestalt

TERMINAL_STATUSES = {
    gestalt.AGENT_EXECUTION_STATUS_SUCCEEDED,
    gestalt.AGENT_EXECUTION_STATUS_FAILED,
    gestalt.AGENT_EXECUTION_STATUS_CANCELED,
}

SESSION_VISIBILITY_COMPANY = "company"
SESSION_VISIBILITY_PRIVATE = "private"


class StoreConflictError(ValueError):
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
    visibility: str
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
    output: gestalt.AgentTurnOutput | None
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


class InMemoryRunStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._sessions: dict[str, StoredSession] = {}
        self._session_idempotency: dict[str, str] = {}
        self._turns: dict[str, StoredTurn] = {}
        self._turn_idempotency: dict[tuple[str, str], str] = {}
        self._events: dict[str, list[StoredTurnEvent]] = {}

    def close(self) -> None:
        with self._lock:
            self._sessions.clear()
            self._session_idempotency.clear()
            self._turns.clear()
            self._turn_idempotency.clear()
            self._events.clear()

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
        with self._lock:
            if session_id in self._sessions:
                return copy.deepcopy(self._sessions[session_id]), False
            if idempotency_key and idempotency_key in self._session_idempotency:
                return copy.deepcopy(self._sessions[self._session_idempotency[idempotency_key]]), False
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
                created_by=dict(created_by),
                visibility=session_visibility_for_create(metadata, created_by),
                created_at=now,
                updated_at=now,
            )
            self._sessions[session_id] = session
            if idempotency_key:
                self._session_idempotency[idempotency_key] = session_id
            return copy.deepcopy(session), True

    def get_session(self, session_id: str) -> StoredSession | None:
        with self._lock:
            session = self._sessions.get(session_id.strip())
            return copy.deepcopy(session) if session is not None else None

    def get_session_by_idempotency_key(self, idempotency_key: str) -> StoredSession | None:
        idempotency_key = idempotency_key.strip()
        if not idempotency_key:
            return None
        with self._lock:
            session_id = self._session_idempotency.get(idempotency_key)
            if not session_id:
                return None
            session = self._sessions.get(session_id)
            return copy.deepcopy(session) if session is not None else None

    def list_sessions(
        self, *, session_ids: list[str] | None = None, subject_id: str = "", state: int = 0, limit: int = 0
    ) -> list[StoredSession]:
        requested_ids = [value.strip() for value in session_ids or [] if value.strip()]
        with self._lock:
            if requested_ids:
                sessions = [self._sessions[value] for value in requested_ids if value in self._sessions]
            else:
                sessions = list(self._sessions.values())
            if not subject_id:
                sessions = []
            else:
                sessions = [session for session in sessions if session_readable_by(session, subject_id)]
            if state:
                sessions = [session for session in sessions if session.state == state]
            sessions = sorted(sessions, key=lambda session: session.last_turn_at or session.updated_at, reverse=True)
            if limit > 0:
                sessions = sessions[:limit]
            return copy.deepcopy(sessions)

    def update_session(
        self, *, session_id: str, client_ref: str = "", state: int = 0, metadata: dict[str, Any] | None = None
    ) -> StoredSession | None:
        with self._lock:
            session = self._sessions.get(session_id.strip())
            if session is None:
                return None
            if client_ref:
                session.client_ref = client_ref
            if state:
                session.state = state
            if metadata is not None:
                session.metadata = copy.deepcopy(metadata)
            session.updated_at = _utcnow()
            return copy.deepcopy(session)

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
        with self._lock:
            if turn_id in self._turns:
                existing = self._turns[turn_id]
                if existing.session_id != session_id:
                    raise StoreConflictError(f"turn_id {turn_id!r} already exists for another session")
                return copy.deepcopy(existing), False
            idem_key = (session_id, idempotency_key)
            if idempotency_key and idem_key in self._turn_idempotency:
                existing = self._turns[self._turn_idempotency[idem_key]]
                if existing.session_id != session_id:
                    raise StoreConflictError(f"idempotency_key {idempotency_key!r} already exists for another session")
                return copy.deepcopy(existing), False
            now = _utcnow()
            turn = StoredTurn(
                turn_id=turn_id,
                session_id=session_id,
                idempotency_key=idempotency_key,
                provider_name=provider_name,
                model=model,
                status=gestalt.AGENT_EXECUTION_STATUS_RUNNING,
                messages=copy.deepcopy(messages),
                output=None,
                status_message="",
                created_by=dict(created_by),
                created_at=now,
                started_at=now,
                completed_at=None,
                execution_ref=execution_ref,
            )
            self._turns[turn_id] = turn
            if idempotency_key:
                self._turn_idempotency[idem_key] = turn_id
            if session_id in self._sessions:
                self._sessions[session_id].last_turn_at = now
                self._sessions[session_id].updated_at = now
            self.append_event(turn_id=turn_id, event_type="turn.started", source=provider_name, data={"model": model})
            return copy.deepcopy(turn), True

    def get_turn(self, turn_id: str) -> StoredTurn | None:
        with self._lock:
            turn = self._turns.get(turn_id.strip())
            return copy.deepcopy(turn) if turn is not None else None

    def list_turns(
        self,
        *,
        session_id: str,
        turn_ids: list[str] | None = None,
        subject_id: str = "",
        status: int = 0,
        limit: int = 0,
    ) -> list[StoredTurn]:
        requested_ids = [value.strip() for value in turn_ids or [] if value.strip()]
        with self._lock:
            if requested_ids:
                turns = [self._turns[value] for value in requested_ids if value in self._turns]
            else:
                turns = [turn for turn in self._turns.values() if turn.session_id == session_id.strip()]
            if subject_id:
                turns = [turn for turn in turns if str(turn.created_by.get("subject_id", "")).strip() == subject_id]
            if status:
                turns = [turn for turn in turns if turn.status == status]
            turns = sorted(turns, key=lambda turn: turn.created_at, reverse=True)
            if limit > 0:
                turns = turns[:limit]
            return copy.deepcopy(turns)

    def complete_turn(self, *, turn_id: str, output: gestalt.AgentTurnOutput) -> StoredTurn | None:
        with self._lock:
            turn = self._turns.get(turn_id.strip())
            if turn is None or turn.status in TERMINAL_STATUSES:
                return copy.deepcopy(turn) if turn is not None else None
            turn.status = gestalt.AGENT_EXECUTION_STATUS_SUCCEEDED
            turn.output = copy.deepcopy(output)
            turn.completed_at = _utcnow()
            self.append_event(
                turn_id=turn.turn_id,
                event_type="assistant.message",
                source=turn.provider_name,
                data=_assistant_message_event_data(output),
            )
            self.append_event(
                turn_id=turn.turn_id,
                event_type="turn.completed",
                source=turn.provider_name,
                data={"status": "succeeded"},
            )
            return copy.deepcopy(turn)

    def fail_turn(self, *, turn_id: str, message: str) -> StoredTurn | None:
        with self._lock:
            turn = self._turns.get(turn_id.strip())
            if turn is None or turn.status in TERMINAL_STATUSES:
                return copy.deepcopy(turn) if turn is not None else None
            turn.status = gestalt.AGENT_EXECUTION_STATUS_FAILED
            turn.status_message = message
            turn.completed_at = _utcnow()
            self.append_event(
                turn_id=turn.turn_id, event_type="turn.failed", source=turn.provider_name, data={"error": message}
            )
            return copy.deepcopy(turn)

    def cancel_turn(self, *, turn_id: str, reason: str) -> StoredTurn | None:
        with self._lock:
            turn = self._turns.get(turn_id.strip())
            if turn is None:
                return None
            if turn.status not in TERMINAL_STATUSES:
                turn.status = gestalt.AGENT_EXECUTION_STATUS_CANCELED
                turn.status_message = reason
                turn.completed_at = _utcnow()
                self.append_event(
                    turn_id=turn.turn_id, event_type="turn.canceled", source=turn.provider_name, data={"reason": reason}
                )
            return copy.deepcopy(turn)

    def append_event(self, *, turn_id: str, event_type: str, source: str, data: dict[str, Any]) -> StoredTurnEvent:
        with self._lock:
            events = self._events.setdefault(turn_id, [])
            event = StoredTurnEvent(
                event_id=f"{turn_id}:{len(events) + 1}",
                turn_id=turn_id,
                seq=len(events) + 1,
                event_type=event_type,
                source=source,
                visibility="external",
                data=copy.deepcopy(data),
                created_at=_utcnow(),
            )
            events.append(event)
            return copy.deepcopy(event)

    def list_turn_events(self, *, turn_id: str, after_seq: int = 0, limit: int = 0) -> list[StoredTurnEvent]:
        with self._lock:
            events = [event for event in self._events.get(turn_id.strip(), []) if event.seq > after_seq]
            if limit > 0:
                events = events[:limit]
            return copy.deepcopy(events)


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


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


def session_visibility_for_create(metadata: dict[str, Any], created_by: dict[str, str]) -> str:
    if _is_slack_agent_session_metadata(metadata) and _is_managed_actor(created_by):
        return SESSION_VISIBILITY_COMPANY
    return SESSION_VISIBILITY_PRIVATE


def session_readable_by(session: StoredSession, subject_id: str) -> bool:
    subject_id = subject_id.strip()
    if not subject_id:
        return False
    if str(session.created_by.get("subject_id", "") or "").strip() == subject_id:
        return True
    return session.visibility == SESSION_VISIBILITY_COMPANY


def session_writable_by(session: StoredSession, subject_id: str) -> bool:
    subject_id = subject_id.strip()
    if not subject_id:
        return False
    return str(session.created_by.get("subject_id", "") or "").strip() == subject_id


def _is_slack_agent_session_metadata(metadata: dict[str, Any]) -> bool:
    slack = metadata.get("slack")
    if not isinstance(slack, dict):
        return False
    return bool(
        str(slack.get("team_id") or "").strip()
        and str(slack.get("channel_id") or "").strip()
        and str(slack.get("root_message_ts") or "").strip()
        and str(slack.get("session_ref") or "").strip()
    )


def _is_managed_actor(actor: dict[str, str]) -> bool:
    subject_id = str(actor.get("subject_id", "") or "").strip()
    subject_kind = str(actor.get("subject_kind", "") or "").strip()
    return subject_kind == "service_account" or subject_id.startswith("service_account:")
