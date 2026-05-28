from __future__ import annotations

import base64
import copy
from datetime import UTC, datetime
from typing import Any

import gestalt

from .store_records import (
    SESSION_VISIBILITY_COMPANY,
    SESSION_VISIBILITY_PRIVATE,
    StoredSession,
    StoredTurn,
    _coerce_datetime,
    _coerce_required_datetime,
    _coerce_string_dict,
)

PROJECTION_SCHEMA_VERSION = 1
PROJECTION_SEP = "\x1f"
PROJECTION_RANGE_SUFFIX = "\x7f"
MAX_INVERTED_SORT_MICROS = 99_999_999_999_999_999_999


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
        "visibility": session.visibility,
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
        visibility=_projection_visibility(record.get("visibility")),
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
        output=None,
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
    if session.visibility == SESSION_VISIBILITY_COMPANY:
        keys.append(_projection_key("session", "visibility", SESSION_VISIBILITY_COMPANY, "all", sort_key, session_id))
        keys.append(
            _projection_key("session", "visibility", SESSION_VISIBILITY_COMPANY, "state", state, sort_key, session_id)
        )
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
        keys.append(
            _projection_key("turn", "session", session, "subject", subject, "status", status, sort_key, turn_id)
        )
    return keys


def _session_projection_prefix(*, subject_id: str = "", state: int = 0, visibility: str = "") -> str:
    subject_id = subject_id.strip()
    visibility = visibility.strip()
    if subject_id and state:
        return _projection_prefix("session", "subject", _projection_value(subject_id), "state", str(state))
    if subject_id:
        return _projection_prefix("session", "subject", _projection_value(subject_id), "all")
    if visibility and state:
        return _projection_prefix("session", "visibility", visibility, "state", str(state))
    if visibility:
        return _projection_prefix("session", "visibility", visibility, "all")
    if state:
        return _projection_prefix("session", "state", str(state))
    return _projection_prefix("session", "all")


def _turn_projection_prefix(*, session_id: str, subject_id: str = "", status: int = 0) -> str:
    session = _projection_value(session_id.strip())
    subject_id = subject_id.strip()
    if subject_id and status:
        return _projection_prefix(
            "turn", "session", session, "subject", _projection_value(subject_id), "status", str(status)
        )
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


def _projection_visibility(value: Any) -> str:
    visibility = str(value or "").strip()
    if visibility in {SESSION_VISIBILITY_PRIVATE, SESSION_VISIBILITY_COMPANY}:
        return visibility
    return SESSION_VISIBILITY_PRIVATE
