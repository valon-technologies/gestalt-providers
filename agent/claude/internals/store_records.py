from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import gestalt

from .subject_id import created_by_subject_id_from_record, is_managed_subject_id

TERMINAL_STATUSES = {
    gestalt.AGENT_EXECUTION_STATUS_SUCCEEDED,
    gestalt.AGENT_EXECUTION_STATUS_FAILED,
    gestalt.AGENT_EXECUTION_STATUS_CANCELED,
}

SESSION_VISIBILITY_COMPANY = "company"
SESSION_VISIBILITY_PRIVATE = "private"


@dataclass(frozen=True, slots=True)
class StoredSession:
    session_id: str
    idempotency_key: str
    provider_name: str
    model: str
    client_ref: str
    state: int
    metadata: dict[str, Any]
    prepared_workspace: dict[str, str] | None
    created_by_subject_id: str
    visibility: str
    created_at: datetime
    updated_at: datetime
    last_turn_at: datetime | None = None


@dataclass(frozen=True, slots=True)
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
    created_by_subject_id: str
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    execution_ref: str


@dataclass(frozen=True, slots=True)
class StoredTurnEvent:
    event_id: str
    turn_id: str
    seq: int
    event_type: str
    source: str
    visibility: str
    data: dict[str, Any]
    created_at: datetime


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
        "created_by_subject_id": session.created_by_subject_id,
        "visibility": session.visibility,
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
        created_by_subject_id=created_by_subject_id_from_record(record),
        visibility=_session_visibility_from_record(record),
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
        "output": _turn_output_to_record(turn.output) if turn.output is not None else None,
        "status_message": turn.status_message,
        "created_by_subject_id": turn.created_by_subject_id,
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
        output=_turn_output_from_record(record.get("output")),
        status_message=str(record.get("status_message") or ""),
        created_by_subject_id=created_by_subject_id_from_record(record),
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


def _turn_output_to_record(output: gestalt.AgentTurnOutput) -> dict[str, Any]:
    if output.text is not None:
        return {"text": str(output.text or "")}
    structured = output.structured
    if structured is None:
        raise ValueError("stored turn output must include text or structured")
    record: dict[str, Any] = {"structured": {"text": str(structured.text or "")}}
    if structured.value is not None:
        record["structured"]["value"] = copy.deepcopy(structured.value)
    return record


def _turn_output_from_record(value: Any) -> gestalt.AgentTurnOutput | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise TypeError(
            f"Stored turn output must be a mapping, got {type(value).__name__}"
        )
    if "text" in value:
        return gestalt.AgentTurnOutput(text=str(value.get("text") or ""))
    structured = value.get("structured")
    if isinstance(structured, dict):
        structured_value = structured.get("value")
        return gestalt.AgentTurnOutput(
            structured=gestalt.AgentTurnStructuredOutput(
                text=str(structured.get("text") or ""),
                value=copy.deepcopy(structured_value)
                if isinstance(structured_value, dict)
                else None,
            )
        )
    return None


def _coerce_messages(raw_value: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_value, list):
        return []
    messages: list[dict[str, Any]] = []
    for item in raw_value:
        if isinstance(item, dict):
            messages.append(copy.deepcopy(item))
    return messages


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


def session_visibility_for_create(metadata: dict[str, Any], created_by_subject_id: str) -> str:
    if _is_slack_agent_session_metadata(metadata) and is_managed_subject_id(
        created_by_subject_id
    ):
        return SESSION_VISIBILITY_COMPANY
    return SESSION_VISIBILITY_PRIVATE


def session_readable_by(session: StoredSession, subject_id: str) -> bool:
    subject_id = subject_id.strip()
    if not subject_id:
        return False
    if session.created_by_subject_id.strip() == subject_id:
        return True
    return session.visibility == SESSION_VISIBILITY_COMPANY


def session_writable_by(session: StoredSession, subject_id: str) -> bool:
    subject_id = subject_id.strip()
    if not subject_id:
        return False
    return session.created_by_subject_id.strip() == subject_id


def _session_visibility_from_record(record: dict[str, Any]) -> str:
    visibility = str(record.get("visibility") or "").strip()
    if visibility in {SESSION_VISIBILITY_PRIVATE, SESSION_VISIBILITY_COMPANY}:
        return visibility
    metadata = _coerce_optional_dict(record.get("metadata")) or {}
    return session_visibility_for_create(metadata, created_by_subject_id_from_record(record))


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


