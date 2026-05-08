from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

import gestalt
import grpc


class ProviderRequestError(ValueError):
    def __init__(self, code: grpc.StatusCode, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class CreateSessionInput:
    session_id: str
    requested_model: str
    metadata: dict[str, Any]
    prepared_workspace: dict[str, str] | None
    idempotency_key: str
    session_start: Any | None
    client_ref: str
    created_by: dict[str, str]

    @classmethod
    def from_proto(cls, request: Any) -> CreateSessionInput:
        session_id = _text(getattr(request, "session_id", ""))
        if not session_id:
            raise _invalid("session_id is required")
        prepared_workspace = None
        try:
            if gestalt.has_field(request, "prepared_workspace"):
                prepared_workspace = _prepared_workspace_to_dict(request.prepared_workspace)
        except ValueError as exc:
            raise _invalid(str(exc)) from exc
        return cls(
            session_id=session_id,
            requested_model=_text(getattr(request, "model", "")),
            metadata=gestalt.struct_to_dict(getattr(request, "metadata", None)),
            prepared_workspace=prepared_workspace,
            idempotency_key=_text(getattr(request, "idempotency_key", "")),
            session_start=request.session_start if gestalt.has_field(request, "session_start") else None,
            client_ref=_text(getattr(request, "client_ref", "")),
            created_by=cast(dict[str, str], gestalt.agent_actor_to_dict(request.created_by)),
        )

    @property
    def has_session_start_hooks(self) -> bool:
        if self.session_start is None:
            return False
        return len(list(getattr(self.session_start, "hooks", []) or [])) > 0


@dataclass(frozen=True, slots=True)
class UpdateSessionInput:
    session_id: str
    client_ref: str
    state: int
    metadata: dict[str, Any] | None

    @classmethod
    def from_proto(cls, request: Any) -> UpdateSessionInput:
        return cls(
            session_id=_text(getattr(request, "session_id", "")),
            client_ref=_text(getattr(request, "client_ref", "")),
            state=int(getattr(request, "state", 0) or 0),
            metadata=gestalt.struct_to_dict(request.metadata) if gestalt.has_field(request, "metadata") else None,
        )


@dataclass(frozen=True, slots=True)
class ListSessionsInput:
    session_ids: list[str]
    subject_id: str
    state: int
    limit: int
    summary_only: bool

    @classmethod
    def from_proto(cls, request: Any) -> ListSessionsInput:
        limit = int(getattr(request, "limit", 0) or 0)
        if limit < 0:
            raise _invalid("limit must be non-negative")
        return cls(
            session_ids=[_text(value) for value in getattr(request, "session_ids", [])],
            subject_id=_subject_id(request),
            state=int(getattr(request, "state", 0) or 0),
            limit=limit,
            summary_only=bool(getattr(request, "summary_only", False)),
        )


@dataclass(frozen=True, slots=True)
class CreateTurnInput:
    turn_id: str
    session_id: str
    idempotency_key: str
    requested_model: str
    messages: list[dict[str, Any]]
    created_by: dict[str, str]
    execution_ref: str
    run_grant: str

    @classmethod
    def from_proto(cls, request: Any) -> CreateTurnInput:
        validate_create_turn_contract(request)
        return cls(
            turn_id=_text(getattr(request, "turn_id", "")),
            session_id=_text(getattr(request, "session_id", "")),
            idempotency_key=_text(getattr(request, "idempotency_key", "")),
            requested_model=_text(getattr(request, "model", "")),
            messages=gestalt.agent_messages_to_dicts(getattr(request, "messages", [])),
            created_by=cast(dict[str, str], gestalt.agent_actor_to_dict(request.created_by)),
            execution_ref=_text(getattr(request, "execution_ref", "")),
            run_grant=_text(getattr(request, "run_grant", "")),
        )


@dataclass(frozen=True, slots=True)
class ListTurnsInput:
    session_id: str
    turn_ids: list[str]
    subject_id: str
    status: int
    limit: int
    summary_only: bool

    @classmethod
    def from_proto(cls, request: Any) -> ListTurnsInput:
        limit = int(getattr(request, "limit", 0) or 0)
        if limit < 0:
            raise _invalid("limit must be non-negative")
        return cls(
            session_id=_text(getattr(request, "session_id", "")),
            turn_ids=[_text(value) for value in getattr(request, "turn_ids", [])],
            subject_id=_subject_id(request),
            status=int(getattr(request, "status", 0) or 0),
            limit=limit,
            summary_only=bool(getattr(request, "summary_only", False)),
        )


@dataclass(frozen=True, slots=True)
class CancelTurnInput:
    turn_id: str
    reason: str

    @classmethod
    def from_proto(cls, request: Any) -> CancelTurnInput:
        return cls(turn_id=_text(getattr(request, "turn_id", "")), reason=_text(getattr(request, "reason", "")))


@dataclass(frozen=True, slots=True)
class ListTurnEventsInput:
    turn_id: str
    after_seq: int
    limit: int

    @classmethod
    def from_proto(cls, request: Any) -> ListTurnEventsInput:
        return cls(
            turn_id=_text(getattr(request, "turn_id", "")),
            after_seq=int(getattr(request, "after_seq", 0) or 0),
            limit=int(getattr(request, "limit", 0) or 0),
        )


def validate_create_turn_contract(request: Any) -> None:
    if int(getattr(request, "tool_source", 0) or 0) != gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG:
        raise _invalid("agent/claude requires toolSource mcp_catalog")
    if not _text(getattr(request, "run_grant", "")):
        raise _invalid("run_grant is required")
    if len(list(getattr(request, "tools", []))) > 0:
        raise _invalid("resolved tools are not supported; use tool_refs with mcp_catalog")
    if gestalt.struct_to_dict(getattr(request, "response_schema", None)):
        raise _invalid("response_schema is not supported by agent/claude")
    if gestalt.struct_to_dict(getattr(request, "model_options", None)):
        raise _invalid("model_options are not supported by agent/claude")
    validate_tool_refs(list(getattr(request, "tool_refs", [])))


def validate_tool_refs(tool_refs: list[Any]) -> None:
    for index, ref in enumerate(tool_refs, start=1):
        _validate_tool_ref(gestalt.agent_tool_ref_to_dict(ref), index)


def prepared_workspace_cwd(value: dict[str, str] | None) -> str:
    if not value:
        return ""
    return _text(value.get("cwd"))


def _validate_tool_ref(tool_ref: dict[str, Any], index: int) -> None:
    plugin = _text(tool_ref.get("plugin"))
    system = _text(tool_ref.get("system"))
    operation = _text(tool_ref.get("operation"))
    connection = _text(tool_ref.get("connection"))
    instance = _text(tool_ref.get("instance"))
    title = _text(tool_ref.get("title"))
    description = _text(tool_ref.get("description"))

    if "*" in {system, operation, connection, instance}:
        raise _invalid("wildcard tool_refs are not supported")
    if plugin == "*":
        if any([system, operation, connection, instance, title, description]):
            raise _invalid(
                f"tool_refs[{index}] global search ref cannot include operation, connection, instance, "
                "title, or description"
            )
        return
    if system:
        if plugin:
            raise _invalid(f"tool_refs[{index}] must set exactly one of plugin or system")
        if system != "workflow":
            raise _invalid(f"tool_refs[{index}].system {system!r} is not supported")
        if not operation:
            raise _invalid(f"tool_refs[{index}].operation is required for system tool refs")
        if any([connection, instance, title, description]):
            raise _invalid(f"tool_refs[{index}] system refs cannot include connection, instance, title, or description")
        return
    if not plugin:
        raise _invalid(f"tool_refs[{index}].plugin is required")


def _prepared_workspace_to_dict(value: Any | None) -> dict[str, str] | None:
    if value is None:
        return None
    workspace = gestalt.prepared_workspace_to_dict(value)
    root = _text(workspace.get("root"))
    cwd = _text(workspace.get("cwd"))
    if not root and not cwd:
        return None
    if not root or not cwd:
        raise ValueError("prepared_workspace root and cwd are required")
    return {"root": root, "cwd": cwd}


def _subject_id(request: Any) -> str:
    subject = gestalt.agent_subject_context_to_dict(getattr(request, "subject", None))
    return _text(subject.get("subject_id"))


def _invalid(message: str) -> ProviderRequestError:
    return ProviderRequestError(grpc.StatusCode.INVALID_ARGUMENT, message)


def _text(value: Any) -> str:
    return str(value or "").strip()
