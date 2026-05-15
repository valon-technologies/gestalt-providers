from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

import gestalt

from .claude_runner import ClaudeTurnProfile
from .config import ClaudeAgentConfig
from .session_start import validate_session_start_user_metadata
from .store import IndexedDBRunStore
from .store_records import StoredSession


@dataclass(frozen=True, slots=True)
class ToolSourceModes:
    none: int
    mcp_catalog: int


@dataclass(frozen=True, slots=True)
class SessionCreateRequest:
    session_id: str
    idempotency_key: str
    model: str
    client_ref: str
    metadata: dict[str, Any]
    prepared_workspace: dict[str, str] | None
    created_by: dict[str, str]
    session_start: Any | None

    @property
    def has_session_start_hooks(self) -> bool:
        return self.session_start is not None and bool(list(getattr(self.session_start, "hooks", []) or []))

    def with_metadata(self, metadata: dict[str, Any]) -> SessionCreateRequest:
        return replace(self, metadata=metadata)


@dataclass(frozen=True, slots=True)
class TurnCreateRequest:
    turn_id: str
    session_id: str
    idempotency_key: str
    model: str
    messages: list[dict[str, Any]]
    created_by: dict[str, str]
    execution_ref: str
    turn_profile: ClaudeTurnProfile


@dataclass(frozen=True, slots=True)
class ToolRefRequest:
    index: int
    plugin: str
    system: str
    operation: str
    connection: str
    instance: str
    title: str
    description: str


def session_create_request_from_provider_request(
    request: gestalt.CreateAgentProviderSessionRequest, *, config: ClaudeAgentConfig
) -> SessionCreateRequest:
    session_id = _required_text(getattr(request, "session_id", ""), field_name="session_id")
    model = config.resolve_model(_text(getattr(request, "model", "")))
    metadata = dict(getattr(request, "metadata", {}) or {})
    validate_session_start_user_metadata(metadata)
    prepared_workspace = _prepared_workspace_from_request(request)
    return SessionCreateRequest(
        session_id=session_id,
        idempotency_key=_text(getattr(request, "idempotency_key", "")),
        model=model,
        client_ref=_text(getattr(request, "client_ref", "")),
        metadata=metadata,
        prepared_workspace=prepared_workspace,
        created_by=gestalt.agent_actor_to_dict(getattr(request, "created_by", None)),
        session_start=getattr(request, "session_start", None),
    )


def turn_create_request_from_provider_request(
    request: gestalt.CreateAgentProviderTurnRequest,
    *,
    config: ClaudeAgentConfig,
    session: StoredSession,
    tool_source_modes: ToolSourceModes,
) -> TurnCreateRequest:
    if not getattr(request, "messages", None):
        raise ValueError("messages must contain at least one entry")

    model = config.resolve_model(_text(getattr(request, "model", "")) or session.model)
    messages = gestalt.agent_messages_to_dicts(request.messages)
    response_schema = _response_schema_from_request(request)
    tool_source = _tool_source_from_request(request)
    if tool_source == tool_source_modes.none:
        if response_schema is None:
            raise ValueError("response_schema is required with toolSource none")
        turn_profile = ClaudeTurnProfile.structured_output(response_schema=response_schema)
    else:
        claude_code_options = config.claude_code.resolve_turn_options(session.metadata)
        turn_profile = ClaudeTurnProfile.catalog(
            run_grant=_text(getattr(request, "run_grant", "")),
            claude_code_options=claude_code_options,
            cwd=prepared_workspace_cwd(session.prepared_workspace),
        )

    return TurnCreateRequest(
        turn_id=_text(getattr(request, "turn_id", "")),
        session_id=_text(getattr(request, "session_id", "")),
        idempotency_key=_text(getattr(request, "idempotency_key", "")),
        model=model,
        messages=messages,
        created_by=gestalt.agent_actor_to_dict(getattr(request, "created_by", None)),
        execution_ref=_text(getattr(request, "execution_ref", "")),
        turn_profile=turn_profile,
    )


def validate_turn_contract(
    request: gestalt.CreateAgentProviderTurnRequest, *, tool_source_modes: ToolSourceModes
) -> None:
    tool_source = _tool_source_from_request(request)
    if tool_source not in {tool_source_modes.mcp_catalog, tool_source_modes.none}:
        raise ValueError("agent/claude requires toolSource none or mcp_catalog")
    if tool_source == tool_source_modes.mcp_catalog and not _text(getattr(request, "run_grant", "")):
        raise ValueError("run_grant is required")
    if getattr(request, "tools", None):
        raise ValueError("resolved tools are not supported by agent/claude")
    if tool_source == tool_source_modes.none and list(getattr(request, "tool_refs", []) or []):
        raise ValueError("tool_refs are not supported with toolSource none")
    if tool_source == tool_source_modes.none and not _has_response_schema(request):
        raise ValueError("response_schema is required with toolSource none")
    if tool_source == tool_source_modes.mcp_catalog and _has_response_schema(request):
        raise ValueError("response_schema is not supported with toolSource mcp_catalog")
    _validate_response_schema(_response_schema_from_request(request))
    if dict(getattr(request, "model_options", {}) or {}):
        raise ValueError("model_options are not supported by agent/claude")
    if tool_source == tool_source_modes.mcp_catalog:
        _validate_tool_refs(list(getattr(request, "tool_refs", []) or []))


def existing_session_for_create(
    store: IndexedDBRunStore, *, session_id: str, idempotency_key: str
) -> StoredSession | None:
    existing = store.get_session(session_id)
    if existing is not None:
        return existing
    if not idempotency_key:
        return None
    return store.get_session_by_idempotency_key(idempotency_key)


def prepared_workspace_cwd(value: dict[str, str] | None) -> str:
    if not value:
        return ""
    return _text(value.get("cwd"))


def request_session_id(request: Any) -> str:
    return _text(getattr(request, "session_id", ""))


def subject_id(subject: Any) -> str:
    return _text(getattr(subject, "subject_id", ""))


def _prepared_workspace_from_request(request: gestalt.CreateAgentProviderSessionRequest) -> dict[str, str] | None:
    prepared_workspace = gestalt.prepared_workspace_to_dict(getattr(request, "prepared_workspace", None))
    if not prepared_workspace:
        return None
    if not prepared_workspace.get("root") or not prepared_workspace.get("cwd"):
        raise ValueError("prepared_workspace root and cwd are required")
    return prepared_workspace


def _response_schema_from_request(request: Any) -> dict[str, Any] | None:
    value = getattr(request, "response_schema", None)
    if value is None:
        return None
    return dict(value)


def _has_response_schema(request: Any) -> bool:
    has_field = getattr(request, "HasField", None)
    if callable(has_field):
        return bool(has_field("response_schema"))
    return _response_schema_from_request(request) is not None


def _validate_response_schema(schema: dict[str, Any] | None) -> None:
    if schema is None:
        return
    if not schema:
        raise ValueError("response_schema must be a non-empty JSON schema object with type 'object'")
    if _text(schema.get("type")) != "object":
        raise ValueError("response_schema.type must be 'object'")


def _validate_tool_refs(tool_refs: list[Any]) -> None:
    for index, ref in enumerate(tool_refs, start=1):
        tool_ref = _tool_ref_request(index=index, ref=ref)
        if "*" in {tool_ref.system, tool_ref.operation, tool_ref.connection, tool_ref.instance}:
            raise ValueError("wildcard tool_refs are not supported")
        if tool_ref.plugin == "*":
            _validate_global_tool_ref(tool_ref)
            return
        if tool_ref.system:
            _validate_system_tool_ref(tool_ref)
            return
        if not tool_ref.plugin:
            raise ValueError(f"tool_refs[{index}].plugin is required")


def _tool_ref_request(*, index: int, ref: Any) -> ToolRefRequest:
    return ToolRefRequest(
        index=index,
        plugin=_text(getattr(ref, "plugin", "")),
        system=_text(getattr(ref, "system", "")),
        operation=_text(getattr(ref, "operation", "")),
        connection=_text(getattr(ref, "connection", "")),
        instance=_text(getattr(ref, "instance", "")),
        title=_text(getattr(ref, "title", "")),
        description=_text(getattr(ref, "description", "")),
    )


def _validate_global_tool_ref(ref: ToolRefRequest) -> None:
    if any([ref.system, ref.operation, ref.connection, ref.instance, ref.title, ref.description]):
        raise ValueError(
            f"tool_refs[{ref.index}] global search ref cannot include operation, connection, instance, "
            "title, or description"
        )


def _validate_system_tool_ref(ref: ToolRefRequest) -> None:
    if ref.plugin:
        raise ValueError(f"tool_refs[{ref.index}] must set exactly one of plugin or system")
    if ref.system != "workflow":
        raise ValueError(f"tool_refs[{ref.index}].system {ref.system!r} is not supported")
    if not ref.operation:
        raise ValueError(f"tool_refs[{ref.index}].operation is required for system tool refs")
    if any([ref.connection, ref.instance, ref.title, ref.description]):
        raise ValueError(
            f"tool_refs[{ref.index}] system refs cannot include connection, instance, title, or description"
        )


def _tool_source_from_request(request: Any) -> int:
    return int(getattr(request, "tool_source", 0) or 0)


def _required_text(value: Any, *, field_name: str) -> str:
    text = _text(value)
    if not text:
        raise ValueError(f"{field_name} is required")
    return text


def _text(value: Any) -> str:
    return str(value or "").strip()
