from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

import gestalt
from jsonschema import exceptions as jsonschema_exceptions
from jsonschema.validators import validator_for

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
    created_by_subject_id: str
    session_start: gestalt.AgentSessionStartConfig | None

    @property
    def has_session_start_hooks(self) -> bool:
        return self.session_start is not None and bool(list(self.session_start.hooks))

    def with_metadata(self, metadata: dict[str, Any]) -> SessionCreateRequest:
        return replace(self, metadata=metadata)


@dataclass(frozen=True, slots=True)
class TurnCreateRequest:
    turn_id: str
    session_id: str
    idempotency_key: str
    model: str
    messages: list[dict[str, Any]]
    created_by_subject_id: str
    execution_ref: str
    turn_profile: ClaudeTurnProfile
    timeout_seconds: float = 0.0


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
    session_id = request.session_id.strip()
    if not session_id:
        raise ValueError("session_id is required")
    model = config.resolve_model(request.model.strip())
    metadata = dict(request.metadata or {})
    validate_session_start_user_metadata(metadata)
    prepared_workspace = _prepared_workspace_from_request(request)
    return SessionCreateRequest(
        session_id=session_id,
        idempotency_key=request.idempotency_key.strip(),
        model=model,
        client_ref=request.client_ref.strip(),
        metadata=metadata,
        prepared_workspace=prepared_workspace,
        created_by_subject_id=request.created_by_subject_id.strip(),
        session_start=request.session_start,
    )


def turn_create_request_from_provider_request(
    request: gestalt.CreateAgentProviderTurnRequest,
    *,
    config: ClaudeAgentConfig,
    session: StoredSession,
    tool_source_modes: ToolSourceModes,
    schema: dict[str, Any] | None,
) -> TurnCreateRequest:
    request_messages = list(request.messages)
    if not request_messages:
        raise ValueError("messages must contain at least one entry")

    model = config.resolve_model(request.model.strip() or session.model)
    messages = gestalt.agent_messages_to_dicts(request_messages)
    if request.tool_source == tool_source_modes.none:
        turn_profile = ClaudeTurnProfile.direct(schema=schema)
    else:
        claude_code_options = config.claude_code.resolve_turn_options(session.metadata)
        turn_profile = ClaudeTurnProfile.catalog(
            run_grant=request.run_grant.strip(),
            schema=schema,
            claude_code_options=claude_code_options,
            cwd=prepared_workspace_cwd(session.prepared_workspace),
        )

    return TurnCreateRequest(
        turn_id=request.turn_id.strip(),
        session_id=request.session_id.strip(),
        idempotency_key=request.idempotency_key.strip(),
        model=model,
        messages=messages,
        created_by_subject_id=request.created_by_subject_id.strip(),
        execution_ref=request.execution_ref.strip(),
        turn_profile=turn_profile,
        timeout_seconds=_timeout_seconds_from_request(request),
    )


def validate_turn_contract(
    request: gestalt.CreateAgentProviderTurnRequest, *, tool_source_modes: ToolSourceModes
) -> dict[str, Any] | None:
    if request.tool_source not in {tool_source_modes.mcp_catalog, tool_source_modes.none}:
        raise ValueError("agent/claude requires toolSource none or mcp_catalog")
    if request.tool_source == tool_source_modes.mcp_catalog and not request.run_grant.strip():
        raise ValueError("run_grant is required")
    if list(request.tools):
        raise ValueError("resolved tools are not supported by agent/claude")
    if request.tool_source == tool_source_modes.none and list(request.tool_refs):
        raise ValueError("tool_refs are not supported with toolSource none")
    schema = _schema_from_output(request.output)
    _validate_schema(schema)
    if dict(request.model_options or {}):
        raise ValueError("model_options are not supported by agent/claude")
    if request.tool_source == tool_source_modes.mcp_catalog:
        _validate_tool_refs(list(request.tool_refs))
    return schema


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
    return value.get("cwd", "").strip()


def _prepared_workspace_from_request(request: gestalt.CreateAgentProviderSessionRequest) -> dict[str, str] | None:
    prepared_workspace = gestalt.prepared_workspace_to_dict(request.prepared_workspace)
    if not prepared_workspace:
        return None
    if not prepared_workspace.get("root") or not prepared_workspace.get("cwd"):
        raise ValueError("prepared_workspace root and cwd are required")
    return prepared_workspace


def _schema_from_output(output: gestalt.AgentOutput | None) -> dict[str, Any] | None:
    if output is None:
        raise ValueError("output is required")
    text_set = output.text is not None
    structured_set = output.structured is not None
    if text_set == structured_set:
        raise ValueError("exactly one of output.text or output.structured is required")
    if structured_set:
        assert output.structured is not None
        return dict(output.structured.schema)
    return None


def _validate_schema(schema: dict[str, Any] | None) -> None:
    if schema is None:
        return
    if not schema:
        raise ValueError("output.structured.schema must be a non-empty JSON schema object with type 'object'")
    if str(schema.get("type") or "").strip() != "object":
        raise ValueError("output.structured.schema.type must be 'object'")
    validator_cls = validator_for(schema)
    try:
        validator_cls.check_schema(schema)
    except jsonschema_exceptions.SchemaError as exc:
        raise ValueError(f"invalid output.structured.schema: {exc.message}") from exc


def _validate_tool_refs(tool_refs: list[gestalt.AgentToolRef]) -> None:
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


def _tool_ref_request(*, index: int, ref: gestalt.AgentToolRef) -> ToolRefRequest:
    return ToolRefRequest(
        index=index,
        plugin=ref.app.strip(),
        system=ref.system.strip(),
        operation=ref.operation.strip(),
        connection=ref.connection.strip(),
        instance=ref.instance.strip(),
        title=ref.title.strip(),
        description=ref.description.strip(),
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


def _timeout_seconds_from_request(request: gestalt.CreateAgentProviderTurnRequest) -> float:
    if request.timeout_seconds < 0:
        raise ValueError("timeout_seconds must be non-negative")
    return float(request.timeout_seconds)
