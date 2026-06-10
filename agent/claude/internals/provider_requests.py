from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

import gestalt
from jsonschema import exceptions as jsonschema_exceptions
from jsonschema.validators import validator_for

from .config import ClaudeAgentConfig
from .session_start import validate_session_start_user_metadata
from .store import IndexedDBRunStore
from .store_records import StoredSession


@dataclass(frozen=True, slots=True)
class ToolSourceModes:
    none: int
    catalog: int


@dataclass(frozen=True, slots=True)
class SessionCreateRequest:
    session_id: str
    idempotency_key: str
    model: str
    client_ref: str
    metadata: dict[str, Any]
    prepared_workspace: dict[str, str] | None
    tool_source: int
    tool_refs: list[gestalt.AgentToolRef]
    listed_tools: list[gestalt.ListedAgentTool]
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
    timeout_seconds: float = 0.0


def session_create_request_from_provider_request(
    request: gestalt.CreateAgentProviderSessionRequest, *, config: ClaudeAgentConfig, tool_source_modes: ToolSourceModes
) -> SessionCreateRequest:
    session_id = request.session_id.strip()
    if not session_id:
        raise ValueError("session_id is required")
    model = config.resolve_model(request.model.strip())
    metadata = dict(request.metadata or {})
    validate_session_start_user_metadata(metadata)
    prepared_workspace = _prepared_workspace_from_request(request)
    tool_source, tool_refs, listed_tools = session_tool_scope_from_config(
        request.tools, tool_source_modes=tool_source_modes
    )
    return SessionCreateRequest(
        session_id=session_id,
        idempotency_key=request.idempotency_key.strip(),
        model=model,
        client_ref=request.client_ref.strip(),
        metadata=metadata,
        prepared_workspace=prepared_workspace,
        tool_source=tool_source,
        tool_refs=tool_refs,
        listed_tools=listed_tools,
        created_by_subject_id=request.created_by_subject_id.strip(),
        session_start=request.session_start,
    )


def turn_create_request_from_provider_request(
    request: gestalt.CreateAgentProviderTurnRequest,
    *,
    config: ClaudeAgentConfig,
    session: StoredSession,
    schema: dict[str, Any] | None,
) -> TurnCreateRequest:
    request_messages = list(request.messages)
    if not request_messages:
        raise ValueError("messages must contain at least one entry")

    model = config.resolve_model(request.model.strip() or session.model)
    messages = gestalt.agent_messages_to_dicts(request_messages)

    return TurnCreateRequest(
        turn_id=request.turn_id.strip(),
        session_id=request.session_id.strip(),
        idempotency_key=request.idempotency_key.strip(),
        model=model,
        messages=messages,
        created_by_subject_id=request.created_by_subject_id.strip(),
        execution_ref=request.execution_ref.strip(),
        timeout_seconds=_timeout_seconds_from_request(request),
    )


def validate_turn_contract(
    request: gestalt.CreateAgentProviderTurnRequest, *, session: StoredSession, tool_source_modes: ToolSourceModes
) -> dict[str, Any] | None:
    tool_source = session.tool_source or tool_source_modes.none
    tool_refs = list(session.tool_refs)
    if tool_source not in {tool_source_modes.catalog, tool_source_modes.none}:
        raise ValueError("agent/claude requires toolSource none or catalog")
    if tool_source == tool_source_modes.catalog and getattr(request, "context", None) is None:
        raise ValueError("request context is required")
    if tool_source == tool_source_modes.none and tool_refs:
        raise ValueError("tool_refs are not supported with toolSource none")
    schema = _schema_from_output(request.output)
    _validate_schema(schema)
    if dict(request.model_options or {}):
        raise ValueError("model_options are not supported by agent/claude")
    return schema


def session_tool_scope_from_config(
    tools: gestalt.AgentToolConfig | None, *, tool_source_modes: ToolSourceModes
) -> tuple[int, list[gestalt.AgentToolRef], list[gestalt.ListedAgentTool]]:
    if tools is None:
        return tool_source_modes.none, [], []
    if isinstance(tools, gestalt.AgentNoTools):
        return tool_source_modes.none, [], []
    if isinstance(tools, gestalt.AgentCatalogToolConfig):
        return tool_source_modes.catalog, list(tools.refs), list(tools.tools)
    raise ValueError("agent session tools must be none or catalog")


def existing_session_for_create(
    store: IndexedDBRunStore, *, session_id: str, idempotency_key: str
) -> StoredSession | None:
    existing = store.get_session(session_id)
    if existing is not None:
        return existing
    if not idempotency_key:
        return None
    return store.get_session_by_idempotency_key(idempotency_key)


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


def _timeout_seconds_from_request(request: gestalt.CreateAgentProviderTurnRequest) -> float:
    if request.timeout_seconds < 0:
        raise ValueError("timeout_seconds must be non-negative")
    return float(request.timeout_seconds)
