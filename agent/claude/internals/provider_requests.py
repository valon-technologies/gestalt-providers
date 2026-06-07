from __future__ import annotations

from dataclasses import dataclass, replace
import re
from typing import Any

import gestalt
from jsonschema import exceptions as jsonschema_exceptions
from jsonschema.validators import validator_for

from .claude_runner import ClaudeTurnProfile
from .config import ClaudeAgentConfig
from .session_start import validate_session_start_user_metadata
from .store import IndexedDBRunStore
from .store_records import StoredSession

_UNSAFE_TOOL_NAME = re.compile(r"[*?,\s\x00-\x1f\x7f]")
_CREDENTIAL_MODES = {"unspecified", "none", "subject"}
MAX_LISTED_TOOLS = 10_000


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
    credential_mode: str
    run_as_set: bool


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
    tool_source_modes: ToolSourceModes,
    tool_source: int,
    schema: dict[str, Any] | None,
) -> TurnCreateRequest:
    request_messages = list(request.messages)
    if not request_messages:
        raise ValueError("messages must contain at least one entry")

    model = config.resolve_model(request.model.strip() or session.model)
    messages = gestalt.agent_messages_to_dicts(request_messages)
    cwd = prepared_workspace_cwd(session.prepared_workspace)
    if tool_source == tool_source_modes.none:
        turn_profile = ClaudeTurnProfile.direct(schema=schema, cwd=cwd)
    else:
        request_context = getattr(request, "context", None)
        if request_context is None:
            raise ValueError("request context is required")
        claude_code_options = config.claude_code.resolve_turn_options(session.metadata)
        turn_profile = ClaudeTurnProfile.catalog(
            request_context=request_context,
            listed_tools=list(session.listed_tools),
            schema=schema,
            claude_code_options=claude_code_options,
            cwd=cwd,
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
    request: gestalt.CreateAgentProviderTurnRequest, *, session: StoredSession, tool_source_modes: ToolSourceModes
) -> tuple[dict[str, Any] | None, int]:
    tool_source = session.tool_source or tool_source_modes.none
    tool_refs = list(session.tool_refs)
    if tool_source not in {tool_source_modes.catalog, tool_source_modes.none}:
        raise ValueError("agent/claude requires toolSource none or catalog")
    if tool_source == tool_source_modes.catalog and getattr(request, "context", None) is None:
        raise ValueError("request context is required")
    if list(request.tools):
        raise ValueError("resolved tools are not supported by agent/claude")
    if tool_source == tool_source_modes.none and tool_refs:
        raise ValueError("tool_refs are not supported with toolSource none")
    schema = _schema_from_output(request.output)
    _validate_schema(schema)
    if dict(request.model_options or {}):
        raise ValueError("model_options are not supported by agent/claude")
    if tool_source == tool_source_modes.catalog:
        _validate_tool_refs(tool_refs)
        _validate_listed_tools(list(session.listed_tools))
    return schema, tool_source


def session_tool_scope_from_config(
    tools: gestalt.AgentToolConfig | None, *, tool_source_modes: ToolSourceModes
) -> tuple[int, list[gestalt.AgentToolRef], list[gestalt.ListedAgentTool]]:
    if tools is None:
        return tool_source_modes.none, [], []
    if isinstance(tools, gestalt.AgentNoTools):
        return tool_source_modes.none, [], []
    if isinstance(tools, gestalt.AgentCatalogToolConfig):
        refs = list(tools.refs)
        listed_tools = list(tools.tools)
        _validate_tool_refs(refs)
        _validate_listed_tools(listed_tools)
        _validate_listed_tools_covered_by_refs(listed_tools, refs)
        return tool_source_modes.catalog, refs, listed_tools
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
        if tool_ref.credential_mode and tool_ref.credential_mode not in _CREDENTIAL_MODES:
            raise ValueError(f"tool_refs[{index}].credential_mode is invalid")
        if tool_ref.run_as_set:
            raise ValueError(f"tool_refs[{index}].run_as is not supported")
        if tool_ref.plugin == "*":
            _validate_global_tool_ref(tool_ref)
            return
        if tool_ref.system:
            _validate_system_tool_ref(tool_ref)
            return
        if not tool_ref.plugin:
            raise ValueError(f"tool_refs[{index}].plugin is required")


def _validate_listed_tools(tools: list[gestalt.ListedAgentTool]) -> None:
    if not tools:
        raise ValueError("tools.catalog.tools are required")
    seen_names: set[str] = set()
    for index, tool in enumerate(tools, start=1):
        mcp_name = str(tool.mcp_name or "").strip()
        if not mcp_name:
            raise ValueError(f"tools.catalog.tools[{index}].mcp_name is required")
        if _UNSAFE_TOOL_NAME.search(mcp_name):
            raise ValueError(f"tools.catalog.tools[{index}].mcp_name is unsafe")
        if mcp_name in seen_names:
            raise ValueError(f"tools.catalog.tools contains duplicate mcp_name {mcp_name!r}")
        seen_names.add(mcp_name)
        if len(seen_names) > MAX_LISTED_TOOLS:
            raise ValueError(f"tools.catalog.tools contains more than {MAX_LISTED_TOOLS} tools")
        ref = tool.ref
        if ref is None or not ref.app.strip() or not ref.operation.strip() or ref.system.strip():
            raise ValueError(f"tools.catalog.tools[{index}] must target an app operation")
        if "*" in {ref.app.strip(), ref.operation.strip(), ref.connection.strip(), ref.instance.strip()}:
            raise ValueError(f"tools.catalog.tools[{index}] must target a concrete app operation")
        credential_mode = ref.credential_mode.strip()
        if credential_mode and credential_mode not in _CREDENTIAL_MODES:
            raise ValueError(f"tools.catalog.tools[{index}].ref.credential_mode is invalid")
        if ref.run_as is not None:
            raise ValueError(f"tools.catalog.tools[{index}].ref.run_as is not supported")


def _validate_listed_tools_covered_by_refs(
    listed_tools: list[gestalt.ListedAgentTool], tool_refs: list[gestalt.AgentToolRef]
) -> None:
    for index, tool in enumerate(listed_tools, start=1):
        ref = tool.ref
        if ref is None:
            raise ValueError(f"tools.catalog.tools[{index}] must target an app operation")
        if not any(_tool_ref_covers_listed_ref(selector, ref) for selector in tool_refs):
            raise ValueError(f"tools.catalog.tools[{index}] is not covered by tools.catalog.refs")


def _tool_ref_covers_listed_ref(selector: gestalt.AgentToolRef, listed: gestalt.AgentToolRef) -> bool:
    if selector.system.strip():
        return False
    selector_app = selector.app.strip()
    if selector_app and selector_app != "*" and selector_app != listed.app.strip():
        return False
    if selector.operation.strip() and selector.operation.strip() != listed.operation.strip():
        return False
    if selector.connection.strip() and selector.connection.strip() != listed.connection.strip():
        return False
    if selector.instance.strip() and selector.instance.strip() != listed.instance.strip():
        return False
    if selector.credential_mode.strip() and selector.credential_mode.strip() != listed.credential_mode.strip():
        return False
    return bool(selector_app)


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
        credential_mode=ref.credential_mode.strip(),
        run_as_set=ref.run_as is not None,
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
