from __future__ import annotations

import logging
import os
import threading
from typing import Any

import gestalt

from internals import ClaudeAgentConfig, ClaudeSDKRunner, ClaudeTurnProfile, IndexedDBRunStore
from internals.claude_runner import ClaudeExecutionCanceled, ClaudeExecutionError
from internals.session_start import (
    prepend_session_start_context,
    run_session_start_hooks,
    validate_session_start_user_metadata,
)
from internals.store import StoreConflictError, StoreUnavailableError, StoredSession, StoredTurn, StoredTurnEvent

logger = logging.getLogger(__name__)


def _agent_tool_source_mode_none() -> int:
    value = getattr(gestalt, "AGENT_TOOL_SOURCE_MODE_NONE", None)
    if value is not None:
        return int(value)
    return int(gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG) + 1


AGENT_TOOL_SOURCE_MODE_NONE = _agent_tool_source_mode_none()
AGENT_TOOL_SOURCE_MODE_MCP_CATALOG = int(gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG)


class ClaudeCodeAgentProvider(
    gestalt.AgentProvider, gestalt.MetadataProvider, gestalt.WarningsProvider, gestalt.Closer
):
    def __init__(self) -> None:
        self._name = "claude"
        self._warnings: list[str] = ["provider has not been configured"]
        self._config: ClaudeAgentConfig | None = None
        self._store: IndexedDBRunStore | None = None
        self._runner: ClaudeSDKRunner | None = None
        self._session_start_lock = threading.Lock()

    def configure(self, name: str, config: dict[str, Any]) -> None:
        self._name = name.strip() or "claude"
        resolved = ClaudeAgentConfig.from_dict(name=self._name, raw_config=config)
        self.close()
        self._config = resolved
        self._store = IndexedDBRunStore(run_store=resolved.run_store, idempotency_store=resolved.idempotency_store)
        self._runner = ClaudeSDKRunner(resolved)
        self._warnings = self._build_warnings(resolved)

    def metadata(self) -> gestalt.ProviderMetadata:
        return gestalt.ProviderMetadata(
            kind=gestalt.ProviderKind.AGENT,
            name=self._name,
            display_name="Claude Agent SDK",
            description="Runs the Claude Agent SDK with Gestalt MCP catalog tools exposed as in-process SDK tools.",
            version="0.0.1-alpha.1",
        )

    def warnings(self) -> list[str]:
        return list(self._warnings)

    def close(self) -> None:
        if self._runner is not None:
            self._runner.close()
        if self._store is not None:
            self._store.close()
        self._store = None
        self._runner = None
        self._config = None

    def create_session(self, request: gestalt.CreateAgentProviderSessionRequest) -> gestalt.AgentSession:
        _, store, config = self._require_runtime()
        session_id = str(request.session_id or "").strip()
        if not session_id:
            raise gestalt.Error(400, "session_id is required")
        try:
            model = config.resolve_model(request.model)
        except ValueError as exc:
            raise gestalt.Error(400, str(exc)) from exc
        metadata = dict(request.metadata or {})
        try:
            validate_session_start_user_metadata(metadata)
        except ValueError as exc:
            raise gestalt.Error(400, str(exc)) from exc
        prepared_workspace = gestalt.prepared_workspace_to_dict(request.prepared_workspace)
        if prepared_workspace and (not prepared_workspace.get("root") or not prepared_workspace.get("cwd")):
            raise gestalt.Error(400, "prepared_workspace root and cwd are required")
        prepared_workspace = prepared_workspace or None
        created_by = gestalt.agent_actor_to_dict(request.created_by)
        session_start = request.session_start
        if _has_session_start_hooks(session_start):
            with self._session_start_lock:
                existing = self._store_call(
                    lambda: _existing_session_for_create(store, session_id, str(request.idempotency_key or "").strip())
                )
                if existing is not None:
                    return _agent_session(existing)
                try:
                    metadata = run_session_start_hooks(session_start, metadata)
                except Exception as exc:
                    raise gestalt.Error(412, str(exc)) from exc
                return self._create_session(
                    store=store,
                    session_id=session_id,
                    idempotency_key=str(request.idempotency_key or "").strip(),
                    model=model,
                    client_ref=str(request.client_ref or "").strip(),
                    metadata=metadata,
                    prepared_workspace=prepared_workspace,
                    created_by=created_by,
                )
        return self._create_session(
            store=store,
            session_id=session_id,
            idempotency_key=str(request.idempotency_key or "").strip(),
            model=model,
            client_ref=str(request.client_ref or "").strip(),
            metadata=metadata,
            prepared_workspace=prepared_workspace,
            created_by=created_by,
        )

    def _create_session(
        self,
        *,
        store: IndexedDBRunStore,
        session_id: str,
        idempotency_key: str,
        model: str,
        client_ref: str,
        metadata: dict[str, Any],
        prepared_workspace: dict[str, str] | None,
        created_by: dict[str, str],
    ) -> Any:
        session, _ = self._store_call(
            lambda: store.create_session(
                session_id=session_id,
                idempotency_key=idempotency_key,
                provider_name=self._name,
                model=model,
                client_ref=client_ref,
                metadata=metadata,
                prepared_workspace=prepared_workspace,
                created_by=created_by,
            )
        )
        return _agent_session(session)

    def get_session(self, request: gestalt.GetAgentProviderSessionRequest) -> gestalt.AgentSession:
        _, store, _ = self._require_runtime()
        session = self._store_call(lambda: store.get_session(str(request.session_id or "").strip()))
        if session is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        return _agent_session(session)

    def list_sessions(
        self, request: gestalt.ListAgentProviderSessionsRequest
    ) -> gestalt.ListAgentProviderSessionsResponse:
        _, store, _ = self._require_runtime()
        limit = int(request.limit or 0)
        if limit < 0:
            raise gestalt.Error(400, "limit must be non-negative")
        summary_only = bool(request.summary_only)
        return gestalt.ListAgentProviderSessionsResponse(
            sessions=[
                _agent_session(session, summary_only=summary_only)
                for session in self._store_call(
                    lambda: store.list_sessions(
                        session_ids=[str(value or "").strip() for value in request.session_ids],
                        subject_id=_subject_id(request.subject),
                        state=int(request.state or 0),
                        limit=limit,
                        summary_only=summary_only,
                    )
                )
            ]
        )

    def update_session(self, request: gestalt.UpdateAgentProviderSessionRequest) -> gestalt.AgentSession:
        _, store, _ = self._require_runtime()
        metadata = dict(request.metadata) if request.metadata is not None else None
        try:
            validate_session_start_user_metadata(metadata)
        except ValueError as exc:
            raise gestalt.Error(400, str(exc)) from exc
        session = self._store_call(
            lambda: store.update_session(
                session_id=str(request.session_id or "").strip(),
                client_ref=str(request.client_ref or "").strip(),
                state=int(request.state or 0),
                metadata=metadata,
            )
        )
        if session is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        return _agent_session(session)

    def create_turn(self, request: gestalt.CreateAgentProviderTurnRequest) -> gestalt.AgentTurn:
        runner, store, config = self._require_runtime()
        _validate_create_turn_request(request)
        session_id = str(request.session_id or "").strip()
        session = self._store_call(lambda: store.get_session(session_id))
        if session is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        if not request.messages:
            raise gestalt.Error(400, "messages must contain at least one entry")
        try:
            model = config.resolve_model(str(request.model or "").strip() or session.model)
        except ValueError as exc:
            raise gestalt.Error(400, str(exc)) from exc

        messages = prepend_session_start_context(gestalt.agent_messages_to_dicts(request.messages), session.metadata)
        response_schema = _response_schema_from_request(request)
        tool_source = int(getattr(request, "tool_source", 0) or 0)
        if tool_source == AGENT_TOOL_SOURCE_MODE_NONE:
            assert response_schema is not None
            turn_profile = ClaudeTurnProfile.structured_output(response_schema=response_schema)
        else:
            cwd = _prepared_workspace_cwd(session.prepared_workspace)
            try:
                claude_code_options = config.claude_code.resolve_turn_options(session.metadata)
            except ValueError as exc:
                raise gestalt.Error(400, str(exc)) from exc
            turn_profile = ClaudeTurnProfile.catalog(
                run_grant=str(request.run_grant or "").strip(), claude_code_options=claude_code_options, cwd=cwd
            )
        try:
            turn, created = self._store_call(
                lambda: store.begin_turn(
                    turn_id=str(request.turn_id or "").strip(),
                    session_id=session_id,
                    idempotency_key=str(request.idempotency_key or "").strip(),
                    provider_name=self._name,
                    model=model,
                    messages=messages,
                    created_by=gestalt.agent_actor_to_dict(request.created_by),
                    execution_ref=str(request.execution_ref or "").strip(),
                )
            )
        except StoreConflictError as exc:
            raise gestalt.Error(409, str(exc)) from exc
        except ValueError as exc:
            raise gestalt.Error(400, str(exc)) from exc
        if created:
            threading.Thread(
                target=self._complete_turn,
                kwargs={
                    "runner": runner,
                    "store": store,
                    "turn_id": turn.turn_id,
                    "session_id": turn.session_id,
                    "model": model,
                    "messages": list(turn.messages),
                    "turn_profile": turn_profile,
                },
                daemon=True,
            ).start()
        return _agent_turn(turn)

    def get_turn(self, request: gestalt.GetAgentProviderTurnRequest) -> gestalt.AgentTurn:
        _, store, _ = self._require_runtime()
        turn = self._store_call(lambda: store.get_turn(str(request.turn_id or "").strip()))
        if turn is None:
            raise gestalt.Error(404, f"agent turn {request.turn_id!r} was not found")
        return _agent_turn(turn)

    def list_turns(self, request: gestalt.ListAgentProviderTurnsRequest) -> gestalt.ListAgentProviderTurnsResponse:
        _, store, _ = self._require_runtime()
        limit = int(request.limit or 0)
        if limit < 0:
            raise gestalt.Error(400, "limit must be non-negative")
        summary_only = bool(request.summary_only)
        return gestalt.ListAgentProviderTurnsResponse(
            turns=[
                _agent_turn(turn, summary_only=summary_only)
                for turn in self._store_call(
                    lambda: store.list_turns(
                        session_id=str(request.session_id or "").strip(),
                        turn_ids=[str(value or "").strip() for value in request.turn_ids],
                        subject_id=_subject_id(request.subject),
                        status=int(request.status or 0),
                        limit=limit,
                        summary_only=summary_only,
                    )
                )
            ]
        )

    def cancel_turn(self, request: gestalt.CancelAgentProviderTurnRequest) -> gestalt.AgentTurn:
        runner, store, _ = self._require_runtime()
        turn = self._store_call(
            lambda: store.cancel_turn(
                turn_id=str(request.turn_id or "").strip(), reason=str(request.reason or "").strip()
            )
        )
        if turn is None:
            raise gestalt.Error(404, f"agent turn {request.turn_id!r} was not found")
        if turn.status == gestalt.AGENT_EXECUTION_STATUS_CANCELED:
            runner.cancel_turn(turn.turn_id)
        return _agent_turn(turn)

    def list_turn_events(
        self, request: gestalt.ListAgentProviderTurnEventsRequest
    ) -> gestalt.ListAgentProviderTurnEventsResponse:
        _, store, _ = self._require_runtime()
        return gestalt.ListAgentProviderTurnEventsResponse(
            events=[
                _agent_turn_event(event)
                for event in self._store_call(
                    lambda: store.list_turn_events(
                        turn_id=str(request.turn_id or "").strip(),
                        after_seq=int(request.after_seq or 0),
                        limit=int(request.limit or 0),
                    )
                )
            ]
        )

    def get_interaction(self, request: gestalt.GetAgentProviderInteractionRequest) -> gestalt.AgentInteraction:
        self._require_runtime()
        raise gestalt.Error(404, f"agent interaction {request.interaction_id!r} was not found")

    def list_interactions(
        self, request: gestalt.ListAgentProviderInteractionsRequest
    ) -> gestalt.ListAgentProviderInteractionsResponse:
        self._require_runtime()
        return gestalt.ListAgentProviderInteractionsResponse(interactions=[])

    def resolve_interaction(self, request: gestalt.ResolveAgentProviderInteractionRequest) -> gestalt.AgentInteraction:
        self._require_runtime()
        raise gestalt.Error(404, f"agent interaction {request.interaction_id!r} was not found")

    def get_capabilities(
        self, request: gestalt.GetAgentProviderCapabilitiesRequest
    ) -> gestalt.AgentProviderCapabilities:
        self._require_runtime()
        caps = gestalt.AgentProviderCapabilities(
            streaming_text=False,
            tool_calls=True,
            parallel_tool_calls=False,
            structured_output=True,
            interactions=False,
            resumable_turns=False,
            reasoning_summaries=False,
            bounded_list_hydration=True,
            supported_tool_sources=[AGENT_TOOL_SOURCE_MODE_NONE, AGENT_TOOL_SOURCE_MODE_MCP_CATALOG],
        )
        if hasattr(caps, "supports_session_start"):
            caps.supports_session_start = True
        if hasattr(caps, "supports_prepared_workspace"):
            caps.supports_prepared_workspace = True
        return caps

    def _require_runtime(self) -> tuple[ClaudeSDKRunner, IndexedDBRunStore, ClaudeAgentConfig]:
        if self._runner is None or self._store is None or self._config is None:
            raise gestalt.Error(412, "agent provider has not been configured")
        return self._runner, self._store, self._config

    def _store_call(self, operation: Any) -> Any:
        try:
            return operation()
        except StoreUnavailableError as exc:
            raise gestalt.Error(412, str(exc)) from exc

    def _complete_turn(
        self,
        *,
        runner: ClaudeSDKRunner,
        store: IndexedDBRunStore,
        turn_id: str,
        session_id: str,
        model: str,
        messages: list[dict[str, Any]],
        turn_profile: ClaudeTurnProfile,
    ) -> None:
        try:
            claude_code_options = turn_profile.claude_code_options
            if claude_code_options is not None and claude_code_options.plugins:
                logger.info(
                    "starting Claude Agent SDK turn with configured Claude Code plugins",
                    extra={
                        "plugin_names": claude_code_options.plugin_names,
                        "plugin_count": len(claude_code_options.plugins),
                    },
                )
            output = runner.run_turn(
                session_id=session_id, turn_id=turn_id, model=model, messages=messages, turn_profile=turn_profile
            )
        except ClaudeExecutionCanceled as exc:
            store.cancel_turn(turn_id=turn_id, reason=str(exc))
        except ClaudeExecutionError as exc:
            store.fail_turn(turn_id=turn_id, message=str(exc))
        except Exception as exc:
            logger.exception("Claude Agent SDK turn failed")
            store.fail_turn(turn_id=turn_id, message=str(exc))
        else:
            store.complete_turn(
                turn_id=turn_id, output_text=output.output_text, structured_output=output.structured_output
            )

    def _build_warnings(self, config: ClaudeAgentConfig) -> list[str]:
        warnings: list[str] = []
        if not config.anthropic_api_key and not os.environ.get("ANTHROPIC_API_KEY"):
            warnings.append("set config.anthropicApiKey or ANTHROPIC_API_KEY before running live Claude turns")
        if config.cli_path and _resolve_claude_cli(config) is None:
            warnings.append(f"configured cliPath {config.cli_path!r} could not be resolved")
        return warnings


def _agent_session(session: StoredSession, *, summary_only: bool = False) -> gestalt.AgentSession:
    return gestalt.AgentSession(
        id=session.session_id,
        provider_name=session.provider_name,
        model=session.model,
        client_ref=session.client_ref,
        state=session.state,
        metadata=None if summary_only else session.metadata,
        created_by=session.created_by or None,
        created_at=session.created_at,
        updated_at=session.updated_at,
        last_turn_at=session.last_turn_at,
    )


def _agent_turn(turn: StoredTurn, *, summary_only: bool = False) -> gestalt.AgentTurn:
    return gestalt.AgentTurn(
        id=turn.turn_id,
        session_id=turn.session_id,
        provider_name=turn.provider_name,
        model=turn.model,
        status=turn.status,
        messages=[] if summary_only else turn.messages,
        output_text="" if summary_only else turn.output_text,
        structured_output=None if summary_only else turn.structured_output,
        status_message=turn.status_message,
        execution_ref=turn.execution_ref,
        created_by=turn.created_by or None,
        created_at=turn.created_at,
        started_at=turn.started_at,
        completed_at=turn.completed_at,
    )


def _agent_turn_event(event: StoredTurnEvent) -> gestalt.AgentTurnEvent:
    return gestalt.AgentTurnEvent(
        id=event.event_id,
        turn_id=event.turn_id,
        seq=event.seq,
        type=event.event_type,
        source=event.source,
        visibility=event.visibility,
        data=event.data,
        created_at=event.created_at,
    )


def _has_session_start_hooks(session_start: Any | None) -> bool:
    return session_start is not None and len(list(getattr(session_start, "hooks", []) or [])) > 0


def _validate_create_turn_request(request: gestalt.CreateAgentProviderTurnRequest) -> None:
    tool_source = int(getattr(request, "tool_source", 0) or 0)
    if tool_source not in {AGENT_TOOL_SOURCE_MODE_MCP_CATALOG, AGENT_TOOL_SOURCE_MODE_NONE}:
        raise gestalt.Error(400, "agent/claude requires toolSource none or mcp_catalog")
    if tool_source == AGENT_TOOL_SOURCE_MODE_MCP_CATALOG and not str(request.run_grant or "").strip():
        raise gestalt.Error(400, "run_grant is required")
    if request.tools:
        raise gestalt.Error(400, "resolved tools are not supported by agent/claude")
    if tool_source == AGENT_TOOL_SOURCE_MODE_NONE and list(request.tool_refs):
        raise gestalt.Error(400, "tool_refs are not supported with toolSource none")
    if tool_source == AGENT_TOOL_SOURCE_MODE_NONE and not _has_response_schema(request):
        raise gestalt.Error(400, "response_schema is required with toolSource none")
    if tool_source == AGENT_TOOL_SOURCE_MODE_MCP_CATALOG and _has_response_schema(request):
        raise gestalt.Error(400, "response_schema is not supported with toolSource mcp_catalog")
    _validate_response_schema(_response_schema_from_request(request))
    if dict(request.model_options or {}):
        raise gestalt.Error(400, "model_options are not supported by agent/claude")
    if tool_source == AGENT_TOOL_SOURCE_MODE_MCP_CATALOG:
        _validate_tool_refs(list(request.tool_refs))


def _response_schema_from_request(request: Any) -> dict[str, Any] | None:
    value = getattr(request, "response_schema", None)
    if value is None:
        return None
    return dict(value)


def _has_response_schema(request: Any) -> bool:
    has_field = getattr(request, "HasField", None)
    return (
        bool(has_field("response_schema"))
        if callable(has_field)
        else _response_schema_from_request(request) is not None
    )


def _validate_response_schema(schema: dict[str, Any] | None) -> None:
    if schema is None:
        return
    if not schema:
        raise gestalt.Error(400, "response_schema must be a non-empty JSON schema object with type 'object'")
    if str(schema.get("type") or "").strip() != "object":
        raise gestalt.Error(400, "response_schema.type must be 'object'")


def _validate_tool_refs(tool_refs: list[Any]) -> None:
    for index, ref in enumerate(tool_refs, start=1):
        plugin = _text(getattr(ref, "plugin", ""))
        system = _text(getattr(ref, "system", ""))
        operation = _text(getattr(ref, "operation", ""))
        connection = _text(getattr(ref, "connection", ""))
        instance = _text(getattr(ref, "instance", ""))
        title = _text(getattr(ref, "title", ""))
        description = _text(getattr(ref, "description", ""))

        if "*" in {system, operation, connection, instance}:
            raise gestalt.Error(400, "wildcard tool_refs are not supported")
        if plugin == "*":
            if any([system, operation, connection, instance, title, description]):
                raise gestalt.Error(
                    400,
                    f"tool_refs[{index}] global search ref cannot include operation, connection, instance, "
                    "title, or description",
                )
            return
        if system:
            if plugin:
                raise gestalt.Error(400, f"tool_refs[{index}] must set exactly one of plugin or system")
            if system != "workflow":
                raise gestalt.Error(400, f"tool_refs[{index}].system {system!r} is not supported")
            if not operation:
                raise gestalt.Error(400, f"tool_refs[{index}].operation is required for system tool refs")
            if any([connection, instance, title, description]):
                raise gestalt.Error(
                    400, f"tool_refs[{index}] system refs cannot include connection, instance, title, or description"
                )
            return
        if not plugin:
            raise gestalt.Error(400, f"tool_refs[{index}].plugin is required")


def _prepared_workspace_cwd(value: dict[str, str] | None) -> str:
    if not value:
        return ""
    return _text(value.get("cwd"))


def _subject_id(subject: Any) -> str:
    return _text(getattr(subject, "subject_id", ""))


def _text(value: Any) -> str:
    return str(value or "").strip()


def _resolve_claude_cli(config: ClaudeAgentConfig) -> str | None:
    binary = config.cli_path
    if not binary:
        return None
    if os.path.sep in binary or (os.path.altsep is not None and os.path.altsep in binary):
        path = binary if os.path.isabs(binary) else os.path.join(config.working_directory or os.getcwd(), binary)
        return path if os.path.isfile(path) and os.access(path, os.X_OK) else None
    return _which(binary)


def _which(binary: str) -> str | None:
    for path in os.environ.get("PATH", "").split(os.pathsep):
        candidate = os.path.join(path, binary)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


def _existing_session_for_create(
    store: IndexedDBRunStore, session_id: str, idempotency_key: str
) -> StoredSession | None:
    existing = store.get_session(session_id)
    if existing is not None:
        return existing
    if not idempotency_key:
        return None
    return store.get_session_by_idempotency_key(idempotency_key)


provider = ClaudeCodeAgentProvider()
