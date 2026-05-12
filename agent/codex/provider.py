from __future__ import annotations

import logging
import os
import threading
from typing import Any

import gestalt

from internals.codex_runner import CodexExecutionCanceled, CodexExecutionError
from internals.codex_runner import CodexMCPRunner
from internals.config import CodexAgentConfig
from internals.session_start import (
    prepend_session_start_context,
    run_session_start_hooks,
    session_start_metadata_paths,
    validate_session_start_user_metadata,
)
from internals.store import StoreConflictError, StoredSession, StoredTurn, StoredTurnEvent
from internals.store import InMemoryRunStore

logger = logging.getLogger(__name__)


class CodexMCPAgentProvider(gestalt.AgentProvider, gestalt.MetadataProvider, gestalt.WarningsProvider, gestalt.Closer):
    def __init__(self) -> None:
        self._name = "codex"
        self._warnings: list[str] = ["provider has not been configured"]
        self._config: CodexAgentConfig | None = None
        self._store: InMemoryRunStore | None = None
        self._runner: CodexMCPRunner | None = None
        self._session_start_lock = threading.Lock()

    def configure(self, name: str, config: dict[str, Any]) -> None:
        self._name = name.strip() or "codex"
        resolved = CodexAgentConfig.from_dict(name=self._name, raw_config=config)
        self.close()
        self._config = resolved
        self._store = InMemoryRunStore()
        self._runner = CodexMCPRunner(resolved)
        self._warnings = self._build_warnings(resolved)

    def metadata(self) -> gestalt.ProviderMetadata:
        return gestalt.ProviderMetadata(
            kind=gestalt.ProviderKind.AGENT,
            name=self._name,
            display_name="Codex MCP Agent",
            description="Runs Codex CLI through its MCP harness with Gestalt MCP catalog tools exposed by grant.",
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
            model = config.resolve_model(str(request.model or ""))
        except ValueError as exc:
            raise gestalt.Error(400, str(exc)) from exc
        metadata = dict(request.metadata or {})
        try:
            validate_session_start_user_metadata(metadata)
        except ValueError as exc:
            raise gestalt.Error(400, str(exc)) from exc
        try:
            prepared_workspace = _prepared_workspace_to_dict(request.prepared_workspace)
        except ValueError as exc:
            raise gestalt.Error(400, str(exc)) from exc
        idempotency_key = str(request.idempotency_key or "").strip()
        session_start = request.session_start
        if session_start is not None and len(list(getattr(session_start, "hooks", []) or [])) > 0:
            with self._session_start_lock:
                existing = _existing_session_for_create(store, session_id, idempotency_key)
                if existing is not None:
                    return _agent_session(existing)
                try:
                    metadata = run_session_start_hooks(session_start, metadata)
                except Exception as exc:
                    raise gestalt.Error(412, str(exc)) from exc
                session, _ = store.create_session(
                    session_id=session_id,
                    idempotency_key=idempotency_key,
                    provider_name=self._name,
                    model=model,
                    client_ref=str(request.client_ref or "").strip(),
                    metadata=metadata,
                    prepared_workspace=prepared_workspace,
                    created_by=gestalt.agent_actor_to_dict(request.created_by),
                )
                return _agent_session(session)
        session, _ = store.create_session(
            session_id=session_id,
            idempotency_key=idempotency_key,
            provider_name=self._name,
            model=model,
            client_ref=str(request.client_ref or "").strip(),
            metadata=metadata,
            prepared_workspace=prepared_workspace,
            created_by=gestalt.agent_actor_to_dict(request.created_by),
        )
        return _agent_session(session)

    def get_session(self, request: gestalt.GetAgentProviderSessionRequest) -> gestalt.AgentSession:
        _, store, _ = self._require_runtime()
        session = store.get_session(str(request.session_id or "").strip())
        if session is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        return _agent_session(session)

    def list_sessions(
        self, request: gestalt.ListAgentProviderSessionsRequest
    ) -> gestalt.ListAgentProviderSessionsResponse:
        _, store, _ = self._require_runtime()
        limit = int(getattr(request, "limit", 0) or 0)
        if limit < 0:
            raise gestalt.Error(400, "limit must be non-negative")
        return gestalt.ListAgentProviderSessionsResponse(
            sessions=[
                _agent_session(session, summary_only=bool(getattr(request, "summary_only", False)))
                for session in store.list_sessions(
                    session_ids=[str(value or "").strip() for value in getattr(request, "session_ids", [])],
                    subject_id=_subject_id(request.subject),
                    state=int(getattr(request, "state", 0) or 0),
                    limit=limit,
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
        session = store.update_session(
            session_id=str(request.session_id or "").strip(),
            client_ref=str(request.client_ref or "").strip(),
            state=int(request.state or 0),
            metadata=metadata,
        )
        if session is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        return _agent_session(session)

    def create_turn(self, request: gestalt.CreateAgentProviderTurnRequest) -> gestalt.AgentTurn:
        runner, store, config = self._require_runtime()
        _validate_create_turn_request(request)
        session = store.get_session(str(request.session_id or "").strip())
        if session is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        if len(list(request.messages)) == 0:
            raise gestalt.Error(400, "messages must contain at least one entry")
        try:
            model = config.resolve_model(str(request.model or "").strip() or session.model)
        except ValueError as exc:
            raise gestalt.Error(400, str(exc)) from exc

        messages = prepend_session_start_context(gestalt.agent_messages_to_dicts(request.messages), session.metadata)
        skill_roots = session_start_metadata_paths(
            session.metadata, "codexSkillRoots", allowed_basenames={"mortgage", "vds", "tools", "rnb"}
        )
        cwd = _prepared_workspace_cwd(session.prepared_workspace)
        try:
            turn, created = store.begin_turn(
                turn_id=str(request.turn_id or "").strip(),
                session_id=str(request.session_id or "").strip(),
                idempotency_key=str(request.idempotency_key or "").strip(),
                provider_name=self._name,
                model=model,
                messages=messages,
                created_by=gestalt.agent_actor_to_dict(request.created_by),
                execution_ref=str(request.execution_ref or "").strip(),
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
                    "run_grant": str(request.run_grant or "").strip(),
                    "skill_roots": skill_roots,
                    "cwd": cwd,
                },
                daemon=True,
            ).start()
        return _agent_turn(turn)

    def get_turn(self, request: gestalt.GetAgentProviderTurnRequest) -> gestalt.AgentTurn:
        _, store, _ = self._require_runtime()
        turn = store.get_turn(str(request.turn_id or "").strip())
        if turn is None:
            raise gestalt.Error(404, f"agent turn {request.turn_id!r} was not found")
        return _agent_turn(turn)

    def list_turns(self, request: gestalt.ListAgentProviderTurnsRequest) -> gestalt.ListAgentProviderTurnsResponse:
        _, store, _ = self._require_runtime()
        limit = int(getattr(request, "limit", 0) or 0)
        if limit < 0:
            raise gestalt.Error(400, "limit must be non-negative")
        return gestalt.ListAgentProviderTurnsResponse(
            turns=[
                _agent_turn(turn, summary_only=bool(getattr(request, "summary_only", False)))
                for turn in store.list_turns(
                    session_id=str(request.session_id or "").strip(),
                    turn_ids=[str(value or "").strip() for value in getattr(request, "turn_ids", [])],
                    subject_id=_subject_id(request.subject),
                    status=int(getattr(request, "status", 0) or 0),
                    limit=limit,
                )
            ]
        )

    def cancel_turn(self, request: gestalt.CancelAgentProviderTurnRequest) -> gestalt.AgentTurn:
        runner, store, _ = self._require_runtime()
        turn = store.cancel_turn(turn_id=str(request.turn_id or "").strip(), reason=str(request.reason or "").strip())
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
                for event in store.list_turn_events(
                    turn_id=str(request.turn_id or "").strip(),
                    after_seq=int(request.after_seq or 0),
                    limit=int(request.limit or 0),
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
            structured_output=False,
            interactions=False,
            resumable_turns=False,
            reasoning_summaries=False,
            bounded_list_hydration=True,
            supported_tool_sources=[gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG],
        )
        if hasattr(caps, "supports_session_start"):
            caps.supports_session_start = True
        if hasattr(caps, "supports_prepared_workspace"):
            caps.supports_prepared_workspace = True
        return caps

    def _require_runtime(self) -> tuple[CodexMCPRunner, InMemoryRunStore, CodexAgentConfig]:
        if self._runner is None or self._store is None or self._config is None:
            raise gestalt.Error(412, "agent provider has not been configured")
        return self._runner, self._store, self._config

    def _complete_turn(
        self,
        *,
        runner: CodexMCPRunner,
        store: InMemoryRunStore,
        turn_id: str,
        session_id: str,
        model: str,
        messages: list[dict[str, Any]],
        run_grant: str,
        skill_roots: list[str],
        cwd: str,
    ) -> None:
        try:
            output = runner.run_turn(
                session_id=session_id,
                turn_id=turn_id,
                model=model,
                messages=messages,
                run_grant=run_grant,
                skill_roots=skill_roots,
                cwd=cwd,
            )
        except CodexExecutionCanceled as exc:
            store.cancel_turn(turn_id=turn_id, reason=str(exc))
        except CodexExecutionError as exc:
            store.fail_turn(turn_id=turn_id, message=str(exc))
        except Exception as exc:
            logger.exception("Codex MCP turn failed")
            store.fail_turn(turn_id=turn_id, message=str(exc))
        else:
            store.complete_turn(turn_id=turn_id, output_text=output)

    def _build_warnings(self, config: CodexAgentConfig) -> list[str]:
        warnings: list[str] = []
        if not config.openai_api_key and not os.environ.get("OPENAI_API_KEY"):
            warnings.append(
                "set config.openaiApiKey or OPENAI_API_KEY before live turns; "
                "agent/codex uses an isolated per-turn CODEX_HOME and does not read Codex CLI login state"
            )
        if _resolve_codex_command(config) is None:
            warnings.append(f"configured codexCommand {config.codex_command!r} could not be resolved")
        if config.sandbox == "workspace-write":
            warnings.append("Codex sandbox workspace-write can modify files under the configured workspace")
        if config.sandbox == "danger-full-access":
            warnings.append("Codex sandbox danger-full-access disables filesystem sandboxing")
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


def _resolve_codex_command(config: CodexAgentConfig) -> str | None:
    binary = config.codex_command
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


def _validate_create_turn_request(request: Any) -> None:
    if int(getattr(request, "tool_source", 0) or 0) != gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG:
        raise gestalt.Error(400, "agent/codex requires toolSource mcp_catalog")
    if not str(request.run_grant or "").strip():
        raise gestalt.Error(400, "run_grant is required")
    if len(list(getattr(request, "tools", []))) > 0:
        raise gestalt.Error(400, "resolved tools are not supported; use tool_refs with mcp_catalog")
    if dict(getattr(request, "response_schema", None) or {}):
        raise gestalt.Error(400, "response_schema is not supported by agent/codex")
    if dict(getattr(request, "model_options", None) or {}):
        raise gestalt.Error(400, "model_options are not supported by agent/codex")
    _validate_tool_refs(list(getattr(request, "tool_refs", [])))


def _validate_tool_refs(tool_refs: list[Any]) -> None:
    if not tool_refs:
        raise gestalt.Error(400, "tool_refs are required for mcp_catalog turns")
    for index, ref in enumerate(tool_refs, start=1):
        plugin = _text(getattr(ref, "plugin", ""))
        system = _text(getattr(ref, "system", ""))
        operation = _text(getattr(ref, "operation", ""))
        connection = _text(getattr(ref, "connection", ""))
        instance = _text(getattr(ref, "instance", ""))
        if not operation:
            raise gestalt.Error(400, f"tool_refs[{index}].operation is required")
        if "*" in {plugin, system, operation, connection, instance}:
            raise gestalt.Error(400, "wildcard tool_refs are not supported")
        if bool(plugin) == bool(system):
            raise gestalt.Error(400, f"tool_refs[{index}] must set exactly one of plugin or system")
        if system and system != "workflow":
            raise gestalt.Error(400, f"tool_refs[{index}].system {system!r} is not supported")


def _prepared_workspace_to_dict(value: Any | None) -> dict[str, str] | None:
    if value is None:
        return None
    root = _text(getattr(value, "root", ""))
    cwd = _text(getattr(value, "cwd", ""))
    if not root and not cwd:
        return None
    if not root or not cwd:
        raise ValueError("prepared_workspace root and cwd are required")
    return {"root": root, "cwd": cwd}


def _prepared_workspace_cwd(value: dict[str, str] | None) -> str:
    if not value:
        return ""
    return _text(value.get("cwd"))


def _existing_session_for_create(
    store: InMemoryRunStore, session_id: str, idempotency_key: str
) -> StoredSession | None:
    existing = store.get_session(session_id)
    if existing is not None:
        return existing
    if not idempotency_key:
        return None
    return store.get_session_by_idempotency_key(idempotency_key)


def _subject_id(subject: Any) -> str:
    return _text(getattr(subject, "subject_id", ""))


def _text(value: Any) -> str:
    return str(value or "").strip()


provider = CodexMCPAgentProvider()
