from __future__ import annotations

import logging
import os
import threading
from typing import Any

import gestalt

from internals.codex_runner import CodexExecutionCanceled, CodexExecutionError, CodexMCPRunner
from internals.codex_runner import validate_schema
from internals.config import CodexAgentConfig
from internals.session_start import (
    prepend_session_start_context,
    run_session_start_hooks,
    session_start_metadata_paths,
    validate_session_start_user_metadata,
)
from internals.store import (
    StoreConflictError,
    StoredSession,
    StoredTurn,
    StoredTurnEvent,
    session_readable_by,
    session_writable_by,
)
from internals.store import InMemoryRunStore
from internals.subject_id import (
    agent_actor_from_created_by_subject_id,
    created_by_subject_id_from_actor,
)

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
        session_id = request.session_id.strip()
        if not session_id:
            raise gestalt.Error(400, "session_id is required")
        try:
            model = config.resolve_model(request.model.strip())
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
        idempotency_key = request.idempotency_key.strip()
        request_subject_id = request.subject.id.strip() if request.subject is not None else ""
        if request.session_start is not None and len(list(request.session_start.hooks)) > 0:
            with self._session_start_lock:
                existing = _existing_session_for_create(store, session_id, idempotency_key)
                if existing is not None:
                    _require_session_readable(existing, request_subject_id)
                    return _agent_session(existing)
                try:
                    metadata = run_session_start_hooks(request.session_start, metadata)
                except Exception as exc:
                    raise gestalt.Error(412, str(exc)) from exc
                session, _ = store.create_session(
                    session_id=session_id,
                    idempotency_key=idempotency_key,
                    provider_name=self._name,
                    model=model,
                    client_ref=request.client_ref.strip(),
                    metadata=metadata,
                    prepared_workspace=prepared_workspace,
                    created_by_subject_id=created_by_subject_id_from_actor(
                        request.created_by
                    ),
                )
                return _agent_session(session)
        session, created = store.create_session(
            session_id=session_id,
            idempotency_key=idempotency_key,
            provider_name=self._name,
            model=model,
            client_ref=request.client_ref.strip(),
            metadata=metadata,
            prepared_workspace=prepared_workspace,
            created_by_subject_id=created_by_subject_id_from_actor(request.created_by),
        )
        if not created:
            _require_session_readable(session, request_subject_id)
        return _agent_session(session)

    def get_session(self, request: gestalt.GetAgentProviderSessionRequest) -> gestalt.AgentSession:
        _, store, _ = self._require_runtime()
        session = store.get_session(request.session_id.strip())
        if session is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        _require_session_readable(session, request.subject.id.strip() if request.subject is not None else "")
        return _agent_session(session)

    def list_sessions(
        self, request: gestalt.ListAgentProviderSessionsRequest
    ) -> gestalt.ListAgentProviderSessionsResponse:
        _, store, _ = self._require_runtime()
        limit = request.limit
        if limit < 0:
            raise gestalt.Error(400, "limit must be non-negative")
        return gestalt.ListAgentProviderSessionsResponse(
            sessions=[
                _agent_session(session, summary_only=bool(request.summary_only))
                for session in store.list_sessions(
                    session_ids=[value.strip() for value in request.session_ids],
                    subject_id=request.subject.id.strip() if request.subject is not None else "",
                    state=request.state,
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
        existing = store.get_session(request.session_id.strip())
        if existing is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        _require_session_writable(existing, request.subject.id.strip() if request.subject is not None else "")
        session = store.update_session(
            session_id=request.session_id.strip(),
            client_ref=request.client_ref.strip(),
            state=request.state,
            metadata=metadata,
        )
        if session is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        return _agent_session(session)

    def create_turn(self, request: gestalt.CreateAgentProviderTurnRequest) -> gestalt.AgentTurn:
        runner, store, config = self._require_runtime()
        schema = _validate_create_turn_request(request)
        session = store.get_session(request.session_id.strip())
        if session is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        _require_session_writable(session, request.subject.id.strip() if request.subject is not None else "")
        if len(list(request.messages)) == 0:
            raise gestalt.Error(400, "messages must contain at least one entry")
        try:
            model = config.resolve_model(request.model.strip() or session.model)
        except ValueError as exc:
            raise gestalt.Error(400, str(exc)) from exc

        messages = prepend_session_start_context(gestalt.agent_messages_to_dicts(request.messages), session.metadata)
        skill_roots = session_start_metadata_paths(
            session.metadata, "codexSkillRoots", allowed_basenames={"mortgage", "vds", "tools", "rnb"}
        )
        cwd = session.prepared_workspace.get("cwd", "").strip() if session.prepared_workspace else ""
        try:
            turn, created = store.begin_turn(
                turn_id=request.turn_id.strip(),
                session_id=request.session_id.strip(),
                idempotency_key=request.idempotency_key.strip(),
                provider_name=self._name,
                model=model,
                messages=messages,
                created_by_subject_id=created_by_subject_id_from_actor(
                    request.created_by
                ),
                execution_ref=request.execution_ref.strip(),
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
                    "run_grant": request.run_grant.strip(),
                    "skill_roots": skill_roots,
                    "cwd": cwd,
                    "schema": schema,
                },
                daemon=True,
            ).start()
        return _agent_turn(turn)

    def get_turn(self, request: gestalt.GetAgentProviderTurnRequest) -> gestalt.AgentTurn:
        _, store, _ = self._require_runtime()
        turn = store.get_turn(request.turn_id.strip())
        if turn is None:
            raise gestalt.Error(404, f"agent turn {request.turn_id!r} was not found")
        _require_session_readable(
            _session_for_turn(store, turn),
            request.subject.id.strip() if request.subject is not None else "",
        )
        return _agent_turn(turn)

    def list_turns(self, request: gestalt.ListAgentProviderTurnsRequest) -> gestalt.ListAgentProviderTurnsResponse:
        _, store, _ = self._require_runtime()
        limit = request.limit
        if limit < 0:
            raise gestalt.Error(400, "limit must be non-negative")
        request_subject_id = request.subject.id.strip() if request.subject is not None else ""
        session_id = request.session_id.strip()
        turn_ids = [value.strip() for value in request.turn_ids]
        if session_id:
            session = store.get_session(session_id)
            if session is None or not session_readable_by(session, request_subject_id):
                return gestalt.ListAgentProviderTurnsResponse(turns=[])
        store_limit = 0 if turn_ids else limit
        turns = _readable_turns(
            store,
            store.list_turns(
                session_id=session_id,
                turn_ids=turn_ids,
                subject_id="",
                status=request.status,
                limit=store_limit,
            ),
            request_subject_id,
        )
        if turn_ids and limit > 0:
            turns = turns[:limit]
        return gestalt.ListAgentProviderTurnsResponse(
            turns=[_agent_turn(turn, summary_only=bool(request.summary_only)) for turn in turns]
        )

    def cancel_turn(self, request: gestalt.CancelAgentProviderTurnRequest) -> gestalt.AgentTurn:
        runner, store, _ = self._require_runtime()
        existing = store.get_turn(request.turn_id.strip())
        if existing is None:
            raise gestalt.Error(404, f"agent turn {request.turn_id!r} was not found")
        _require_session_writable(
            _session_for_turn(store, existing),
            request.subject.id.strip() if request.subject is not None else "",
        )
        turn = store.cancel_turn(turn_id=request.turn_id.strip(), reason=request.reason.strip())
        if turn is None:
            raise gestalt.Error(404, f"agent turn {request.turn_id!r} was not found")
        if turn.status == gestalt.AGENT_EXECUTION_STATUS_CANCELED:
            runner.cancel_turn(turn.turn_id)
        return _agent_turn(turn)

    def list_turn_events(
        self, request: gestalt.ListAgentProviderTurnEventsRequest
    ) -> gestalt.ListAgentProviderTurnEventsResponse:
        _, store, _ = self._require_runtime()
        turn = store.get_turn(request.turn_id.strip())
        if turn is None:
            return gestalt.ListAgentProviderTurnEventsResponse(events=[])
        session = _session_for_turn(store, turn)
        if not session_readable_by(session, request.subject.id.strip() if request.subject is not None else ""):
            return gestalt.ListAgentProviderTurnEventsResponse(events=[])
        return gestalt.ListAgentProviderTurnEventsResponse(
            events=[
                _agent_turn_event(event)
                for event in store.list_turn_events(
                    turn_id=request.turn_id.strip(),
                    after_seq=request.after_seq,
                    limit=request.limit,
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
            interactions=False,
            resumable_turns=False,
            reasoning_summaries=False,
            bounded_list_hydration=True,
            supported_tool_sources=[gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG],
            supports_session_start=True,
            supports_prepared_workspace=True,
        )
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
        schema: dict[str, Any] | None,
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
                schema=schema,
            )
        except CodexExecutionCanceled as exc:
            store.cancel_turn(turn_id=turn_id, reason=str(exc))
        except CodexExecutionError as exc:
            store.fail_turn(turn_id=turn_id, message=str(exc))
        except Exception as exc:
            logger.exception("Codex MCP turn failed")
            store.fail_turn(turn_id=turn_id, message=str(exc))
        else:
            store.complete_turn(turn_id=turn_id, output=output)

    def _build_warnings(self, config: CodexAgentConfig) -> list[str]:
        warnings: list[str] = []
        if not config.openai_api_key:
            warnings.append(
                "set config.openaiApiKey before live turns; "
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
        created_by=agent_actor_from_created_by_subject_id(session.created_by_subject_id),
        created_at=session.created_at,
        updated_at=session.updated_at,
        last_turn_at=session.last_turn_at,
    )


def _session_for_turn(store: InMemoryRunStore, turn: StoredTurn) -> StoredSession:
    session = store.get_session(turn.session_id)
    if session is None:
        raise gestalt.Error(404, f"agent session {turn.session_id!r} was not found")
    return session


def _readable_turns(store: InMemoryRunStore, turns: list[StoredTurn], request_subject_id: str) -> list[StoredTurn]:
    if not request_subject_id:
        return []
    readable: list[StoredTurn] = []
    session_cache: dict[str, StoredSession | None] = {}
    for turn in turns:
        session = session_cache.get(turn.session_id)
        if turn.session_id not in session_cache:
            session = store.get_session(turn.session_id)
            session_cache[turn.session_id] = session
        if session is not None and session_readable_by(session, request_subject_id):
            readable.append(turn)
    return readable


def _require_session_readable(session: StoredSession, request_subject_id: str) -> None:
    if not session_readable_by(session, request_subject_id):
        raise gestalt.Error(404, f"agent session {session.session_id!r} was not found")


def _require_session_writable(session: StoredSession, request_subject_id: str) -> None:
    if not session_writable_by(session, request_subject_id):
        raise gestalt.Error(403, f"agent session {session.session_id!r} is owned by another subject")


def _agent_turn(turn: StoredTurn, *, summary_only: bool = False) -> gestalt.AgentTurn:
    return gestalt.AgentTurn(
        id=turn.turn_id,
        session_id=turn.session_id,
        provider_name=turn.provider_name,
        model=turn.model,
        status=turn.status,
        messages=[] if summary_only else turn.messages,
        output=None if summary_only else turn.output,
        status_message=turn.status_message,
        execution_ref=turn.execution_ref,
        created_by=agent_actor_from_created_by_subject_id(turn.created_by_subject_id),
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


def _validate_create_turn_request(request: gestalt.CreateAgentProviderTurnRequest) -> dict[str, Any] | None:
    if request.tool_source != gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG:
        raise gestalt.Error(400, "agent/codex requires toolSource mcp_catalog")
    if not request.run_grant.strip():
        raise gestalt.Error(400, "run_grant is required")
    if len(list(request.tools)) > 0:
        raise gestalt.Error(400, "resolved tools are not supported; use tool_refs with mcp_catalog")
    try:
        schema = _schema_from_output(request.output)
        if schema is not None:
            validate_schema(schema)
    except CodexExecutionError as exc:
        raise gestalt.Error(400, str(exc)) from exc
    if dict(request.model_options or {}):
        raise gestalt.Error(400, "model_options are not supported by agent/codex")
    _validate_tool_refs(list(request.tool_refs))
    return schema


def _schema_from_output(output: gestalt.AgentOutput | None) -> dict[str, Any] | None:
    if output is None:
        raise gestalt.Error(400, "output is required")
    text_set = output.text is not None
    structured_set = output.structured is not None
    if text_set == structured_set:
        raise gestalt.Error(400, "exactly one of output.text or output.structured is required")
    if structured_set:
        assert output.structured is not None
        return dict(output.structured.schema)
    return None


def _validate_tool_refs(tool_refs: list[gestalt.AgentToolRef]) -> None:
    if not tool_refs:
        raise gestalt.Error(400, "tool_refs are required for mcp_catalog turns")
    for index, ref in enumerate(tool_refs, start=1):
        plugin = ref.app.strip()
        system = ref.system.strip()
        operation = ref.operation.strip()
        connection = ref.connection.strip()
        instance = ref.instance.strip()
        if not operation:
            raise gestalt.Error(400, f"tool_refs[{index}].operation is required")
        if "*" in {plugin, system, operation, connection, instance}:
            raise gestalt.Error(400, "wildcard tool_refs are not supported")
        if bool(plugin) == bool(system):
            raise gestalt.Error(400, f"tool_refs[{index}] must set exactly one of plugin or system")
        if system and system != "workflow":
            raise gestalt.Error(400, f"tool_refs[{index}].system {system!r} is not supported")


def _existing_session_for_create(
    store: InMemoryRunStore, session_id: str, idempotency_key: str
) -> StoredSession | None:
    existing = store.get_session(session_id)
    if existing is not None:
        return existing
    if not idempotency_key:
        return None
    return store.get_session_by_idempotency_key(idempotency_key)


provider = CodexMCPAgentProvider()
