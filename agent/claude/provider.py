from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable
from typing import Any

import gestalt

from internals import ClaudeAgentConfig, ClaudeSDKRunner, IndexedDBRunStore
from internals.claude_runner import ClaudeExecutionCanceled, ClaudeExecutionError
from internals.provider_requests import (
    SessionCreateRequest,
    ToolSourceModes,
    existing_session_for_create,
    session_create_request_from_provider_request,
    turn_create_request_from_provider_request,
    validate_turn_contract,
)
from internals.provider_responses import agent_session, agent_turn, agent_turn_event
from internals.session_start import (
    prepend_session_start_context,
    run_session_start_hooks,
    validate_session_start_user_metadata,
)
from internals.store import StoreConflictError, StoreUnavailableError
from internals.store_records import StoredSession, StoredTurn, session_readable_by, session_writable_by

logger = logging.getLogger(__name__)


AGENT_TOOL_SOURCE_MODE_NONE = int(gestalt.AGENT_TOOL_SOURCE_MODE_NONE)
AGENT_TOOL_SOURCE_MODE_CATALOG = int(gestalt.AGENT_TOOL_SOURCE_MODE_CATALOG)


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
        request_subject_id = request.subject.id.strip() if request.subject is not None else ""
        tool_source_modes = ToolSourceModes(none=AGENT_TOOL_SOURCE_MODE_NONE, catalog=AGENT_TOOL_SOURCE_MODE_CATALOG)
        try:
            create_request = session_create_request_from_provider_request(
                request, config=config, tool_source_modes=tool_source_modes
            )
        except ValueError as exc:
            raise gestalt.Error(400, str(exc)) from exc

        if create_request.has_session_start_hooks:
            with self._session_start_lock:
                existing = self._store_call(
                    lambda: existing_session_for_create(
                        store,
                        created_by_subject_id=create_request.created_by_subject_id,
                        idempotency_key=create_request.idempotency_key,
                    )
                )
                if existing is not None:
                    _require_session_readable(existing, request_subject_id)
                    return agent_session(existing)
                try:
                    create_request = create_request.with_metadata(
                        run_session_start_hooks(create_request.session_start, create_request.metadata)
                    )
                except Exception as exc:
                    raise gestalt.Error(412, str(exc)) from exc
                return self._create_session(store=store, request=create_request, request_subject_id=request_subject_id)

        return self._create_session(store=store, request=create_request, request_subject_id=request_subject_id)

    def _create_session(
        self, *, store: IndexedDBRunStore, request: SessionCreateRequest, request_subject_id: str
    ) -> gestalt.AgentSession:
        session, created = self._store_call(
            lambda: store.create_session(
                idempotency_key=request.idempotency_key,
                provider_name=self._name,
                model=request.model,
                client_ref=request.client_ref,
                metadata=request.metadata,
                prepared_workspace=request.prepared_workspace,
                tool_source=request.tool_source,
                tool_refs=request.tool_refs,
                listed_tools=request.listed_tools,
                created_by_subject_id=request.created_by_subject_id,
            )
        )
        if not created:
            _require_session_readable(session, request_subject_id)
        return agent_session(session)

    def get_session(self, request: gestalt.GetAgentProviderSessionRequest) -> gestalt.AgentSession:
        _, store, _ = self._require_runtime()
        session = self._store_call(lambda: store.get_session(request.session_id.strip()))
        if session is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        _require_session_readable(session, request.subject.id.strip() if request.subject is not None else "")
        return agent_session(session)

    def list_sessions(
        self, request: gestalt.ListAgentProviderSessionsRequest
    ) -> gestalt.ListAgentProviderSessionsResponse:
        _, store, _ = self._require_runtime()
        limit = request.limit
        if limit < 0:
            raise gestalt.Error(400, "limit must be non-negative")
        summary_only = bool(request.summary_only)
        return gestalt.ListAgentProviderSessionsResponse(
            sessions=[
                agent_session(session, summary_only=summary_only)
                for session in self._store_call(
                    lambda: store.list_sessions(
                        session_ids=[value.strip() for value in request.session_ids],
                        subject_id=request.subject.id.strip() if request.subject is not None else "",
                        state=request.state,
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
        session = self._store_call(lambda: store.get_session(request.session_id.strip()))
        if session is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        _require_session_writable(session, request.subject.id.strip() if request.subject is not None else "")
        session = self._store_call(
            lambda: store.update_session(
                session_id=request.session_id.strip(),
                client_ref=request.client_ref.strip(),
                state=request.state,
                metadata=metadata,
            )
        )
        if session is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        return agent_session(session)

    def create_turn(self, request: gestalt.CreateAgentProviderTurnRequest) -> gestalt.AgentTurn:
        runner, store, config = self._require_runtime()
        tool_source_modes = ToolSourceModes(none=AGENT_TOOL_SOURCE_MODE_NONE, catalog=AGENT_TOOL_SOURCE_MODE_CATALOG)
        session_id = request.session_id.strip()
        session = self._store_call(lambda: store.get_session(session_id))
        if session is None:
            raise gestalt.Error(404, f"agent session {request.session_id!r} was not found")
        _require_session_writable(session, request.subject.id.strip() if request.subject is not None else "")
        try:
            schema = validate_turn_contract(request, session=session, tool_source_modes=tool_source_modes)
        except ValueError as exc:
            raise gestalt.Error(400, str(exc)) from exc
        try:
            create_request = turn_create_request_from_provider_request(
                request,
                config=config,
                session=session,
                schema=schema,
            )
        except ValueError as exc:
            raise gestalt.Error(400, str(exc)) from exc
        messages = prepend_session_start_context(create_request.messages, session.metadata)
        try:
            turn, created = self._store_call(
                lambda: store.begin_turn(
                    turn_id=create_request.turn_id,
                    session_id=create_request.session_id,
                    idempotency_key=create_request.idempotency_key,
                    provider_name=self._name,
                    model=create_request.model,
                    messages=messages,
                    created_by_subject_id=create_request.created_by_subject_id,
                    execution_ref=create_request.execution_ref,
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
                    "session": session,
                    "model": create_request.model,
                    "messages": list(turn.messages),
                    "request_context": getattr(request, "context", None),
                    "schema": schema,
                    "permission_mode": create_request.permission_mode,
                    "timeout_seconds": create_request.timeout_seconds,
                },
                daemon=True,
            ).start()
        return agent_turn(turn)

    def get_turn(self, request: gestalt.GetAgentProviderTurnRequest) -> gestalt.AgentTurn:
        _, store, _ = self._require_runtime()
        turn = self._store_call(lambda: store.get_turn(request.turn_id.strip()))
        if turn is None:
            raise gestalt.Error(404, f"agent turn {request.turn_id!r} was not found")
        session = self._session_for_turn(store, turn)
        _require_session_readable(session, request.subject.id.strip() if request.subject is not None else "")
        return agent_turn(turn)

    def list_turns(self, request: gestalt.ListAgentProviderTurnsRequest) -> gestalt.ListAgentProviderTurnsResponse:
        _, store, _ = self._require_runtime()
        limit = request.limit
        if limit < 0:
            raise gestalt.Error(400, "limit must be non-negative")
        summary_only = bool(request.summary_only)
        request_subject_id = request.subject.id.strip() if request.subject is not None else ""
        session_id = request.session_id.strip()
        turn_ids = [value.strip() for value in request.turn_ids]
        if session_id:
            session = self._store_call(lambda: store.get_session(session_id))
            if session is None or not session_readable_by(session, request_subject_id):
                return gestalt.ListAgentProviderTurnsResponse(turns=[])
        store_limit = 0 if turn_ids else limit
        turns = self._readable_turns(
            store,
            self._store_call(
                lambda: store.list_turns(
                    session_id=session_id,
                    turn_ids=turn_ids,
                    subject_id="",
                    status=request.status,
                    limit=store_limit,
                    summary_only=summary_only,
                )
            ),
            request_subject_id,
        )
        if turn_ids and limit > 0:
            turns = turns[:limit]
        return gestalt.ListAgentProviderTurnsResponse(
            turns=[agent_turn(turn, summary_only=summary_only) for turn in turns]
        )

    def cancel_turn(self, request: gestalt.CancelAgentProviderTurnRequest) -> gestalt.AgentTurn:
        runner, store, _ = self._require_runtime()
        existing = self._store_call(lambda: store.get_turn(request.turn_id.strip()))
        if existing is None:
            raise gestalt.Error(404, f"agent turn {request.turn_id!r} was not found")
        _require_session_writable(
            self._session_for_turn(store, existing), request.subject.id.strip() if request.subject is not None else ""
        )
        turn = self._store_call(
            lambda: store.cancel_turn(turn_id=request.turn_id.strip(), reason=request.reason.strip())
        )
        if turn is None:
            raise gestalt.Error(404, f"agent turn {request.turn_id!r} was not found")
        if turn.status == gestalt.AGENT_EXECUTION_STATUS_CANCELED:
            runner.cancel_turn(turn.turn_id)
        return agent_turn(turn)

    def list_turn_events(
        self, request: gestalt.ListAgentProviderTurnEventsRequest
    ) -> gestalt.ListAgentProviderTurnEventsResponse:
        _, store, _ = self._require_runtime()
        turn = self._store_call(lambda: store.get_turn(request.turn_id.strip()))
        if turn is None:
            return gestalt.ListAgentProviderTurnEventsResponse(events=[])
        session = self._session_for_turn(store, turn)
        if not session_readable_by(session, request.subject.id.strip() if request.subject is not None else ""):
            return gestalt.ListAgentProviderTurnEventsResponse(events=[])
        return gestalt.ListAgentProviderTurnEventsResponse(
            events=[
                agent_turn_event(event)
                for event in self._store_call(
                    lambda: store.list_turn_events(
                        turn_id=request.turn_id.strip(), after_seq=request.after_seq, limit=request.limit
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
            interactions=False,
            resumable_turns=False,
            reasoning_summaries=False,
            bounded_list_hydration=True,
            supported_tool_sources=[AGENT_TOOL_SOURCE_MODE_NONE, AGENT_TOOL_SOURCE_MODE_CATALOG],
            supports_session_start=True,
            supports_prepared_workspace=True,
        )
        return caps

    def _require_runtime(self) -> tuple[ClaudeSDKRunner, IndexedDBRunStore, ClaudeAgentConfig]:
        if self._runner is None or self._store is None or self._config is None:
            raise gestalt.Error(412, "agent provider has not been configured")
        return self._runner, self._store, self._config

    def _store_call(self, operation: Callable[[], Any]) -> Any:
        try:
            return operation()
        except StoreUnavailableError as exc:
            raise gestalt.Error(412, str(exc)) from exc

    def _session_for_turn(self, store: IndexedDBRunStore, turn: StoredTurn) -> StoredSession:
        session = self._store_call(lambda: store.get_session(turn.session_id))
        if session is None:
            raise gestalt.Error(404, f"agent session {turn.session_id!r} was not found")
        return session

    def _readable_turns(
        self, store: IndexedDBRunStore, turns: list[StoredTurn], request_subject_id: str
    ) -> list[StoredTurn]:
        if not request_subject_id:
            return []
        readable: list[StoredTurn] = []
        session_cache: dict[str, StoredSession | None] = {}
        for turn in turns:
            session = session_cache.get(turn.session_id)
            if turn.session_id not in session_cache:
                session = self._store_call(lambda session_id=turn.session_id: store.get_session(session_id))
                session_cache[turn.session_id] = session
            if session is not None and session_readable_by(session, request_subject_id):
                readable.append(turn)
        return readable

    def _complete_turn(
        self,
        *,
        runner: ClaudeSDKRunner,
        store: IndexedDBRunStore,
        turn_id: str,
        session: StoredSession,
        model: str,
        messages: list[dict[str, Any]],
        request_context: Any | None,
        schema: dict[str, Any] | None,
        permission_mode: str,
        timeout_seconds: float,
    ) -> None:
        try:
            output = runner.run_turn(
                session=session,
                turn_id=turn_id,
                model=model,
                messages=messages,
                request_context=request_context,
                schema=schema,
                permission_mode=permission_mode,
                timeout_seconds=timeout_seconds,
            )
        except ClaudeExecutionCanceled as exc:
            store.cancel_turn(turn_id=turn_id, reason=str(exc))
        except ClaudeExecutionError as exc:
            store.fail_turn(turn_id=turn_id, message=str(exc))
        except Exception as exc:
            logger.exception("Claude Agent SDK turn failed")
            store.fail_turn(turn_id=turn_id, message=str(exc))
        else:
            store.complete_turn(turn_id=turn_id, output=output)

    def _build_warnings(self, config: ClaudeAgentConfig) -> list[str]:
        warnings: list[str] = []
        if not config.anthropic_api_key:
            warnings.append("set config.anthropicApiKey before running live Claude turns")
        if config.cli_path and _resolve_claude_cli(config) is None:
            warnings.append(f"configured cliPath {config.cli_path!r} could not be resolved")
        return warnings


def _require_session_readable(session: StoredSession, request_subject_id: str) -> None:
    if not session_readable_by(session, request_subject_id):
        raise gestalt.Error(404, f"agent session {session.session_id!r} was not found")


def _require_session_writable(session: StoredSession, request_subject_id: str) -> None:
    if not session_writable_by(session, request_subject_id):
        raise gestalt.Error(403, f"agent session {session.session_id!r} is owned by another subject")


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


provider = ClaudeCodeAgentProvider()
