from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable
from typing import Any, Never, TypeVar

import gestalt
import grpc

from internals import ClaudeAgentConfig, ClaudeSDKRunner, IndexedDBRunStore
from internals.claude_code_config import ClaudeCodeTurnOptions
from internals.claude_runner import ClaudeExecutionCanceled, ClaudeExecutionError
from internals.provider_io import (
    CancelTurnInput,
    CreateSessionInput,
    CreateTurnInput,
    ListSessionsInput,
    ListTurnEventsInput,
    ListTurnsInput,
    ProviderRequestError,
    UpdateSessionInput,
    prepared_workspace_cwd,
)
from internals.session_start import (
    prepend_session_start_context,
    run_session_start_hooks,
    validate_session_start_user_metadata,
)
from internals.store import StoreConflictError, StoreUnavailableError, StoredSession, StoredTurn, StoredTurnEvent

logger = logging.getLogger(__name__)
T = TypeVar("T")


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

    def CreateSession(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, config = self._require_runtime(context)
        create = _parse_request(context, lambda: CreateSessionInput.from_proto(request))
        try:
            model = config.resolve_model(create.requested_model)
        except ValueError as exc:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        try:
            validate_session_start_user_metadata(create.metadata)
        except ValueError as exc:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        metadata = create.metadata
        if create.has_session_start_hooks:
            with self._session_start_lock:
                existing = self._store_call(
                    context, lambda: _existing_session_for_create(store, create.session_id, create.idempotency_key)
                )
                if existing is not None:
                    return _session_to_proto(existing)
                try:
                    metadata = run_session_start_hooks(create.session_start, metadata)
                except Exception as exc:
                    _abort(context, grpc.StatusCode.FAILED_PRECONDITION, str(exc))
                return self._create_session(context=context, store=store, create=create, model=model, metadata=metadata)
        return self._create_session(context=context, store=store, create=create, model=model, metadata=metadata)

    def _create_session(
        self,
        *,
        context: grpc.ServicerContext,
        store: IndexedDBRunStore,
        create: CreateSessionInput,
        model: str,
        metadata: dict[str, Any],
    ) -> Any:
        session, _ = self._store_call(
            context,
            lambda: store.create_session(
                session_id=create.session_id,
                idempotency_key=create.idempotency_key,
                provider_name=self._name,
                model=model,
                client_ref=create.client_ref,
                metadata=metadata,
                prepared_workspace=create.prepared_workspace,
                created_by=create.created_by,
            ),
        )
        return _session_to_proto(session)

    def GetSession(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        session = self._store_call(context, lambda: store.get_session(str(request.session_id or "").strip()))
        if session is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent session {request.session_id!r} was not found")
            raise RuntimeError("unreachable after context.abort")
        return _session_to_proto(session)

    def ListSessions(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        query = _parse_request(context, lambda: ListSessionsInput.from_proto(request))
        return gestalt.ListAgentProviderSessionsResponse(
            sessions=[
                _session_to_proto(session, summary_only=query.summary_only)
                for session in self._store_call(
                    context,
                    lambda: store.list_sessions(
                        session_ids=query.session_ids,
                        subject_id=query.subject_id,
                        state=query.state,
                        limit=query.limit,
                        summary_only=query.summary_only,
                    ),
                )
            ]
        )

    def UpdateSession(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        update = _parse_request(context, lambda: UpdateSessionInput.from_proto(request))
        try:
            validate_session_start_user_metadata(update.metadata)
        except ValueError as exc:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        session = self._store_call(
            context,
            lambda: store.update_session(
                session_id=update.session_id, client_ref=update.client_ref, state=update.state, metadata=update.metadata
            ),
        )
        if session is None:
            _abort(context, grpc.StatusCode.NOT_FOUND, f"agent session {request.session_id!r} was not found")
        return _session_to_proto(session)

    def CreateTurn(self, request: Any, context: grpc.ServicerContext) -> Any:
        runner, store, config = self._require_runtime(context)
        create = _parse_request(context, lambda: CreateTurnInput.from_proto(request))
        session = self._store_call(context, lambda: store.get_session(create.session_id))
        if session is None:
            _abort(context, grpc.StatusCode.NOT_FOUND, f"agent session {request.session_id!r} was not found")
        if not create.messages:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, "messages must contain at least one entry")
        try:
            model = config.resolve_model(create.requested_model or session.model)
        except ValueError as exc:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, str(exc))

        messages = prepend_session_start_context(create.messages, session.metadata)
        cwd = prepared_workspace_cwd(session.prepared_workspace)
        try:
            claude_code_options = config.claude_code.resolve_turn_options(session.metadata)
        except ValueError as exc:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        try:
            turn, created = self._store_call(
                context,
                lambda: store.begin_turn(
                    turn_id=create.turn_id,
                    session_id=create.session_id,
                    idempotency_key=create.idempotency_key,
                    provider_name=self._name,
                    model=model,
                    messages=messages,
                    created_by=create.created_by,
                    execution_ref=create.execution_ref,
                ),
            )
        except StoreConflictError as exc:
            _abort(context, grpc.StatusCode.ALREADY_EXISTS, str(exc))
        except ValueError as exc:
            _abort(context, grpc.StatusCode.INVALID_ARGUMENT, str(exc))
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
                    "run_grant": create.run_grant,
                    "claude_code_options": claude_code_options,
                    "cwd": cwd,
                },
                daemon=True,
            ).start()
        return _turn_to_proto(turn)

    def GetTurn(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        turn = self._store_call(context, lambda: store.get_turn(str(request.turn_id or "").strip()))
        if turn is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent turn {request.turn_id!r} was not found")
            raise RuntimeError("unreachable after context.abort")
        return _turn_to_proto(turn)

    def ListTurns(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        query = _parse_request(context, lambda: ListTurnsInput.from_proto(request))
        return gestalt.ListAgentProviderTurnsResponse(
            turns=[
                _turn_to_proto(turn, summary_only=query.summary_only)
                for turn in self._store_call(
                    context,
                    lambda: store.list_turns(
                        session_id=query.session_id,
                        turn_ids=query.turn_ids,
                        subject_id=query.subject_id,
                        status=query.status,
                        limit=query.limit,
                        summary_only=query.summary_only,
                    ),
                )
            ]
        )

    def CancelTurn(self, request: Any, context: grpc.ServicerContext) -> Any:
        runner, store, _ = self._require_runtime(context)
        cancel = CancelTurnInput.from_proto(request)
        turn = self._store_call(context, lambda: store.cancel_turn(turn_id=cancel.turn_id, reason=cancel.reason))
        if turn is None:
            _abort(context, grpc.StatusCode.NOT_FOUND, f"agent turn {request.turn_id!r} was not found")
        if turn.status == gestalt.AGENT_EXECUTION_STATUS_CANCELED:
            runner.cancel_turn(turn.turn_id)
        return _turn_to_proto(turn)

    def ListTurnEvents(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        query = ListTurnEventsInput.from_proto(request)
        return gestalt.ListAgentProviderTurnEventsResponse(
            events=[
                _turn_event_to_proto(event)
                for event in self._store_call(
                    context,
                    lambda: store.list_turn_events(turn_id=query.turn_id, after_seq=query.after_seq, limit=query.limit),
                )
            ]
        )

    def GetInteraction(self, request: Any, context: grpc.ServicerContext) -> Any:
        self._require_runtime(context)
        context.abort(grpc.StatusCode.NOT_FOUND, f"agent interaction {request.interaction_id!r} was not found")

    def ListInteractions(self, request: Any, context: grpc.ServicerContext) -> Any:
        self._require_runtime(context)
        return gestalt.ListAgentProviderInteractionsResponse(interactions=[])

    def ResolveInteraction(self, request: Any, context: grpc.ServicerContext) -> Any:
        self._require_runtime(context)
        context.abort(grpc.StatusCode.NOT_FOUND, f"agent interaction {request.interaction_id!r} was not found")

    def GetCapabilities(self, request: Any, context: grpc.ServicerContext) -> Any:
        self._require_runtime(context)
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

    def _require_runtime(
        self, context: grpc.ServicerContext
    ) -> tuple[ClaudeSDKRunner, IndexedDBRunStore, ClaudeAgentConfig]:
        if self._runner is None or self._store is None or self._config is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "agent provider has not been configured")
        return self._runner, self._store, self._config

    def _store_call(self, context: grpc.ServicerContext, operation: Any) -> Any:
        try:
            return operation()
        except StoreUnavailableError as exc:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(exc))
            raise RuntimeError("unreachable after context.abort") from exc

    def _complete_turn(
        self,
        *,
        runner: ClaudeSDKRunner,
        store: IndexedDBRunStore,
        turn_id: str,
        session_id: str,
        model: str,
        messages: list[dict[str, Any]],
        run_grant: str,
        claude_code_options: ClaudeCodeTurnOptions,
        cwd: str,
    ) -> None:
        try:
            if claude_code_options.plugins:
                logger.info(
                    "starting Claude Agent SDK turn with configured Claude Code plugins",
                    extra={
                        "plugin_names": claude_code_options.plugin_names,
                        "plugin_count": len(claude_code_options.plugins),
                    },
                )
            output = runner.run_turn(
                session_id=session_id,
                turn_id=turn_id,
                model=model,
                messages=messages,
                run_grant=run_grant,
                claude_code_options=claude_code_options,
                cwd=cwd,
            )
        except ClaudeExecutionCanceled as exc:
            store.cancel_turn(turn_id=turn_id, reason=str(exc))
        except ClaudeExecutionError as exc:
            store.fail_turn(turn_id=turn_id, message=str(exc))
        except Exception as exc:
            logger.exception("Claude Agent SDK turn failed")
            store.fail_turn(turn_id=turn_id, message=str(exc))
        else:
            store.complete_turn(turn_id=turn_id, output_text=output)

    def _build_warnings(self, config: ClaudeAgentConfig) -> list[str]:
        warnings: list[str] = []
        if not config.anthropic_api_key and not os.environ.get("ANTHROPIC_API_KEY"):
            warnings.append("set config.anthropicApiKey or ANTHROPIC_API_KEY before running live Claude turns")
        if config.cli_path and _resolve_claude_cli(config) is None:
            warnings.append(f"configured cliPath {config.cli_path!r} could not be resolved")
        return warnings


def _parse_request(context: grpc.ServicerContext, parse: Callable[[], T]) -> T:
    try:
        return parse()
    except ProviderRequestError as exc:
        _abort(context, exc.code, str(exc))


def _abort(context: grpc.ServicerContext, code: grpc.StatusCode, message: str) -> Never:
    context.abort(code, message)
    raise RuntimeError("unreachable after context.abort")


def _session_to_proto(session: StoredSession, *, summary_only: bool = False) -> Any:
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


def _turn_to_proto(turn: StoredTurn, *, summary_only: bool = False) -> Any:
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


def _turn_event_to_proto(event: StoredTurnEvent) -> Any:
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
