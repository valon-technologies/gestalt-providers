import logging
import os
import threading
from typing import Any, cast

import gestalt
import grpc

from internals import SimpleAgentConfig, SimpleAgentOrchestrator, SimpleRunStore
from internals.store import StoredSession

logger = logging.getLogger(__name__)


class SimpleAgentRuntimeProvider(
    gestalt.AgentProvider, gestalt.MetadataProvider, gestalt.WarningsProvider, gestalt.Closer
):
    def __init__(self) -> None:
        self._name = "simple"
        self._warnings: list[str] = ["provider has not been configured"]
        self._config: SimpleAgentConfig | None = None
        self._store: SimpleRunStore | None = None
        self._orchestrator: SimpleAgentOrchestrator | None = None
        self._runtime_generation = 0

    def configure(self, name: str, config: dict[str, Any]) -> None:
        self._name = name.strip() or "simple"
        self._set_runtime(SimpleAgentConfig.from_dict(name=self._name, raw_config=config))

    def metadata(self) -> gestalt.ProviderMetadata:
        return gestalt.ProviderMetadata(
            kind=gestalt.ProviderKind.AGENT,
            name=self._name,
            display_name="Simple Agent",
            description="Simple multi-model agent provider for Gestalt with tool calling over the OpenAI and Anthropic SDKs.",
            version="0.0.1-alpha.34",
        )

    def warnings(self) -> list[str]:
        return list(self._warnings)

    def close(self) -> None:
        self._runtime_generation += 1
        if self._store is not None:
            self._store.close()

    def CreateSession(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, config = self._require_runtime(context)
        session_id = str(request.session_id or "").strip()
        if not session_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "session_id is required")
        try:
            resolved_model = config.resolve_model(str(request.model or ""))
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))

        try:
            started, _ = store.create_session(
                session_id=session_id,
                idempotency_key=str(request.idempotency_key or "").strip(),
                provider_name=self._name,
                model=resolved_model,
                client_ref=str(request.client_ref or "").strip(),
                metadata=gestalt.struct_to_dict(request.metadata),
                created_by=cast(dict[str, str], gestalt.agent_actor_to_dict(request.created_by)),
            )
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        return _session_to_proto(started)

    def GetSession(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        session = store.get_session(request.session_id)
        if session is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent session {request.session_id!r} was not found")
            raise RuntimeError("unreachable after context.abort")
        return _session_to_proto(session)

    def ListSessions(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        limit = int(getattr(request, "limit", 0) or 0)
        if limit < 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "limit must be non-negative")
        summary_only = bool(getattr(request, "summary_only", False))
        return gestalt.ListAgentProviderSessionsResponse(
            sessions=[
                _session_to_proto(session, summary_only=summary_only)
                for session in store.list_sessions(
                    session_ids=[str(value or "").strip() for value in getattr(request, "session_ids", [])],
                    subject_id=_subject_id(request),
                    state=int(getattr(request, "state", 0) or 0),
                    limit=limit,
                    summary_only=summary_only,
                )
            ]
        )

    def UpdateSession(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        session = store.update_session(
            session_id=str(request.session_id or "").strip(),
            client_ref=str(request.client_ref or "").strip(),
            state=int(request.state or 0),
            metadata=gestalt.struct_to_dict(request.metadata) if gestalt.has_field(request, "metadata") else None,
        )
        if session is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent session {request.session_id!r} was not found")
            raise RuntimeError("unreachable after context.abort")
        return _session_to_proto(session)

    def CreateTurn(self, request: Any, context: grpc.ServicerContext) -> Any:
        orchestrator, store, _ = self._require_runtime(context)
        session = store.get_session(str(request.session_id or "").strip())
        if session is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent session {request.session_id!r} was not found")
        return orchestrator.create_turn(
            request,
            context,
            session_model="" if session is None else session.model,
            provider_name=self._name if session is None else session.provider_name,
        )

    def GetTurn(self, request: Any, context: grpc.ServicerContext) -> Any:
        orchestrator, store, _ = self._require_runtime(context)
        turn = store.get_turn(request.turn_id)
        if turn is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent turn {request.turn_id!r} was not found")
            raise RuntimeError("unreachable after context.abort")
        return orchestrator.turn_to_proto(turn)

    def ListTurns(self, request: Any, context: grpc.ServicerContext) -> Any:
        orchestrator, store, _ = self._require_runtime(context)
        limit = int(getattr(request, "limit", 0) or 0)
        if limit < 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "limit must be non-negative")
        summary_only = bool(getattr(request, "summary_only", False))
        return gestalt.ListAgentProviderTurnsResponse(
            turns=[
                orchestrator.turn_to_proto(turn, summary_only=summary_only)
                for turn in store.list_turns(
                    str(request.session_id or "").strip(),
                    turn_ids=[str(value or "").strip() for value in getattr(request, "turn_ids", [])],
                    subject_id=_subject_id(request),
                    status=int(getattr(request, "status", 0) or 0),
                    limit=limit,
                    summary_only=summary_only,
                )
            ]
        )

    def CancelTurn(self, request: Any, context: grpc.ServicerContext) -> Any:
        orchestrator, store, _ = self._require_runtime(context)
        turn = store.cancel_turn(str(request.turn_id or "").strip(), str(request.reason or "").strip())
        if turn is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent turn {request.turn_id!r} was not found")
            raise RuntimeError("unreachable after context.abort")
        return orchestrator.turn_to_proto(turn)

    def ListTurnEvents(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        return gestalt.ListAgentProviderTurnEventsResponse(
            events=[
                _turn_event_to_proto(event)
                for event in store.list_turn_events(
                    turn_id=str(request.turn_id or "").strip(),
                    after_seq=int(request.after_seq or 0),
                    limit=int(request.limit or 0),
                )
            ]
        )

    def GetInteraction(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, _, _ = self._require_runtime(context)
        context.abort(grpc.StatusCode.NOT_FOUND, f"agent interaction {request.interaction_id!r} was not found")

    def ListInteractions(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, _, _ = self._require_runtime(context)
        return gestalt.ListAgentProviderInteractionsResponse(interactions=[])

    def ResolveInteraction(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, _, _ = self._require_runtime(context)
        context.abort(grpc.StatusCode.NOT_FOUND, f"agent interaction {request.interaction_id!r} was not found")

    def GetCapabilities(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, _, config = self._require_runtime(context)
        return gestalt.AgentProviderCapabilities(
            streaming_text=False,
            tool_calls=True,
            parallel_tool_calls=False,
            structured_output=True,
            interactions=False,
            resumable_turns=config.resume.enabled,
            reasoning_summaries=False,
            bounded_list_hydration=True,
            supported_tool_sources=[gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG],
        )

    def _require_runtime(
        self, context: grpc.ServicerContext
    ) -> tuple[SimpleAgentOrchestrator, SimpleRunStore, SimpleAgentConfig]:
        if self._orchestrator is None or self._store is None or self._config is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "agent provider has not been configured")
        return self._orchestrator, self._store, self._config

    def _set_runtime(self, config: SimpleAgentConfig) -> None:
        self._runtime_generation += 1
        resume_generation = self._runtime_generation
        if self._store is not None:
            self._store.close()
        self._config = config
        self._apply_backend_env(config)
        store = SimpleRunStore(run_store=config.run_store, idempotency_store=config.idempotency_store)
        orchestrator = SimpleAgentOrchestrator(config=config, store=store)
        self._store = store
        self._orchestrator = orchestrator
        self._warnings = self._build_warnings(config)
        if config.resume.enabled:
            threading.Thread(
                target=self._resume_incomplete_turns, args=(orchestrator, resume_generation), daemon=True
            ).start()

    def _resume_incomplete_turns(self, orchestrator: SimpleAgentOrchestrator, generation: int) -> None:
        def is_current_runtime() -> bool:
            return generation == self._runtime_generation

        if not is_current_runtime():
            return
        try:
            orchestrator.resume_incomplete_turns(should_continue=is_current_runtime)
        except grpc.RpcError:
            pass
        except Exception as exc:
            logger.warning("failed to resume incomplete turns during startup: %s", exc)

    def _build_warnings(self, config: SimpleAgentConfig) -> list[str]:
        warnings: list[str] = []
        if not config.default_model:
            warnings.append("set config.defaultModel or pass request.model for every turn")
        return warnings

    def _apply_backend_env(self, config: SimpleAgentConfig) -> None:
        if config.anthropic_api_key:
            os.environ["ANTHROPIC_API_KEY"] = config.anthropic_api_key
        if config.openai_api_key:
            os.environ["OPENAI_API_KEY"] = config.openai_api_key


def _session_to_proto(session: StoredSession, *, summary_only: bool = False) -> Any:
    proto = gestalt.AgentSession(
        id=session.session_id,
        provider_name=session.provider_name,
        model=session.model,
        client_ref=session.client_ref,
        state=session.state,
    )
    if session.metadata and not summary_only:
        proto.metadata.CopyFrom(gestalt.struct_from_dict(session.metadata))
    if session.created_by:
        proto.created_by.CopyFrom(gestalt.agent_actor_from_dict(session.created_by))
    proto.created_at.CopyFrom(gestalt.timestamp_from_datetime(session.created_at))
    proto.updated_at.CopyFrom(gestalt.timestamp_from_datetime(session.updated_at))
    if session.last_turn_at is not None:
        proto.last_turn_at.CopyFrom(gestalt.timestamp_from_datetime(session.last_turn_at))
    return proto


def _turn_event_to_proto(event: Any) -> Any:
    proto = gestalt.AgentTurnEvent(
        id=event.event_id,
        turn_id=event.turn_id,
        seq=event.seq,
        type=event.event_type,
        source=event.source,
        visibility=event.visibility,
    )
    if event.data:
        proto.data.CopyFrom(gestalt.struct_from_dict(event.data))
    proto.created_at.CopyFrom(gestalt.timestamp_from_datetime(event.created_at))
    return proto


def _subject_id(request: Any) -> str:
    subject = gestalt.agent_subject_context_to_dict(getattr(request, "subject", None))
    return str(subject.get("subject_id") or "").strip()


provider = SimpleAgentRuntimeProvider()
