import os
from datetime import UTC, datetime
from typing import Any

import gestalt
import grpc
from google.protobuf import json_format
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from internals import SimpleAgentConfig, SimpleAgentOrchestrator, SimpleRunStore
from internals.agent_proto_compat import agent_pb2
from internals.store import StoredSession

struct_pb2: Any = _struct_pb2
timestamp_pb2: Any = _timestamp_pb2


class SimpleAgentRuntimeProvider(
    gestalt.AgentProvider, gestalt.MetadataProvider, gestalt.WarningsProvider, gestalt.Closer
):
    def __init__(self) -> None:
        self._name = "simple"
        self._warnings: list[str] = ["provider has not been configured"]
        self._config: SimpleAgentConfig | None = None
        self._store: SimpleRunStore | None = None
        self._orchestrator: SimpleAgentOrchestrator | None = None

    def configure(self, name: str, config: dict[str, Any]) -> None:
        self._name = name.strip() or "simple"
        self._set_runtime(SimpleAgentConfig.from_dict(name=self._name, raw_config=config))

    def metadata(self) -> gestalt.ProviderMetadata:
        return gestalt.ProviderMetadata(
            kind=gestalt.ProviderKind.AGENT,
            name=self._name,
            display_name="Simple Agent",
            description="Simple multi-model agent provider for Gestalt with tool calling over the OpenAI and Anthropic SDKs.",
            version="0.0.1-alpha.15",
        )

    def warnings(self) -> list[str]:
        return list(self._warnings)

    def close(self) -> None:
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
                metadata=_struct_to_dict(request.metadata),
                created_by=_actor_to_dict(request.created_by),
            )
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        return _session_to_proto(started)

    def GetSession(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        session = store.get_session(request.session_id)
        if session is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"agent session {request.session_id!r} was not found",
            )
            raise RuntimeError("unreachable after context.abort")
        return _session_to_proto(session)

    def ListSessions(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        return agent_pb2.ListAgentProviderSessionsResponse(
            sessions=[
                _session_to_proto(session) for session in store.list_sessions()
            ]
        )

    def UpdateSession(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        session = store.update_session(
            session_id=str(request.session_id or "").strip(),
            client_ref=str(request.client_ref or "").strip(),
            state=int(request.state or 0),
            metadata=_struct_to_dict(request.metadata) if request.HasField("metadata") else None,
        )
        if session is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"agent session {request.session_id!r} was not found",
            )
            raise RuntimeError("unreachable after context.abort")
        return _session_to_proto(session)

    def CreateTurn(self, request: Any, context: grpc.ServicerContext) -> Any:
        orchestrator, store, _ = self._require_runtime(context)
        session = store.get_session(str(request.session_id or "").strip())
        if session is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"agent session {request.session_id!r} was not found",
            )
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
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"agent turn {request.turn_id!r} was not found",
            )
            raise RuntimeError("unreachable after context.abort")
        return orchestrator.turn_to_proto(turn)

    def ListTurns(self, request: Any, context: grpc.ServicerContext) -> Any:
        orchestrator, store, _ = self._require_runtime(context)
        return agent_pb2.ListAgentProviderTurnsResponse(
            turns=[
                orchestrator.turn_to_proto(turn)
                for turn in store.list_turns(str(request.session_id or "").strip())
            ]
        )

    def CancelTurn(self, request: Any, context: grpc.ServicerContext) -> Any:
        orchestrator, store, _ = self._require_runtime(context)
        turn = store.cancel_turn(
            str(request.turn_id or "").strip(),
            str(request.reason or "").strip(),
        )
        if turn is None:
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"agent turn {request.turn_id!r} was not found",
            )
            raise RuntimeError("unreachable after context.abort")
        return orchestrator.turn_to_proto(turn)

    def ListTurnEvents(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        return agent_pb2.ListAgentProviderTurnEventsResponse(
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
        context.abort(
            grpc.StatusCode.NOT_FOUND,
            f"agent interaction {request.interaction_id!r} was not found",
        )

    def ListInteractions(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, _, _ = self._require_runtime(context)
        return agent_pb2.ListAgentProviderInteractionsResponse(interactions=[])

    def ResolveInteraction(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, _, _ = self._require_runtime(context)
        context.abort(
            grpc.StatusCode.NOT_FOUND,
            f"agent interaction {request.interaction_id!r} was not found",
        )

    def GetCapabilities(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, _, _ = self._require_runtime(context)
        return agent_pb2.AgentProviderCapabilities(
            streaming_text=False,
            tool_calls=True,
            parallel_tool_calls=False,
            structured_output=True,
            interactions=False,
            resumable_turns=False,
            reasoning_summaries=False,
        )

    def _require_runtime(
        self, context: grpc.ServicerContext
    ) -> tuple[SimpleAgentOrchestrator, SimpleRunStore, SimpleAgentConfig]:
        if self._orchestrator is None or self._store is None or self._config is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "agent provider has not been configured")
        return self._orchestrator, self._store, self._config

    def _set_runtime(self, config: SimpleAgentConfig) -> None:
        if self._store is not None:
            self._store.close()
        self._config = config
        self._apply_backend_env(config)
        self._store = SimpleRunStore(run_store=config.run_store, idempotency_store=config.idempotency_store)
        self._orchestrator = SimpleAgentOrchestrator(config=config, store=self._store)
        self._warnings = self._build_warnings(config)

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


def _session_to_proto(session: StoredSession) -> Any:
    proto = agent_pb2.AgentSession(
        id=session.session_id,
        provider_name=session.provider_name,
        model=session.model,
        client_ref=session.client_ref,
        state=session.state,
    )
    if session.metadata:
        proto.metadata.CopyFrom(_dict_to_struct(session.metadata))
    if session.created_by:
        proto.created_by.CopyFrom(
            agent_pb2.AgentActor(
                subject_id=session.created_by.get("subject_id", ""),
                subject_kind=session.created_by.get("subject_kind", ""),
                display_name=session.created_by.get("display_name", ""),
                auth_source=session.created_by.get("auth_source", ""),
            )
        )
    proto.created_at.CopyFrom(_datetime_to_timestamp(session.created_at))
    proto.updated_at.CopyFrom(_datetime_to_timestamp(session.updated_at))
    if session.last_turn_at is not None:
        proto.last_turn_at.CopyFrom(_datetime_to_timestamp(session.last_turn_at))
    return proto


def _turn_event_to_proto(event: Any) -> Any:
    proto = agent_pb2.AgentTurnEvent(
        id=event.event_id,
        turn_id=event.turn_id,
        seq=event.seq,
        type=event.event_type,
        source=event.source,
        visibility=event.visibility,
    )
    if event.data:
        proto.data.CopyFrom(_dict_to_struct(event.data))
    proto.created_at.CopyFrom(_datetime_to_timestamp(event.created_at))
    return proto


def _dict_to_struct(value: dict[str, Any]) -> Any:
    struct = struct_pb2.Struct()
    struct.update(value)
    return struct


def _struct_to_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    return json_format.MessageToDict(value)


def _actor_to_dict(actor: Any) -> dict[str, str]:
    return {
        "subject_id": str(getattr(actor, "subject_id", "") or ""),
        "subject_kind": str(getattr(actor, "subject_kind", "") or ""),
        "display_name": str(getattr(actor, "display_name", "") or ""),
        "auth_source": str(getattr(actor, "auth_source", "") or ""),
    }


def _datetime_to_timestamp(value: datetime) -> Any:
    stamp = timestamp_pb2.Timestamp()
    stamp.FromDatetime(value.astimezone(UTC))
    return stamp



provider = SimpleAgentRuntimeProvider()
