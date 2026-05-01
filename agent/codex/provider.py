from __future__ import annotations

import logging
import os
import threading
from datetime import UTC, datetime
from typing import Any, cast

import gestalt
import grpc
from google.protobuf import json_format
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from gestalt.gen.v1 import agent_pb2 as _agent_pb2
from internals.codex_runner import CodexExecutionCanceled, CodexExecutionError
from internals.codex_runner import CodexMCPRunner
from internals.config import CodexAgentConfig
from internals.store import StoreConflictError, StoredSession, StoredTurn, StoredTurnEvent
from internals.store import InMemoryRunStore

agent_pb2: Any = cast(Any, _agent_pb2)
struct_pb2: Any = cast(Any, _struct_pb2)
timestamp_pb2: Any = cast(Any, _timestamp_pb2)
logger = logging.getLogger(__name__)


class CodexMCPAgentProvider(gestalt.AgentProvider, gestalt.MetadataProvider, gestalt.WarningsProvider, gestalt.Closer):
    def __init__(self) -> None:
        self._name = "codex"
        self._warnings: list[str] = ["provider has not been configured"]
        self._config: CodexAgentConfig | None = None
        self._store: InMemoryRunStore | None = None
        self._runner: CodexMCPRunner | None = None

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
            version="0.0.1-alpha.2",
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
        session_id = str(request.session_id or "").strip()
        if not session_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "session_id is required")
        try:
            model = config.resolve_model(str(request.model or ""))
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        session, _ = store.create_session(
            session_id=session_id,
            idempotency_key=str(request.idempotency_key or "").strip(),
            provider_name=self._name,
            model=model,
            client_ref=str(request.client_ref or "").strip(),
            metadata=_struct_to_dict(request.metadata),
            created_by=_actor_to_dict(request.created_by),
        )
        return _session_to_proto(session)

    def GetSession(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        session = store.get_session(str(request.session_id or "").strip())
        if session is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent session {request.session_id!r} was not found")
            raise RuntimeError("unreachable after context.abort")
        return _session_to_proto(session)

    def ListSessions(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        limit = int(getattr(request, "limit", 0) or 0)
        if limit < 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "limit must be non-negative")
        return agent_pb2.ListAgentProviderSessionsResponse(
            sessions=[
                _session_to_proto(session, summary_only=bool(getattr(request, "summary_only", False)))
                for session in store.list_sessions(
                    session_ids=[str(value or "").strip() for value in getattr(request, "session_ids", [])],
                    subject_id=_subject_id(request),
                    state=int(getattr(request, "state", 0) or 0),
                    limit=limit,
                )
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
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent session {request.session_id!r} was not found")
            raise RuntimeError("unreachable after context.abort")
        return _session_to_proto(session)

    def CreateTurn(self, request: Any, context: grpc.ServicerContext) -> Any:
        runner, store, config = self._require_runtime(context)
        _validate_create_turn_request(request, context)
        session = store.get_session(str(request.session_id or "").strip())
        if session is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent session {request.session_id!r} was not found")
            raise RuntimeError("unreachable after context.abort")
        if len(list(request.messages)) == 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "messages must contain at least one entry")
        try:
            model = config.resolve_model(str(request.model or "").strip() or session.model)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))

        try:
            turn, created = store.begin_turn(
                turn_id=str(request.turn_id or "").strip(),
                session_id=str(request.session_id or "").strip(),
                idempotency_key=str(request.idempotency_key or "").strip(),
                provider_name=self._name,
                model=model,
                messages=_messages_to_dicts(request.messages),
                created_by=_actor_to_dict(request.created_by),
                execution_ref=str(request.execution_ref or "").strip(),
            )
        except StoreConflictError as exc:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, str(exc))
            raise RuntimeError("unreachable after context.abort")
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
            raise RuntimeError("unreachable after context.abort")
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
                    "tool_grant": str(request.tool_grant or "").strip(),
                },
                daemon=True,
            ).start()
        return _turn_to_proto(turn)

    def GetTurn(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        turn = store.get_turn(str(request.turn_id or "").strip())
        if turn is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent turn {request.turn_id!r} was not found")
            raise RuntimeError("unreachable after context.abort")
        return _turn_to_proto(turn)

    def ListTurns(self, request: Any, context: grpc.ServicerContext) -> Any:
        _, store, _ = self._require_runtime(context)
        limit = int(getattr(request, "limit", 0) or 0)
        if limit < 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "limit must be non-negative")
        return agent_pb2.ListAgentProviderTurnsResponse(
            turns=[
                _turn_to_proto(turn, summary_only=bool(getattr(request, "summary_only", False)))
                for turn in store.list_turns(
                    session_id=str(request.session_id or "").strip(),
                    turn_ids=[str(value or "").strip() for value in getattr(request, "turn_ids", [])],
                    subject_id=_subject_id(request),
                    status=int(getattr(request, "status", 0) or 0),
                    limit=limit,
                )
            ]
        )

    def CancelTurn(self, request: Any, context: grpc.ServicerContext) -> Any:
        runner, store, _ = self._require_runtime(context)
        turn = store.cancel_turn(turn_id=str(request.turn_id or "").strip(), reason=str(request.reason or "").strip())
        if turn is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"agent turn {request.turn_id!r} was not found")
            raise RuntimeError("unreachable after context.abort")
        if turn.status == agent_pb2.AGENT_EXECUTION_STATUS_CANCELED:
            runner.cancel_turn(turn.turn_id)
        return _turn_to_proto(turn)

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
        self._require_runtime(context)
        context.abort(grpc.StatusCode.NOT_FOUND, f"agent interaction {request.interaction_id!r} was not found")

    def ListInteractions(self, request: Any, context: grpc.ServicerContext) -> Any:
        self._require_runtime(context)
        return agent_pb2.ListAgentProviderInteractionsResponse(interactions=[])

    def ResolveInteraction(self, request: Any, context: grpc.ServicerContext) -> Any:
        self._require_runtime(context)
        context.abort(grpc.StatusCode.NOT_FOUND, f"agent interaction {request.interaction_id!r} was not found")

    def GetCapabilities(self, request: Any, context: grpc.ServicerContext) -> Any:
        self._require_runtime(context)
        return agent_pb2.AgentProviderCapabilities(
            streaming_text=False,
            tool_calls=True,
            parallel_tool_calls=False,
            structured_output=False,
            interactions=False,
            resumable_turns=False,
            reasoning_summaries=False,
            bounded_list_hydration=True,
            supported_tool_sources=[agent_pb2.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG],
        )

    def _require_runtime(
        self, context: grpc.ServicerContext
    ) -> tuple[CodexMCPRunner, InMemoryRunStore, CodexAgentConfig]:
        if self._runner is None or self._store is None or self._config is None:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "agent provider has not been configured")
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
        tool_grant: str,
    ) -> None:
        try:
            output = runner.run_turn(
                session_id=session_id, turn_id=turn_id, model=model, messages=messages, tool_grant=tool_grant
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


def _session_to_proto(session: StoredSession, *, summary_only: bool = False) -> Any:
    proto = agent_pb2.AgentSession(
        id=session.session_id,
        provider_name=session.provider_name,
        model=session.model,
        client_ref=session.client_ref,
        state=session.state,
    )
    if session.metadata and not summary_only:
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


def _turn_to_proto(turn: StoredTurn, *, summary_only: bool = False) -> Any:
    proto = agent_pb2.AgentTurn(
        id=turn.turn_id,
        session_id=turn.session_id,
        provider_name=turn.provider_name,
        model=turn.model,
        status=turn.status,
        output_text="" if summary_only else turn.output_text,
        status_message=turn.status_message,
        execution_ref=turn.execution_ref,
    )
    if not summary_only:
        proto.messages.extend(_messages_from_dicts(turn.messages))
    if turn.created_by:
        proto.created_by.CopyFrom(
            agent_pb2.AgentActor(
                subject_id=turn.created_by.get("subject_id", ""),
                subject_kind=turn.created_by.get("subject_kind", ""),
                display_name=turn.created_by.get("display_name", ""),
                auth_source=turn.created_by.get("auth_source", ""),
            )
        )
    proto.created_at.CopyFrom(_datetime_to_timestamp(turn.created_at))
    if turn.started_at is not None:
        proto.started_at.CopyFrom(_datetime_to_timestamp(turn.started_at))
    if turn.completed_at is not None:
        proto.completed_at.CopyFrom(_datetime_to_timestamp(turn.completed_at))
    return proto


def _turn_event_to_proto(event: StoredTurnEvent) -> Any:
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


def _validate_create_turn_request(request: Any, context: grpc.ServicerContext) -> None:
    if int(getattr(request, "tool_source", 0) or 0) != agent_pb2.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "agent/codex requires toolSource mcp_catalog")
    if not str(request.tool_grant or "").strip():
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tool_grant is required")
    if len(list(getattr(request, "tools", []))) > 0:
        context.abort(
            grpc.StatusCode.INVALID_ARGUMENT, "resolved tools are not supported; use tool_refs with mcp_catalog"
        )
    if _struct_to_dict(getattr(request, "response_schema", None)):
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "response_schema is not supported by agent/codex")
    if _struct_to_dict(getattr(request, "provider_options", None)):
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "provider_options are not supported by agent/codex")
    _validate_tool_refs(list(getattr(request, "tool_refs", [])), context)


def _validate_tool_refs(tool_refs: list[Any], context: grpc.ServicerContext) -> None:
    if not tool_refs:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "tool_refs are required for mcp_catalog turns")
    for index, ref in enumerate(tool_refs, start=1):
        plugin = str(getattr(ref, "plugin", "") or "").strip()
        system = str(getattr(ref, "system", "") or "").strip()
        operation = str(getattr(ref, "operation", "") or "").strip()
        connection = str(getattr(ref, "connection", "") or "").strip()
        instance = str(getattr(ref, "instance", "") or "").strip()
        if not operation:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"tool_refs[{index}].operation is required")
        if "*" in {plugin, system, operation, connection, instance}:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "wildcard tool_refs are not supported")
        if bool(plugin) == bool(system):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, f"tool_refs[{index}] must set exactly one of plugin or system"
            )
        if system and system != "workflow":
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"tool_refs[{index}].system {system!r} is not supported")


def _messages_to_dicts(messages: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for message in messages:
        item: dict[str, Any] = {"role": str(message.role or ""), "text": str(message.text or "")}
        parts: list[dict[str, Any]] = []
        for part in message.parts:
            part_item: dict[str, Any] = {"type": int(part.type or 0)}
            if part.text:
                part_item["text"] = part.text
            if part.HasField("json"):
                part_item["json"] = json_format.MessageToDict(part.json)
            if part.HasField("tool_call"):
                part_item["tool_call"] = {
                    "id": part.tool_call.id,
                    "tool_id": part.tool_call.tool_id,
                    "arguments": json_format.MessageToDict(part.tool_call.arguments),
                }
            if part.HasField("tool_result"):
                part_item["tool_result"] = {
                    "tool_call_id": part.tool_result.tool_call_id,
                    "status": part.tool_result.status,
                    "content": part.tool_result.content,
                    "output": json_format.MessageToDict(part.tool_result.output),
                }
            if part.HasField("image_ref"):
                part_item["image_ref"] = {"uri": part.image_ref.uri, "mime_type": part.image_ref.mime_type}
            parts.append(part_item)
        if parts:
            item["parts"] = parts
        out.append(item)
    return out


def _messages_from_dicts(messages: list[dict[str, Any]]) -> list[Any]:
    out = []
    for message in messages:
        proto = agent_pb2.AgentMessage(role=str(message.get("role") or ""), text=str(message.get("text") or ""))
        for part in message.get("parts") or []:
            if not isinstance(part, dict):
                continue
            part_proto = agent_pb2.AgentMessagePart(type=int(part.get("type") or 0), text=str(part.get("text") or ""))
            if isinstance(part.get("json"), dict):
                part_proto.json.CopyFrom(_dict_to_struct(part["json"]))
            if isinstance(part.get("tool_call"), dict):
                call = part["tool_call"]
                part_proto.tool_call.id = str(call.get("id") or "")
                part_proto.tool_call.tool_id = str(call.get("tool_id") or "")
                if isinstance(call.get("arguments"), dict):
                    part_proto.tool_call.arguments.CopyFrom(_dict_to_struct(call["arguments"]))
            if isinstance(part.get("tool_result"), dict):
                result = part["tool_result"]
                part_proto.tool_result.tool_call_id = str(result.get("tool_call_id") or "")
                part_proto.tool_result.status = int(result.get("status") or 0)
                part_proto.tool_result.content = str(result.get("content") or "")
                if isinstance(result.get("output"), dict):
                    part_proto.tool_result.output.CopyFrom(_dict_to_struct(result["output"]))
            if isinstance(part.get("image_ref"), dict):
                image = part["image_ref"]
                part_proto.image_ref.uri = str(image.get("uri") or "")
                part_proto.image_ref.mime_type = str(image.get("mime_type") or "")
            proto.parts.append(part_proto)
        out.append(proto)
    return out


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


def _subject_id(request: Any) -> str:
    subject = getattr(request, "subject", None)
    return str(getattr(subject, "subject_id", "") or "").strip()


def _datetime_to_timestamp(value: datetime) -> Any:
    stamp = timestamp_pb2.Timestamp()
    stamp.FromDatetime(value.astimezone(UTC))
    return stamp


provider = CodexMCPAgentProvider()
