from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import gestalt
import grpc
from google.protobuf import json_format
from google.protobuf import struct_pb2 as _struct_pb2

struct_pb2: Any = _struct_pb2


class ProviderRequestError(ValueError):
    def __init__(self, code: grpc.StatusCode, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class CreateSessionInput:
    session_id: str
    requested_model: str
    metadata: dict[str, Any]
    prepared_workspace: dict[str, str] | None
    idempotency_key: str
    session_start: Any | None
    client_ref: str
    created_by: dict[str, str]

    @classmethod
    def from_proto(cls, request: Any) -> CreateSessionInput:
        session_id = _text(getattr(request, "session_id", ""))
        if not session_id:
            raise _invalid("session_id is required")
        try:
            prepared_workspace = prepared_workspace_to_dict(optional_field(request, "prepared_workspace"))
        except ValueError as exc:
            raise _invalid(str(exc)) from exc
        return cls(
            session_id=session_id,
            requested_model=_text(getattr(request, "model", "")),
            metadata=struct_to_dict(getattr(request, "metadata", None)),
            prepared_workspace=prepared_workspace,
            idempotency_key=_text(getattr(request, "idempotency_key", "")),
            session_start=optional_field(request, "session_start"),
            client_ref=_text(getattr(request, "client_ref", "")),
            created_by=actor_to_dict(getattr(request, "created_by", None)),
        )

    @property
    def has_session_start_hooks(self) -> bool:
        if self.session_start is None:
            return False
        return len(list(getattr(self.session_start, "hooks", []) or [])) > 0


@dataclass(frozen=True, slots=True)
class UpdateSessionInput:
    session_id: str
    client_ref: str
    state: int
    metadata: dict[str, Any] | None

    @classmethod
    def from_proto(cls, request: Any) -> UpdateSessionInput:
        metadata_field = optional_field(request, "metadata")
        return cls(
            session_id=_text(getattr(request, "session_id", "")),
            client_ref=_text(getattr(request, "client_ref", "")),
            state=int(getattr(request, "state", 0) or 0),
            metadata=None if metadata_field is None else struct_to_dict(metadata_field),
        )


@dataclass(frozen=True, slots=True)
class ListSessionsInput:
    session_ids: list[str]
    subject_id: str
    state: int
    limit: int
    summary_only: bool

    @classmethod
    def from_proto(cls, request: Any) -> ListSessionsInput:
        limit = int(getattr(request, "limit", 0) or 0)
        if limit < 0:
            raise _invalid("limit must be non-negative")
        return cls(
            session_ids=[_text(value) for value in getattr(request, "session_ids", [])],
            subject_id=subject_id(request),
            state=int(getattr(request, "state", 0) or 0),
            limit=limit,
            summary_only=bool(getattr(request, "summary_only", False)),
        )


@dataclass(frozen=True, slots=True)
class CreateTurnInput:
    turn_id: str
    session_id: str
    idempotency_key: str
    requested_model: str
    messages: list[dict[str, Any]]
    created_by: dict[str, str]
    execution_ref: str
    run_grant: str

    @classmethod
    def from_proto(cls, request: Any) -> CreateTurnInput:
        validate_create_turn_contract(request)
        return cls(
            turn_id=_text(getattr(request, "turn_id", "")),
            session_id=_text(getattr(request, "session_id", "")),
            idempotency_key=_text(getattr(request, "idempotency_key", "")),
            requested_model=_text(getattr(request, "model", "")),
            messages=messages_to_dicts(getattr(request, "messages", [])),
            created_by=actor_to_dict(getattr(request, "created_by", None)),
            execution_ref=_text(getattr(request, "execution_ref", "")),
            run_grant=_text(getattr(request, "run_grant", "")),
        )


@dataclass(frozen=True, slots=True)
class ListTurnsInput:
    session_id: str
    turn_ids: list[str]
    subject_id: str
    status: int
    limit: int
    summary_only: bool

    @classmethod
    def from_proto(cls, request: Any) -> ListTurnsInput:
        limit = int(getattr(request, "limit", 0) or 0)
        if limit < 0:
            raise _invalid("limit must be non-negative")
        return cls(
            session_id=_text(getattr(request, "session_id", "")),
            turn_ids=[_text(value) for value in getattr(request, "turn_ids", [])],
            subject_id=subject_id(request),
            status=int(getattr(request, "status", 0) or 0),
            limit=limit,
            summary_only=bool(getattr(request, "summary_only", False)),
        )


@dataclass(frozen=True, slots=True)
class CancelTurnInput:
    turn_id: str
    reason: str

    @classmethod
    def from_proto(cls, request: Any) -> CancelTurnInput:
        return cls(turn_id=_text(getattr(request, "turn_id", "")), reason=_text(getattr(request, "reason", "")))


@dataclass(frozen=True, slots=True)
class ListTurnEventsInput:
    turn_id: str
    after_seq: int
    limit: int

    @classmethod
    def from_proto(cls, request: Any) -> ListTurnEventsInput:
        return cls(
            turn_id=_text(getattr(request, "turn_id", "")),
            after_seq=int(getattr(request, "after_seq", 0) or 0),
            limit=int(getattr(request, "limit", 0) or 0),
        )


@dataclass(frozen=True, slots=True)
class ToolRefInput:
    plugin: str
    system: str
    operation: str
    connection: str
    instance: str
    title: str
    description: str

    @classmethod
    def from_proto(cls, ref: Any) -> ToolRefInput:
        return cls(
            plugin=_text(getattr(ref, "plugin", "")),
            system=_text(getattr(ref, "system", "")),
            operation=_text(getattr(ref, "operation", "")),
            connection=_text(getattr(ref, "connection", "")),
            instance=_text(getattr(ref, "instance", "")),
            title=_text(getattr(ref, "title", "")),
            description=_text(getattr(ref, "description", "")),
        )

    def validate(self, index: int) -> None:
        if "*" in {self.system, self.operation, self.connection, self.instance}:
            raise _invalid("wildcard tool_refs are not supported")
        if self.plugin == "*":
            self._validate_global_ref(index)
            return
        if self.system:
            self._validate_system_ref(index)
            return
        if not self.plugin:
            raise _invalid(f"tool_refs[{index}].plugin is required")

    def _validate_global_ref(self, index: int) -> None:
        if any([self.system, self.operation, self.connection, self.instance, self.title, self.description]):
            raise _invalid(
                f"tool_refs[{index}] global search ref cannot include operation, connection, instance, "
                "title, or description"
            )

    def _validate_system_ref(self, index: int) -> None:
        if self.plugin:
            raise _invalid(f"tool_refs[{index}] must set exactly one of plugin or system")
        if self.system != "workflow":
            raise _invalid(f"tool_refs[{index}].system {self.system!r} is not supported")
        if not self.operation:
            raise _invalid(f"tool_refs[{index}].operation is required for system tool refs")
        if any([self.connection, self.instance, self.title, self.description]):
            raise _invalid(f"tool_refs[{index}] system refs cannot include connection, instance, title, or description")


def validate_create_turn_contract(request: Any) -> None:
    if int(getattr(request, "tool_source", 0) or 0) != gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG:
        raise _invalid("agent/claude requires toolSource mcp_catalog")
    if not _text(getattr(request, "run_grant", "")):
        raise _invalid("run_grant is required")
    if len(list(getattr(request, "tools", []))) > 0:
        raise _invalid("resolved tools are not supported; use tool_refs with mcp_catalog")
    if struct_to_dict(getattr(request, "response_schema", None)):
        raise _invalid("response_schema is not supported by agent/claude")
    if struct_to_dict(getattr(request, "model_options", None)):
        raise _invalid("model_options are not supported by agent/claude")
    validate_tool_refs(list(getattr(request, "tool_refs", [])))


def validate_tool_refs(tool_refs: list[Any]) -> None:
    for index, ref in enumerate(tool_refs, start=1):
        ToolRefInput.from_proto(ref).validate(index)


def messages_to_dicts(messages: Any) -> list[dict[str, Any]]:
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


def messages_from_dicts(messages: list[dict[str, Any]]) -> list[Any]:
    out = []
    for message in messages:
        proto = gestalt.AgentMessage(role=str(message.get("role") or ""), text=str(message.get("text") or ""))
        for part in message.get("parts") or []:
            if not isinstance(part, dict):
                continue
            part_proto = gestalt.AgentMessagePart(type=int(part.get("type") or 0), text=str(part.get("text") or ""))
            if isinstance(part.get("json"), dict):
                part_proto.json.CopyFrom(dict_to_struct(part["json"]))
            if isinstance(part.get("tool_call"), dict):
                call = part["tool_call"]
                part_proto.tool_call.id = str(call.get("id") or "")
                part_proto.tool_call.tool_id = str(call.get("tool_id") or "")
                if isinstance(call.get("arguments"), dict):
                    part_proto.tool_call.arguments.CopyFrom(dict_to_struct(call["arguments"]))
            if isinstance(part.get("tool_result"), dict):
                result = part["tool_result"]
                part_proto.tool_result.tool_call_id = str(result.get("tool_call_id") or "")
                part_proto.tool_result.status = int(result.get("status") or 0)
                part_proto.tool_result.content = str(result.get("content") or "")
                if isinstance(result.get("output"), dict):
                    part_proto.tool_result.output.CopyFrom(dict_to_struct(result["output"]))
            if isinstance(part.get("image_ref"), dict):
                image = part["image_ref"]
                part_proto.image_ref.uri = str(image.get("uri") or "")
                part_proto.image_ref.mime_type = str(image.get("mime_type") or "")
            proto.parts.append(part_proto)
        out.append(proto)
    return out


def dict_to_struct(value: dict[str, Any]) -> Any:
    struct = struct_pb2.Struct()
    struct.update(value)
    return struct


def struct_to_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    return json_format.MessageToDict(value)


def prepared_workspace_to_dict(value: Any | None) -> dict[str, str] | None:
    if value is None:
        return None
    root = _text(getattr(value, "root", ""))
    cwd = _text(getattr(value, "cwd", ""))
    if not root and not cwd:
        return None
    if not root or not cwd:
        raise ValueError("prepared_workspace root and cwd are required")
    return {"root": root, "cwd": cwd}


def prepared_workspace_cwd(value: dict[str, str] | None) -> str:
    if not value:
        return ""
    return _text(value.get("cwd"))


def optional_field(message: Any, field_name: str) -> Any | None:
    if message is None or not hasattr(message, field_name):
        return None
    has_field = getattr(message, "HasField", None)
    if callable(has_field):
        try:
            if not has_field(field_name):
                return None
        except ValueError:
            return None
    return getattr(message, field_name)


def actor_to_dict(actor: Any) -> dict[str, str]:
    return {
        "subject_id": str(getattr(actor, "subject_id", "") or ""),
        "subject_kind": str(getattr(actor, "subject_kind", "") or ""),
        "display_name": str(getattr(actor, "display_name", "") or ""),
        "auth_source": str(getattr(actor, "auth_source", "") or ""),
    }


def subject_id(request: Any) -> str:
    subject = getattr(request, "subject", None)
    return _text(getattr(subject, "subject_id", ""))


def _invalid(message: str) -> ProviderRequestError:
    return ProviderRequestError(grpc.StatusCode.INVALID_ARGUMENT, message)


def _text(value: Any) -> str:
    return str(value or "").strip()
