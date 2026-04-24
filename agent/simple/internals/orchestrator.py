import json
import re
import threading
import copy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import gestalt
import grpc
from google.protobuf import json_format
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from jsonschema import ValidationError, validate

from .agent_proto_compat import agent_pb2
from .config import SimpleAgentConfig
from .model_backend import ModelBackend
from .store import SimpleRunStore, StoredRun

struct_pb2: Any = _struct_pb2
timestamp_pb2: Any = _timestamp_pb2


class SimpleAgentOrchestrator:
    def __init__(self, *, config: SimpleAgentConfig, store: SimpleRunStore) -> None:
        self._config = config
        self._store = store
        self._backend = ModelBackend(config)

    def create_turn(
        self,
        request: Any,
        context: grpc.ServicerContext,
        *,
        session_model: str = "",
        provider_name: str = "",
    ) -> Any:
        turn_id = str(request.turn_id or "").strip()
        if not turn_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "turn_id is required")
        session_id = str(request.session_id or "").strip()
        if not session_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "session_id is required")
        if len(list(request.messages)) == 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "messages must contain at least one entry")

        try:
            resolved_model = self._config.resolve_model(
                str(request.model or "").strip() or session_model
            )
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))

        try:
            started, created = self._store.begin_turn(
                turn_id=turn_id,
                session_id=session_id,
                idempotency_key=str(request.idempotency_key or "").strip(),
                provider_name=provider_name.strip(),
                model=resolved_model,
                messages=_project_messages(request.messages),
                created_by=_actor_to_dict(request.created_by),
                execution_ref=str(request.execution_ref or "").strip(),
            )
        except ValueError as exc:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, str(exc))
        if not created:
            return self.turn_to_proto(started)

        prepared = PreparedTurn(
            session_id=session_id,
            turn_id=turn_id,
            resolved_model=resolved_model,
            messages=list(started.messages),
            response_schema=_struct_to_dict(request.response_schema),
            provider_options=_struct_to_dict(request.provider_options),
            tool_specs_and_names=_resolved_tools_to_openai(request.tools),
        )
        threading.Thread(target=self._complete_turn, args=(prepared,), daemon=True).start()
        return self.turn_to_proto(started)

    def _complete_turn(self, prepared: "PreparedTurn") -> None:
        tool_specs, function_name_to_tool_id = prepared.tool_specs_and_names
        conversation = _build_initial_conversation(
            system_prompt=self._config.system_prompt,
            projected_messages=prepared.messages,
            response_schema=prepared.response_schema,
        )

        try:
            with gestalt.AgentHost() as host:
                for _ in range(self._config.max_steps):
                    canceled = self._store.get_turn(prepared.turn_id)
                    if canceled is None:
                        return
                    if canceled.status == agent_pb2.AGENT_RUN_STATUS_CANCELED:
                        return

                    step = self._backend.complete(
                        model=prepared.resolved_model,
                        messages=conversation,
                        tools=tool_specs,
                        provider_options=prepared.provider_options,
                    )
                    conversation.append(step.assistant_message)

                    if step.tool_calls:
                        for tool_call in step.tool_calls:
                            resolved_tool_id = function_name_to_tool_id.get(tool_call.tool_id, "")
                            if not resolved_tool_id:
                                self._store.mark_turn_failed(
                                    turn_id=prepared.turn_id,
                                    messages=prepared.messages,
                                    status_message=f"model requested unknown tool {tool_call.tool_id!r}",
                                )
                                return

                            canceled = self._store.get_turn(prepared.turn_id)
                            if canceled is not None and canceled.status == agent_pb2.AGENT_RUN_STATUS_CANCELED:
                                return

                            tool_response = host.execute_tool(
                                agent_pb2.ExecuteAgentToolRequest(
                                    session_id=prepared.session_id,
                                    turn_id=prepared.turn_id,
                                    tool_call_id=tool_call.call_id,
                                    tool_id=resolved_tool_id,
                                    arguments=_dict_to_struct(tool_call.arguments),
                                )
                            )
                            conversation.append(
                                {
                                    "role": "tool",
                                    "tool_call_id": tool_call.call_id,
                                    "content": str(tool_response.body or ""),
                                }
                            )
                        continue

                    final_text = step.output_text.strip()
                    if not final_text:
                        self._store.mark_turn_failed(
                            turn_id=prepared.turn_id,
                            messages=prepared.messages,
                            status_message="model returned no final text and no tool calls",
                        )
                        return

                    structured_output = _parse_structured_output(
                        output_text=final_text, response_schema=prepared.response_schema
                    )
                    self._store.mark_turn_succeeded(
                        turn_id=prepared.turn_id,
                        messages=_append_assistant_message(prepared.messages, final_text),
                        output_text=final_text,
                        structured_output=structured_output,
                    )
                    return
        except ValidationError as exc:
            self._store.mark_turn_failed(
                turn_id=prepared.turn_id,
                messages=prepared.messages,
                status_message=f"response_schema validation failed: {exc.message}",
            )
            return
        except Exception as exc:
            self._store.mark_turn_failed(
                turn_id=prepared.turn_id,
                messages=prepared.messages,
                status_message=str(exc),
            )
            return

        self._store.mark_turn_failed(
            turn_id=prepared.turn_id,
            messages=prepared.messages,
            status_message=f"run exceeded maxSteps ({self._config.max_steps})",
        )

    def turn_to_proto(self, run: StoredRun) -> Any:
        proto = agent_pb2.AgentTurn(
            id=run.run_id,
            session_id=run.session_ref,
            provider_name=run.provider_name,
            model=run.model,
            status=run.status,
            messages=[_message_from_dict(message) for message in run.messages],
            output_text=run.output_text,
            status_message=run.status_message,
            execution_ref=run.execution_ref,
        )
        if run.structured_output:
            proto.structured_output.CopyFrom(_dict_to_struct(run.structured_output))
        if run.created_by:
            proto.created_by.CopyFrom(
                agent_pb2.AgentActor(
                    subject_id=run.created_by.get("subject_id", ""),
                    subject_kind=run.created_by.get("subject_kind", ""),
                    display_name=run.created_by.get("display_name", ""),
                    auth_source=run.created_by.get("auth_source", ""),
                )
            )
        proto.created_at.CopyFrom(_datetime_to_timestamp(run.created_at))
        if run.started_at is not None:
            proto.started_at.CopyFrom(_datetime_to_timestamp(run.started_at))
        if run.completed_at is not None:
            proto.completed_at.CopyFrom(_datetime_to_timestamp(run.completed_at))
        return proto


@dataclass(frozen=True, slots=True)
class PreparedTurn:
    session_id: str
    turn_id: str
    resolved_model: str
    messages: list[dict[str, Any]]
    response_schema: dict[str, Any]
    provider_options: dict[str, Any]
    tool_specs_and_names: tuple[list[dict[str, Any]], dict[str, str]]


def _build_initial_conversation(
    *, system_prompt: str, projected_messages: list[dict[str, Any]], response_schema: dict[str, Any] | None
) -> list[dict[str, Any]]:
    conversation: list[dict[str, Any]] = []
    if system_prompt:
        conversation.append({"role": "system", "content": system_prompt})
    if response_schema:
        conversation.append(
            {
                "role": "system",
                "content": (
                    "Return only valid JSON that matches this schema. "
                    "Do not wrap the JSON in markdown fences.\n"
                    f"{json.dumps(response_schema, separators=(',', ':'))}"
                ),
            }
        )
    for message in projected_messages:
        conversation_message = _conversation_message_from_agent_message(message)
        if conversation_message is not None:
            conversation.append(conversation_message)
    return conversation


def _project_messages(messages: Any) -> list[dict[str, Any]]:
    return [_message_to_dict(message) for message in messages]


def _append_assistant_message(messages: list[dict[str, Any]], output_text: str) -> list[dict[str, Any]]:
    projected = copy.deepcopy(messages)
    projected.append(
        {
            "role": "assistant",
            "text": output_text,
            "parts": [{"type": "AGENT_MESSAGE_PART_TYPE_TEXT", "text": output_text}],
        }
    )
    return projected


def _message_to_dict(message: Any) -> dict[str, Any]:
    return json_format.MessageToDict(message, preserving_proto_field_name=True)


def _message_from_dict(raw_message: dict[str, Any]) -> Any:
    message = agent_pb2.AgentMessage()
    json_format.ParseDict(raw_message, message)
    return message


def _conversation_message_from_agent_message(raw_message: dict[str, Any]) -> dict[str, Any] | None:
    role = str(raw_message.get("role", "") or "").strip()
    if not role:
        return None

    if role == "assistant":
        tool_calls = _tool_calls_from_message_parts(raw_message)
        content = _message_content_text(raw_message)
        message: dict[str, Any] = {"role": role}
        if content:
            message["content"] = content
        elif tool_calls:
            message["content"] = None
        else:
            message["content"] = ""
        if tool_calls:
            message["tool_calls"] = tool_calls
        return message

    if role == "tool":
        return {
            "role": role,
            "tool_call_id": _tool_call_id_from_message_parts(raw_message),
            "content": _tool_result_content_from_message_parts(raw_message) or _message_content_text(raw_message),
        }

    return {"role": role, "content": _message_content_text(raw_message)}


def _message_content_text(raw_message: dict[str, Any]) -> str:
    direct_text = str(raw_message.get("text", "") or "")
    if direct_text:
        return direct_text

    parts = raw_message.get("parts")
    if not isinstance(parts, list):
        return ""

    content_parts: list[str] = []
    for part in parts:
        if not isinstance(part, dict):
            continue
        part_type = _part_type(part)
        if part_type == "AGENT_MESSAGE_PART_TYPE_TEXT":
            text = str(part.get("text", "") or "")
            if text:
                content_parts.append(text)
        elif part_type == "AGENT_MESSAGE_PART_TYPE_JSON":
            raw_json = part.get("json")
            if isinstance(raw_json, dict):
                content_parts.append(json.dumps(raw_json, separators=(",", ":")))
    return "\n".join(part for part in content_parts if part)


def _tool_calls_from_message_parts(raw_message: dict[str, Any]) -> list[dict[str, Any]]:
    parts = raw_message.get("parts")
    if not isinstance(parts, list):
        return []

    tool_calls: list[dict[str, Any]] = []
    for part in parts:
        if not isinstance(part, dict) or _part_type(part) != "AGENT_MESSAGE_PART_TYPE_TOOL_CALL":
            continue
        raw_tool_call = part.get("tool_call")
        if not isinstance(raw_tool_call, dict):
            continue
        tool_call_id = str(raw_tool_call.get("id", "") or "").strip()
        tool_name = str(raw_tool_call.get("tool_id", "") or "").strip()
        if not tool_call_id or not tool_name:
            continue
        arguments = raw_tool_call.get("arguments")
        if not isinstance(arguments, dict):
            arguments = {}
        tool_calls.append(
            {
                "id": tool_call_id,
                "type": "function",
                "function": {"name": tool_name, "arguments": json.dumps(arguments, separators=(",", ":"))},
            }
        )
    return tool_calls


def _tool_call_id_from_message_parts(raw_message: dict[str, Any]) -> str:
    parts = raw_message.get("parts")
    if not isinstance(parts, list):
        return ""
    for part in parts:
        if not isinstance(part, dict) or _part_type(part) != "AGENT_MESSAGE_PART_TYPE_TOOL_RESULT":
            continue
        raw_tool_result = part.get("tool_result")
        if isinstance(raw_tool_result, dict):
            return str(raw_tool_result.get("tool_call_id", "") or "").strip()
    return ""


def _tool_result_content_from_message_parts(raw_message: dict[str, Any]) -> str:
    parts = raw_message.get("parts")
    if not isinstance(parts, list):
        return ""
    for part in parts:
        if not isinstance(part, dict) or _part_type(part) != "AGENT_MESSAGE_PART_TYPE_TOOL_RESULT":
            continue
        raw_tool_result = part.get("tool_result")
        if not isinstance(raw_tool_result, dict):
            continue
        content = str(raw_tool_result.get("content", "") or "")
        if content:
            return content
        output = raw_tool_result.get("output")
        if isinstance(output, dict):
            return json.dumps(output, separators=(",", ":"))
    return ""


def _part_type(part: dict[str, Any]) -> str:
    raw_value = part.get("type")
    if isinstance(raw_value, str):
        return raw_value
    if raw_value == agent_pb2.AGENT_MESSAGE_PART_TYPE_TEXT:
        return "AGENT_MESSAGE_PART_TYPE_TEXT"
    if raw_value == agent_pb2.AGENT_MESSAGE_PART_TYPE_JSON:
        return "AGENT_MESSAGE_PART_TYPE_JSON"
    if raw_value == agent_pb2.AGENT_MESSAGE_PART_TYPE_TOOL_CALL:
        return "AGENT_MESSAGE_PART_TYPE_TOOL_CALL"
    if raw_value == agent_pb2.AGENT_MESSAGE_PART_TYPE_TOOL_RESULT:
        return "AGENT_MESSAGE_PART_TYPE_TOOL_RESULT"
    if raw_value == agent_pb2.AGENT_MESSAGE_PART_TYPE_IMAGE_REF:
        return "AGENT_MESSAGE_PART_TYPE_IMAGE_REF"
    return ""


def _resolved_tools_to_openai(tools: Any) -> tuple[list[dict[str, Any]], dict[str, str]]:
    formatted: list[dict[str, Any]] = []
    function_name_to_tool_id: dict[str, str] = {}
    for tool in tools:
        tool_id = str(tool.id or "").strip()
        if not tool_id:
            continue
        function_name = _sanitize_function_name(str(tool.name or "").strip() or tool_id)
        if function_name in function_name_to_tool_id:
            suffix = 2
            while f"{function_name}_{suffix}" in function_name_to_tool_id:
                suffix += 1
            function_name = f"{function_name}_{suffix}"
        function_name_to_tool_id[function_name] = tool_id
        description = str(tool.description or "").strip()
        if str(tool.name or "").strip() and str(tool.name or "").strip() != tool_id:
            prefix = f"{tool.name.strip()}: "
            description = prefix + description if description else tool.name.strip()
        parameters = _struct_to_dict(tool.parameters_schema) or {"type": "object", "properties": {}}
        if not isinstance(parameters, dict):
            parameters = {"type": "object", "properties": {}}
        if "type" not in parameters:
            parameters["type"] = "object"
        formatted.append(
            {
                "type": "function",
                "function": {"name": function_name, "description": description, "parameters": parameters},
            }
        )
    return formatted, function_name_to_tool_id


def _sanitize_function_name(raw_value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_-]+", "_", raw_value).strip("_")
    if normalized:
        return normalized
    return "tool"


def _parse_structured_output(*, output_text: str, response_schema: dict[str, Any] | None) -> dict[str, Any] | None:
    if not response_schema:
        return None
    parsed = json.loads(output_text)
    validate(instance=parsed, schema=response_schema)
    if not isinstance(parsed, dict):
        raise ValueError(
            "structured output must be a JSON object because AgentTurn.structured_output is a protobuf Struct"
        )
    return parsed


def _struct_to_dict(message: Any) -> dict[str, Any]:
    if message is None:
        return {}
    if not getattr(message, "fields", None):
        return {}
    return json_format.MessageToDict(message)


def _dict_to_struct(data: dict[str, Any]) -> Any:
    struct = struct_pb2.Struct()
    struct.update(data)
    return struct


def _actor_to_dict(actor: Any) -> dict[str, str]:
    if actor is None:
        return {}
    return {
        "subject_id": str(getattr(actor, "subject_id", "") or "").strip(),
        "subject_kind": str(getattr(actor, "subject_kind", "") or "").strip(),
        "display_name": str(getattr(actor, "display_name", "") or "").strip(),
        "auth_source": str(getattr(actor, "auth_source", "") or "").strip(),
    }


def _datetime_to_timestamp(value: datetime) -> Any:
    timestamp = timestamp_pb2.Timestamp()
    timestamp.FromDatetime(value.astimezone(UTC))
    return timestamp
