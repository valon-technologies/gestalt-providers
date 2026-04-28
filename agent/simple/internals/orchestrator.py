import json
import re
import threading
import copy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import gestalt
import grpc
from google.protobuf import json_format
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from jsonschema import ValidationError, validate

from gestalt.gen.v1 import agent_pb2 as _agent_pb2

from .config import SimpleAgentConfig
from .model_backend import ModelBackend
from .store import SimpleRunStore, StoredRun

agent_pb2: Any = cast(Any, _agent_pb2)
struct_pb2: Any = _struct_pb2
timestamp_pb2: Any = _timestamp_pb2

TOOL_SEARCH_FUNCTION_NAME = "gestalt_search_tools"
TOOL_SEARCH_TOOL_ID = "__gestalt_search_tools__"
TOOL_SEARCH_DEFAULT_MAX_RESULTS = 8
TOOL_SEARCH_MAX_RESULTS = 20
TOOL_SEARCH_SYSTEM_PROMPT = (
    "When a user asks you to use an external integration or read external data and the needed tool is not already "
    f"available, call `{TOOL_SEARCH_FUNCTION_NAME}` before saying you do not have access."
)
TOOL_SEARCH_TOOL_SPEC = {
    "type": "function",
    "function": {
        "name": TOOL_SEARCH_FUNCTION_NAME,
        "description": (
            "Search the authorized Gestalt integration tool catalog for tools relevant to the task. "
            "Use this when a needed integration tool is not already available."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Short natural-language description of the tool or integration needed.",
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of matching tools to return.",
                    "minimum": 1,
                    "maximum": TOOL_SEARCH_MAX_RESULTS,
                },
            },
            "required": ["query"],
        },
    },
}


class SimpleAgentOrchestrator:
    def __init__(self, *, config: SimpleAgentConfig, store: SimpleRunStore) -> None:
        self._config = config
        self._store = store
        self._backend = ModelBackend(config)

    def create_turn(
        self, request: Any, context: grpc.ServicerContext, *, session_model: str = "", provider_name: str = ""
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
            resolved_model = self._config.resolve_model(str(request.model or "").strip() or session_model)
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
            provider_name=provider_name.strip() or self._config.name,
            resolved_model=resolved_model,
            messages=list(started.messages),
            response_schema=_struct_to_dict(request.response_schema),
            provider_options=_struct_to_dict(request.provider_options),
            tool_specs_and_names=_tool_registry_from_resolved_tools(request.tools),
        )
        self._store.append_turn_event(
            turn_id=turn_id,
            event_type="turn.started",
            source=prepared.provider_name,
            data={"session_id": session_id, "model": resolved_model},
        )
        threading.Thread(target=self._complete_turn, args=(prepared,), daemon=True).start()
        return self.turn_to_proto(started)

    def _complete_turn(self, prepared: "PreparedTurn") -> None:
        tool_specs, function_name_to_tool_id, loaded_tool_ids = _copy_tool_registry(prepared.tool_specs_and_names)
        conversation = _build_initial_conversation(
            system_prompt=self._config.system_prompt,
            projected_messages=prepared.messages,
            response_schema=prepared.response_schema,
        )

        try:
            canceled = self._store.get_turn(prepared.turn_id)
            if canceled is None:
                return
            if canceled.status == agent_pb2.AGENT_EXECUTION_STATUS_CANCELED:
                return
            for _ in range(self._config.max_steps):
                canceled = self._store.get_turn(prepared.turn_id)
                if canceled is None:
                    return
                if canceled.status == agent_pb2.AGENT_EXECUTION_STATUS_CANCELED:
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
                            self._fail_turn(
                                prepared=prepared,
                                messages=prepared.messages,
                                status_message=f"model requested unknown tool {tool_call.tool_id!r}",
                            )
                            return

                        canceled = self._store.get_turn(prepared.turn_id)
                        if canceled is not None and canceled.status == agent_pb2.AGENT_EXECUTION_STATUS_CANCELED:
                            return

                        self._store.append_turn_event(
                            turn_id=prepared.turn_id,
                            event_type="tool.started",
                            source=prepared.provider_name,
                            data={
                                "tool_call_id": tool_call.call_id,
                                "tool_id": resolved_tool_id,
                                "arguments": tool_call.arguments,
                            },
                        )
                        if resolved_tool_id == TOOL_SEARCH_TOOL_ID:
                            with gestalt.AgentHost() as host:
                                tool_response_body = _search_tools_for_model(
                                    host=host,
                                    prepared=prepared,
                                    tool_call_arguments=tool_call.arguments,
                                    tool_specs=tool_specs,
                                    function_name_to_tool_id=function_name_to_tool_id,
                                    loaded_tool_ids=loaded_tool_ids,
                                )
                            self._store.append_turn_event(
                                turn_id=prepared.turn_id,
                                event_type="tool.completed",
                                source=prepared.provider_name,
                                data={"tool_call_id": tool_call.call_id, "tool_id": resolved_tool_id, "status": 200},
                            )
                            conversation.append(
                                {"role": "tool", "tool_call_id": tool_call.call_id, "content": tool_response_body}
                            )
                            continue

                        validation_error = _tool_arguments_validation_error(
                            tool_name=tool_call.tool_id, arguments=tool_call.arguments, tool_specs=tool_specs
                        )
                        if validation_error:
                            self._store.append_turn_event(
                                turn_id=prepared.turn_id,
                                event_type="tool.completed",
                                source=prepared.provider_name,
                                data={
                                    "tool_call_id": tool_call.call_id,
                                    "tool_id": resolved_tool_id,
                                    "status": 400,
                                    "error": validation_error,
                                },
                            )
                            conversation.append(
                                _tool_result_message(
                                    tool_call_id=tool_call.call_id, content=validation_error, is_error=True
                                )
                            )
                            continue

                        with gestalt.AgentHost() as host:
                            tool_response = host.execute_tool(
                                agent_pb2.ExecuteAgentToolRequest(
                                    session_id=prepared.session_id,
                                    turn_id=prepared.turn_id,
                                    tool_call_id=tool_call.call_id,
                                    tool_id=resolved_tool_id,
                                    arguments=_dict_to_struct(tool_call.arguments),
                                )
                            )
                        canceled = self._store.get_turn(prepared.turn_id)
                        if canceled is not None and canceled.status == agent_pb2.AGENT_EXECUTION_STATUS_CANCELED:
                            return
                        self._store.append_turn_event(
                            turn_id=prepared.turn_id,
                            event_type="tool.completed",
                            source=prepared.provider_name,
                            data={
                                "tool_call_id": tool_call.call_id,
                                "tool_id": resolved_tool_id,
                                "status": int(tool_response.status or 0),
                            },
                        )
                        conversation.append(
                            _tool_result_message(
                                tool_call_id=tool_call.call_id,
                                content=str(tool_response.body or ""),
                                is_error=int(tool_response.status or 0) >= 400,
                            )
                        )
                    continue

                final_text = step.output_text.strip()
                if not final_text:
                    self._fail_turn(
                        prepared=prepared,
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
            self._fail_turn(
                prepared=prepared,
                messages=prepared.messages,
                status_message=f"response_schema validation failed: {exc.message}",
            )
            return
        except Exception as exc:
            self._fail_turn(prepared=prepared, messages=prepared.messages, status_message=str(exc))
            return

        self._fail_turn(
            prepared=prepared,
            messages=prepared.messages,
            status_message=f"run exceeded maxSteps ({self._config.max_steps})",
        )

    def _fail_turn(self, *, prepared: "PreparedTurn", messages: list[dict[str, Any]], status_message: str) -> None:
        self._store.mark_turn_failed(turn_id=prepared.turn_id, messages=messages, status_message=status_message)

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
    provider_name: str
    resolved_model: str
    messages: list[dict[str, Any]]
    response_schema: dict[str, Any]
    provider_options: dict[str, Any]
    tool_specs_and_names: tuple[list[dict[str, Any]], dict[str, str], set[str]]


def _search_tools_for_model(
    *,
    host: Any,
    prepared: PreparedTurn,
    tool_call_arguments: dict[str, Any],
    tool_specs: list[dict[str, Any]],
    function_name_to_tool_id: dict[str, str],
    loaded_tool_ids: set[str],
) -> str:
    query = str(tool_call_arguments.get("query", "") or "").strip()
    if not query:
        query = _tool_search_query(prepared.messages)
    response = host.search_tools(
        agent_pb2.SearchAgentToolsRequest(
            session_id=prepared.session_id,
            turn_id=prepared.turn_id,
            query=query,
            max_results=_tool_search_max_results(tool_call_arguments.get("max_results")),
        )
    )
    available_tools = _register_resolved_tools(
        response.tools,
        tool_specs=tool_specs,
        function_name_to_tool_id=function_name_to_tool_id,
        loaded_tool_ids=loaded_tool_ids,
    )
    return json.dumps({"tools": available_tools}, separators=(",", ":"))


def _tool_arguments_validation_error(
    *, tool_name: str, arguments: dict[str, Any], tool_specs: list[dict[str, Any]]
) -> str:
    schema = _tool_parameters_schema(tool_name=tool_name, tool_specs=tool_specs)
    if not schema:
        return ""
    try:
        validate(instance=arguments, schema=schema)
    except ValidationError as exc:
        return f"Tool arguments failed schema validation for {tool_name}: {exc.message}"
    return ""


def _tool_parameters_schema(*, tool_name: str, tool_specs: list[dict[str, Any]]) -> dict[str, Any] | None:
    tool_name = tool_name.strip()
    if not tool_name:
        return None
    for tool_spec in tool_specs:
        function_spec = tool_spec.get("function")
        if not isinstance(function_spec, dict):
            continue
        if str(function_spec.get("name", "") or "").strip() != tool_name:
            continue
        parameters = function_spec.get("parameters")
        if isinstance(parameters, dict):
            return parameters
        return None
    return None


def _tool_result_message(*, tool_call_id: str, content: str, is_error: bool = False) -> dict[str, Any]:
    message: dict[str, Any] = {"role": "tool", "tool_call_id": tool_call_id, "content": content}
    if is_error:
        message["is_error"] = True
    return message


def _tool_search_max_results(raw_value: Any) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return TOOL_SEARCH_DEFAULT_MAX_RESULTS
    if value <= 0:
        return TOOL_SEARCH_DEFAULT_MAX_RESULTS
    return min(value, TOOL_SEARCH_MAX_RESULTS)


def _tool_search_query(messages: list[dict[str, Any]]) -> str:
    parts = [_message_content_text(message) for message in messages]
    return "\n".join(part for part in parts if part).strip()


def _build_initial_conversation(
    *, system_prompt: str, projected_messages: list[dict[str, Any]], response_schema: dict[str, Any] | None
) -> list[dict[str, Any]]:
    conversation: list[dict[str, Any]] = []
    if system_prompt:
        conversation.append({"role": "system", "content": system_prompt})
    conversation.append({"role": "system", "content": TOOL_SEARCH_SYSTEM_PROMPT})
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


def _tool_registry_from_resolved_tools(tools: Any) -> tuple[list[dict[str, Any]], dict[str, str], set[str]]:
    tool_specs = [copy.deepcopy(TOOL_SEARCH_TOOL_SPEC)]
    function_name_to_tool_id = {TOOL_SEARCH_FUNCTION_NAME: TOOL_SEARCH_TOOL_ID}
    loaded_tool_ids: set[str] = set()
    _register_resolved_tools(
        tools, tool_specs=tool_specs, function_name_to_tool_id=function_name_to_tool_id, loaded_tool_ids=loaded_tool_ids
    )
    return tool_specs, function_name_to_tool_id, loaded_tool_ids


def _copy_tool_registry(
    registry: tuple[list[dict[str, Any]], dict[str, str], set[str]],
) -> tuple[list[dict[str, Any]], dict[str, str], set[str]]:
    tool_specs, function_name_to_tool_id, loaded_tool_ids = registry
    return copy.deepcopy(tool_specs), dict(function_name_to_tool_id), set(loaded_tool_ids)


def _register_resolved_tools(
    tools: Any, *, tool_specs: list[dict[str, Any]], function_name_to_tool_id: dict[str, str], loaded_tool_ids: set[str]
) -> list[dict[str, Any]]:
    available_tools: list[dict[str, Any]] = []
    for tool in tools:
        tool_id = str(tool.id or "").strip()
        if not tool_id:
            continue
        if tool_id in loaded_tool_ids:
            function_name = _function_name_for_tool_id(tool_id, function_name_to_tool_id)
            if function_name:
                available_tools.append(
                    _tool_search_result_entry(
                        function_name=function_name, tool=tool, description=_tool_description(tool)
                    )
                )
            continue
        function_name = _unique_function_name(str(tool.name or "").strip() or tool_id, function_name_to_tool_id)
        function_name_to_tool_id[function_name] = tool_id
        loaded_tool_ids.add(tool_id)
        description = _tool_description(tool)
        parameters = _struct_to_dict(tool.parameters_schema) or {"type": "object", "properties": {}}
        if not isinstance(parameters, dict):
            parameters = {"type": "object", "properties": {}}
        if "type" not in parameters:
            parameters["type"] = "object"
        tool_specs.append(
            {
                "type": "function",
                "function": {"name": function_name, "description": description, "parameters": parameters},
            }
        )
        available_tools.append(
            _tool_search_result_entry(function_name=function_name, tool=tool, description=description)
        )
    return available_tools


def _function_name_for_tool_id(tool_id: str, function_name_to_tool_id: dict[str, str]) -> str:
    for function_name, mapped_tool_id in function_name_to_tool_id.items():
        if mapped_tool_id == tool_id:
            return function_name
    return ""


def _tool_description(tool: Any) -> str:
    tool_id = str(tool.id or "").strip()
    tool_name = str(tool.name or "").strip()
    description = str(tool.description or "").strip()
    if tool_name and tool_name != tool_id:
        prefix = f"{tool_name}: "
        description = prefix + description if description else tool_name
    return description


def _unique_function_name(raw_value: str, function_name_to_tool_id: dict[str, str]) -> str:
    function_name = _sanitize_function_name(raw_value)
    if function_name not in function_name_to_tool_id:
        return function_name
    suffix = 2
    while f"{function_name}_{suffix}" in function_name_to_tool_id:
        suffix += 1
    return f"{function_name}_{suffix}"


def _tool_search_result_entry(*, function_name: str, tool: Any, description: str) -> dict[str, Any]:
    return {"name": function_name, "description": description}


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
