import json
import re
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

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

agent_pb2: Any = _agent_pb2
struct_pb2: Any = _struct_pb2
timestamp_pb2: Any = _timestamp_pb2


class SimpleAgentOrchestrator:
    def __init__(self, *, config: SimpleAgentConfig, store: SimpleRunStore) -> None:
        self._config = config
        self._store = store
        self._backend = ModelBackend(config)

    def start_run(self, request: Any, context: grpc.ServicerContext) -> Any:
        run_id = str(request.run_id or "").strip()
        if not run_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "run_id is required")
        if len(list(request.messages)) == 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "messages must contain at least one entry")

        try:
            resolved_model = self._config.resolve_model(str(request.model or ""))
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))

        started, created = self._store.begin_run(
            run_id=run_id,
            idempotency_key=str(request.idempotency_key or "").strip(),
            provider_name=str(request.provider_name or "").strip(),
            model=resolved_model,
            messages=_project_messages(request.messages),
            session_ref=str(request.session_ref or "").strip(),
            created_by=_actor_to_dict(request.created_by),
            execution_ref=str(request.execution_ref or "").strip(),
        )
        if not created:
            return self.run_to_proto(started)

        prepared = PreparedRun(
            run_id=run_id,
            resolved_model=resolved_model,
            messages=list(started.messages),
            response_schema=_struct_to_dict(request.response_schema),
            provider_options=_struct_to_dict(request.provider_options),
            tool_specs_and_names=_resolved_tools_to_openai(request.tools),
        )
        threading.Thread(target=self._complete_run, args=(prepared,), daemon=True).start()
        return self.run_to_proto(started)

    def _complete_run(self, prepared: "PreparedRun") -> None:
        tool_specs, function_name_to_tool_id = prepared.tool_specs_and_names
        conversation = _build_initial_conversation(
            system_prompt=self._config.system_prompt,
            projected_messages=prepared.messages,
            response_schema=prepared.response_schema,
        )

        try:
            with gestalt.AgentHost() as host:
                for _ in range(self._config.max_steps):
                    canceled = self._store.get_run(prepared.run_id)
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
                                self._store.mark_failed(
                                    run_id=prepared.run_id,
                                    messages=prepared.messages,
                                    status_message=f"model requested unknown tool {tool_call.tool_id!r}",
                                )
                                return

                            canceled = self._store.get_run(prepared.run_id)
                            if canceled is not None and canceled.status == agent_pb2.AGENT_RUN_STATUS_CANCELED:
                                return

                            tool_response = host.execute_tool(
                                agent_pb2.ExecuteAgentToolRequest(
                                    run_id=prepared.run_id,
                                    tool_call_id=tool_call.call_id,
                                    tool_id=resolved_tool_id,
                                    arguments=_dict_to_struct(tool_call.arguments),
                                )
                            )
                            conversation.append(
                                {
                                    "role": "tool",
                                    "tool_call_id": tool_call.call_id,
                                    "name": tool_call.tool_id,
                                    "content": str(tool_response.body or ""),
                                }
                            )
                        continue

                    final_text = step.output_text.strip()
                    if not final_text:
                        self._store.mark_failed(
                            run_id=prepared.run_id,
                            messages=prepared.messages,
                            status_message="model returned no final text and no tool calls",
                        )
                        return

                    structured_output = _parse_structured_output(
                        output_text=final_text, response_schema=prepared.response_schema
                    )
                    self._store.mark_succeeded(
                        run_id=prepared.run_id,
                        messages=_append_assistant_message(prepared.messages, final_text),
                        output_text=final_text,
                        structured_output=structured_output,
                    )
                    return
        except ValidationError as exc:
            self._store.mark_failed(
                run_id=prepared.run_id,
                messages=prepared.messages,
                status_message=f"response_schema validation failed: {exc.message}",
            )
            return
        except Exception as exc:
            self._store.mark_failed(run_id=prepared.run_id, messages=prepared.messages, status_message=str(exc))
            return

        self._store.mark_failed(
            run_id=prepared.run_id,
            messages=prepared.messages,
            status_message=f"run exceeded maxSteps ({self._config.max_steps})",
        )

    def run_to_proto(self, run: StoredRun) -> Any:
        proto = agent_pb2.BoundAgentRun(
            id=run.run_id,
            provider_name=run.provider_name,
            model=run.model,
            status=run.status,
            messages=[agent_pb2.AgentMessage(role=message["role"], text=message["text"]) for message in run.messages],
            output_text=run.output_text,
            status_message=run.status_message,
            session_ref=run.session_ref,
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
class PreparedRun:
    run_id: str
    resolved_model: str
    messages: list[dict[str, str]]
    response_schema: dict[str, Any]
    provider_options: dict[str, Any]
    tool_specs_and_names: tuple[list[dict[str, Any]], dict[str, str]]


def _build_initial_conversation(
    *, system_prompt: str, projected_messages: list[dict[str, str]], response_schema: dict[str, Any] | None
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
        conversation.append({"role": message["role"], "content": message["text"]})
    return conversation


def _project_messages(messages: Any) -> list[dict[str, str]]:
    return [{"role": str(message.role or "").strip(), "text": str(message.text or "")} for message in messages]


def _append_assistant_message(messages: list[dict[str, str]], output_text: str) -> list[dict[str, str]]:
    projected = list(messages)
    projected.append({"role": "assistant", "text": output_text})
    return projected


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
            "structured output must be a JSON object because BoundAgentRun.structured_output is a protobuf Struct"
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
