import copy
import json
import re
import threading
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Callable, cast

import gestalt
import grpc
from google.protobuf import json_format
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from jsonschema import ValidationError, validate

from gestalt.gen.v1 import agent_pb2 as _agent_pb2

from .config import SimpleAgentConfig
from .model_backend import ModelBackend
from .store import SimpleRunStore, StoredRun, StoredTurnCheckpoint

agent_pb2: Any = cast(Any, _agent_pb2)
struct_pb2: Any = _struct_pb2
timestamp_pb2: Any = _timestamp_pb2

TOOL_SEARCH_FUNCTION_NAME = "gestalt_search_tools"
TOOL_SEARCH_TOOL_ID = "__gestalt_search_tools__"
TOOL_SEARCH_LEGACY_DEFAULT_MAX_RESULTS = 8
TOOL_SEARCH_ADAPTIVE_DEFAULT_MAX_RESULTS = 3
TOOL_SEARCH_DEFAULT_CANDIDATE_LIMIT = 10
TOOL_SEARCH_MAX_RESULTS = 20
TOOL_SEARCH_MAX_CANDIDATES = 20
CHECKPOINT_SCHEMA_VERSION = 1
PHASE_MODEL_NEXT = "model_next"
PHASE_TOOL_READY = "tool_ready"
PHASE_TOOL_INFLIGHT = "tool_inflight"
PHASE_TOOL_RESULT_RECORDED = "tool_result_recorded"
PHASE_TERMINAL = "terminal"
_TERMINAL_STATUSES = {
    agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED,
    agent_pb2.AGENT_EXECUTION_STATUS_FAILED,
    agent_pb2.AGENT_EXECUTION_STATUS_CANCELED,
}
TOOL_SEARCH_SYSTEM_PROMPT = (
    "When a user asks you to use an external integration or read external data and the needed tool is not already "
    f"available, call `{TOOL_SEARCH_FUNCTION_NAME}` before saying you do not have access."
)
TOOL_SEARCH_ADAPTIVE_SYSTEM_PROMPT = (
    " If the tool search result includes compact candidates, call the same tool with `load_refs` from the candidate "
    "refs to load the exact tool schemas you need."
)
TOOL_SEARCH_TOOL_SPEC: dict[str, Any] = {
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
                    "description": (
                        "Short natural-language description of the tool or integration needed. Use this to discover "
                        "candidate tools."
                    ),
                },
                "max_results": {
                    "type": "integer",
                    "description": (
                        "Maximum number of full tool schemas to load. Use 0 with load_refs to let the server load "
                        "requested refs up to its cap."
                    ),
                    "minimum": 0,
                    "maximum": TOOL_SEARCH_MAX_RESULTS,
                },
                "candidate_limit": {
                    "type": "integer",
                    "description": "Maximum number of compact candidate refs to return for later loading.",
                    "minimum": 0,
                    "maximum": TOOL_SEARCH_MAX_CANDIDATES,
                },
                "load_refs": {
                    "type": "array",
                    "description": "Candidate refs from a previous search result to load as full tool schemas.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "system": {"type": "string"},
                            "plugin": {"type": "string"},
                            "operation": {"type": "string"},
                            "connection": {"type": "string"},
                            "instance": {"type": "string"},
                        },
                        "required": ["plugin", "operation"],
                        "additionalProperties": False,
                    },
                },
            },
        },
    },
}


class SimpleAgentOrchestrator:
    def __init__(self, *, config: SimpleAgentConfig, store: SimpleRunStore) -> None:
        self._config = config
        self._store = store
        self._backend = ModelBackend(config)
        self._scheduled_lock = threading.Lock()
        self._scheduled_turns: set[str] = set()
        self._worker_id = f"agent/simple:{config.name}:{uuid.uuid4().hex}"

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

        projected_messages = _project_messages(request.messages)
        response_schema = _struct_to_dict(request.response_schema)
        provider_options = _struct_to_dict(request.provider_options)
        tool_specs_and_names = _tool_registry_from_resolved_tools(request.tools)
        prepared_seed = _resume_seed_from_request(
            messages=projected_messages,
            response_schema=response_schema,
            provider_options=provider_options,
            tool_grant=str(request.tool_grant or "").strip(),
            tool_specs_and_names=tool_specs_and_names,
            system_prompt=self._config.system_prompt,
        )

        try:
            start_event_source = provider_name.strip() or self._config.name
            started, created = self._store.begin_turn(
                turn_id=turn_id,
                session_id=session_id,
                idempotency_key=str(request.idempotency_key or "").strip(),
                provider_name=provider_name.strip(),
                model=resolved_model,
                messages=projected_messages,
                created_by=_actor_to_dict(request.created_by),
                execution_ref=str(request.execution_ref or "").strip(),
                resume_seed=prepared_seed,
                start_event_source=start_event_source,
                start_event_data={"session_id": session_id, "model": resolved_model},
            )
        except ValueError as exc:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, str(exc))
        if not created:
            return self.turn_to_proto(started)

        self.schedule_turn(turn_id)
        return self.turn_to_proto(started)

    def resume_incomplete_turns(self, *, should_continue: Callable[[], bool] | None = None) -> None:
        if not self._config.resume.enabled:
            return
        if should_continue is not None and not should_continue():
            return
        for turn_id in self._store.list_recoverable_turn_ids(limit=self._config.resume.startup_scan_limit):
            if should_continue is not None and not should_continue():
                return
            self.schedule_turn(turn_id)

    def schedule_turn(self, turn_id: str) -> None:
        turn_id = turn_id.strip()
        if not turn_id:
            return
        with self._scheduled_lock:
            if turn_id in self._scheduled_turns:
                return
            self._scheduled_turns.add(turn_id)
        threading.Thread(target=self._complete_turn_by_id, args=(turn_id,), daemon=True).start()

    def _complete_turn_by_id(self, turn_id: str) -> None:
        claimed = False
        heartbeat_stop: threading.Event | None = None
        heartbeat: threading.Thread | None = None
        try:
            run = self._store.get_turn(turn_id)
            if run is None or run.status in _TERMINAL_STATUSES:
                return
            checkpoint = self._store.get_turn_checkpoint(turn_id)
            if checkpoint is None:
                checkpoint = self._store.ensure_turn_checkpoint_from_seed(run)
            if checkpoint is None:
                self._fail_run(
                    run=run,
                    messages=list(run.messages),
                    status_message="turn has no durable checkpoint and cannot be resumed safely",
                )
                return
            claimed = self._store.claim_turn_lease(
                turn_id, owner=self._worker_id, lease_seconds=self._config.resume.worker_lease_seconds
            )
            if not claimed:
                return
            heartbeat_stop, heartbeat = self._start_lease_heartbeat(turn_id)
            self._run_turn_loop(turn_id)
        finally:
            if heartbeat_stop is not None:
                heartbeat_stop.set()
            if heartbeat is not None:
                heartbeat.join(timeout=1.0)
            if claimed:
                try:
                    self._store.release_turn_lease(turn_id, owner=self._worker_id)
                except (grpc.RpcError, RuntimeError):
                    pass
            with self._scheduled_lock:
                self._scheduled_turns.discard(turn_id)

    def _start_lease_heartbeat(self, turn_id: str) -> tuple[threading.Event, threading.Thread]:
        stop = threading.Event()
        lease_seconds = self._config.resume.worker_lease_seconds
        interval = max(1.0, min(10.0, lease_seconds / 3.0))

        def renew_loop() -> None:
            while not stop.wait(interval):
                try:
                    if not self._store.renew_turn_lease(turn_id, owner=self._worker_id, lease_seconds=lease_seconds):
                        return
                except (grpc.RpcError, RuntimeError):
                    return

        thread = threading.Thread(target=renew_loop, daemon=True)
        thread.start()
        return stop, thread

    def _run_turn_loop(self, turn_id: str) -> None:
        try:
            for _ in range(self._config.max_steps + 1):
                run = self._store.get_turn(turn_id)
                if run is None or run.status in _TERMINAL_STATUSES:
                    return
                if run.status == agent_pb2.AGENT_EXECUTION_STATUS_CANCELED:
                    return
                checkpoint = self._store.get_turn_checkpoint(turn_id)
                if checkpoint is None:
                    checkpoint = self._store.ensure_turn_checkpoint_from_seed(run)
                if checkpoint is None:
                    self._fail_run(
                        run=run,
                        messages=list(run.messages),
                        status_message="turn has no durable checkpoint and cannot be resumed safely",
                    )
                    return
                if checkpoint.lease_owner != self._worker_id:
                    return

                self._store.append_turn_event_once(
                    event_key=f"{turn_id}:turn.started",
                    turn_id=turn_id,
                    event_type="turn.started",
                    source=checkpoint.provider_name,
                    data={"session_id": checkpoint.session_id, "model": checkpoint.model},
                )
                if checkpoint.phase in {PHASE_TOOL_READY, PHASE_TOOL_RESULT_RECORDED}:
                    if not self._continue_pending_tool(run=run, checkpoint=checkpoint):
                        return
                    continue
                if checkpoint.phase == PHASE_TOOL_INFLIGHT:
                    pending = checkpoint.pending_tool_call or {}
                    result = self._store.get_tool_result(
                        turn_id=turn_id, tool_call_id=str(pending.get("tool_call_id") or "")
                    )
                    if result is not None:
                        self._record_tool_result_in_conversation(
                            checkpoint=replace(checkpoint, phase=PHASE_TOOL_RESULT_RECORDED), result=result
                        )
                        continue
                    self._fail_run(
                        run=run,
                        messages=checkpoint.messages,
                        status_message=_uncertain_tool_status_message(checkpoint.pending_tool_call),
                        lease_owner=self._worker_id,
                    )
                    return
                if checkpoint.phase == PHASE_TERMINAL:
                    return
                if checkpoint.phase != PHASE_MODEL_NEXT:
                    self._fail_run(
                        run=run,
                        messages=checkpoint.messages,
                        status_message=f"checkpoint phase {checkpoint.phase!r} cannot be resumed safely",
                        lease_owner=self._worker_id,
                    )
                    return
                if checkpoint.step_index >= self._config.max_steps:
                    self._fail_run(
                        run=run,
                        messages=checkpoint.messages,
                        status_message=f"run exceeded maxSteps ({self._config.max_steps})",
                        lease_owner=self._worker_id,
                    )
                    return
                if not self._run_model_step(run=run, checkpoint=checkpoint):
                    return
            run = self._store.get_turn(turn_id)
            if run is not None and run.status not in _TERMINAL_STATUSES:
                self._fail_run(
                    run=run,
                    messages=list(run.messages),
                    status_message=f"run exceeded maxSteps ({self._config.max_steps})",
                    lease_owner=self._worker_id,
                )
        except ValidationError as exc:
            run = self._store.get_turn(turn_id)
            if run is not None:
                self._fail_run(
                    run=run,
                    messages=_checkpoint_messages(self._store.get_turn_checkpoint(turn_id), fallback=run.messages),
                    status_message=f"response_schema validation failed: {exc.message}",
                    lease_owner=self._worker_id,
                )
        except Exception as exc:
            run = self._store.get_turn(turn_id)
            if run is not None:
                self._fail_run(
                    run=run,
                    messages=_checkpoint_messages(self._store.get_turn_checkpoint(turn_id), fallback=run.messages),
                    status_message=str(exc),
                    lease_owner=self._worker_id,
                )

    def _run_model_step(self, *, run: StoredRun, checkpoint: StoredTurnCheckpoint) -> bool:
        tool_specs, function_name_to_tool_id, loaded_tool_ids = _tool_registry_from_checkpoint(checkpoint)
        step = self._backend.complete(
            model=checkpoint.model,
            messages=list(checkpoint.conversation),
            tools=tool_specs,
            provider_options=checkpoint.provider_options,
        )
        if step.tool_calls:
            conversation = list(checkpoint.conversation)
            conversation.append(copy.deepcopy(step.assistant_message))
            assistant_message_index = len(conversation) - 1
            pending_tool_calls: list[dict[str, Any]] = []
            for tool_call in step.tool_calls:
                resolved_tool_id = function_name_to_tool_id.get(tool_call.tool_id, "")
                if not resolved_tool_id:
                    self._fail_run(
                        run=run,
                        messages=checkpoint.messages,
                        status_message=f"model requested unknown tool {tool_call.tool_id!r}",
                        lease_owner=self._worker_id,
                    )
                    return False
                pending = _pending_tool_call(
                    tool_call_id=tool_call.call_id,
                    tool_name=tool_call.tool_id,
                    resolved_tool_id=resolved_tool_id,
                    arguments=tool_call.arguments,
                    assistant_message_index=assistant_message_index,
                )
                pending_tool_calls.append(pending)
            pending = pending_tool_calls[0]
            pending["remaining_tool_calls"] = pending_tool_calls[1:]
            next_checkpoint = replace(
                checkpoint,
                phase=PHASE_TOOL_READY,
                conversation=copy.deepcopy(conversation),
                tool_specs=copy.deepcopy(tool_specs),
                function_name_to_tool_id=dict(function_name_to_tool_id),
                loaded_tool_ids=sorted(loaded_tool_ids),
                pending_tool_call=pending,
                step_index=checkpoint.step_index + 1,
                updated_at=datetime.now(tz=UTC),
            )
            self._store.put_turn_checkpoint(next_checkpoint, lease_owner=self._worker_id)
            return self._continue_pending_tool(run=run, checkpoint=next_checkpoint)

        final_text = step.output_text.strip()
        if not final_text:
            self._fail_run(
                run=run,
                messages=checkpoint.messages,
                status_message="model returned no final text and no tool calls",
                lease_owner=self._worker_id,
            )
            return False
        structured_output = _parse_structured_output(output_text=final_text, response_schema=checkpoint.response_schema)
        terminal_checkpoint = replace(
            checkpoint,
            phase=PHASE_TERMINAL,
            conversation=list(checkpoint.conversation),
            updated_at=datetime.now(tz=UTC),
        )
        completed = self._store.mark_turn_succeeded(
            turn_id=checkpoint.turn_id,
            messages=_append_assistant_message(checkpoint.messages, final_text),
            output_text=final_text,
            structured_output=structured_output,
            checkpoint=terminal_checkpoint,
            lease_owner=self._worker_id,
        )
        if completed.status != agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED:
            return False
        return False

    def _continue_pending_tool(self, *, run: StoredRun, checkpoint: StoredTurnCheckpoint) -> bool:
        prepared = _prepared_from_checkpoint(checkpoint)
        pending = checkpoint.pending_tool_call or {}
        tool_call_id = str(pending.get("tool_call_id") or "")
        tool_name = str(pending.get("tool_name") or "")
        resolved_tool_id = str(pending.get("resolved_tool_id") or "")
        if not tool_call_id or not tool_name or not resolved_tool_id:
            self._fail_run(
                run=run,
                messages=checkpoint.messages,
                status_message="checkpoint is missing pending tool call",
                lease_owner=self._worker_id,
            )
            return False
        tool_specs, function_name_to_tool_id, loaded_tool_ids = _tool_registry_from_checkpoint(checkpoint)
        arguments = copy.deepcopy(pending.get("arguments") or {})
        if checkpoint.phase == PHASE_TOOL_RESULT_RECORDED:
            result = self._store.get_tool_result(turn_id=checkpoint.turn_id, tool_call_id=tool_call_id)
            if result is None:
                self._fail_run(
                    run=run,
                    messages=checkpoint.messages,
                    status_message="checkpoint references missing tool result",
                    lease_owner=self._worker_id,
                )
                return False
            return self._record_tool_result_in_conversation(checkpoint=checkpoint, result=result)

        execution_arguments = copy.deepcopy(arguments)
        validation_error = _tool_arguments_validation_error(
            tool_name=tool_name, arguments=execution_arguments, tool_specs=tool_specs
        )
        self._store.append_turn_event_once(
            event_key=f"{checkpoint.turn_id}:tool:{tool_call_id}:started",
            turn_id=checkpoint.turn_id,
            event_type="tool.started",
            source=checkpoint.provider_name,
            data={"tool_call_id": tool_call_id, "tool_id": resolved_tool_id, "arguments": execution_arguments},
        )
        inflight = replace(
            checkpoint,
            phase=PHASE_TOOL_INFLIGHT,
            pending_tool_call={
                **pending,
                "arguments": copy.deepcopy(arguments),
                "execution_arguments": copy.deepcopy(execution_arguments),
            },
            updated_at=datetime.now(tz=UTC),
        )
        self._store.put_turn_checkpoint(inflight, lease_owner=self._worker_id)
        if validation_error:
            result = {"status": 400, "body": validation_error, "is_error": True, "tool_id": resolved_tool_id}
            try:
                self._store.put_tool_result(
                    turn_id=checkpoint.turn_id,
                    tool_call_id=tool_call_id,
                    result=result,
                    lease_owner=self._worker_id,
                )
            except RuntimeError:
                return False
            self._store.append_turn_event_once(
                event_key=f"{checkpoint.turn_id}:tool:{tool_call_id}:completed",
                turn_id=checkpoint.turn_id,
                event_type="tool.completed",
                source=checkpoint.provider_name,
                data={
                    "tool_call_id": tool_call_id,
                    "tool_id": resolved_tool_id,
                    "status": 400,
                    "error": validation_error,
                },
            )
            return self._record_tool_result_in_conversation(
                checkpoint=replace(inflight, phase=PHASE_TOOL_RESULT_RECORDED), result=result
            )

        if resolved_tool_id == TOOL_SEARCH_TOOL_ID:
            with gestalt.AgentHost() as host:
                body = _search_tools_for_model(
                    host=host,
                    prepared=prepared,
                    tool_call_arguments=arguments,
                    tool_specs=tool_specs,
                    function_name_to_tool_id=function_name_to_tool_id,
                    loaded_tool_ids=loaded_tool_ids,
                )
            result = {"status": 200, "body": body, "tool_id": resolved_tool_id}
            updated = replace(
                inflight,
                tool_specs=copy.deepcopy(tool_specs),
                function_name_to_tool_id=dict(function_name_to_tool_id),
                loaded_tool_ids=sorted(loaded_tool_ids),
            )
            try:
                self._store.put_tool_result(
                    turn_id=checkpoint.turn_id,
                    tool_call_id=tool_call_id,
                    result=result,
                    lease_owner=self._worker_id,
                )
            except RuntimeError:
                return False
            self._store.append_turn_event_once(
                event_key=f"{checkpoint.turn_id}:tool:{tool_call_id}:completed",
                turn_id=checkpoint.turn_id,
                event_type="tool.completed",
                source=checkpoint.provider_name,
                data={"tool_call_id": tool_call_id, "tool_id": resolved_tool_id, "status": 200},
            )
            return self._record_tool_result_in_conversation(
                checkpoint=replace(updated, phase=PHASE_TOOL_RESULT_RECORDED), result=result
            )

        with gestalt.AgentHost() as host:
            tool_response = host.execute_tool(
                _execute_tool_request(
                    checkpoint=checkpoint,
                    tool_call_id=tool_call_id,
                    resolved_tool_id=resolved_tool_id,
                    execution_arguments=execution_arguments,
                )
            )
        current = self._store.get_turn(checkpoint.turn_id)
        if current is not None and current.status == agent_pb2.AGENT_EXECUTION_STATUS_CANCELED:
            return False
        result = {
            "status": int(tool_response.status or 0),
            "body": str(tool_response.body or ""),
            "is_error": int(tool_response.status or 0) >= 400,
            "tool_id": resolved_tool_id,
        }
        try:
            self._store.put_tool_result(
                turn_id=checkpoint.turn_id, tool_call_id=tool_call_id, result=result, lease_owner=self._worker_id
            )
        except RuntimeError:
            return False
        self._store.append_turn_event_once(
            event_key=f"{checkpoint.turn_id}:tool:{tool_call_id}:completed",
            turn_id=checkpoint.turn_id,
            event_type="tool.completed",
            source=checkpoint.provider_name,
            data={"tool_call_id": tool_call_id, "tool_id": resolved_tool_id, "status": result["status"]},
        )
        return self._record_tool_result_in_conversation(
            checkpoint=replace(inflight, phase=PHASE_TOOL_RESULT_RECORDED), result=result
        )

    def _record_tool_result_in_conversation(self, *, checkpoint: StoredTurnCheckpoint, result: dict[str, Any]) -> bool:
        pending = checkpoint.pending_tool_call or {}
        tool_call_id = str(pending.get("tool_call_id") or "")
        conversation = copy.deepcopy(checkpoint.conversation)
        conversation.append(
            _tool_result_message(
                tool_call_id=tool_call_id, content=str(result.get("body") or ""), is_error=bool(result.get("is_error"))
            )
        )
        remaining_tool_calls = _remaining_tool_calls_from_pending(pending)
        if remaining_tool_calls:
            next_pending = remaining_tool_calls[0]
            next_pending["remaining_tool_calls"] = remaining_tool_calls[1:]
            tool_specs, _, _ = _tool_registry_from_checkpoint(checkpoint)
            self._store.put_turn_checkpoint(
                replace(
                    checkpoint,
                    phase=PHASE_TOOL_READY,
                    conversation=conversation,
                    pending_tool_call=next_pending,
                    updated_at=datetime.now(tz=UTC),
                ),
                lease_owner=self._worker_id,
            )
            return True
        self._store.put_turn_checkpoint(
            replace(
                checkpoint,
                phase=PHASE_MODEL_NEXT,
                conversation=conversation,
                pending_tool_call=None,
                updated_at=datetime.now(tz=UTC),
            ),
            lease_owner=self._worker_id,
        )
        return True

    def _fail_run(
        self, *, run: StoredRun, messages: list[dict[str, Any]], status_message: str, lease_owner: str = ""
    ) -> None:
        cleaned_status_message = status_message.strip()
        try:
            failed = self._store.mark_turn_failed(
                turn_id=run.run_id, messages=messages, status_message=cleaned_status_message, lease_owner=lease_owner
            )
        except RuntimeError:
            if lease_owner:
                return
            raise
        if failed.status != agent_pb2.AGENT_EXECUTION_STATUS_FAILED:
            return

    def turn_to_proto(self, run: StoredRun, *, summary_only: bool = False) -> Any:
        proto = agent_pb2.AgentTurn(
            id=run.run_id,
            session_id=run.session_ref,
            provider_name=run.provider_name,
            model=run.model,
            status=run.status,
            messages=[] if summary_only else [_message_from_dict(message) for message in run.messages],
            output_text="" if summary_only else run.output_text,
            status_message=run.status_message,
            execution_ref=run.execution_ref,
        )
        if run.structured_output and not summary_only:
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
    tool_grant: str
    tool_specs_and_names: tuple[list[dict[str, Any]], dict[str, str], set[str]]


def _resume_seed_from_request(
    *,
    messages: list[dict[str, Any]],
    response_schema: dict[str, Any],
    provider_options: dict[str, Any],
    tool_grant: str,
    tool_specs_and_names: tuple[list[dict[str, Any]], dict[str, str], set[str]],
    system_prompt: str,
) -> dict[str, Any]:
    tool_specs, function_name_to_tool_id, loaded_tool_ids = _copy_tool_registry(tool_specs_and_names)
    return {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "phase": PHASE_MODEL_NEXT,
        "messages": copy.deepcopy(messages),
        "conversation": _build_initial_conversation(
            system_prompt=system_prompt, projected_messages=messages, response_schema=response_schema
        ),
        "response_schema": copy.deepcopy(response_schema),
        "provider_options": copy.deepcopy(provider_options),
        "tool_grant": tool_grant,
        "tool_specs": copy.deepcopy(tool_specs),
        "function_name_to_tool_id": dict(function_name_to_tool_id),
        "loaded_tool_ids": sorted(loaded_tool_ids),
        "step_index": 0,
        "pending_tool_call": None,
    }


def _prepared_from_checkpoint(checkpoint: StoredTurnCheckpoint) -> PreparedTurn:
    return PreparedTurn(
        session_id=checkpoint.session_id,
        turn_id=checkpoint.turn_id,
        provider_name=checkpoint.provider_name,
        resolved_model=checkpoint.model,
        messages=copy.deepcopy(checkpoint.messages),
        response_schema=copy.deepcopy(checkpoint.response_schema),
        provider_options=copy.deepcopy(checkpoint.provider_options),
        tool_grant=checkpoint.tool_grant,
        tool_specs_and_names=_tool_registry_from_checkpoint(checkpoint),
    )


def _tool_registry_from_checkpoint(
    checkpoint: StoredTurnCheckpoint,
) -> tuple[list[dict[str, Any]], dict[str, str], set[str]]:
    return (
        copy.deepcopy(checkpoint.tool_specs),
        dict(checkpoint.function_name_to_tool_id),
        set(checkpoint.loaded_tool_ids),
    )


def _pending_tool_call(
    *,
    tool_call_id: str,
    tool_name: str,
    resolved_tool_id: str,
    arguments: dict[str, Any],
    assistant_message_index: int | None = None,
) -> dict[str, Any]:
    pending: dict[str, Any] = {
        "tool_call_id": tool_call_id,
        "tool_name": tool_name,
        "resolved_tool_id": resolved_tool_id,
        "arguments": copy.deepcopy(arguments),
    }
    if assistant_message_index is not None:
        pending["assistant_message_index"] = assistant_message_index
    return pending


def _remaining_tool_calls_from_pending(pending_tool_call: dict[str, Any]) -> list[dict[str, Any]]:
    raw_remaining = pending_tool_call.get("remaining_tool_calls")
    if not isinstance(raw_remaining, list):
        return []
    return [copy.deepcopy(call) for call in raw_remaining if isinstance(call, dict)]


def _uncertain_tool_status_message(pending_tool_call: dict[str, Any] | None) -> str:
    tool_call_id = ""
    if isinstance(pending_tool_call, dict):
        tool_call_id = str(pending_tool_call.get("tool_call_id") or "").strip()
    if not tool_call_id:
        tool_call_id = "unknown"
    return f"tool call {tool_call_id} may have executed before provider restart; refusing to replay without a durable completed result"


def _execute_tool_request(
    *, checkpoint: StoredTurnCheckpoint, tool_call_id: str, resolved_tool_id: str, execution_arguments: dict[str, Any]
) -> Any:
    request = agent_pb2.ExecuteAgentToolRequest(
        session_id=checkpoint.session_id,
        turn_id=checkpoint.turn_id,
        tool_call_id=tool_call_id,
        tool_id=resolved_tool_id,
        arguments=_dict_to_struct(execution_arguments),
        tool_grant=checkpoint.tool_grant,
    )
    request.idempotency_key = _tool_invocation_idempotency_key(checkpoint=checkpoint, tool_call_id=tool_call_id)
    return request


def _tool_invocation_idempotency_key(*, checkpoint: StoredTurnCheckpoint, tool_call_id: str) -> str:
    return f"agent/simple:{checkpoint.provider_name}:{checkpoint.turn_id}:{tool_call_id}"


def _checkpoint_messages(
    checkpoint: StoredTurnCheckpoint | None, *, fallback: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    if checkpoint is not None and checkpoint.messages:
        return copy.deepcopy(checkpoint.messages)
    return copy.deepcopy(fallback)


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
    load_refs = _tool_search_load_refs(tool_call_arguments.get("load_refs"))
    if not query and not load_refs:
        query = _tool_search_query(prepared.messages)
    request = agent_pb2.SearchAgentToolsRequest(
        session_id=prepared.session_id,
        turn_id=prepared.turn_id,
        query=query,
        tool_grant=prepared.tool_grant,
    )
    adaptive_supported = _proto_message_has_field(request, "candidate_limit")
    load_refs_supported = _proto_message_has_field(request, "load_refs")
    candidate_limit = _tool_search_candidate_limit(
        tool_call_arguments.get("candidate_limit"), default=0 if load_refs else TOOL_SEARCH_DEFAULT_CANDIDATE_LIMIT
    )
    request.max_results = _tool_search_max_results(
        tool_call_arguments.get("max_results"),
        default=(
            0
            if load_refs
            else TOOL_SEARCH_ADAPTIVE_DEFAULT_MAX_RESULTS
            if adaptive_supported and candidate_limit > 0
            else TOOL_SEARCH_LEGACY_DEFAULT_MAX_RESULTS
        ),
        allow_zero=bool(load_refs),
    )
    if adaptive_supported:
        request.candidate_limit = candidate_limit
    if load_refs:
        if not load_refs_supported:
            return json.dumps(
                {
                    "tools": [],
                    "error": "This provider SDK does not support loading tool refs yet; search by query instead.",
                },
                separators=(",", ":"),
            )
        request.load_refs.extend(load_refs)
    response = host.search_tools(request)
    available_tools = _register_resolved_tools(
        response.tools,
        tool_specs=tool_specs,
        function_name_to_tool_id=function_name_to_tool_id,
        loaded_tool_ids=loaded_tool_ids,
    )
    body: dict[str, Any] = {"tools": available_tools}
    candidates = _tool_search_candidate_entries(getattr(response, "candidates", []))
    if candidates:
        body["candidates"] = candidates
    if bool(getattr(response, "has_more", False)):
        body["has_more"] = True
    return json.dumps(body, separators=(",", ":"))


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


def _tool_search_max_results(raw_value: Any, *, default: int, allow_zero: bool = False) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return default
    if value < 0:
        return default
    if value == 0:
        return 0 if allow_zero else default
    return min(value, TOOL_SEARCH_MAX_RESULTS)


def _tool_search_candidate_limit(raw_value: Any, *, default: int) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return default
    if value <= 0:
        return 0
    return min(value, TOOL_SEARCH_MAX_CANDIDATES)


def _tool_search_load_refs(raw_value: Any) -> list[Any]:
    if not isinstance(raw_value, list):
        return []
    refs: list[Any] = []
    for item in raw_value:
        if not isinstance(item, dict):
            continue
        plugin = str(item.get("plugin", "") or "").strip()
        operation = str(item.get("operation", "") or "").strip()
        if not plugin or not operation:
            continue
        ref = agent_pb2.AgentToolRef(plugin=plugin, operation=operation)
        _set_proto_string_field(ref, "system", item.get("system"))
        _set_proto_string_field(ref, "connection", item.get("connection"))
        _set_proto_string_field(ref, "instance", item.get("instance"))
        refs.append(ref)
    return refs


def _tool_search_candidate_entries(candidates: Any) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for candidate in candidates:
        ref = _tool_ref_to_dict(getattr(candidate, "ref", None))
        if not ref:
            continue
        entry: dict[str, Any] = {"ref": ref}
        candidate_id = str(getattr(candidate, "id", "") or "").strip()
        if candidate_id:
            entry["id"] = candidate_id
        name = str(getattr(candidate, "name", "") or "").strip()
        if name:
            entry["name"] = name
        description = str(getattr(candidate, "description", "") or "").strip()
        if description:
            entry["description"] = description
        parameters = [str(param).strip() for param in getattr(candidate, "parameters", []) if str(param).strip()]
        if parameters:
            entry["parameters"] = parameters
        entries.append(entry)
    return entries


def _tool_ref_to_dict(ref: Any) -> dict[str, str]:
    if ref is None:
        return {}
    out: dict[str, str] = {}
    for field in ("system", "plugin", "operation", "connection", "instance"):
        value = str(getattr(ref, field, "") or "").strip()
        if value:
            out[field] = value
    if not out.get("plugin") or not out.get("operation"):
        return {}
    return out


def _proto_message_has_field(message: Any, field_name: str) -> bool:
    descriptor = getattr(message, "DESCRIPTOR", None)
    fields_by_name = getattr(descriptor, "fields_by_name", {})
    return field_name in fields_by_name


def _tool_search_adaptive_supported() -> bool:
    return (
        _proto_message_has_field(agent_pb2.SearchAgentToolsRequest(), "candidate_limit")
        and _proto_message_has_field(agent_pb2.SearchAgentToolsRequest(), "load_refs")
        and _proto_message_has_field(agent_pb2.SearchAgentToolsResponse(), "candidates")
        and _proto_message_has_field(agent_pb2.SearchAgentToolsResponse(), "has_more")
    )


def _tool_search_system_prompt() -> str:
    if _tool_search_adaptive_supported():
        return TOOL_SEARCH_SYSTEM_PROMPT + TOOL_SEARCH_ADAPTIVE_SYSTEM_PROMPT
    return TOOL_SEARCH_SYSTEM_PROMPT


def _tool_search_tool_spec() -> dict[str, Any]:
    tool_spec = copy.deepcopy(TOOL_SEARCH_TOOL_SPEC)
    function_spec = cast(dict[str, Any], tool_spec["function"])
    parameters = cast(dict[str, Any], function_spec["parameters"])
    if _tool_search_adaptive_supported():
        return tool_spec

    properties = cast(dict[str, Any], parameters["properties"])
    properties.pop("candidate_limit", None)
    properties.pop("load_refs", None)
    max_results = cast(dict[str, Any], properties["max_results"])
    max_results["minimum"] = 1
    parameters["required"] = ["query"]
    return tool_spec


def _set_proto_string_field(message: Any, field_name: str, raw_value: Any) -> None:
    if not _proto_message_has_field(message, field_name):
        return
    value = str(raw_value or "").strip()
    if value:
        setattr(message, field_name, value)


def _tool_search_query(messages: list[dict[str, Any]]) -> str:
    parts = [_message_content_text(message) for message in messages]
    return "\n".join(part for part in parts if part).strip()


def _build_initial_conversation(
    *, system_prompt: str, projected_messages: list[dict[str, Any]], response_schema: dict[str, Any] | None
) -> list[dict[str, Any]]:
    conversation: list[dict[str, Any]] = []
    if system_prompt:
        conversation.append({"role": "system", "content": system_prompt})
    conversation.append({"role": "system", "content": _tool_search_system_prompt()})
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
    tool_specs = [_tool_search_tool_spec()]
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
        parameters = copy.deepcopy(_struct_to_dict(tool.parameters_schema))
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
