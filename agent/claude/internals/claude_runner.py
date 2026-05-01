from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import re
import tempfile
import threading
from dataclasses import dataclass
from typing import Any, Callable, Literal, cast

import gestalt
from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    create_sdk_mcp_server,
    tool,
)
from google.protobuf import struct_pb2 as _struct_pb2
from mcp.types import ToolAnnotations

from gestalt.gen.v1 import agent_pb2 as _agent_pb2

from .config import ClaudeAgentConfig

agent_pb2: Any = cast(Any, _agent_pb2)
struct_pb2: Any = _struct_pb2
logger = logging.getLogger(__name__)

MCP_SERVER_NAME = "gestalt"
DEFAULT_PAGE_SIZE = 100
MAX_LISTED_TOOLS = 1000
MAX_PAGES = 100
MAX_ERROR_TEXT = 4000
_UNSAFE_TOOL_NAME = re.compile(r"[*?,\s\x00-\x1f\x7f]")


class ClaudeExecutionError(RuntimeError):
    pass


class ClaudeExecutionCanceled(ClaudeExecutionError):
    pass


ClientFactory = Callable[..., Any]
PermissionMode = Literal["default", "acceptEdits", "plan", "bypassPermissions", "dontAsk", "auto"]


@dataclass(slots=True)
class ToolEntry:
    tool_id: str
    mcp_name: str
    title: str
    description: str
    input_schema: dict[str, Any]
    annotations: ToolAnnotations | None


@dataclass(slots=True)
class _ActiveTurn:
    loop: asyncio.AbstractEventLoop
    client: Any | None = None


class ClaudeSDKRunner:
    def __init__(self, config: ClaudeAgentConfig, *, client_factory: ClientFactory | None = None) -> None:
        self._config = config
        self._client_factory = client_factory or ClaudeSDKClient
        self._lock = threading.RLock()
        self._active_turns: dict[str, _ActiveTurn] = {}
        self._canceled_turns: set[str] = set()

    def run_turn(
        self, *, session_id: str, turn_id: str, model: str, messages: list[dict[str, Any]], tool_grant: str
    ) -> str:
        try:
            return asyncio.run(
                asyncio.wait_for(
                    self._run_turn(
                        session_id=session_id,
                        turn_id=turn_id,
                        model=model,
                        messages=messages,
                        tool_grant=tool_grant,
                    ),
                    timeout=self._config.timeout_seconds,
                )
            )
        except TimeoutError as exc:
            self.cancel_turn(turn_id)
            raise ClaudeExecutionError(f"Claude Agent SDK timed out after {self._config.timeout_seconds:g}s") from exc

    def cancel_turn(self, turn_id: str) -> None:
        turn_id = turn_id.strip()
        if not turn_id:
            return
        active: _ActiveTurn | None = None
        with self._lock:
            self._canceled_turns.add(turn_id)
            active = self._active_turns.get(turn_id)
        if active is not None and active.client is not None:
            _schedule_interrupt(active.loop, active.client)

    def close(self) -> None:
        with self._lock:
            active_turns = list(self._active_turns.items())
            self._canceled_turns.update(turn_id for turn_id, _active in active_turns)
        for _turn_id, active in active_turns:
            if active.client is not None:
                _schedule_interrupt(active.loop, active.client)

    async def _run_turn(
        self, *, session_id: str, turn_id: str, model: str, messages: list[dict[str, Any]], tool_grant: str
    ) -> str:
        loop = asyncio.get_running_loop()
        self._register_active_turn(turn_id, _ActiveTurn(loop=loop))
        try:
            self._raise_if_canceled(turn_id)
            prompt = _messages_to_prompt(messages)
            if not prompt:
                raise ClaudeExecutionError("turn prompt is empty")

            listed_tools = await asyncio.to_thread(
                _list_tools, session_id=session_id, turn_id=turn_id, tool_grant=tool_grant
            )
            sdk_tools = _sdk_tools(
                listed_tools=listed_tools, session_id=session_id, turn_id=turn_id, tool_grant=tool_grant
            )
            with tempfile.TemporaryDirectory(prefix="gestalt-claude-sdk-") as config_dir:
                options = self._options(model=model, sdk_tools=sdk_tools, listed_tools=listed_tools)
                _set_config_dir(options, config_dir)
                client = self._client_factory(options=options)
                self._register_active_client(turn_id, client)
                self._raise_if_canceled(turn_id)
                await client.connect()
                await client.query(prompt, session_id=turn_id)
                output = await self._receive_result(client, turn_id=turn_id)
                self._raise_if_canceled(turn_id)
                return output
        finally:
            active = self._unregister_active_turn(turn_id)
            if active is not None and active.client is not None:
                try:
                    await active.client.disconnect()
                except Exception:
                    logger.exception("failed to disconnect Claude SDK client")

    async def _receive_result(self, client: Any, *, turn_id: str) -> str:
        text_blocks: list[str] = []
        tool_blocks: list[str] = []
        result_message: Any | None = None
        async for message in client.receive_response():
            self._raise_if_canceled(turn_id)
            if isinstance(message, AssistantMessage):
                for block in message.content:
                    if isinstance(block, TextBlock):
                        text_blocks.append(block.text)
                    elif isinstance(block, ToolUseBlock):
                        tool_blocks.append(
                            json.dumps(
                                {"tool_use": {"id": block.id, "name": block.name, "input": block.input}},
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                        )
                    elif isinstance(block, ToolResultBlock):
                        tool_blocks.append(
                            json.dumps(
                                {
                                    "tool_result": {
                                        "tool_use_id": block.tool_use_id,
                                        "content": _jsonable(block.content),
                                        "is_error": block.is_error,
                                    }
                                },
                                sort_keys=True,
                                separators=(",", ":"),
                            )
                        )
            elif isinstance(message, ResultMessage):
                result_message = message
                break

        if result_message is None:
            raise ClaudeExecutionError("Claude Agent SDK response ended without a result")
        subtype = str(getattr(result_message, "subtype", "") or "")
        if bool(getattr(result_message, "is_error", False)) or subtype != "success":
            detail = str(getattr(result_message, "result", "") or getattr(result_message, "stop_reason", "") or subtype)
            if self._is_canceled(turn_id) or subtype in {"interrupted", "canceled", "cancelled"}:
                raise ClaudeExecutionCanceled(_truncate(detail or "Claude Agent SDK turn was canceled"))
            raise ClaudeExecutionError(_truncate(detail or f"Claude Agent SDK returned {subtype!r}"))

        result_text = str(getattr(result_message, "result", "") or "").strip()
        if result_text:
            return result_text
        if text_blocks:
            return "\n".join(block for block in text_blocks if block).strip()
        if tool_blocks:
            return "\n".join(tool_blocks).strip()
        return ""

    def _options(self, *, model: str, sdk_tools: list[Any], listed_tools: list[ToolEntry]) -> Any:
        env: dict[str, str] = {}
        if self._config.anthropic_api_key:
            env["ANTHROPIC_API_KEY"] = self._config.anthropic_api_key
        return ClaudeAgentOptions(
            tools=[],
            allowed_tools=[f"mcp__{MCP_SERVER_NAME}__{entry.mcp_name}" for entry in listed_tools],
            mcp_servers={MCP_SERVER_NAME: create_sdk_mcp_server(MCP_SERVER_NAME, tools=sdk_tools)},
            model=model,
            cwd=self._config.working_directory or None,
            system_prompt=self._config.system_prompt or None,
            permission_mode=cast(PermissionMode, self._config.permission_mode),
            cli_path=self._config.cli_path or None,
            env=env,
            setting_sources=[],
            skills=[],
            plugins=[],
            agents=None,
        )

    def _register_active_turn(self, turn_id: str, active: _ActiveTurn) -> None:
        with self._lock:
            self._active_turns[turn_id] = active

    def _register_active_client(self, turn_id: str, client: Any) -> None:
        should_interrupt = False
        with self._lock:
            active = self._active_turns.get(turn_id)
            if active is not None:
                active.client = client
            should_interrupt = turn_id in self._canceled_turns
        if should_interrupt:
            active = self._active_turns.get(turn_id)
            if active is not None:
                _schedule_interrupt(active.loop, client)

    def _unregister_active_turn(self, turn_id: str) -> _ActiveTurn | None:
        with self._lock:
            active = self._active_turns.pop(turn_id, None)
            self._canceled_turns.discard(turn_id)
            return active

    def _raise_if_canceled(self, turn_id: str) -> None:
        if self._is_canceled(turn_id):
            raise ClaudeExecutionCanceled("Claude Agent SDK turn was canceled")

    def _is_canceled(self, turn_id: str) -> bool:
        with self._lock:
            return turn_id in self._canceled_turns


def _list_tools(*, session_id: str, turn_id: str, tool_grant: str) -> list[ToolEntry]:
    page_token = ""
    seen_tokens: set[str] = set()
    tools: list[ToolEntry] = []
    seen_names: set[str] = set()
    pages = 0
    with gestalt.AgentHost() as host:
        while True:
            pages += 1
            if pages > MAX_PAGES:
                raise ClaudeExecutionError(f"ListTools exceeded {MAX_PAGES} pages")
            if page_token in seen_tokens:
                raise ClaudeExecutionError(f"ListTools repeated page token {page_token!r}")
            seen_tokens.add(page_token)
            response = host.list_tools(
                agent_pb2.ListAgentToolsRequest(
                    session_id=session_id,
                    turn_id=turn_id,
                    page_size=DEFAULT_PAGE_SIZE,
                    page_token=page_token,
                    tool_grant=tool_grant,
                )
            )
            for listed in response.tools:
                entry = _tool_entry(listed)
                if entry.mcp_name in seen_names:
                    raise ClaudeExecutionError(f"ListTools returned duplicate mcp_name {entry.mcp_name!r}")
                seen_names.add(entry.mcp_name)
                tools.append(entry)
                if len(tools) > MAX_LISTED_TOOLS:
                    raise ClaudeExecutionError(f"ListTools returned more than {MAX_LISTED_TOOLS} tools")
            page_token = str(response.next_page_token or "").strip()
            if not page_token:
                break
    if not tools:
        raise ClaudeExecutionError("ListTools returned no tools for the requested grant")
    return tools


def _tool_entry(tool_proto: Any) -> ToolEntry:
    tool_id = str(tool_proto.id or "").strip()
    mcp_name = str(tool_proto.mcp_name or "").strip()
    if not tool_id:
        raise ClaudeExecutionError("ListTools returned a tool without an id")
    if not mcp_name:
        raise ClaudeExecutionError("ListTools returned a tool without an mcp_name")
    if _UNSAFE_TOOL_NAME.search(mcp_name):
        raise ClaudeExecutionError(f"ListTools returned unsafe mcp_name {mcp_name!r}")
    return ToolEntry(
        tool_id=tool_id,
        mcp_name=mcp_name,
        title=str(tool_proto.title or "").strip(),
        description=str(tool_proto.description or "").strip(),
        input_schema=_schema_from_json(str(tool_proto.input_schema or "")),
        annotations=_annotations(tool_proto.annotations, title=str(tool_proto.title or "").strip()),
    )


def _sdk_tools(*, listed_tools: list[ToolEntry], session_id: str, turn_id: str, tool_grant: str) -> list[Any]:
    execute_lock = asyncio.Lock()
    sequence = 0

    def make_handler(entry: ToolEntry) -> Any:
        @tool(entry.mcp_name, entry.description or entry.title or entry.mcp_name, entry.input_schema, annotations=entry.annotations)
        async def handler(args: dict[str, Any]) -> dict[str, Any]:
            nonlocal sequence
            async with execute_lock:
                sequence += 1
                tool_call_id = f"sdk-{sequence}"
                response = await asyncio.to_thread(
                    _execute_tool,
                    session_id=session_id,
                    turn_id=turn_id,
                    tool_grant=tool_grant,
                    entry=entry,
                    tool_call_id=tool_call_id,
                    arguments=args,
                )
                body = str(getattr(response, "body", "") or "")
                status = int(getattr(response, "status", 0) or 0)
                if not body:
                    body = "{}"
                return {"content": [{"type": "text", "text": body}], "is_error": status >= 400}

        return handler

    return [make_handler(entry) for entry in listed_tools]


def _execute_tool(
    *, session_id: str, turn_id: str, tool_grant: str, entry: ToolEntry, tool_call_id: str, arguments: dict[str, Any]
) -> Any:
    struct = struct_pb2.Struct()
    struct.update(arguments or {})
    with gestalt.AgentHost() as host:
        return host.execute_tool(
            agent_pb2.ExecuteAgentToolRequest(
                session_id=session_id,
                turn_id=turn_id,
                tool_call_id=tool_call_id,
                tool_id=entry.tool_id,
                arguments=struct,
                tool_grant=tool_grant,
                idempotency_key=f"agent/claude-sdk:{turn_id}:{tool_call_id}:{entry.mcp_name}",
            )
        )


def _schema_from_json(value: str) -> dict[str, Any]:
    value = value.strip()
    if not value:
        return {"type": "object", "additionalProperties": True}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {"type": "object", "additionalProperties": True}
    if not isinstance(payload, dict):
        return {"type": "object", "additionalProperties": True}
    if payload.get("type") != "object":
        payload = {"type": "object", "properties": {}, "additionalProperties": True}
    return payload


def _annotations(annotations: Any, *, title: str) -> ToolAnnotations | None:
    values: dict[str, Any] = {}
    if title:
        values["title"] = title
    for proto_name, sdk_name in (
        ("read_only_hint", "readOnlyHint"),
        ("idempotent_hint", "idempotentHint"),
        ("destructive_hint", "destructiveHint"),
        ("open_world_hint", "openWorldHint"),
    ):
        try:
            has_field = annotations.HasField(proto_name)
        except ValueError:
            has_field = False
        if has_field:
            values[sdk_name] = bool(getattr(annotations, proto_name))
    return ToolAnnotations(**values) if values else None


def _messages_to_prompt(messages: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for index, message in enumerate(messages, start=1):
        role = str(message.get("role") or "user").strip() or "user"
        content = _message_content(message)
        if not content:
            continue
        lines.append(f"<message {index} role={json.dumps(role)}>\n{content}\n</message {index}>")
    return "\n\n".join(lines).strip()


def _message_content(message: dict[str, Any]) -> str:
    chunks: list[str] = []
    text = str(message.get("text") or "").strip()
    if text:
        chunks.append(text)
    for part in message.get("parts") or []:
        if not isinstance(part, dict):
            continue
        part_text = str(part.get("text") or "").strip()
        if part_text:
            chunks.append(part_text)
        if "json" in part:
            chunks.append(json.dumps(part["json"], sort_keys=True, separators=(",", ":")))
        tool_call = part.get("tool_call")
        if isinstance(tool_call, dict):
            chunks.append(json.dumps({"tool_call": tool_call}, sort_keys=True, separators=(",", ":")))
        tool_result = part.get("tool_result")
        if isinstance(tool_result, dict):
            chunks.append(json.dumps({"tool_result": tool_result}, sort_keys=True, separators=(",", ":")))
        image_ref = part.get("image_ref")
        if isinstance(image_ref, dict):
            chunks.append(json.dumps({"image_ref": image_ref}, sort_keys=True, separators=(",", ":")))
    return "\n".join(chunks).strip()


def _schedule_interrupt(loop: asyncio.AbstractEventLoop, client: Any) -> concurrent.futures.Future[Any] | None:
    try:
        return asyncio.run_coroutine_threadsafe(client.interrupt(), loop)
    except RuntimeError:
        return None


def _set_config_dir(options: Any, config_dir: str) -> None:
    env = dict(getattr(options, "env", {}) or {})
    env["CLAUDE_CONFIG_DIR"] = config_dir
    options.env = env


def _jsonable(value: Any) -> Any:
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if hasattr(value, "model_dump"):
        return _jsonable(value.model_dump())
    return str(value)


def _truncate(value: str) -> str:
    value = value.strip()
    if len(value) <= MAX_ERROR_TEXT:
        return value
    return value[:MAX_ERROR_TEXT] + "..."
