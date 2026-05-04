from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import tempfile
import threading
from dataclasses import dataclass
from typing import Any, Callable, Literal, cast

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
)

from .config import ClaudeAgentConfig
from .mcp_bridge import (
    MCP_SERVER_NAME,
    allow_gestalt_mcp_tool,
    allowed_gestalt_mcp_tools,
    create_gestalt_sdk_mcp_server,
)

logger = logging.getLogger(__name__)

MAX_ERROR_TEXT = 4000
GESTALT_MCP_CATALOG_PROMPT = (
    "Gestalt MCP catalog tools are available through the `gestalt` MCP server for connected apps such as "
    "Linear, GitHub, Slack, Gmail, Google Drive, Google Calendar, Google Docs, Google Sheets, BigQuery, Datadog, "
    "PagerDuty, Notion, Figma, Ramp, Ashby, and other Valon integrations. When a user asks for data or actions "
    "in an external service, use Claude Code native tool search over the `gestalt` MCP server before concluding "
    "the tool is unavailable. Do not infer tool availability from Claude Code built-in tools only."
)


class ClaudeExecutionError(RuntimeError):
    pass


class ClaudeExecutionCanceled(ClaudeExecutionError):
    pass


ClientFactory = Callable[..., Any]
PermissionMode = Literal["default", "acceptEdits", "plan", "bypassPermissions", "dontAsk", "auto"]


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
        self, *, session_id: str, turn_id: str, model: str, messages: list[dict[str, Any]], run_grant: str
    ) -> str:
        try:
            return asyncio.run(
                asyncio.wait_for(
                    self._run_turn(
                        session_id=session_id, turn_id=turn_id, model=model, messages=messages, run_grant=run_grant
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
        self, *, session_id: str, turn_id: str, model: str, messages: list[dict[str, Any]], run_grant: str
    ) -> str:
        loop = asyncio.get_running_loop()
        self._register_active_turn(turn_id, _ActiveTurn(loop=loop))
        try:
            self._raise_if_canceled(turn_id)
            prompt = _messages_to_prompt(messages)
            if not prompt:
                raise ClaudeExecutionError("turn prompt is empty")

            with tempfile.TemporaryDirectory(prefix="gestalt-claude-sdk-") as config_dir:
                options = self._options(model=model, session_id=session_id, turn_id=turn_id, run_grant=run_grant)
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

    def _options(self, *, model: str, session_id: str, turn_id: str, run_grant: str) -> Any:
        env: dict[str, str] = {"ENABLE_TOOL_SEARCH": "auto:5"}
        if self._config.anthropic_api_key:
            env["ANTHROPIC_API_KEY"] = self._config.anthropic_api_key
        return ClaudeAgentOptions(
            tools=[],
            allowed_tools=allowed_gestalt_mcp_tools(),
            mcp_servers={
                MCP_SERVER_NAME: create_gestalt_sdk_mcp_server(
                    session_id=session_id, turn_id=turn_id, run_grant=run_grant
                )
            },
            model=model,
            cwd=self._config.working_directory or None,
            system_prompt=_system_prompt(self._config.system_prompt),
            permission_mode=cast(PermissionMode, self._config.permission_mode),
            cli_path=self._config.cli_path or None,
            env=env,
            setting_sources=[],
            skills=[],
            plugins=[],
            agents=None,
            can_use_tool=allow_gestalt_mcp_tool,
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


def _messages_to_prompt(messages: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for index, message in enumerate(messages, start=1):
        role = str(message.get("role") or "user").strip() or "user"
        content = _message_content(message)
        if not content:
            continue
        lines.append(f"<message {index} role={json.dumps(role)}>\n{content}\n</message {index}>")
    return "\n\n".join(lines).strip()


def _system_prompt(configured: str) -> str:
    configured = str(configured or "").strip()
    if not configured:
        return GESTALT_MCP_CATALOG_PROMPT
    return f"{configured}\n\n{GESTALT_MCP_CATALOG_PROMPT}"


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
