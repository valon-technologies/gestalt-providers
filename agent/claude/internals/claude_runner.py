from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import os
import tempfile
import threading
from dataclasses import dataclass, field
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

from .claude_code_config import ClaudeCodeTurnOptions
from .config import ClaudeAgentConfig
from .mcp_bridge import (
    MCP_SERVER_NAME,
    allowed_gestalt_mcp_tools,
    create_gestalt_sdk_mcp_server,
    create_tool_permission_callback,
)

logger = logging.getLogger(__name__)

MAX_ERROR_TEXT = 4000
GESTALT_MCP_CATALOG_PROMPT = (
    "Gestalt MCP catalog tools are available through the `gestalt` MCP server for connected apps such as "
    "Linear, GitHub, Slack, Gmail, Google Drive, Google Calendar, Google Docs, Google Sheets, BigQuery, Datadog, "
    "PagerDuty, Notion, Figma, Ramp, Ashby, and other configured integrations. When a user asks for data or actions "
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


@dataclass(slots=True)
class _ClaudeResponse:
    text_blocks: list[str] = field(default_factory=list)
    tool_blocks: list[str] = field(default_factory=list)

    def output_fallback(self) -> str:
        if self.text_blocks:
            return "\n".join(block for block in self.text_blocks if block).strip()
        if self.tool_blocks:
            return "\n".join(self.tool_blocks).strip()
        return ""


@dataclass(frozen=True, slots=True)
class ClaudeTurnResult:
    output_text: str
    structured_output: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ClaudeTurnProfile:
    kind: Literal["catalog", "structured_output"]
    run_grant: str = ""
    response_schema: dict[str, Any] | None = None
    claude_code_options: ClaudeCodeTurnOptions | None = None
    cwd: str | None = None

    @classmethod
    def catalog(
        cls,
        *,
        run_grant: str,
        response_schema: dict[str, Any] | None = None,
        claude_code_options: ClaudeCodeTurnOptions,
        cwd: str,
    ) -> "ClaudeTurnProfile":
        return cls(
            kind="catalog",
            run_grant=run_grant,
            response_schema=response_schema,
            claude_code_options=claude_code_options,
            cwd=cwd,
        )

    @classmethod
    def structured_output(cls, *, response_schema: dict[str, Any]) -> "ClaudeTurnProfile":
        return cls(kind="structured_output", response_schema=response_schema)

    @property
    def uses_catalog_tools(self) -> bool:
        return self.kind == "catalog"

    @property
    def requires_structured_output(self) -> bool:
        return self.response_schema is not None


class ClaudeSDKRunner:
    def __init__(self, config: ClaudeAgentConfig, *, client_factory: ClientFactory | None = None) -> None:
        self._config = config
        self._client_factory = client_factory or ClaudeSDKClient
        self._lock = threading.RLock()
        self._active_turns: dict[str, _ActiveTurn] = {}
        self._canceled_turns: set[str] = set()

    def run_turn(
        self,
        *,
        session_id: str,
        turn_id: str,
        model: str,
        messages: list[dict[str, Any]],
        turn_profile: ClaudeTurnProfile,
        timeout_seconds: float = 0.0,
    ) -> ClaudeTurnResult:
        effective_timeout = timeout_seconds if timeout_seconds > 0 else self._config.timeout_seconds
        try:
            return asyncio.run(
                asyncio.wait_for(
                    self._run_turn(
                        session_id=session_id,
                        turn_id=turn_id,
                        model=model,
                        messages=messages,
                        turn_profile=turn_profile,
                    ),
                    timeout=effective_timeout,
                )
            )
        except TimeoutError as exc:
            self.cancel_turn(turn_id)
            raise ClaudeExecutionError(f"Claude Agent SDK timed out after {effective_timeout:g}s") from exc

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
        self,
        *,
        session_id: str,
        turn_id: str,
        model: str,
        messages: list[dict[str, Any]],
        turn_profile: ClaudeTurnProfile,
    ) -> ClaudeTurnResult:
        loop = asyncio.get_running_loop()
        self._register_active_turn(turn_id, _ActiveTurn(loop=loop))
        try:
            self._raise_if_canceled(turn_id)
            prompt = _messages_to_prompt(messages)
            if not prompt:
                raise ClaudeExecutionError("turn prompt is empty")

            with tempfile.TemporaryDirectory(prefix="gestalt-claude-sdk-") as config_dir:
                options = self._options(model=model, session_id=session_id, turn_id=turn_id, turn_profile=turn_profile)
                _set_config_dir(options, config_dir)
                client = self._client_factory(options=options)
                self._register_active_client(turn_id, client)
                self._raise_if_canceled(turn_id)
                await client.connect()
                await client.query(prompt, session_id=session_id)
                output = await self._receive_result(client, turn_id=turn_id, turn_profile=turn_profile)
                self._raise_if_canceled(turn_id)
                return output
        finally:
            active = self._unregister_active_turn(turn_id)
            if active is not None and active.client is not None:
                try:
                    await active.client.disconnect()
                except Exception:
                    logger.exception("failed to disconnect Claude SDK client")

    async def _receive_result(self, client: Any, *, turn_id: str, turn_profile: ClaudeTurnProfile) -> ClaudeTurnResult:
        response = _ClaudeResponse()
        result_message: Any | None = None
        async for message in client.receive_response():
            self._raise_if_canceled(turn_id)
            result_message = _capture_response_message(response, message)
            if result_message is not None:
                break

        return _result_output(
            response,
            result_message,
            requires_structured_output=turn_profile.requires_structured_output,
            canceled=self._is_canceled(turn_id),
        )

    def _options(
        self,
        *,
        model: str,
        session_id: str,
        turn_id: str,
        turn_profile: ClaudeTurnProfile | None = None,
        run_grant: str = "",
        response_schema: dict[str, Any] | None = None,
        claude_code_options: ClaudeCodeTurnOptions | None = None,
        cwd: str = "",
    ) -> Any:
        if turn_profile is None:
            if response_schema is not None and not str(run_grant or "").strip():
                turn_profile = ClaudeTurnProfile.structured_output(response_schema=response_schema)
            else:
                if claude_code_options is None:
                    claude_code_options = self._config.claude_code.resolve_turn_options({})
                turn_profile = ClaudeTurnProfile.catalog(
                    run_grant=str(run_grant or "").strip(),
                    response_schema=response_schema,
                    claude_code_options=claude_code_options,
                    cwd=cwd,
                )
        if turn_profile.uses_catalog_tools:
            return self._catalog_options(model=model, session_id=session_id, turn_id=turn_id, turn_profile=turn_profile)
        return self._structured_output_options(model=model, turn_profile=turn_profile)

    def _base_options_kwargs(
        self, *, model: str, env: dict[str, str], cwd: str | None, system_prompt: str | None
    ) -> dict[str, Any]:
        return {
            "model": model,
            "cwd": cwd,
            "system_prompt": system_prompt,
            "permission_mode": cast(PermissionMode, self._config.permission_mode),
            "cli_path": self._config.cli_path or None,
            "env": env,
            "agents": None,
            "stderr": _log_claude_stderr,
        }

    def _catalog_options(
        self, *, model: str, session_id: str, turn_id: str, turn_profile: ClaudeTurnProfile
    ) -> ClaudeAgentOptions:
        claude_code_options = turn_profile.claude_code_options
        assert claude_code_options is not None
        env: dict[str, str] = {"ENABLE_TOOL_SEARCH": "auto:5"}
        anthropic_api_key = self._config.anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        if anthropic_api_key:
            env["ANTHROPIC_API_KEY"] = anthropic_api_key
        if claude_code_options.disable_auto_memory:
            env["CLAUDE_CODE_DISABLE_AUTO_MEMORY"] = "1"
        return ClaudeAgentOptions(
            **self._base_options_kwargs(
                model=model,
                env=env,
                cwd=turn_profile.cwd or self._config.working_directory or None,
                system_prompt=_system_prompt(self._config.system_prompt),
            ),
            tools=allowed_gestalt_mcp_tools() + claude_code_options.base_tools,
            allowed_tools=allowed_gestalt_mcp_tools() + claude_code_options.allowed_tools,
            mcp_servers={
                MCP_SERVER_NAME: create_gestalt_sdk_mcp_server(
                    session_id=session_id, turn_id=turn_id, run_grant=turn_profile.run_grant
                )
            },
            setting_sources=list(claude_code_options.setting_sources),
            skills=claude_code_options.sdk_skills,
            plugins=claude_code_options.sdk_plugins,
            can_use_tool=create_tool_permission_callback(claude_code_options.tool_permissions),
            output_format=_output_format(turn_profile.response_schema),
        )

    def _structured_output_options(self, *, model: str, turn_profile: ClaudeTurnProfile) -> ClaudeAgentOptions:
        anthropic_api_key = self._config.anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY", "")
        env: dict[str, str] = {}
        if anthropic_api_key:
            env["ANTHROPIC_API_KEY"] = anthropic_api_key
        system_prompt = _configured_system_prompt(self._config.system_prompt)
        schema = turn_profile.response_schema
        assert schema is not None
        return ClaudeAgentOptions(
            **self._base_options_kwargs(model=model, env=env, cwd=None, system_prompt=system_prompt),
            tools=[],
            allowed_tools=[],
            mcp_servers={},
            setting_sources=[],
            skills=[],
            plugins=[],
            output_format={"type": "json_schema", "schema": schema},
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


def _capture_response_message(response: _ClaudeResponse, message: Any) -> Any | None:
    if isinstance(message, AssistantMessage):
        _capture_assistant_message(response, message)
        return None
    if isinstance(message, ResultMessage):
        return message
    return None


def _capture_assistant_message(response: _ClaudeResponse, message: AssistantMessage) -> None:
    for block in message.content:
        if isinstance(block, TextBlock):
            response.text_blocks.append(block.text)
        elif isinstance(block, ToolUseBlock):
            response.tool_blocks.append(_tool_use_json(block))
        elif isinstance(block, ToolResultBlock):
            response.tool_blocks.append(_tool_result_json(block))


def _tool_use_json(block: ToolUseBlock) -> str:
    return json.dumps(
        {"tool_use": {"id": block.id, "name": block.name, "input": block.input}}, sort_keys=True, separators=(",", ":")
    )


def _tool_result_json(block: ToolResultBlock) -> str:
    return json.dumps(
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


def _result_output(
    response: _ClaudeResponse, result_message: Any | None, *, requires_structured_output: bool, canceled: bool
) -> ClaudeTurnResult:
    if result_message is None:
        raise ClaudeExecutionError("Claude Agent SDK response ended without a result")
    subtype = _result_subtype(result_message)
    if bool(getattr(result_message, "is_error", False)) or subtype != "success":
        _raise_result_error(result_message, subtype=subtype, canceled=canceled)

    result_text = str(getattr(result_message, "result", "") or "").strip()
    structured_output = _structured_output(result_message, required=requires_structured_output)
    return ClaudeTurnResult(output_text=result_text or response.output_fallback(), structured_output=structured_output)


def _raise_result_error(result_message: Any, *, subtype: str, canceled: bool) -> None:
    detail = str(getattr(result_message, "result", "") or getattr(result_message, "stop_reason", "") or subtype)
    if canceled or subtype in {"interrupted", "canceled", "cancelled"}:
        raise ClaudeExecutionCanceled(_truncate(detail or "Claude Agent SDK turn was canceled"))
    raise ClaudeExecutionError(_truncate(detail or f"Claude Agent SDK returned {subtype!r}"))


def _result_subtype(result_message: Any) -> str:
    return str(getattr(result_message, "subtype", "") or "")


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
    configured = _configured_system_prompt(configured) or ""
    if not configured:
        return GESTALT_MCP_CATALOG_PROMPT
    return f"{configured}\n\n{GESTALT_MCP_CATALOG_PROMPT}"


def _configured_system_prompt(configured: str) -> str | None:
    configured = str(configured or "").strip()
    return configured or None


def _structured_output(result_message: Any, *, required: bool) -> dict[str, Any] | None:
    value = getattr(result_message, "structured_output", None)
    if value is None:
        if required:
            raise ClaudeExecutionError("Claude Agent SDK response did not include structured output")
        return None
    value = _jsonable(value)
    if not isinstance(value, dict):
        raise ClaudeExecutionError("Claude Agent SDK structured output must be a JSON object")
    return value


def _output_format(response_schema: dict[str, Any] | None) -> dict[str, Any] | None:
    if response_schema is None:
        return None
    return {"type": "json_schema", "schema": response_schema}


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


def _log_claude_stderr(line: str) -> None:
    text = str(line or "").strip()
    if text:
        logger.warning("Claude Agent SDK stderr: %s", _truncate(text))


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
