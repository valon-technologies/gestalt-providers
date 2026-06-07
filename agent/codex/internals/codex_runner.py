from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import pathlib
import shutil
import tempfile
import threading
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Protocol

import gestalt
from jsonschema import exceptions as jsonschema_exceptions
from jsonschema.validators import validator_for
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp import types as mcp_types

from .config import CodexAgentConfig
from .gestalt_mcp_bridge import BridgeContext
from .http_bridge import BridgeHTTPServer
from .tool_bridge import ToolBridgeError, ToolEntry, list_tools

logger = logging.getLogger(__name__)

CODEX_TOOL_NAME = "codex"


class CodexExecutionError(RuntimeError):
    pass


class CodexExecutionCanceled(CodexExecutionError):
    pass


class CodexMCPServerProtocol(Protocol):
    async def connect(self) -> None: ...

    async def list_tools(self) -> list[mcp_types.Tool]: ...

    async def call_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> mcp_types.CallToolResult: ...

    async def cleanup(self) -> None: ...


class ServerFactory(Protocol):
    def __call__(
        self,
        *,
        params: dict[str, Any],
        name: str,
        client_session_timeout_seconds: float,
    ) -> CodexMCPServerProtocol: ...


@dataclass(slots=True)
class _ActiveTurn:
    loop: asyncio.AbstractEventLoop
    server: CodexMCPServerProtocol | None = None


class CodexMCPStdioServer:
    def __init__(self, *, params: dict[str, Any], name: str, client_session_timeout_seconds: float) -> None:
        self._name = name
        self._timeout = timedelta(seconds=client_session_timeout_seconds)
        self._params = StdioServerParameters(
            command=str(params.get("command") or ""),
            args=[str(arg) for arg in params.get("args") or []],
            env={str(key): str(value) for key, value in (params.get("env") or {}).items()},
            cwd=params.get("cwd"),
        )
        self._stack: AsyncExitStack | None = None
        self._session: ClientSession | None = None

    async def connect(self) -> None:
        if self._session is not None:
            return
        stack = AsyncExitStack()
        try:
            read_stream, write_stream = await stack.enter_async_context(stdio_client(self._params))
            session = await stack.enter_async_context(
                ClientSession(read_stream, write_stream, read_timeout_seconds=self._timeout)
            )
            await session.initialize()
        except BaseException:
            await stack.aclose()
            raise
        self._stack = stack
        self._session = session

    async def list_tools(self) -> list[mcp_types.Tool]:
        session = self._require_session()
        result = await session.list_tools()
        return list(result.tools)

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> mcp_types.CallToolResult:
        session = self._require_session()
        return await session.call_tool(name, arguments, read_timeout_seconds=self._timeout)

    async def cleanup(self) -> None:
        stack = self._stack
        self._stack = None
        self._session = None
        if stack is not None:
            await stack.aclose()

    def _require_session(self) -> ClientSession:
        if self._session is None:
            raise CodexExecutionError(f"{self._name} MCP server is not connected")
        return self._session


class CodexMCPRunner:
    def __init__(self, config: CodexAgentConfig, *, server_factory: ServerFactory | None = None) -> None:
        self._config = config
        self._server_factory = server_factory or CodexMCPStdioServer
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
        request_context: Any,
        skill_roots: list[str] | None = None,
        cwd: str = "",
        schema: dict[str, Any] | None = None,
    ) -> gestalt.AgentTurnOutput:
        try:
            return asyncio.run(
                asyncio.wait_for(
                    self._run_turn(
                        session_id=session_id,
                        turn_id=turn_id,
                        model=model,
                        messages=messages,
                        request_context=request_context,
                        skill_roots=skill_roots or [],
                        cwd=cwd,
                        schema=schema,
                    ),
                    timeout=self._config.timeout_seconds,
                )
            )
        except TimeoutError as exc:
            raise CodexExecutionError(f"Codex MCP turn timed out after {self._config.timeout_seconds:g}s") from exc

    def cancel_turn(self, turn_id: str) -> None:
        turn_id = turn_id.strip()
        if not turn_id:
            return
        active: _ActiveTurn | None = None
        with self._lock:
            self._canceled_turns.add(turn_id)
            active = self._active_turns.get(turn_id)
        if active is not None and active.server is not None:
            _schedule_cleanup(active.loop, active.server)

    def close(self) -> None:
        with self._lock:
            active_turns = list(self._active_turns.items())
            self._canceled_turns.update(turn_id for turn_id, _active in active_turns)
        for _turn_id, active in active_turns:
            if active.server is not None:
                _schedule_cleanup(active.loop, active.server)

    async def _run_turn(
        self,
        *,
        session_id: str,
        turn_id: str,
        model: str,
        messages: list[dict[str, Any]],
        request_context: Any,
        skill_roots: list[str],
        cwd: str,
        schema: dict[str, Any] | None,
    ) -> gestalt.AgentTurnOutput:
        loop = asyncio.get_running_loop()
        self._register_active_turn(turn_id, _ActiveTurn(loop=loop))
        server: CodexMCPServerProtocol | None = None
        bridge: BridgeHTTPServer | None = None
        try:
            self._raise_if_canceled(turn_id)
            prompt = messages_to_prompt(messages)
            if not prompt:
                raise CodexExecutionError("turn prompt is empty")
            if schema is not None:
                prompt = structured_output_prompt(prompt, schema)

            try:
                listed_tools = await asyncio.to_thread(
                    list_tools,
                    session_id=session_id,
                    turn_id=turn_id,
                    request_context=request_context,
                    timeout_seconds=self._config.timeout_seconds,
                )
            except ToolBridgeError as exc:
                raise CodexExecutionError(str(exc)) from exc
            self._raise_if_canceled(turn_id)

            bridge = BridgeHTTPServer(
                BridgeContext(
                    session_id=session_id,
                    turn_id=turn_id,
                    request_context=request_context,
                    timeout_seconds=self._config.timeout_seconds,
                )
            )
            bridge.start()
            self._raise_if_canceled(turn_id)

            with tempfile.TemporaryDirectory(prefix="gestalt-codex-home-") as codex_home:
                _materialize_codex_skills(codex_home=codex_home, skill_roots=skill_roots)
                server = self._server_factory(
                    params=self._server_params(codex_home=codex_home, cwd=cwd),
                    name="Codex CLI",
                    client_session_timeout_seconds=self._config.timeout_seconds,
                )
                self._register_active_server(turn_id, server)
                self._raise_if_canceled(turn_id)
                await server.connect()
                self._raise_if_canceled(turn_id)
                await self._assert_codex_tool(server)
                result = await server.call_tool(
                    CODEX_TOOL_NAME,
                    self._codex_tool_arguments(
                        model=model, prompt=prompt, listed_tools=listed_tools, bridge_url=bridge.url, cwd=cwd
                    ),
                )
                self._raise_if_canceled(turn_id)
                output_text = normalize_codex_result(result)
                if schema is not None:
                    return gestalt.AgentTurnOutput(
                        structured=gestalt.AgentTurnStructuredOutput(
                            text=output_text,
                            value=structured_output_from_text(output_text, schema),
                        )
                    )
                return gestalt.AgentTurnOutput(text=output_text)
        except asyncio.CancelledError:
            self.cancel_turn(turn_id)
            raise
        finally:
            active = self._unregister_active_turn(turn_id)
            cleanup_server = server or (active.server if active is not None else None)
            if cleanup_server is not None:
                try:
                    await cleanup_server.cleanup()
                except Exception:
                    logger.exception("failed to clean up Codex MCP server")
            if bridge is not None:
                try:
                    await asyncio.to_thread(bridge.stop)
                except Exception:
                    logger.exception("failed to stop Gestalt MCP HTTP bridge")

    def _server_params(self, *, codex_home: str, cwd: str = "") -> dict[str, Any]:
        env: dict[str, str] = {"CODEX_HOME": codex_home}
        if self._config.openai_api_key:
            env["OPENAI_API_KEY"] = self._config.openai_api_key
        params: dict[str, Any] = {
            "command": self._config.codex_command,
            "args": list(self._config.codex_args),
            "env": env,
        }
        working_directory = cwd or self._config.working_directory
        if working_directory:
            params["cwd"] = working_directory
        return params

    async def _assert_codex_tool(self, server: CodexMCPServerProtocol) -> None:
        tools = await server.list_tools()
        if CODEX_TOOL_NAME not in {tool.name for tool in tools}:
            raise CodexExecutionError("Codex MCP server did not expose the codex tool")

    def _codex_tool_arguments(
        self, *, model: str, prompt: str, listed_tools: list[ToolEntry], bridge_url: str, cwd: str = ""
    ) -> dict[str, Any]:
        arguments: dict[str, Any] = {
            "prompt": prompt,
            "approval-policy": self._config.approval_policy,
            "sandbox": self._config.sandbox,
            "include-plan-tool": False,
            "config": self._codex_config(listed_tools=listed_tools, bridge_url=bridge_url),
        }
        if model:
            arguments["model"] = model
        working_directory = cwd or self._config.working_directory
        if working_directory:
            arguments["cwd"] = working_directory
        if self._config.system_prompt:
            arguments["base-instructions"] = self._config.system_prompt
        return arguments

    def _codex_config(self, *, listed_tools: list[ToolEntry], bridge_url: str) -> dict[str, Any]:
        return {
            "approval_policy": self._config.approval_policy,
            "sandbox_mode": self._config.sandbox,
            "web_search": "disabled",
            "history": {"persistence": "none"},
            "memories": {"generate_memories": False, "use_memories": False},
            "features": {"apps": False, "multi_agent": False, "codex_hooks": False},
            "skills": {"config": []},
            "shell_environment_policy": {"inherit": "core", "exclude": ["OPENAI_API_KEY", "GESTALT_*"]},
            "mcp_servers": {
                "gestalt": {
                    "url": bridge_url,
                    "enabled_tools": [entry.mcp_name for entry in listed_tools],
                    "startup_timeout_sec": self._config.timeout_seconds,
                    "tool_timeout_sec": self._config.timeout_seconds,
                    "required": True,
                }
            },
        }

    def _register_active_turn(self, turn_id: str, active: _ActiveTurn) -> None:
        with self._lock:
            self._active_turns[turn_id] = active

    def _register_active_server(self, turn_id: str, server: CodexMCPServerProtocol) -> None:
        should_cleanup = False
        with self._lock:
            active = self._active_turns.get(turn_id)
            if active is not None:
                active.server = server
            should_cleanup = turn_id in self._canceled_turns
        if should_cleanup:
            active = self._active_turns.get(turn_id)
            if active is not None:
                _schedule_cleanup(active.loop, server)

    def _unregister_active_turn(self, turn_id: str) -> _ActiveTurn:
        with self._lock:
            active = self._active_turns.pop(turn_id, None) or _ActiveTurn(loop=asyncio.get_running_loop())
            self._canceled_turns.discard(turn_id)
            return active

    def _raise_if_canceled(self, turn_id: str) -> None:
        if self._is_canceled(turn_id):
            raise CodexExecutionCanceled("Codex MCP turn was canceled")

    def _is_canceled(self, turn_id: str) -> bool:
        with self._lock:
            return turn_id in self._canceled_turns


def normalize_codex_result(result: mcp_types.CallToolResult) -> str:
    structured = result.structuredContent
    if isinstance(structured, dict):
        content = structured.get("content")
        if content is not None:
            return _stringify_content(content)

    text_parts: list[str] = []
    for item in result.content:
        if isinstance(item, mcp_types.TextContent):
            text_parts.append(item.text)
            continue
    return "\n".join(part for part in text_parts if part).strip()


def _materialize_codex_skills(*, codex_home: str, skill_roots: list[str]) -> None:
    if not skill_roots:
        return
    target_root = pathlib.Path(codex_home) / "skills"
    target_root.mkdir(parents=True, exist_ok=True)
    used_names: set[str] = set()
    for raw_root in skill_roots:
        source_root = pathlib.Path(raw_root)
        if not source_root.is_dir():
            continue
        bundle_name = source_root.parent.name
        for skill_dir in sorted(source_root.iterdir(), key=lambda path: path.name):
            if not skill_dir.is_dir() or not (skill_dir / "SKILL.md").is_file():
                continue
            target_name = skill_dir.name
            if target_name in used_names:
                target_name = f"{bundle_name}-{skill_dir.name}"
            used_names.add(target_name)
            target = target_root / target_name
            try:
                target.symlink_to(skill_dir, target_is_directory=True)
            except OSError:
                shutil.copytree(skill_dir, target, symlinks=True, dirs_exist_ok=True)


def messages_to_prompt(messages: list[dict[str, Any]]) -> str:
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


def structured_output_prompt(prompt: str, schema: dict[str, Any]) -> str:
    return (
        f"{prompt}\n\n"
        "<gestalt_structured_output>\n"
        "Return only one JSON object matching this JSON Schema. Do not wrap it in Markdown or include explanatory text.\n"
        f"{json.dumps(schema, sort_keys=True, separators=(',', ':'))}\n"
        "</gestalt_structured_output>"
    )


def validate_schema(schema: dict[str, Any]) -> None:
    if not schema or str(schema.get("type") or "").strip() != "object":
        raise CodexExecutionError("output.structured.schema must be a non-empty JSON schema object with type 'object'")
    validator_cls = validator_for(schema)
    try:
        validator_cls.check_schema(schema)
    except jsonschema_exceptions.SchemaError as exc:
        raise CodexExecutionError(f"invalid output.structured.schema: {exc.message}") from exc


def structured_output_from_text(text: str, schema: dict[str, Any]) -> dict[str, Any]:
    validate_schema(schema)
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        value = _extract_json_object(text)
    if not isinstance(value, dict):
        raise CodexExecutionError("structured output must be a JSON object")
    validator_cls = validator_for(schema)
    validator = validator_cls(schema)
    try:
        validator.validate(value)
    except jsonschema_exceptions.ValidationError as exc:
        raise CodexExecutionError(f"structured output did not match output schema: {exc.message}") from exc
    return value


def _extract_json_object(text: str) -> dict[str, Any]:
    decoder = json.JSONDecoder()
    start = text.find("{")
    while start >= 0:
        try:
            value, _end = decoder.raw_decode(text[start:])
        except json.JSONDecodeError:
            pass
        else:
            if isinstance(value, dict):
                return value
        start = text.find("{", start + 1)
    raise CodexExecutionError("structured output did not contain a JSON object")


def _schedule_cleanup(
    loop: asyncio.AbstractEventLoop, server: CodexMCPServerProtocol
) -> concurrent.futures.Future[Any] | None:
    try:
        return asyncio.run_coroutine_threadsafe(server.cleanup(), loop)
    except RuntimeError:
        return None


def _stringify_content(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            text = _stringify_content(item)
            if text:
                parts.append(text)
        return "\n".join(parts).strip()
    if isinstance(value, dict):
        text = value.get("text")
        if text is not None:
            return str(text).strip()
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    if value is None:
        return ""
    return str(value).strip()
