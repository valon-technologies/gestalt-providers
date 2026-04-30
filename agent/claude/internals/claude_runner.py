from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
from typing import Any

from . import mcp_server
from .config import ClaudeAgentConfig

MCP_SERVER_NAME = "gestalt"
MCP_SERVER_MODULE = mcp_server.__name__
MCP_TOOL_ALLOWLIST = f"mcp__{MCP_SERVER_NAME}__*"
MAX_ERROR_TEXT = 4000


class ClaudeExecutionError(RuntimeError):
    pass


class ClaudeCodeRunner:
    def __init__(self, config: ClaudeAgentConfig) -> None:
        self._config = config
        self._lock = threading.RLock()
        self._active_processes: dict[str, subprocess.Popen[str]] = {}
        self._canceled_turns: set[str] = set()

    def run_turn(
        self, *, session_id: str, turn_id: str, model: str, messages: list[dict[str, Any]], tool_grant: str
    ) -> str:
        self._raise_if_canceled(turn_id)
        prompt = _messages_to_prompt(messages)
        if not prompt:
            raise ClaudeExecutionError("turn prompt is empty")

        mcp_config = self._mcp_config(session_id=session_id, turn_id=turn_id, tool_grant=tool_grant)
        with tempfile.NamedTemporaryFile("w", suffix=".mcp.json", encoding="utf-8", delete=False) as file:
            json.dump(mcp_config, file)
            config_path = file.name
        try:
            process = subprocess.Popen(
                self._command(model=model, mcp_config_path=config_path),
                cwd=self._config.working_directory or None,
                env=os.environ.copy(),
                text=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self._track_process(turn_id, process)
            stdout, stderr = process.communicate(input=prompt, timeout=self._config.timeout_seconds)
        except FileNotFoundError as exc:
            raise ClaudeExecutionError(f"Claude Code binary {self._config.claude_binary!r} was not found") from exc
        except subprocess.TimeoutExpired as exc:
            self._terminate_process(process)
            raise ClaudeExecutionError(f"Claude Code timed out after {self._config.timeout_seconds:g}s") from exc
        finally:
            self._untrack_process(turn_id)
            try:
                os.unlink(config_path)
            except OSError:
                pass

        if process.returncode != 0:
            detail = (stderr or stdout or "").strip()
            raise ClaudeExecutionError(f"Claude Code exited with status {process.returncode}: {_truncate(detail)}")
        return _parse_claude_output(stdout)

    def cancel_turn(self, turn_id: str) -> None:
        turn_id = turn_id.strip()
        if not turn_id:
            return
        with self._lock:
            self._canceled_turns.add(turn_id)
            process = self._active_processes.get(turn_id)
        if process is not None:
            self._terminate_process(process)

    def close(self) -> None:
        with self._lock:
            processes = list(self._active_processes.values())
            self._active_processes.clear()
            self._canceled_turns.clear()
        for process in processes:
            self._terminate_process(process)

    def _command(self, *, model: str, mcp_config_path: str) -> list[str]:
        command = [
            self._config.claude_binary,
            "--print",
            "--input-format",
            "text",
            "--output-format",
            "json",
            "--no-session-persistence",
            "--strict-mcp-config",
            "--mcp-config",
            mcp_config_path,
            "--tools",
            "",
            "--allowedTools",
            MCP_TOOL_ALLOWLIST,
            "--permission-mode",
            self._config.permission_mode,
            "--model",
            model,
        ]
        if self._config.system_prompt:
            command.extend(["--system-prompt", self._config.system_prompt])
        return command

    def _mcp_config(self, *, session_id: str, turn_id: str, tool_grant: str) -> dict[str, Any]:
        python_path = os.path.dirname(os.path.dirname(__file__))
        inherited_python_path = os.environ.get("PYTHONPATH", "")
        if inherited_python_path:
            python_path = f"{python_path}{os.pathsep}{inherited_python_path}"
        env = {
            "GESTALT_CLAUDE_SESSION_ID": session_id,
            "GESTALT_CLAUDE_TURN_ID": turn_id,
            "GESTALT_CLAUDE_TOOL_GRANT": tool_grant,
            "PYTHONPATH": python_path,
        }
        for key in ("GESTALT_AGENT_HOST_SOCKET", "GESTALT_AGENT_HOST_SOCKET_TOKEN"):
            value = os.environ.get(key, "")
            if value:
                env[key] = value
        args = ["-m", MCP_SERVER_MODULE]
        if getattr(sys, "frozen", False):
            args = []
            env["GESTALT_CLAUDE_RUN_MCP_SERVER"] = "1"
        return {"mcpServers": {MCP_SERVER_NAME: {"command": sys.executable, "args": args, "env": env}}}

    def _raise_if_canceled(self, turn_id: str) -> None:
        with self._lock:
            if turn_id in self._canceled_turns:
                self._canceled_turns.discard(turn_id)
                raise ClaudeExecutionError("Claude Code turn was canceled")

    def _track_process(self, turn_id: str, process: subprocess.Popen[str]) -> None:
        should_terminate = False
        with self._lock:
            self._active_processes[turn_id] = process
            should_terminate = turn_id in self._canceled_turns
        if should_terminate:
            self._terminate_process(process)

    def _untrack_process(self, turn_id: str) -> None:
        with self._lock:
            self._active_processes.pop(turn_id, None)
            self._canceled_turns.discard(turn_id)

    def _terminate_process(self, process: subprocess.Popen[str]) -> None:
        if process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass


def _messages_to_prompt(messages: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for message in messages:
        role = str(message.get("role") or "user").strip() or "user"
        content = _message_content(message)
        if not content:
            continue
        lines.append(f"{role}: {content}")
    return "\n\n".join(lines).strip()


def _message_content(message: dict[str, Any]) -> str:
    text = str(message.get("text") or "").strip()
    if text:
        return text
    parts: list[str] = []
    for part in message.get("parts") or []:
        if not isinstance(part, dict):
            continue
        part_text = str(part.get("text") or "").strip()
        if part_text:
            parts.append(part_text)
            continue
        if "json" in part:
            parts.append(json.dumps(part["json"], sort_keys=True))
            continue
        tool_result = part.get("tool_result")
        if isinstance(tool_result, dict):
            content = str(tool_result.get("content") or "").strip()
            if content:
                parts.append(content)
    return "\n".join(parts).strip()


def _parse_claude_output(stdout: str) -> str:
    text = stdout.strip()
    if not text:
        return ""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return text
    if isinstance(payload, dict):
        for key in ("result", "content", "text", "output"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        if isinstance(payload.get("message"), dict):
            content = payload["message"].get("content")
            if isinstance(content, str) and content.strip():
                return content.strip()
        return json.dumps(payload, sort_keys=True)
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, sort_keys=True)


def _truncate(value: str) -> str:
    value = value.strip()
    if len(value) <= MAX_ERROR_TEXT:
        return value
    return value[:MAX_ERROR_TEXT] + "..."
