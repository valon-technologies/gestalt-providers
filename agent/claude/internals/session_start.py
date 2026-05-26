from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Mapping
from typing import Any

import gestalt

RESERVED_PREFIX = "__gestalt.lifecycle.sessionStart"
RESULTS_PREFIX = f"{RESERVED_PREFIX}.results"
ADDITIONAL_CONTEXT_KEY = f"{RESERVED_PREFIX}.additionalContext"
RESERVED_METADATA_KEYS = {"cwd", "workspacePath", "worktreePath"}

_DEFAULT_ENV_KEYS = ("HOME", "PATH", "SHELL", "TMPDIR", "USER", "LOGNAME", "LANG", "LC_ALL")


def run_session_start_hooks(
    session_start: gestalt.AgentSessionStartConfig, metadata: dict[str, Any]
) -> dict[str, Any]:
    hooks = list(session_start.hooks)
    if not hooks:
        return dict(metadata)
    merged = dict(metadata)
    context_chunks: list[str] = []
    for hook in hooks:
        result, additional_context = _run_hook(hook)
        hook_id = hook.id.strip()
        merged[f"{RESULTS_PREFIX}.{hook_id}"] = result
        if additional_context:
            context_chunks.append(additional_context)
    if context_chunks:
        merged[ADDITIONAL_CONTEXT_KEY] = "\n\n".join(context_chunks)
    return merged


def validate_session_start_user_metadata(metadata: dict[str, Any] | None) -> None:
    if metadata is None:
        return
    for key in metadata:
        key = str(key)
        if key.startswith(RESERVED_PREFIX) or key in RESERVED_METADATA_KEYS:
            raise ValueError(f"agent session metadata key {key!r} is reserved for Gestalt lifecycle or workspace data")


def prepend_session_start_context(messages: list[dict[str, Any]], metadata: dict[str, Any]) -> list[dict[str, Any]]:
    context = str(metadata.get(ADDITIONAL_CONTEXT_KEY) or "").strip()
    if not context:
        return messages
    return [
        {"role": "system", "text": f"Session start context:\n\n{context}", "metadata": {"source": RESERVED_PREFIX}},
        *messages,
    ]


def _run_hook(hook: gestalt.AgentSessionStartHook) -> tuple[dict[str, Any], str]:
    hook_id = hook.id.strip()
    hook_type = hook.type.strip() or "command"
    if hook_type != "command":
        raise ValueError(f"sessionStart hook {hook_id!r} type {hook_type!r} is not supported")
    command = [str(part) for part in hook.command if str(part).strip()]
    if not command:
        raise ValueError(f"sessionStart hook {hook_id!r} command is required")
    timeout = _parse_timeout(hook.timeout)
    try:
        completed = subprocess.run(
            command,
            cwd=hook.cwd or None,
            env=_hook_env(hook.env),
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        timeout_detail = str(hook.timeout or timeout or "").strip()
        raise RuntimeError(f"sessionStart hook {hook_id!r} timed out after {timeout_detail}") from exc
    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    if completed.returncode != 0:
        detail = stderr.strip() or stdout.strip() or f"exit code {completed.returncode}"
        raise RuntimeError(f"sessionStart hook {hook_id!r} failed: {detail}")
    stdout_payload = _json_stdout_payload(stdout)
    result: dict[str, Any] = {
        "status": "succeeded",
        "exitCode": completed.returncode,
        "timeout": hook.timeout,
        "timedOut": False,
    }
    if hook.output is not None and hook.output.metadata:
        payload_metadata = stdout_payload.get("metadata")
        if isinstance(payload_metadata, dict):
            result["metadata"] = payload_metadata
        result["stdout"] = stdout
        result["stderr"] = stderr
    additional_context = ""
    if hook.output is not None and hook.output.additional_context:
        payload_context = stdout_payload.get("additionalContext")
        additional_context = str(payload_context).strip() if payload_context is not None else stdout.strip()
    return result, additional_context


def _json_stdout_payload(stdout: str) -> dict[str, Any]:
    stdout = stdout.strip()
    if not stdout:
        return {}
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _hook_env(explicit: Mapping[str, str]) -> dict[str, str]:
    env = {key: value for key in _DEFAULT_ENV_KEYS if (value := os.environ.get(key)) is not None}
    env.update(explicit)
    return env


def _parse_timeout(value: str) -> float | None:
    value = value.strip()
    if not value:
        return None
    if value.endswith("ms"):
        return float(value[:-2]) / 1000
    if value.endswith("s"):
        return float(value[:-1])
    if value.endswith("m"):
        return float(value[:-1]) * 60
    return float(value)
