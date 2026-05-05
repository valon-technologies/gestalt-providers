from __future__ import annotations

import json
import os
import subprocess
from typing import Any

RESERVED_PREFIX = "__gestalt.lifecycle.sessionStart"
RESULTS_PREFIX = f"{RESERVED_PREFIX}.results"
ADDITIONAL_CONTEXT_KEY = f"{RESERVED_PREFIX}.additionalContext"

_DEFAULT_ENV_KEYS = ("HOME", "PATH", "SHELL", "TMPDIR", "USER", "LOGNAME", "LANG", "LC_ALL")


def run_session_start_hooks(session_start: Any, metadata: dict[str, Any]) -> dict[str, Any]:
    hooks = list(getattr(session_start, "hooks", []) or [])
    if not hooks:
        return dict(metadata)
    merged = dict(metadata)
    context_chunks: list[str] = []
    for hook in hooks:
        result, additional_context = _run_hook(hook)
        hook_id = str(getattr(hook, "id", "") or "").strip()
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
        if str(key).startswith(RESERVED_PREFIX):
            raise ValueError(f"agent session metadata key {key!r} is reserved for Gestalt lifecycle data")


def prepend_session_start_context(messages: list[dict[str, Any]], metadata: dict[str, Any]) -> list[dict[str, Any]]:
    context = str(metadata.get(ADDITIONAL_CONTEXT_KEY) or "").strip()
    if not context:
        return messages
    return [
        {"role": "system", "text": f"Session start context:\n\n{context}", "metadata": {"source": RESERVED_PREFIX}},
        *messages,
    ]


def session_start_metadata_paths(
    metadata: dict[str, Any], key: str, *, allowed_basenames: set[str] | None = None
) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()
    for hook_metadata in session_start_result_metadata(metadata):
        raw_paths = hook_metadata.get(key)
        if not isinstance(raw_paths, list):
            continue
        for raw_path in raw_paths:
            path = str(raw_path or "").strip()
            if not path:
                continue
            if allowed_basenames is not None and os.path.basename(os.path.dirname(path)) not in allowed_basenames:
                continue
            if not os.path.isdir(path):
                continue
            if path in seen:
                continue
            seen.add(path)
            paths.append(path)
    return paths


def session_start_result_metadata(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    for key, value in metadata.items():
        if not str(key).startswith(f"{RESULTS_PREFIX}.") or not isinstance(value, dict):
            continue
        parsed = value.get("metadata")
        if isinstance(parsed, dict):
            values.append(parsed)
            continue
        stdout = value.get("stdout")
        if isinstance(stdout, str):
            stdout_payload = _json_stdout_payload(stdout)
            payload_metadata = stdout_payload.get("metadata")
            if isinstance(payload_metadata, dict):
                values.append(payload_metadata)
    return values


def _run_hook(hook: Any) -> tuple[dict[str, Any], str]:
    hook_id = str(getattr(hook, "id", "") or "").strip()
    hook_type = str(getattr(hook, "type", "") or "command").strip() or "command"
    if hook_type != "command":
        raise ValueError(f"sessionStart hook {hook_id!r} type {hook_type!r} is not supported")
    command = [str(part) for part in getattr(hook, "command", []) if str(part).strip()]
    if not command:
        raise ValueError(f"sessionStart hook {hook_id!r} command is required")
    timeout_value = str(getattr(hook, "timeout", "") or "")
    timeout = _parse_timeout(timeout_value)
    try:
        completed = subprocess.run(
            command,
            cwd=str(getattr(hook, "cwd", "") or "") or None,
            env=_hook_env(getattr(hook, "env", {}) or {}),
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        timeout_detail = timeout_value.strip() or str(timeout or "").strip()
        raise RuntimeError(f"sessionStart hook {hook_id!r} timed out after {timeout_detail}") from exc
    stdout = completed.stdout or ""
    stderr = completed.stderr or ""
    if completed.returncode != 0:
        detail = stderr.strip() or stdout.strip() or f"exit code {completed.returncode}"
        raise RuntimeError(f"sessionStart hook {hook_id!r} failed: {detail}")
    output = getattr(hook, "output", None)
    stdout_payload = _json_stdout_payload(stdout)
    result: dict[str, Any] = {
        "status": "succeeded",
        "exitCode": completed.returncode,
        "timeout": timeout_value,
        "timedOut": False,
    }
    if bool(getattr(output, "metadata", False)):
        payload_metadata = stdout_payload.get("metadata")
        if isinstance(payload_metadata, dict):
            result["metadata"] = payload_metadata
        result["stdout"] = stdout
        result["stderr"] = stderr
    additional_context = ""
    if bool(getattr(output, "additional_context", False)):
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


def _hook_env(explicit: Any) -> dict[str, str]:
    env = {key: value for key in _DEFAULT_ENV_KEYS if (value := os.environ.get(key)) is not None}
    for key, value in dict(explicit).items():
        env[str(key)] = str(value)
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
