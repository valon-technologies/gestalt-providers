from __future__ import annotations

import hashlib
import json
from typing import Any

from google.protobuf import struct_pb2 as _struct_pb2
from gestalt.gen.v1 import agent_pb2 as _agent_pb2

from .config import get_github_config
from .constants import (
    BOT_COMMIT_FILES_OPERATION,
    BOT_CREATE_PULL_REQUEST_OPERATION,
    BOT_OPEN_PULL_REQUEST_OPERATION,
    DEFAULT_AGENT_SYSTEM_PROMPT,
    MAX_AGENT_PAYLOAD_CHARS,
)

agent_pb2: Any = _agent_pb2
struct_pb2: Any = _struct_pb2


def build_agent_session_request(summary: dict[str, Any]) -> Any:
    config = get_github_config()
    request = agent_pb2.AgentManagerCreateSessionRequest(
        provider_name=config.agent_provider,
        model=config.agent_model,
        client_ref=agent_session_ref(summary),
        idempotency_key=agent_session_idempotency_key(summary),
    )
    request.metadata.CopyFrom(agent_session_metadata(summary))
    return request


def build_agent_turn_request(
    payload: dict[str, Any],
    summary: dict[str, Any],
    session_id: str,
) -> Any:
    config = get_github_config()
    request = agent_pb2.AgentManagerCreateTurnRequest(
        session_id=session_id,
        model=config.agent_model,
        messages=[
            agent_pb2.AgentMessage(role="system", text=agent_system_prompt()),
            agent_pb2.AgentMessage(
                role="user", text=agent_user_prompt(payload, summary)
            ),
        ],
        tool_refs=[
            agent_pb2.AgentToolRef(
                plugin="github",
                operation=BOT_COMMIT_FILES_OPERATION,
            ),
            agent_pb2.AgentToolRef(
                plugin="github",
                operation=BOT_OPEN_PULL_REQUEST_OPERATION,
            ),
            agent_pb2.AgentToolRef(
                plugin="github",
                operation=BOT_CREATE_PULL_REQUEST_OPERATION,
            ),
        ],
        tool_source=agent_pb2.AGENT_TOOL_SOURCE_MODE_NATIVE_SEARCH,
        idempotency_key=agent_turn_idempotency_key(payload, summary),
    )
    request.metadata.CopyFrom(agent_turn_metadata(summary))
    if config.agent_provider_options:
        request.provider_options.CopyFrom(dict_to_struct(config.agent_provider_options))
    return request


def agent_session_metadata(summary: dict[str, Any]) -> Any:
    metadata = {
        key: summary[key]
        for key in (
            "installation_id",
            "repository",
            "repository_owner",
            "repository_name",
            "number",
            "head_ref",
            "base_ref",
        )
        if key in summary
    }
    metadata["session_ref"] = agent_session_ref(summary)
    return dict_to_struct({"github": metadata})


def agent_turn_metadata(summary: dict[str, Any]) -> Any:
    metadata = dict(summary)
    metadata["session_ref"] = agent_session_ref(summary)
    return dict_to_struct({"github": metadata})


def agent_system_prompt() -> str:
    config = get_github_config()
    if not config.agent_system_prompt:
        return DEFAULT_AGENT_SYSTEM_PROMPT
    return DEFAULT_AGENT_SYSTEM_PROMPT + "\n\n" + config.agent_system_prompt.strip()


def agent_user_prompt(payload: dict[str, Any], summary: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, sort_keys=True, indent=2)
    if len(payload_json) > MAX_AGENT_PAYLOAD_CHARS:
        payload_json = payload_json[:MAX_AGENT_PAYLOAD_CHARS] + "\n...<truncated>"
    lines = [
        "GitHub App webhook:",
        f"installation_id: {summary.get('installation_id', '')}",
        f"event_type: {summary.get('event_type', '')}",
        f"repository: {summary.get('repository', '')}",
        f"action: {summary.get('action', '')}",
        f"sender: {summary.get('sender', '')}",
    ]
    if "number" in summary:
        lines.append(f"number: {summary['number']}")
    lines.extend(["", "Payload:", payload_json])
    return "\n".join(lines)


def agent_session_ref(summary: dict[str, Any]) -> str:
    installation_id = summary.get("installation_id", "")
    repo = summary.get("repository", "")
    number = summary.get("number", "")
    if repo and number:
        return f"github:{installation_id}:{repo}:{number}"
    if repo:
        return f"github:{installation_id}:{repo}"
    return f"github:{installation_id}"


def agent_session_idempotency_key(summary: dict[str, Any]) -> str:
    return f"github:session:{agent_session_ref(summary)}"


def agent_turn_idempotency_key(payload: dict[str, Any], summary: dict[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    repo = summary.get("repository", "")
    event_type = summary.get("event_type", "")
    action = summary.get("action", "")
    return f"github:event:{repo}:{event_type}:{action}:{digest}"


def agent_execution_status_name(status: int) -> str:
    if not status:
        return ""
    try:
        return agent_pb2.AgentExecutionStatus.Name(status)
    except ValueError:
        return str(status)


def dict_to_struct(data: dict[str, Any]) -> Any:
    struct = struct_pb2.Struct()
    struct.update(data)
    return struct
