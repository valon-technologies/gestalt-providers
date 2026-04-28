from __future__ import annotations

import hashlib
import json
from typing import Any

from google.protobuf import json_format
from google.protobuf import struct_pb2 as _struct_pb2
from gestalt.gen.v1 import agent_pb2 as _agent_pb2
from gestalt.gen.v1 import workflow_pb2 as _workflow_pb2

from .config import get_github_config
from .constants import (
    BOT_COMMIT_FILES_OPERATION,
    BOT_CREATE_PULL_REQUEST_OPERATION,
    BOT_OPEN_PULL_REQUEST_OPERATION,
    DEFAULT_AGENT_SYSTEM_PROMPT,
    GITHUB_WORKFLOW_SIGNAL_NAME,
    MAX_AGENT_PAYLOAD_CHARS,
)
from .workflow_dispatch import build_workflow_event

agent_pb2: Any = _agent_pb2
struct_pb2: Any = _struct_pb2
workflow_pb2: Any = _workflow_pb2


def build_workflow_signal_or_start_request(
    payload: dict[str, Any],
    summary: dict[str, Any],
) -> Any:
    config = get_github_config()
    request = workflow_pb2.WorkflowManagerSignalOrStartRunRequest(
        provider_name=config.workflow_provider,
        workflow_key=agent_session_ref(summary),
        idempotency_key=agent_turn_idempotency_key(payload, summary),
        target=workflow_agent_target(summary),
        signal=workflow_pb2.WorkflowSignal(
            name=GITHUB_WORKFLOW_SIGNAL_NAME,
            idempotency_key=agent_turn_idempotency_key(payload, summary),
        ),
    )
    request.signal.payload.CopyFrom(workflow_signal_payload(payload, summary))
    request.signal.metadata.CopyFrom(agent_turn_metadata(summary))
    return request


def workflow_agent_target(summary: dict[str, Any]) -> Any:
    config = get_github_config()
    agent = workflow_pb2.BoundWorkflowAgentTarget(
        provider_name=config.agent_provider,
        model=config.agent_model,
        prompt=workflow_agent_prompt(),
        messages=[
            agent_pb2.AgentMessage(role="system", text=agent_system_prompt()),
        ],
        tool_refs=agent_tool_refs(),
        tool_source=agent_pb2.AGENT_TOOL_SOURCE_MODE_NATIVE_SEARCH,
    )
    agent.metadata.CopyFrom(agent_session_metadata(summary))
    if config.agent_provider_options:
        agent.provider_options.CopyFrom(dict_to_struct(config.agent_provider_options))
    return workflow_pb2.BoundWorkflowTarget(agent=agent)


def workflow_signal_payload(payload: dict[str, Any], summary: dict[str, Any]) -> Any:
    event = build_workflow_event(payload)
    data = json_format.MessageToDict(event.data, preserving_proto_field_name=True)
    safe_payload = data.get("payload")
    if not isinstance(safe_payload, dict):
        safe_payload = {}
    data["user_prompt"] = agent_user_prompt(safe_payload, summary)
    return dict_to_struct(data)


def workflow_agent_prompt() -> str:
    return "\n".join(
        [
            "Handle GitHub App webhooks delivered in the final workflow signal batch.",
            "Each signal payload includes user_prompt, summary, and redacted GitHub webhook fields.",
            "Use the payload's user_prompt as the current GitHub request.",
        ]
    )


def agent_tool_refs() -> list[Any]:
    return [
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
    ]


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


def agent_turn_idempotency_key(payload: dict[str, Any], summary: dict[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    repo = summary.get("repository", "")
    event_type = summary.get("event_type", "")
    action = summary.get("action", "")
    return f"github:event:{repo}:{event_type}:{action}:{digest}"


def dict_to_struct(data: dict[str, Any]) -> Any:
    struct = struct_pb2.Struct()
    struct.update(data)
    return struct
