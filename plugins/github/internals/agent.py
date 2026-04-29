from __future__ import annotations

import hashlib
import json
from typing import Any

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
    MAX_AGENT_USER_PROMPT_CHARS,
)
from .webhook import bounded_text
from .workflow_dispatch import workflow_signal_data

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
    data = workflow_signal_data(payload, summary)
    agent_request = data.get("agent_request")
    if not isinstance(agent_request, dict):
        agent_request = {}
        data["agent_request"] = agent_request
    agent_request["user_prompt"] = agent_user_prompt(agent_request, summary)
    return dict_to_struct(data)


def workflow_agent_prompt() -> str:
    return "\n".join(
        [
            "Handle GitHub App webhooks delivered in the final workflow signal batch.",
            "Each signal payload includes summary and compact agent_request fields.",
            "Use agent_request.user_prompt as the current GitHub request.",
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


def agent_user_prompt(agent_request: dict[str, Any], summary: dict[str, Any]) -> str:
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
    subject = agent_request.get("subject")
    if isinstance(subject, dict) and subject.get("html_url"):
        lines.append(f"url: {subject['html_url']}")
    for key in ("pull_request", "issue", "comment", "review"):
        value = agent_request.get(key)
        if isinstance(value, dict):
            lines.extend(_prompt_section(key, value))
    ref_lines = _ref_prompt_lines(agent_request)
    if ref_lines:
        lines.extend(["", "ref:"] + ref_lines)
    return bounded_text("\n".join(lines), MAX_AGENT_USER_PROMPT_CHARS)


def _prompt_section(name: str, value: dict[str, Any]) -> list[str]:
    lines = ["", f"{name}:"]
    for key in (
        "number",
        "title",
        "state",
        "html_url",
        "head_ref",
        "base_ref",
        "id",
        "user",
        "body",
    ):
        nested = value.get(key)
        if nested not in ("", 0, None):
            lines.append(f"{key}: {nested}")
    return lines


def _ref_prompt_lines(agent_request: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for key in (
        "ref",
        "base_ref",
        "before",
        "after",
        "compare",
        "ref_type",
        "created",
        "deleted",
        "forced",
    ):
        if key in agent_request:
            lines.append(f"{key}: {agent_request[key]}")
    head_commit = agent_request.get("head_commit")
    if isinstance(head_commit, dict):
        for key in ("id", "message", "url"):
            value = head_commit.get(key)
            if value:
                lines.append(f"head_commit.{key}: {value}")
    return lines


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
