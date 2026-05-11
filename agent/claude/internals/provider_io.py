from __future__ import annotations

from typing import Any

import gestalt


def request_text(request: Any, field: str) -> str:
    return _text(getattr(request, field, ""))


def request_metadata(request: Any) -> dict[str, Any]:
    return gestalt.struct_to_dict(getattr(request, "metadata", None))


def optional_request_metadata(request: Any) -> dict[str, Any] | None:
    if not gestalt.has_field(request, "metadata"):
        return None
    return gestalt.struct_to_dict(request.metadata)


def request_prepared_workspace(request: Any) -> dict[str, str] | None:
    if not gestalt.has_field(request, "prepared_workspace"):
        return None
    return _prepared_workspace_to_dict(request.prepared_workspace)


def request_session_start(request: Any) -> Any | None:
    return request.session_start if gestalt.has_field(request, "session_start") else None


def request_subject_id(request: Any) -> str:
    subject = gestalt.agent_subject_context_to_dict(getattr(request, "subject", None))
    return _text(subject.get("subject_id"))


def request_limit(request: Any) -> int:
    limit = int(getattr(request, "limit", 0) or 0)
    if limit < 0:
        raise ValueError("limit must be non-negative")
    return limit


def has_session_start_hooks(session_start: Any | None) -> bool:
    if session_start is None:
        return False
    return len(list(getattr(session_start, "hooks", []) or [])) > 0


def validate_create_turn_request(request: Any) -> None:
    if int(getattr(request, "tool_source", 0) or 0) != gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG:
        raise ValueError("agent/claude requires toolSource mcp_catalog")
    if not request_text(request, "run_grant"):
        raise ValueError("run_grant is required")
    if len(list(getattr(request, "tools", []))) > 0:
        raise ValueError("resolved tools are not supported; use tool_refs with mcp_catalog")
    if gestalt.struct_to_dict(getattr(request, "response_schema", None)):
        raise ValueError("response_schema is not supported by agent/claude")
    if gestalt.struct_to_dict(getattr(request, "model_options", None)):
        raise ValueError("model_options are not supported by agent/claude")
    validate_tool_refs(list(getattr(request, "tool_refs", [])))


def validate_tool_refs(tool_refs: list[Any]) -> None:
    for index, ref in enumerate(tool_refs, start=1):
        _validate_tool_ref(gestalt.agent_tool_ref_to_dict(ref), index)


def prepared_workspace_cwd(value: dict[str, str] | None) -> str:
    if not value:
        return ""
    return _text(value.get("cwd"))


def _validate_tool_ref(tool_ref: dict[str, Any], index: int) -> None:
    plugin = _text(tool_ref.get("plugin"))
    system = _text(tool_ref.get("system"))
    operation = _text(tool_ref.get("operation"))
    connection = _text(tool_ref.get("connection"))
    instance = _text(tool_ref.get("instance"))
    title = _text(tool_ref.get("title"))
    description = _text(tool_ref.get("description"))

    if "*" in {system, operation, connection, instance}:
        raise ValueError("wildcard tool_refs are not supported")
    if plugin == "*":
        if any([system, operation, connection, instance, title, description]):
            raise ValueError(
                f"tool_refs[{index}] global search ref cannot include operation, connection, instance, "
                "title, or description"
            )
        return
    if system:
        if plugin:
            raise ValueError(f"tool_refs[{index}] must set exactly one of plugin or system")
        if system != "workflow":
            raise ValueError(f"tool_refs[{index}].system {system!r} is not supported")
        if not operation:
            raise ValueError(f"tool_refs[{index}].operation is required for system tool refs")
        if any([connection, instance, title, description]):
            raise ValueError(
                f"tool_refs[{index}] system refs cannot include connection, instance, title, or description"
            )
        return
    if not plugin:
        raise ValueError(f"tool_refs[{index}].plugin is required")


def _prepared_workspace_to_dict(value: Any | None) -> dict[str, str] | None:
    if value is None:
        return None
    workspace = gestalt.prepared_workspace_to_dict(value)
    root = _text(workspace.get("root"))
    cwd = _text(workspace.get("cwd"))
    if not root and not cwd:
        return None
    if not root or not cwd:
        raise ValueError("prepared_workspace root and cwd are required")
    return {"root": root, "cwd": cwd}


def _text(value: Any) -> str:
    return str(value or "").strip()
