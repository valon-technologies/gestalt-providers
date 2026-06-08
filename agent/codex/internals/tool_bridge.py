from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass
from typing import Any

import gestalt
from mcp import types as mcp_types


DEFAULT_HOST_RPC_TIMEOUT_SECONDS = 30.0
MAX_LISTED_TOOLS = 1000
_UNSAFE_TOOL_NAME = re.compile(r"[*?,\s\x00-\x1f\x7f]")


class ToolBridgeError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ToolEntry:
    mcp_name: str
    title: str
    description: str
    ref: gestalt.AgentToolRef
    input_schema: dict[str, Any]
    annotations: mcp_types.ToolAnnotations | None


class ToolExecutor:
    def __init__(
        self, *, turn_id: str, request_context: Any, timeout_seconds: float = DEFAULT_HOST_RPC_TIMEOUT_SECONDS
    ) -> None:
        self._turn_id = turn_id
        self._request_context = request_context
        self._timeout_seconds = timeout_seconds
        self._lock = threading.Lock()
        self._sequence = 0

    def execute(self, *, entry: ToolEntry, arguments: dict[str, Any]) -> gestalt.Response[bytes]:
        with self._lock:
            self._sequence += 1
            sequence = self._sequence
        idempotency_key = f"agent/codex-mcp:{self._turn_id}:{sequence}:{entry.mcp_name}"
        return execute_tool(
            request_context=self._request_context,
            entry=entry,
            idempotency_key=idempotency_key,
            arguments=arguments,
            timeout_seconds=self._timeout_seconds,
        )


def list_tools(*, listed_tools: list[gestalt.ListedAgentTool]) -> list[ToolEntry]:
    tools: list[ToolEntry] = []
    seen_names: set[str] = set()
    for listed in listed_tools:
        entry = tool_entry(listed)
        if entry.mcp_name in seen_names:
            raise ToolBridgeError(f"tools.catalog.tools returned duplicate mcp_name {entry.mcp_name!r}")
        seen_names.add(entry.mcp_name)
        tools.append(entry)
        if len(tools) > MAX_LISTED_TOOLS:
            raise ToolBridgeError(f"tools.catalog.tools returned more than {MAX_LISTED_TOOLS} tools")
    if not tools:
        raise ToolBridgeError("tools.catalog.tools is empty for the requested tool scope")
    return tools


def tool_entry(tool: gestalt.ListedAgentTool) -> ToolEntry:
    mcp_name = tool.mcp_name.strip()
    if _UNSAFE_TOOL_NAME.search(mcp_name):
        raise ToolBridgeError(f"tools.catalog.tools returned unsafe mcp_name {mcp_name!r}")
    ref = tool.ref
    if ref is None or not ref.app.strip() or not ref.operation.strip() or ref.system.strip():
        raise ToolBridgeError(f"tools.catalog.tools returned non-app tool {mcp_name!r}")
    return ToolEntry(
        mcp_name=mcp_name,
        title=str(tool.title or "").strip(),
        description=str(tool.description or "").strip(),
        ref=ref,
        input_schema=schema_from_json(str(tool.input_schema or "")),
        annotations=tool_annotations(tool.annotations, title=str(tool.title or "").strip()),
    )


def execute_tool(
    *,
    request_context: Any,
    entry: ToolEntry,
    idempotency_key: str,
    arguments: dict[str, Any],
    timeout_seconds: float = DEFAULT_HOST_RPC_TIMEOUT_SECONDS,
) -> gestalt.Response[bytes]:
    request = gestalt.Request(context=request_context)
    with request.app() as app:
        try:
            return app.invoke_raw(
                entry.ref.app,
                entry.ref.operation,
                arguments or {},
                connection=entry.ref.connection,
                instance=entry.ref.instance,
                idempotency_key=idempotency_key,
                credential_mode=entry.ref.credential_mode,
                timeout_seconds=timeout_seconds,
            )
        except Exception as exc:
            raise ToolBridgeError(str(exc) or exc.__class__.__name__) from exc


def mcp_tool(entry: ToolEntry) -> mcp_types.Tool:
    return mcp_types.Tool(
        name=entry.mcp_name,
        title=entry.title or None,
        description=entry.description or entry.title or entry.mcp_name,
        inputSchema=entry.input_schema,
        annotations=entry.annotations,
    )


def mcp_tool_result(response: gestalt.Response[bytes]) -> mcp_types.CallToolResult:
    body = operation_body_text(response.body)
    status = int(response.status or 0)
    if not body:
        body = "{}"
    return mcp_types.CallToolResult(content=[mcp_types.TextContent(type="text", text=body)], isError=status >= 400)


def operation_body_text(body: object) -> str:
    if body is None:
        return ""
    if isinstance(body, bytes | bytearray | memoryview):
        return bytes(body).decode("utf-8", errors="replace")
    return str(body)


def schema_from_json(value: str) -> dict[str, Any]:
    value = value.strip()
    if not value:
        return {"type": "object", "additionalProperties": True}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {"type": "object", "additionalProperties": True}
    if not isinstance(payload, dict):
        return {"type": "object", "additionalProperties": True}
    projected = project_object_schema(payload)
    if projected is None:
        return {"type": "object", "properties": {}, "additionalProperties": True}
    return projected


def project_object_schema(schema: dict[str, Any]) -> dict[str, Any] | None:
    if not schema_type_supports_object(schema.get("type")):
        return None
    properties = schema_properties(schema)
    if properties is None:
        return None
    projected_properties = dict(properties)
    required: set[str] = set()
    for key, union_required in (("allOf", True), ("oneOf", False), ("anyOf", False)):
        if not merge_schema_branches(
            branches=schema.get(key), properties=projected_properties, required=required, union_required=union_required
        ):
            return None
    required.update(schema_required(schema.get("required"), projected_properties))
    projected: dict[str, Any] = {"type": "object"}
    additional_properties = schema.get("additionalProperties")
    if isinstance(additional_properties, bool):
        projected["additionalProperties"] = additional_properties
    if projected_properties:
        projected["properties"] = projected_properties
    if required:
        projected["required"] = sorted(required)
    return projected


def schema_type_supports_object(value: Any) -> bool:
    if value is None:
        return True
    if value == "object":
        return True
    return isinstance(value, list) and "object" in value


def schema_properties(schema: dict[str, Any]) -> dict[str, Any] | None:
    properties = schema.get("properties")
    if properties is None:
        return {}
    if not isinstance(properties, dict):
        return None
    return properties


def merge_schema_branches(
    *, branches: Any, properties: dict[str, Any], required: set[str], union_required: bool
) -> bool:
    if branches is None:
        return True
    if not isinstance(branches, list):
        return False
    for branch in branches:
        if not isinstance(branch, dict):
            return False
        projected = project_object_schema(branch)
        if projected is None:
            return False
        branch_properties = projected.get("properties")
        if isinstance(branch_properties, dict):
            for name, value in branch_properties.items():
                if name in properties and properties[name] != value:
                    return False
                properties[name] = value
        if union_required:
            required.update(schema_required(projected.get("required"), properties))
    return True


def schema_required(value: Any, properties: dict[str, Any]) -> set[str]:
    if not isinstance(value, list):
        return set()
    return {item for item in value if isinstance(item, str) and item in properties}


def tool_annotations(
    annotations: gestalt.AgentToolAnnotations | None, *, title: str
) -> mcp_types.ToolAnnotations | None:
    values: dict[str, Any] = {}
    if title:
        values["title"] = title
    if annotations is not None:
        if annotations.read_only_hint is not None:
            values["readOnlyHint"] = bool(annotations.read_only_hint)
        if annotations.idempotent_hint is not None:
            values["idempotentHint"] = bool(annotations.idempotent_hint)
        if annotations.destructive_hint is not None:
            values["destructiveHint"] = bool(annotations.destructive_hint)
        if annotations.open_world_hint is not None:
            values["openWorldHint"] = bool(annotations.open_world_hint)
    return mcp_types.ToolAnnotations(**values) if values else None
