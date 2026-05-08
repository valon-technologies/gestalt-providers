from __future__ import annotations

import json
import re
import threading
from dataclasses import dataclass
from typing import Any, cast

import gestalt
import grpc
from mcp import types as mcp_types


DEFAULT_PAGE_SIZE = 100
DEFAULT_HOST_RPC_TIMEOUT_SECONDS = 30.0
MAX_LISTED_TOOLS = 1000
MAX_PAGES = 100
_UNSAFE_TOOL_NAME = re.compile(r"[*?,\s\x00-\x1f\x7f]")


class ToolBridgeError(RuntimeError):
    pass


@dataclass(slots=True)
class ToolEntry:
    tool_id: str
    mcp_name: str
    title: str
    description: str
    input_schema: dict[str, Any]
    annotations: mcp_types.ToolAnnotations | None


class ToolExecutor:
    def __init__(
        self,
        *,
        session_id: str,
        turn_id: str,
        run_grant: str,
        timeout_seconds: float = DEFAULT_HOST_RPC_TIMEOUT_SECONDS,
    ) -> None:
        self._session_id = session_id
        self._turn_id = turn_id
        self._run_grant = run_grant
        self._timeout_seconds = timeout_seconds
        self._lock = threading.Lock()
        self._sequence = 0

    def execute(self, *, entry: ToolEntry, arguments: dict[str, Any]) -> Any:
        with self._lock:
            self._sequence += 1
            sequence = self._sequence
        tool_call_id = f"mcp-{sequence}"
        idempotency_key = f"agent/codex-mcp:{self._turn_id}:{sequence}:{entry.mcp_name}"
        return execute_tool(
            session_id=self._session_id,
            turn_id=self._turn_id,
            run_grant=self._run_grant,
            entry=entry,
            tool_call_id=tool_call_id,
            idempotency_key=idempotency_key,
            arguments=arguments,
            timeout_seconds=self._timeout_seconds,
        )


def list_tools(
    *, session_id: str, turn_id: str, run_grant: str, timeout_seconds: float = DEFAULT_HOST_RPC_TIMEOUT_SECONDS
) -> list[ToolEntry]:
    page_token = ""
    seen_tokens: set[str] = set()
    tools: list[ToolEntry] = []
    seen_names: set[str] = set()
    pages = 0
    with gestalt.AgentHost() as host:
        while True:
            pages += 1
            if pages > MAX_PAGES:
                raise ToolBridgeError(f"ListTools exceeded {MAX_PAGES} pages")
            if page_token in seen_tokens:
                raise ToolBridgeError(f"ListTools repeated page token {page_token!r}")
            seen_tokens.add(page_token)
            request = gestalt.ListAgentToolsRequest(
                session_id=session_id,
                turn_id=turn_id,
                page_size=DEFAULT_PAGE_SIZE,
                page_token=page_token,
                run_grant=run_grant,
            )
            response = _agent_host_call(
                host=host, rpc_name="ListTools", request=request, timeout_seconds=timeout_seconds
            )
            for listed in response.tools:
                entry = tool_entry(listed)
                if entry.mcp_name in seen_names:
                    raise ToolBridgeError(f"ListTools returned duplicate mcp_name {entry.mcp_name!r}")
                seen_names.add(entry.mcp_name)
                tools.append(entry)
                if len(tools) > MAX_LISTED_TOOLS:
                    raise ToolBridgeError(f"ListTools returned more than {MAX_LISTED_TOOLS} tools")
            page_token = str(response.next_page_token or "").strip()
            if not page_token:
                break
    if not tools:
        raise ToolBridgeError("ListTools returned no tools for the requested grant")
    return tools


def tool_entry(tool_proto: Any) -> ToolEntry:
    tool_id = str(tool_proto.id or "").strip()
    mcp_name = str(tool_proto.mcp_name or "").strip()
    if not tool_id:
        raise ToolBridgeError("ListTools returned a tool without an id")
    if not mcp_name:
        raise ToolBridgeError("ListTools returned a tool without an mcp_name")
    if _UNSAFE_TOOL_NAME.search(mcp_name):
        raise ToolBridgeError(f"ListTools returned unsafe mcp_name {mcp_name!r}")
    return ToolEntry(
        tool_id=tool_id,
        mcp_name=mcp_name,
        title=str(tool_proto.title or "").strip(),
        description=str(tool_proto.description or "").strip(),
        input_schema=schema_from_json(str(tool_proto.input_schema or "")),
        annotations=tool_annotations(tool_proto.annotations, title=str(tool_proto.title or "").strip()),
    )


def execute_tool(
    *,
    session_id: str,
    turn_id: str,
    run_grant: str,
    entry: ToolEntry,
    tool_call_id: str,
    idempotency_key: str,
    arguments: dict[str, Any],
    timeout_seconds: float = DEFAULT_HOST_RPC_TIMEOUT_SECONDS,
) -> Any:
    with gestalt.AgentHost() as host:
        request = gestalt.ExecuteAgentToolRequest(
            session_id=session_id,
            turn_id=turn_id,
            tool_call_id=tool_call_id,
            tool_id=entry.tool_id,
            arguments=gestalt.struct_from_dict(arguments or {}),
            run_grant=run_grant,
            idempotency_key=idempotency_key,
        )
        return _agent_host_call(host=host, rpc_name="ExecuteTool", request=request, timeout_seconds=timeout_seconds)


def mcp_tool(entry: ToolEntry) -> mcp_types.Tool:
    return mcp_types.Tool(
        name=entry.mcp_name,
        title=entry.title or None,
        description=entry.description or entry.title or entry.mcp_name,
        inputSchema=entry.input_schema,
        annotations=entry.annotations,
    )


def mcp_tool_result(response: Any) -> mcp_types.CallToolResult:
    body = str(getattr(response, "body", "") or "")
    status = int(getattr(response, "status", 0) or 0)
    if not body:
        body = "{}"
    return mcp_types.CallToolResult(content=[mcp_types.TextContent(type="text", text=body)], isError=status >= 400)


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
    if payload.get("type") != "object":
        payload = {"type": "object", "properties": {}, "additionalProperties": True}
    return payload


def tool_annotations(annotations_proto: Any, *, title: str) -> mcp_types.ToolAnnotations | None:
    values: dict[str, Any] = {}
    if title:
        values["title"] = title
    for proto_name, sdk_name in (
        ("read_only_hint", "readOnlyHint"),
        ("idempotent_hint", "idempotentHint"),
        ("destructive_hint", "destructiveHint"),
        ("open_world_hint", "openWorldHint"),
    ):
        if gestalt.has_field(annotations_proto, proto_name):
            values[sdk_name] = bool(getattr(annotations_proto, proto_name))
    return mcp_types.ToolAnnotations(**values) if values else None


def _agent_host_call(*, host: gestalt.AgentHost, rpc_name: str, request: Any, timeout_seconds: float) -> Any:
    timeout = _coerce_timeout_seconds(timeout_seconds)
    try:
        return getattr(getattr(host, "_stub"), rpc_name)(request, timeout=timeout)
    except grpc.RpcError as exc:
        raise ToolBridgeError(_grpc_error_message(rpc_name, exc)) from exc


def _coerce_timeout_seconds(value: float) -> float:
    try:
        timeout = float(value)
    except (TypeError, ValueError):
        timeout = DEFAULT_HOST_RPC_TIMEOUT_SECONDS
    return timeout if timeout > 0 else DEFAULT_HOST_RPC_TIMEOUT_SECONDS


def _grpc_error_message(rpc_name: str, exc: grpc.RpcError) -> str:
    error = cast(Any, exc)
    code_value = error.code()
    code = code_value.name if code_value is not None else "UNKNOWN"
    details = error.details() or "gRPC call failed"
    return f"AgentHost.{rpc_name} failed: {code}: {details}"
