from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from typing import Any, cast

import gestalt
from claude_agent_sdk.types import McpSdkServerConfig, PermissionResultAllow, PermissionResultDeny
from google.protobuf import struct_pb2 as _struct_pb2
from mcp.server import Server
from mcp.types import (
    CallToolResult,
    ListToolsRequest,
    ListToolsResult,
    PaginatedRequestParams,
    TextContent,
    Tool,
    ToolAnnotations,
)


struct_pb2: Any = _struct_pb2
logger = logging.getLogger(__name__)

MCP_SERVER_NAME = "gestalt"
DEFAULT_PAGE_SIZE = 10_000
MAX_PAGES = 1_000
MAX_CATALOG_TOOLS = 10_000
TOOL_ERROR_NAME = "gestalt__tools_unavailable"
TOOL_ERROR_MAX_CHARS = 1200
_UNSAFE_TOOL_NAME = re.compile(r"[*?,\s\x00-\x1f\x7f]")
_GESTALT_MCP_TOOL_PREFIX = f"mcp__{MCP_SERVER_NAME}__"


@dataclass(slots=True)
class ToolEntry:
    tool_id: str
    mcp_name: str
    title: str
    description: str
    input_schema: dict[str, Any]
    annotations: ToolAnnotations | None


class GestaltMCPBridge:
    def __init__(self, *, session_id: str, turn_id: str, run_grant: str) -> None:
        self._session_id = session_id
        self._turn_id = turn_id
        self._run_grant = run_grant
        self._entries: dict[str, ToolEntry] = {}
        self._all_entries: list[ToolEntry] | None = None
        self._catalog_loaded = False
        self._execute_lock = asyncio.Lock()
        self._sequence = 0
        self.server = Server(MCP_SERVER_NAME)

        @self.server.list_tools()
        async def list_tools(req: ListToolsRequest) -> ListToolsResult:
            return await self.list_tools(req)

        @self.server.call_tool(validate_input=False)
        async def call_tool(name: str, arguments: dict[str, Any]) -> CallToolResult:
            return await self.call_tool(name, arguments)

    async def list_tools(self, req: ListToolsRequest) -> ListToolsResult:
        page_token = ""
        if req.params is not None:
            page_token = str(req.params.cursor or "").strip()
        try:
            if page_token:
                entries, next_page_token = await asyncio.to_thread(self._list_entries, page_token)
            else:
                entries = await asyncio.to_thread(self._list_all_entries)
                next_page_token = ""
        except Exception as exc:
            entry = _tool_error_entry(exc)
            self._entries[entry.mcp_name] = entry
            return ListToolsResult(tools=[_mcp_tool(entry)])
        tools: list[Tool] = []
        for entry in entries:
            self._remember_entry(entry)
            tools.append(_mcp_tool(entry))
        return ListToolsResult(tools=tools, nextCursor=next_page_token or None)

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> CallToolResult:
        tool_name = str(name or "").strip()
        entry = self._entries.get(tool_name)
        if entry is None:
            try:
                entry = await asyncio.to_thread(self._find_entry, tool_name)
            except Exception as exc:
                return _tool_error_result(exc)
        if entry.mcp_name == TOOL_ERROR_NAME:
            return _tool_error_result(RuntimeError(entry.description))
        return await self._execute_entry(entry, arguments or {})

    async def _execute_entry(self, entry: ToolEntry, arguments: dict[str, Any]) -> CallToolResult:
        async with self._execute_lock:
            self._sequence += 1
            tool_call_id = f"sdk-{self._sequence}"
            try:
                response = await asyncio.to_thread(
                    _execute_tool,
                    session_id=self._session_id,
                    turn_id=self._turn_id,
                    run_grant=self._run_grant,
                    entry=entry,
                    tool_call_id=tool_call_id,
                    arguments=arguments,
                )
            except Exception as exc:
                return _tool_error_result(exc)
        body = str(getattr(response, "body", "") or "")
        status = int(getattr(response, "status", 0) or 0)
        if not body:
            body = "{}"
        return CallToolResult(content=[TextContent(type="text", text=body)], isError=status >= 400)

    def _remember_entry(self, entry: ToolEntry) -> None:
        self._entries[entry.mcp_name] = entry

    def _remember_entries(self, entries: list[ToolEntry]) -> None:
        for entry in entries:
            self._remember_entry(entry)

    def _list_all_entries(self) -> list[ToolEntry]:
        if self._all_entries is not None:
            return list(self._all_entries)
        page_token = ""
        seen_tokens: set[str] = set()
        all_entries: list[ToolEntry] = []
        for _ in range(MAX_PAGES):
            if page_token in seen_tokens:
                raise ValueError(f"ListTools repeated page token {page_token!r}")
            seen_tokens.add(page_token)
            entries, next_page_token = self._list_entries(page_token)
            self._remember_entries(entries)
            all_entries.extend(entries)
            if len(all_entries) > MAX_CATALOG_TOOLS:
                raise ValueError(f"ListTools exceeded {MAX_CATALOG_TOOLS} tools")
            page_token = next_page_token
            if not page_token:
                self._catalog_loaded = True
                self._all_entries = list(all_entries)
                return all_entries
        if page_token:
            raise ValueError(f"ListTools exceeded {MAX_PAGES} pages")
        return all_entries

    def _find_entry(self, mcp_name: str) -> ToolEntry:
        if not mcp_name:
            raise ValueError("tool name is required")
        if entry := self._entries.get(mcp_name):
            return entry
        if self._catalog_loaded:
            raise ValueError(f"tool {mcp_name!r} is not available in the current grant")
        for entry in self._list_all_entries():
            if entry.mcp_name == mcp_name:
                return entry
        raise ValueError(f"tool {mcp_name!r} is not available in the current grant")

    def _list_entries(self, page_token: str) -> tuple[list[ToolEntry], str]:
        with gestalt.AgentHost() as host:
            response = host.list_tools(
                gestalt.ListAgentToolsRequest(
                    session_id=self._session_id,
                    turn_id=self._turn_id,
                    page_size=DEFAULT_PAGE_SIZE,
                    page_token=page_token,
                    run_grant=self._run_grant,
                )
            )
        return [_tool_entry(tool) for tool in response.tools], str(response.next_page_token or "").strip()


def create_gestalt_sdk_mcp_server(*, session_id: str, turn_id: str, run_grant: str) -> McpSdkServerConfig:
    install_sdk_mcp_pagination_patch()
    bridge = GestaltMCPBridge(session_id=session_id, turn_id=turn_id, run_grant=run_grant)
    return McpSdkServerConfig(type="sdk", name=MCP_SERVER_NAME, instance=bridge.server)


def allowed_gestalt_mcp_tools() -> list[str]:
    return [f"{_GESTALT_MCP_TOOL_PREFIX}*"]


async def allow_gestalt_mcp_tool(
    tool_name: str, _arguments: dict[str, Any], _context: Any
) -> PermissionResultAllow | PermissionResultDeny:
    if str(tool_name or "").startswith(_GESTALT_MCP_TOOL_PREFIX):
        return PermissionResultAllow(behavior="allow")
    return PermissionResultDeny(behavior="deny", message=f"tool {tool_name!r} is not allowed", interrupt=False)


def install_sdk_mcp_pagination_patch() -> None:
    try:
        from claude_agent_sdk._internal import query as sdk_query
    except Exception:  # pragma: no cover
        logger.exception("failed to import Claude SDK query internals for MCP pagination patch")
        return

    query_cls = getattr(sdk_query, "Query", None)
    if query_cls is None or getattr(query_cls, "_gestalt_mcp_pagination_patch", False):
        return
    original = query_cls._handle_sdk_mcp_request

    async def patched(self: Any, server_name: str, message: dict[str, Any]) -> dict[str, Any]:
        method = message.get("method")
        if method != "tools/list":
            return await original(self, server_name, message)
        if server_name not in self.sdk_mcp_servers:
            return {
                "jsonrpc": "2.0",
                "id": message.get("id"),
                "error": {"code": -32601, "message": f"Server {server_name!r} not found"},
            }
        server = self.sdk_mcp_servers[server_name]
        handler = server.request_handlers.get(ListToolsRequest)
        if handler is None:
            return {
                "jsonrpc": "2.0",
                "id": message.get("id"),
                "error": {"code": -32601, "message": "tools/list is not supported"},
            }
        try:
            request = _list_tools_request_from_message(message)
            result = await handler(request)
            response_result: dict[str, Any] = {"tools": [_tool_to_json(tool) for tool in result.root.tools]}
            next_cursor = str(result.root.nextCursor or "").strip()
            if next_cursor:
                response_result["nextCursor"] = next_cursor
            return {"jsonrpc": "2.0", "id": message.get("id"), "result": response_result}
        except Exception as exc:
            return {"jsonrpc": "2.0", "id": message.get("id"), "error": {"code": -32603, "message": str(exc)}}

    query_cls._handle_sdk_mcp_request = patched
    query_cls._gestalt_mcp_pagination_patch = True


def _list_tools_request_from_message(message: dict[str, Any]) -> ListToolsRequest:
    params = message.get("params")
    if isinstance(params, dict) and str(params.get("cursor") or "").strip():
        return ListToolsRequest(method="tools/list", params=PaginatedRequestParams(cursor=str(params["cursor"])))
    return ListToolsRequest(method="tools/list")


def _tool_to_json(tool: Tool) -> dict[str, Any]:
    input_schema = cast(Any, tool.inputSchema)
    if hasattr(input_schema, "model_dump"):
        input_schema_json = input_schema.model_dump()
    else:
        input_schema_json = input_schema or {}
    out: dict[str, Any] = {"name": tool.name, "description": tool.description, "inputSchema": input_schema_json}
    if tool.annotations:
        out["annotations"] = tool.annotations.model_dump(exclude_none=True)
    if tool.meta:
        out["_meta"] = tool.meta
    return out


def _mcp_tool(entry: ToolEntry) -> Tool:
    return Tool(
        name=entry.mcp_name,
        description=entry.description or entry.title or entry.mcp_name,
        inputSchema=entry.input_schema,
        annotations=entry.annotations,
    )


def _tool_error_entry(exc: Exception) -> ToolEntry:
    message = _tool_error_message(exc)
    return ToolEntry(
        tool_id="",
        mcp_name=TOOL_ERROR_NAME,
        title="Gestalt tools unavailable",
        description=(
            "Gestalt tool discovery failed. Use this diagnostic tool, then tell the user the "
            f"integration needs attention before retrying. Error: {message}"
        ),
        input_schema={"type": "object", "additionalProperties": False},
        annotations=ToolAnnotations(title="Gestalt tools unavailable", readOnlyHint=True),
    )


def _tool_error_result(exc: Exception) -> CallToolResult:
    body = json.dumps({"ok": False, "error": _tool_error_message(exc)}, ensure_ascii=False)
    return CallToolResult(content=[TextContent(type="text", text=body)], isError=True)


def _tool_error_message(exc: Exception) -> str:
    message = str(exc).strip() or exc.__class__.__name__
    message = " ".join(message.split())
    if len(message) > TOOL_ERROR_MAX_CHARS:
        return message[: TOOL_ERROR_MAX_CHARS - 3].rstrip() + "..."
    return message


def _tool_entry(tool_proto: Any) -> ToolEntry:
    tool_id = str(tool_proto.id or "").strip()
    mcp_name = str(tool_proto.mcp_name or "").strip()
    if not tool_id:
        raise ValueError("ListTools returned a tool without an id")
    if not mcp_name:
        raise ValueError("ListTools returned a tool without an mcp_name")
    if _UNSAFE_TOOL_NAME.search(mcp_name):
        raise ValueError(f"ListTools returned unsafe mcp_name {mcp_name!r}")
    return ToolEntry(
        tool_id=tool_id,
        mcp_name=mcp_name,
        title=str(tool_proto.title or "").strip(),
        description=str(tool_proto.description or "").strip(),
        input_schema=_schema_from_json(str(tool_proto.input_schema or "")),
        annotations=_annotations(tool_proto.annotations, title=str(tool_proto.title or "").strip()),
    )


def _execute_tool(
    *, session_id: str, turn_id: str, run_grant: str, entry: ToolEntry, tool_call_id: str, arguments: dict[str, Any]
) -> Any:
    struct = struct_pb2.Struct()
    struct.update(arguments or {})
    with gestalt.AgentHost() as host:
        return host.execute_tool(
            gestalt.ExecuteAgentToolRequest(
                session_id=session_id,
                turn_id=turn_id,
                tool_call_id=tool_call_id,
                tool_id=entry.tool_id,
                arguments=struct,
                run_grant=run_grant,
                idempotency_key=f"agent/claude-sdk:{turn_id}:{tool_call_id}:{entry.mcp_name}",
            )
        )


def _schema_from_json(value: str) -> dict[str, Any]:
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


def _annotations(value: Any, *, title: str) -> ToolAnnotations | None:
    if value is None:
        return ToolAnnotations(title=title) if title else None
    payload: dict[str, str | bool | None] = {"title": title or str(getattr(value, "title", "") or "").strip() or None}
    for proto_name, sdk_name in (
        ("read_only_hint", "readOnlyHint"),
        ("destructive_hint", "destructiveHint"),
        ("idempotent_hint", "idempotentHint"),
        ("open_world_hint", "openWorldHint"),
    ):
        hint = _optional_bool(value, proto_name)
        if hint is not None:
            payload[sdk_name] = hint
    return ToolAnnotations.model_validate({k: v for k, v in payload.items() if v is not None})


def _optional_bool(value: Any, field_name: str) -> bool | None:
    has_field = getattr(value, "HasField", None)
    if callable(has_field):
        try:
            if not has_field(field_name):
                return None
        except ValueError:
            pass
    raw = getattr(value, field_name, None)
    if raw is None:
        return None
    if not callable(has_field) and raw is False:
        return None
    return bool(raw)
