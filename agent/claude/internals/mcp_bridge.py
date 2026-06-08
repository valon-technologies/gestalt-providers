from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, cast

import gestalt
from claude_agent_sdk.types import (
    McpSdkServerConfig,
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)
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

from .claude_code_config import ClaudeCodeToolPermissions


logger = logging.getLogger(__name__)

MCP_SERVER_NAME = "gestalt"
DEFAULT_PAGE_SIZE = 10_000
MAX_PAGES = 1_000
MAX_CATALOG_TOOLS = 10_000
TOOL_ERROR_NAME = "gestalt__tools_unavailable"
TOOL_ERROR_MAX_CHARS = 1200
TOOL_SEARCH_METADATA_MAX_CHARS = 800
_UNSAFE_TOOL_NAME = re.compile(r"[*?,\s\x00-\x1f\x7f]")
_GESTALT_MCP_TOOL_PREFIX = f"mcp__{MCP_SERVER_NAME}__"


@dataclass(frozen=True, slots=True)
class ToolEntry:
    mcp_name: str
    title: str
    description: str
    ref: gestalt.AgentToolRef
    tags: tuple[str, ...]
    search_text: str
    input_schema: dict[str, Any]
    annotations: ToolAnnotations | None


class GestaltMCPBridge:
    def __init__(
        self,
        *,
        turn_id: str,
        request_context: Any,
        listed_tools: list[gestalt.ListedAgentTool],
        timeout_seconds: float = 0.0,
    ) -> None:
        self._turn_id = turn_id
        self._request_context = request_context
        self._listed_tools = list(listed_tools)
        self._timeout_seconds = timeout_seconds if timeout_seconds > 0 else None
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

            def execute_tool() -> gestalt.Response[str]:
                request = gestalt.Request(context=self._request_context)
                with request.app() as app:
                    return app.invoke(
                        entry.ref.app,
                        entry.ref.operation,
                        arguments or {},
                        connection=entry.ref.connection,
                        instance=entry.ref.instance,
                        idempotency_key=f"agent/claude-sdk:{self._turn_id}:{tool_call_id}:{entry.mcp_name}",
                        credential_mode=entry.ref.credential_mode,
                        timeout_seconds=self._timeout_seconds,
                    )

            try:
                response = await asyncio.to_thread(execute_tool)
            except Exception as exc:
                return _tool_error_result(exc)
        body = str(response.body or "")
        status = int(response.status or 0)
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
            _validate_unique_entries(all_entries)
            if len(all_entries) > MAX_CATALOG_TOOLS:
                raise ValueError(f"ListTools exceeded {MAX_CATALOG_TOOLS} tools")
            page_token = next_page_token
            if not page_token:
                if not all_entries:
                    raise ValueError("tools.catalog.tools is empty for the requested tool scope")
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
            raise ValueError(f"tool {mcp_name!r} is not available in the current tool scope")
        for entry in self._list_all_entries():
            if entry.mcp_name == mcp_name:
                return entry
        raise ValueError(f"tool {mcp_name!r} is not available in the current tool scope")

    def _list_entries(self, page_token: str) -> tuple[list[ToolEntry], str]:
        offset = _page_token_offset(page_token)
        next_offset = offset + DEFAULT_PAGE_SIZE
        tools = self._listed_tools[offset:next_offset]
        next_page_token = str(next_offset) if next_offset < len(self._listed_tools) else ""
        return [_tool_entry(tool) for tool in tools], next_page_token


def create_gestalt_sdk_mcp_server(
    *, turn_id: str, request_context: Any, listed_tools: list[gestalt.ListedAgentTool], timeout_seconds: float = 0.0
) -> McpSdkServerConfig:
    install_sdk_mcp_pagination_patch()
    bridge = GestaltMCPBridge(
        turn_id=turn_id,
        request_context=request_context,
        listed_tools=listed_tools,
        timeout_seconds=timeout_seconds,
    )
    return McpSdkServerConfig(type="sdk", name=MCP_SERVER_NAME, instance=bridge.server)


def allowed_gestalt_mcp_tools() -> list[str]:
    return [f"{_GESTALT_MCP_TOOL_PREFIX}*"]


ToolPermissionCallback = Callable[
    [str, dict[str, Any], ToolPermissionContext], Awaitable[PermissionResultAllow | PermissionResultDeny]
]


def create_tool_permission_callback(permissions: ClaudeCodeToolPermissions | None) -> ToolPermissionCallback:
    async def can_use_tool(
        tool_name: str, arguments: dict[str, Any], _context: ToolPermissionContext
    ) -> PermissionResultAllow | PermissionResultDeny:
        return _allow_tool(tool_name, arguments or {}, permissions=permissions)

    return can_use_tool


def _allow_tool(
    tool_name: str, arguments: dict[str, Any], *, permissions: ClaudeCodeToolPermissions | None
) -> PermissionResultAllow | PermissionResultDeny:
    name = str(tool_name or "")
    if name.startswith(_GESTALT_MCP_TOOL_PREFIX):
        return PermissionResultAllow(behavior="allow")
    if permissions is not None and permissions.allows(name, arguments):
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
        description=_tool_description(entry),
        inputSchema=entry.input_schema,
        annotations=entry.annotations,
    )


def _tool_error_entry(exc: Exception) -> ToolEntry:
    message = _tool_error_message(exc)
    return ToolEntry(
        mcp_name=TOOL_ERROR_NAME,
        title="Gestalt tools unavailable",
        description=(
            "Gestalt tool discovery failed. Use this diagnostic tool, then tell the user the "
            f"integration needs attention before retrying. Error: {message}"
        ),
        ref=gestalt.AgentToolRef(),
        tags=(),
        search_text="",
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


def _tool_entry(tool: gestalt.ListedAgentTool) -> ToolEntry:
    mcp_name = tool.mcp_name.strip()
    if not mcp_name:
        raise ValueError("tools.catalog.tools returned a tool without mcp_name")
    if _UNSAFE_TOOL_NAME.search(mcp_name):
        raise ValueError(f"tools.catalog.tools returned unsafe mcp_name {mcp_name!r}")
    ref = tool.ref
    if ref is None or not ref.app.strip() or not ref.operation.strip() or ref.system.strip():
        raise ValueError(f"tools.catalog.tools returned non-app tool {mcp_name!r}")
    return ToolEntry(
        mcp_name=mcp_name,
        title=str(tool.title or "").strip(),
        description=str(tool.description or "").strip(),
        ref=ref,
        tags=_string_list(tool.tags),
        search_text=str(tool.search_text or "").strip(),
        input_schema=_schema_from_json(str(tool.input_schema or "")),
        annotations=_annotations(tool.annotations, title=str(tool.title or "").strip()),
    )


def _validate_unique_entries(entries: list[ToolEntry]) -> None:
    seen_names: set[str] = set()
    for entry in entries:
        if entry.mcp_name in seen_names:
            raise ValueError(f"tools.catalog.tools returned duplicate mcp_name {entry.mcp_name!r}")
        seen_names.add(entry.mcp_name)


def _page_token_offset(page_token: str) -> int:
    token = str(page_token or "").strip()
    if not token:
        return 0
    try:
        offset = int(token)
    except ValueError as exc:
        raise ValueError(f"invalid tools/list cursor {page_token!r}") from exc
    if offset < 0:
        raise ValueError(f"invalid tools/list cursor {page_token!r}")
    return offset


def _tool_description(entry: ToolEntry) -> str:
    description = entry.description or entry.title or entry.mcp_name
    metadata = _tool_search_metadata(entry)
    if not metadata:
        return description
    return f"{description}\n\nSearch metadata: {metadata}."


def _tool_search_metadata(entry: ToolEntry) -> str:
    values: list[str] = []
    seen: set[str] = set()
    for value in [*entry.tags, entry.search_text]:
        for part in _metadata_parts(value):
            normalized = part.lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            values.append(part)
    if not values:
        return ""
    text = ", ".join(values)
    if len(text) > TOOL_SEARCH_METADATA_MAX_CHARS:
        text = text[: TOOL_SEARCH_METADATA_MAX_CHARS - 3].rstrip(" ,") + "..."
    return text


def _metadata_parts(value: str) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    if "," in raw:
        return [part.strip() for part in raw.split(",") if part.strip()]
    return [raw]


def _string_list(value: Any) -> tuple[str, ...]:
    out: list[str] = []
    for item in value or []:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return tuple(out)


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
    projected = _project_object_schema(payload)
    if projected is None:
        return {"type": "object", "properties": {}, "additionalProperties": True}
    return projected


def _project_object_schema(schema: dict[str, Any]) -> dict[str, Any] | None:
    if not _schema_type_supports_object(schema.get("type")):
        return None
    properties = _schema_properties(schema)
    if properties is None:
        return None
    projected_properties = dict(properties)
    required: set[str] = set()
    for key, union_required in (("allOf", True), ("oneOf", False), ("anyOf", False)):
        if not _merge_schema_branches(
            branches=schema.get(key), properties=projected_properties, required=required, union_required=union_required
        ):
            return None
    required.update(_schema_required(schema.get("required"), projected_properties))
    projected: dict[str, Any] = {"type": "object"}
    additional_properties = schema.get("additionalProperties")
    if isinstance(additional_properties, bool):
        projected["additionalProperties"] = additional_properties
    if projected_properties:
        projected["properties"] = projected_properties
    if required:
        projected["required"] = sorted(required)
    return projected


def _schema_type_supports_object(value: Any) -> bool:
    if value is None:
        return True
    if value == "object":
        return True
    return isinstance(value, list) and "object" in value


def _schema_properties(schema: dict[str, Any]) -> dict[str, Any] | None:
    properties = schema.get("properties")
    if properties is None:
        return {}
    if not isinstance(properties, dict):
        return None
    return properties


def _merge_schema_branches(
    *, branches: Any, properties: dict[str, Any], required: set[str], union_required: bool
) -> bool:
    if branches is None:
        return True
    if not isinstance(branches, list):
        return False
    for branch in branches:
        if not isinstance(branch, dict):
            return False
        projected = _project_object_schema(branch)
        if projected is None:
            return False
        branch_properties = projected.get("properties")
        if isinstance(branch_properties, dict):
            for name, value in branch_properties.items():
                if name in properties and properties[name] != value:
                    return False
                properties[name] = value
        if union_required:
            required.update(_schema_required(projected.get("required"), properties))
    return True


def _schema_required(value: Any, properties: dict[str, Any]) -> set[str]:
    if not isinstance(value, list):
        return set()
    return {item for item in value if isinstance(item, str) and item in properties}


def _annotations(value: gestalt.AgentToolAnnotations | None, *, title: str) -> ToolAnnotations | None:
    if value is None:
        return ToolAnnotations(title=title) if title else None
    payload: dict[str, str | bool | None] = {"title": title or None}
    if value.read_only_hint is not None:
        payload["readOnlyHint"] = bool(value.read_only_hint)
    if value.destructive_hint is not None:
        payload["destructiveHint"] = bool(value.destructive_hint)
    if value.idempotent_hint is not None:
        payload["idempotentHint"] = bool(value.idempotent_hint)
    if value.open_world_hint is not None:
        payload["openWorldHint"] = bool(value.open_world_hint)
    return ToolAnnotations.model_validate({k: v for k, v in payload.items() if v is not None})
