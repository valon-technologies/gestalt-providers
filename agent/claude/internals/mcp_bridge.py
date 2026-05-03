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
DIRECT_CATALOG_TOOL_LIMIT = 50
CATALOG_SEARCH_TOOL = "gestalt_catalog_search"
CATALOG_EXECUTE_TOOL = "gestalt_catalog_execute"
_UNSAFE_TOOL_NAME = re.compile(r"[*?,\s\x00-\x1f\x7f]")
_SEARCH_TERM = re.compile(r"[a-z0-9]+")
_GESTALT_MCP_TOOL_PREFIX = f"mcp__{MCP_SERVER_NAME}__"
_SEARCH_STOP_TERMS = frozenset(
    {
        "a",
        "an",
        "and",
        "find",
        "for",
        "from",
        "get",
        "in",
        "list",
        "me",
        "my",
        "of",
        "show",
        "the",
        "to",
        "use",
        "with",
    }
)
_SEARCH_ALIASES = {
    "pr": ("github", "pull", "request"),
    "prs": ("github", "pull", "request"),
    "pullrequest": ("github", "pull", "request"),
    "pullrequests": ("github", "pull", "request"),
    "ticket": ("linear", "issue"),
    "tickets": ("linear", "issue"),
}


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
        self._entries_by_id: dict[str, ToolEntry] = {}
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
        if page_token:
            entries, next_page_token = await asyncio.to_thread(self._list_entries, page_token)
        else:
            entries, complete = await asyncio.to_thread(self._list_entries_until, DIRECT_CATALOG_TOOL_LIMIT)
            next_page_token = ""
            if not complete or len(entries) > DIRECT_CATALOG_TOOL_LIMIT:
                return ListToolsResult(tools=_catalog_tools(), nextCursor=None)
        tools: list[Tool] = []
        for entry in entries:
            self._remember_entry(entry)
            tools.append(_mcp_tool(entry))
        return ListToolsResult(tools=tools, nextCursor=next_page_token or None)

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> CallToolResult:
        tool_name = str(name or "").strip()
        if tool_name == CATALOG_SEARCH_TOOL:
            return await asyncio.to_thread(self._search_catalog, arguments or {})
        if tool_name == CATALOG_EXECUTE_TOOL:
            return await self._execute_catalog(arguments or {})
        entry = self._entries.get(tool_name)
        if entry is None:
            entry = await asyncio.to_thread(self._find_entry, tool_name)
        return await self._execute_entry(entry, arguments or {})

    async def _execute_entry(self, entry: ToolEntry, arguments: dict[str, Any]) -> CallToolResult:
        async with self._execute_lock:
            self._sequence += 1
            tool_call_id = f"sdk-{self._sequence}"
            response = await asyncio.to_thread(
                _execute_tool,
                session_id=self._session_id,
                turn_id=self._turn_id,
                run_grant=self._run_grant,
                entry=entry,
                tool_call_id=tool_call_id,
                arguments=arguments,
            )
        body = str(getattr(response, "body", "") or "")
        status = int(getattr(response, "status", 0) or 0)
        if not body:
            body = "{}"
        return CallToolResult(content=[TextContent(type="text", text=body)], isError=status >= 400)

    def _remember_entry(self, entry: ToolEntry) -> None:
        self._entries[entry.mcp_name] = entry
        self._entries_by_id[entry.tool_id] = entry

    def _remember_entries(self, entries: list[ToolEntry]) -> None:
        for entry in entries:
            self._remember_entry(entry)

    def _list_entries_until(self, limit: int) -> tuple[list[ToolEntry], bool]:
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
            if not next_page_token:
                self._catalog_loaded = True
                self._all_entries = list(all_entries)
                return all_entries, True
            if len(all_entries) > limit:
                return all_entries, False
            page_token = next_page_token
        raise ValueError(f"ListTools exceeded {MAX_PAGES} pages")

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

    def _find_entry_by_id(self, tool_id: str) -> ToolEntry:
        if not tool_id:
            raise ValueError("tool_id is required")
        if entry := self._entries_by_id.get(tool_id):
            return entry
        for entry in self._list_all_entries():
            if entry.tool_id == tool_id:
                return entry
        raise ValueError(f"tool_id {tool_id!r} is not available in the current grant")

    def _search_catalog(self, arguments: dict[str, Any]) -> CallToolResult:
        query = str(arguments.get("query") or "").strip()
        plugin = str(arguments.get("plugin") or "").strip().lower()
        try:
            raw_limit = int(arguments.get("limit") or 5)
        except (TypeError, ValueError):
            raw_limit = 5
        limit = max(1, min(raw_limit, 20))
        entries = self._list_all_entries()
        scored = sorted(
            ((score, entry) for entry in entries if (score := _search_score(entry, query=query, plugin=plugin)) > 0),
            key=lambda item: (-item[0], item[1].mcp_name),
        )
        body = {
            "query": query,
            "tools": [_tool_search_result(entry) for _, entry in scored[:limit]],
        }
        return CallToolResult(content=[TextContent(type="text", text=json.dumps(body, separators=(",", ":")))])

    async def _execute_catalog(self, arguments: dict[str, Any]) -> CallToolResult:
        tool_id = str(arguments.get("tool_id") or "").strip()
        mcp_name = str(arguments.get("mcp_name") or "").strip()
        tool_arguments = arguments.get("arguments") or {}
        if not isinstance(tool_arguments, dict):
            raise ValueError("arguments must be an object")
        if tool_id:
            entry = await asyncio.to_thread(self._find_entry_by_id, tool_id)
        elif mcp_name:
            entry = await asyncio.to_thread(self._find_entry, mcp_name)
        else:
            raise ValueError("tool_id or mcp_name is required")
        return await self._execute_entry(entry, tool_arguments)

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


def _catalog_tools() -> list[Tool]:
    return [
        Tool(
            name=CATALOG_SEARCH_TOOL,
            description=(
                "Search the Gestalt MCP catalog for connected-app tools. Use this first when the user asks for "
                "Linear, GitHub, Slack, Gmail, Google Drive, Google Calendar, Google Docs, Google Sheets, BigQuery, "
                "Datadog, PagerDuty, Notion, Figma, Ramp, Ashby, or other Valon integration data/actions. The result "
                "includes tool_id, mcp_name, description, and input_schema values for the best matching tools."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Natural-language search query."},
                    "plugin": {"type": "string", "description": "Optional plugin hint such as github or linear."},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 20, "default": 5},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            annotations=ToolAnnotations(title="Search Gestalt MCP catalog", readOnlyHint=True),
        ),
        Tool(
            name=CATALOG_EXECUTE_TOOL,
            description=(
                "Execute a tool returned by gestalt_catalog_search. Pass either tool_id or mcp_name from the search "
                "result, plus arguments matching that tool's input_schema."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "tool_id": {"type": "string", "description": "Preferred tool_id from search results."},
                    "mcp_name": {"type": "string", "description": "Fallback mcp_name from search results."},
                    "arguments": {
                        "type": "object",
                        "description": "Arguments matching the selected tool's input_schema.",
                        "additionalProperties": True,
                    },
                },
                "required": ["arguments"],
                "anyOf": [{"required": ["tool_id"]}, {"required": ["mcp_name"]}],
                "additionalProperties": False,
            },
            annotations=ToolAnnotations(title="Execute Gestalt MCP catalog tool", openWorldHint=True),
        ),
    ]


def _tool_search_result(entry: ToolEntry) -> dict[str, Any]:
    return {
        "tool_id": entry.tool_id,
        "mcp_name": entry.mcp_name,
        "title": entry.title,
        "description": entry.description,
        "input_schema": entry.input_schema,
    }


def _search_score(entry: ToolEntry, *, query: str, plugin: str) -> int:
    name = entry.mcp_name.lower()
    title = entry.title.lower()
    description = entry.description.lower()
    score = 0
    if plugin:
        normalized_plugin = re.sub(r"[^a-z0-9]+", "_", plugin).strip("_")
        if name == normalized_plugin or name.startswith(f"{normalized_plugin}__"):
            score += 20
        else:
            return 0
    terms = _query_terms(query)
    if not terms:
        return score or 1
    for term in terms:
        if term in name:
            score += 8
        if term in title:
            score += 4
        if term in description:
            score += 2
    return score


def _query_terms(query: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for term in _SEARCH_TERM.findall(query.lower()):
        candidates = (term, *_SEARCH_ALIASES.get(term, ()))
        for candidate in candidates:
            if candidate in _SEARCH_STOP_TERMS or candidate in seen:
                continue
            seen.add(candidate)
            out.append(candidate)
    return out


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
