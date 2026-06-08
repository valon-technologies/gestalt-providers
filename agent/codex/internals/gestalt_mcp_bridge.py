from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from mcp import types as mcp_types
from mcp.server import Server

from .tool_bridge import DEFAULT_HOST_RPC_TIMEOUT_SECONDS, ToolBridgeError, ToolEntry, ToolExecutor
from .tool_bridge import list_tools, mcp_tool, mcp_tool_result


@dataclass(frozen=True, slots=True)
class BridgeContext:
    turn_id: str
    request_context: Any
    listed_tools: list[Any]
    timeout_seconds: float = DEFAULT_HOST_RPC_TIMEOUT_SECONDS


def create_server(context: BridgeContext) -> Server[Any, Any]:
    server: Server[Any, Any] = Server("gestalt")
    entries_by_name: dict[str, ToolEntry] = {}
    executor = ToolExecutor(
        turn_id=context.turn_id,
        request_context=context.request_context,
        timeout_seconds=context.timeout_seconds,
    )

    @server.list_tools()
    async def handle_list_tools() -> list[mcp_types.Tool]:
        entries = await asyncio.to_thread(
            list_tools,
            listed_tools=context.listed_tools,
        )
        entries_by_name.clear()
        entries_by_name.update({entry.mcp_name: entry for entry in entries})
        return [mcp_tool(entry) for entry in entries]

    @server.call_tool()
    async def handle_call_tool(name: str, arguments: dict[str, Any]) -> mcp_types.CallToolResult:
        entry = entries_by_name.get(name)
        if entry is None:
            try:
                for listed in await handle_list_tools():
                    del listed
            except ToolBridgeError as exc:
                return _error_result(str(exc))
            entry = entries_by_name.get(name)
        if entry is None:
            return _error_result(f"unknown tool {name!r}")
        try:
            response = await asyncio.to_thread(executor.execute, entry=entry, arguments=arguments)
        except ToolBridgeError as exc:
            return _error_result(str(exc))
        return mcp_tool_result(response)

    return server


def _error_result(message: str) -> mcp_types.CallToolResult:
    return mcp_types.CallToolResult(content=[mcp_types.TextContent(type="text", text=message)], isError=True)
