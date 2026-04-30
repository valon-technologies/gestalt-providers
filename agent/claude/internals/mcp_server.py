from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Any, TextIO, cast

import gestalt
from google.protobuf import struct_pb2 as _struct_pb2

from gestalt.gen.v1 import agent_pb2 as _agent_pb2

agent_pb2: Any = cast(Any, _agent_pb2)
struct_pb2: Any = _struct_pb2

JSONRPC_VERSION = "2.0"
DEFAULT_PROTOCOL_VERSION = "2025-03-26"
DEFAULT_PAGE_SIZE = 100


@dataclass(slots=True)
class ToolEntry:
    tool_id: str
    name: str
    title: str
    description: str
    input_schema: dict[str, Any]
    output_schema: dict[str, Any]
    annotations: dict[str, Any]


class GestaltMCPServer:
    def __init__(self) -> None:
        self._session_id = os.environ.get("GESTALT_CLAUDE_SESSION_ID", "").strip()
        self._turn_id = os.environ.get("GESTALT_CLAUDE_TURN_ID", "").strip()
        self._tool_grant = os.environ.get("GESTALT_CLAUDE_TOOL_GRANT", "").strip()
        self._tools_by_name: dict[str, ToolEntry] = {}
        self._tools_loaded = False

    def serve(self, *, stdin: TextIO = sys.stdin, stdout: TextIO = sys.stdout) -> None:
        for line in stdin:
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
                response = self._handle_message(message)
            except Exception as exc:
                print(f"gestalt mcp server error: {exc}", file=sys.stderr)
                response = _error_response(None, -32603, str(exc))
            if response is None:
                continue
            stdout.write(json.dumps(response, separators=(",", ":")) + "\n")
            stdout.flush()

    def _handle_message(self, message: Any) -> Any:
        if isinstance(message, list):
            responses = [response for item in message if (response := self._handle_single(item)) is not None]
            return responses if responses else None
        return self._handle_single(message)

    def _handle_single(self, message: Any) -> dict[str, Any] | None:
        if not isinstance(message, dict):
            return _error_response(None, -32600, "request must be an object")
        request_id = message.get("id")
        method = str(message.get("method") or "").strip()
        params = message.get("params") if isinstance(message.get("params"), dict) else {}
        if not method:
            return _error_response(request_id, -32600, "method is required")
        if method.startswith("notifications/"):
            return None
        try:
            if method == "initialize":
                return _success_response(request_id, self._initialize_result(params))
            if method == "ping":
                return _success_response(request_id, {})
            if method == "tools/list":
                return _success_response(request_id, {"tools": self._list_mcp_tools()})
            if method == "tools/call":
                return _success_response(request_id, self._call_tool(params, request_id=request_id))
            return _error_response(request_id, -32601, f"unsupported method {method!r}")
        except Exception as exc:
            return _error_response(request_id, -32603, str(exc))

    def _initialize_result(self, params: dict[str, Any]) -> dict[str, Any]:
        protocol_version = str(params.get("protocolVersion") or DEFAULT_PROTOCOL_VERSION)
        return {
            "protocolVersion": protocol_version,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "gestalt", "version": "0.0.1"},
        }

    def _list_mcp_tools(self) -> list[dict[str, Any]]:
        self._load_tools()
        out = []
        for tool in self._tools_by_name.values():
            item: dict[str, Any] = {
                "name": tool.name,
                "description": tool.description or tool.title,
                "inputSchema": tool.input_schema,
            }
            if tool.annotations:
                item["annotations"] = tool.annotations
            out.append(item)
        return out

    def _call_tool(self, params: dict[str, Any], *, request_id: Any) -> dict[str, Any]:
        self._load_tools()
        name = str(params.get("name") or "").strip()
        if not name:
            raise ValueError("tools/call params.name is required")
        tool = self._tools_by_name.get(name)
        if tool is None:
            raise ValueError(f"tool {name!r} is not available")
        raw_arguments = params.get("arguments")
        if raw_arguments is None:
            raw_arguments = {}
        if not isinstance(raw_arguments, dict):
            raise ValueError("tools/call params.arguments must be an object")

        arguments = struct_pb2.Struct()
        arguments.update(raw_arguments)
        tool_call_id = f"mcp-{_request_id_token(request_id)}"
        with gestalt.AgentHost() as host:
            response = host.execute_tool(
                agent_pb2.ExecuteAgentToolRequest(
                    session_id=self._session_id,
                    turn_id=self._turn_id,
                    tool_call_id=tool_call_id,
                    tool_id=tool.tool_id,
                    arguments=arguments,
                    tool_grant=self._tool_grant,
                    idempotency_key=f"agent/claude:{self._turn_id}:{tool_call_id}",
                )
            )
        body = str(getattr(response, "body", "") or "")
        status = int(getattr(response, "status", 0) or 0)
        if not body:
            body = "{}"
        return {"content": [{"type": "text", "text": body}], "isError": status >= 400}

    def _load_tools(self) -> None:
        if self._tools_loaded:
            return
        page_token = ""
        tools: dict[str, ToolEntry] = {}
        with gestalt.AgentHost() as host:
            while True:
                response = host.list_tools(
                    agent_pb2.ListAgentToolsRequest(
                        session_id=self._session_id,
                        turn_id=self._turn_id,
                        page_size=_page_size(),
                        page_token=page_token,
                        tool_grant=self._tool_grant,
                    )
                )
                for listed in response.tools:
                    entry = _tool_entry(listed)
                    tools[entry.name] = entry
                page_token = str(response.next_page_token or "").strip()
                if not page_token:
                    break
        self._tools_by_name = tools
        self._tools_loaded = True


def _tool_entry(tool: Any) -> ToolEntry:
    return ToolEntry(
        tool_id=str(tool.id or "").strip(),
        name=str(tool.mcp_name or "").strip(),
        title=str(tool.title or "").strip(),
        description=str(tool.description or "").strip(),
        input_schema=_schema_from_json(str(tool.input_schema or "")),
        output_schema=_schema_from_json(str(tool.output_schema or "")),
        annotations=_annotations_to_dict(tool.annotations),
    )


def _schema_from_json(value: str) -> dict[str, Any]:
    value = value.strip()
    if not value:
        return {"type": "object", "additionalProperties": True}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {"type": "object", "additionalProperties": True}
    return payload if isinstance(payload, dict) else {"type": "object", "additionalProperties": True}


def _annotations_to_dict(annotations: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    fields = (
        ("read_only_hint", "readOnlyHint"),
        ("idempotent_hint", "idempotentHint"),
        ("destructive_hint", "destructiveHint"),
        ("open_world_hint", "openWorldHint"),
    )
    for proto_name, json_name in fields:
        try:
            has_field = annotations.HasField(proto_name)
        except ValueError:
            has_field = False
        if has_field:
            out[json_name] = bool(getattr(annotations, proto_name))
    return out


def _request_id_token(request_id: Any) -> str:
    if isinstance(request_id, str | int | float):
        return str(request_id)
    if request_id is None:
        return "none"
    return json.dumps(request_id, sort_keys=True, separators=(",", ":"))


def _page_size() -> int:
    try:
        value = int(os.environ.get("GESTALT_CLAUDE_LIST_PAGE_SIZE", ""))
    except ValueError:
        return DEFAULT_PAGE_SIZE
    return value if value > 0 else DEFAULT_PAGE_SIZE


def _success_response(request_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": JSONRPC_VERSION, "id": request_id, "result": result}


def _error_response(request_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": JSONRPC_VERSION, "id": request_id, "error": {"code": code, "message": message}}


def main() -> int:
    GestaltMCPServer().serve()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
