from __future__ import annotations

import asyncio
import os
import socket
import tempfile
import time
import unittest
from concurrent import futures
from typing import Any, cast

import grpc
from agents.mcp import MCPServerStreamableHttp
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from mcp import types as mcp_types

import provider as provider_module
from gestalt import ENV_AGENT_HOST_SOCKET, ENV_AGENT_HOST_SOCKET_TOKEN, ProviderKind, _runtime
from gestalt._gen.v1 import agent_pb2 as _agent_pb2
from gestalt._gen.v1 import agent_pb2_grpc as _agent_pb2_grpc
from gestalt._gen.v1 import runtime_pb2 as _runtime_pb2
from gestalt._gen.v1 import runtime_pb2_grpc as _runtime_pb2_grpc
from internals.codex_runner import normalize_codex_result
from internals.gestalt_mcp_bridge import BridgeContext
from internals.http_bridge import BridgeHTTPServer
from internals.tool_bridge import MAX_LISTED_TOOLS, ToolBridgeError, list_tools

agent_pb2: Any = cast(Any, _agent_pb2)
agent_pb2_grpc: Any = _agent_pb2_grpc
empty_pb2: Any = _empty_pb2
runtime_pb2: Any = _runtime_pb2
runtime_pb2_grpc: Any = _runtime_pb2_grpc
struct_pb2: Any = _struct_pb2

_runtime_server: grpc.Server | None = None
_host_server: grpc.Server | None = None
_runtime_socket = ""
_host_socket = ""
_host_servicer: "_FakeAgentHost | None" = None
_previous_agent_host_socket: str | None = None
_previous_agent_host_token: str | None = None
_previous_openai_api_key: str | None = None


class _FakeAgentHost(agent_pb2_grpc.AgentHostServicer):
    def __init__(self) -> None:
        self.mode = "normal"
        self.list_requests: list[dict[str, Any]] = []
        self.execute_requests: list[dict[str, Any]] = []

    def reset(self) -> None:
        self.mode = "normal"
        self.list_requests.clear()
        self.execute_requests.clear()

    def ListTools(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        self.list_requests.append(
            {
                "session_id": request.session_id,
                "turn_id": request.turn_id,
                "page_size": request.page_size,
                "page_token": request.page_token,
                "run_grant": request.run_grant,
            }
        )
        if self.mode == "list-slow":
            time.sleep(0.2)
        if self.mode == "duplicate":
            response = agent_pb2.ListAgentToolsResponse()
            _add_tool(response, tool_id="tool-1", mcp_name="duplicate")
            _add_tool(response, tool_id="tool-2", mcp_name="duplicate")
            return response
        if self.mode == "unsafe":
            response = agent_pb2.ListAgentToolsResponse()
            _add_tool(response, tool_id="tool-unsafe", mcp_name="bad tool")
            return response
        if self.mode == "repeat-token":
            response = agent_pb2.ListAgentToolsResponse()
            if request.page_token == "":
                response.next_page_token = "loop"
            elif request.page_token == "loop":
                response.next_page_token = "loop"
            return response
        if self.mode == "too-many":
            response = agent_pb2.ListAgentToolsResponse()
            for index in range(MAX_LISTED_TOOLS + 1):
                _add_tool(response, tool_id=f"tool-{index}", mcp_name=f"tool__{index}")
            return response

        response = agent_pb2.ListAgentToolsResponse()
        if request.page_token == "":
            _add_tool(
                response,
                tool_id="tool-linear-issues",
                mcp_name="linear__issues",
                title="Search Linear issues",
                description="Search Linear issues by text",
                input_schema='{"type":"object","properties":{"query":{"type":"string"}}}',
            )
            response.next_page_token = "page-2"
        elif request.page_token == "page-2":
            _add_tool(
                response,
                tool_id="tool-github-pulls",
                mcp_name="github__pulls_list",
                title="List GitHub pull requests",
                description="List pull requests from GitHub",
                input_schema='{"type":"object"}',
            )
        return response

    def ExecuteTool(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        self.execute_requests.append(
            {
                "session_id": request.session_id,
                "turn_id": request.turn_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id,
                "run_grant": request.run_grant,
                "idempotency_key": request.idempotency_key,
                "arguments": dict(request.arguments),
            }
        )
        if self.mode == "execute-slow":
            time.sleep(0.2)
        return agent_pb2.ExecuteAgentToolResponse(status=200, body='{"ok":true}')


class _FakeCodexMCPServer:
    mode = "success"
    result_style = "structured"
    instances: list["_FakeCodexMCPServer"] = []

    def __init__(self, *, params: dict[str, Any], name: str, client_session_timeout_seconds: float) -> None:
        self.params = params
        self.name = name
        self.client_session_timeout_seconds = client_session_timeout_seconds
        self.connected = False
        self.cleanup_count = 0
        self.called_tool = ""
        self.called_arguments: dict[str, Any] = {}
        self._cleanup_event: asyncio.Event | None = None
        self.instances.append(self)

    async def connect(self) -> None:
        self.connected = True
        self._cleanup_event = asyncio.Event()

    async def list_tools(self) -> list[Any]:
        return [
            mcp_types.Tool(name="codex", description="Run a Codex session", inputSchema={"type": "object"}),
            mcp_types.Tool(name="codex-reply", description="Continue a Codex session", inputSchema={"type": "object"}),
        ]

    async def call_tool(
        self, tool_name: str, arguments: dict[str, Any] | None, meta: dict[str, Any] | None = None
    ) -> Any:
        del meta
        self.called_tool = tool_name
        self.called_arguments = dict(arguments or {})
        if self.mode == "cancel":
            assert self._cleanup_event is not None
            await self._cleanup_event.wait()
            return _structured_result("canceled after cleanup")
        if self.mode == "hang":
            await asyncio.Event().wait()
        if self.mode == "failure":
            raise RuntimeError("boom")
        if self.result_style == "content":
            return mcp_types.CallToolResult(
                content=[
                    mcp_types.TextContent(type="text", text="Codex text part 1"),
                    mcp_types.TextContent(type="text", text="Codex text part 2"),
                ]
            )
        return _structured_result("Codex completed")

    async def cleanup(self) -> None:
        self.cleanup_count += 1
        if self._cleanup_event is not None:
            self._cleanup_event.set()


class CodexProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        assert _host_servicer is not None
        _host_servicer.reset()
        _FakeCodexMCPServer.mode = "success"
        _FakeCodexMCPServer.result_style = "structured"
        _FakeCodexMCPServer.instances.clear()

    def test_provider_completes_turn_through_codex_mcp_with_catalog_tools(self) -> None:
        host = _host_servicer
        assert host is not None
        lifecycle, provider_client = _configure_provider()
        capabilities = provider_client.GetCapabilities(agent_pb2.GetAgentProviderCapabilitiesRequest())
        self.assertFalse(capabilities.streaming_text)
        self.assertTrue(capabilities.tool_calls)
        self.assertFalse(capabilities.parallel_tool_calls)
        self.assertFalse(capabilities.structured_output)
        self.assertFalse(capabilities.interactions)
        self.assertFalse(capabilities.resumable_turns)
        self.assertTrue(capabilities.bounded_list_hydration)
        self.assertEqual(list(capabilities.supported_tool_sources), [agent_pb2.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG])
        self.assertEqual(lifecycle.GetProviderIdentity(empty_pb2.Empty()).name, "codex")

        created = provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(
                session_id="session-codex", created_by=agent_pb2.AgentActor(subject_id="user-123", subject_kind="human")
            )
        )
        self.assertEqual(created.model, "")
        started = provider_client.CreateTurn(
            _turn_request(
                turn_id="turn-codex",
                session_id="session-codex",
                messages=[agent_pb2.AgentMessage(role="user", text="List my Linear issues")],
                execution_ref="exec-codex",
            )
        )
        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)

        fetched = _wait_for_turn(provider_client, "turn-codex", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.output_text, "Codex completed")

        self.assertEqual(len(_FakeCodexMCPServer.instances), 1)
        fake_server = _FakeCodexMCPServer.instances[0]
        self.assertTrue(fake_server.connected)
        self.assertGreaterEqual(fake_server.cleanup_count, 1)
        self.assertEqual(fake_server.called_tool, "codex")
        self.assertNotIn("model", fake_server.called_arguments)
        self.assertIn("List my Linear issues", fake_server.called_arguments["prompt"])
        self.assertIn('<message 1 role="user">', fake_server.called_arguments["prompt"])
        self.assertEqual(fake_server.called_arguments["approval-policy"], "never")
        self.assertEqual(fake_server.called_arguments["sandbox"], "read-only")
        self.assertFalse(fake_server.called_arguments["include-plan-tool"])

        config = fake_server.called_arguments["config"]
        self.assertEqual(config["approval_policy"], "never")
        self.assertEqual(config["sandbox_mode"], "read-only")
        self.assertEqual(config["web_search"], "disabled")
        self.assertEqual(config["history"]["persistence"], "none")
        self.assertFalse(config["memories"]["generate_memories"])
        self.assertFalse(config["memories"]["use_memories"])
        self.assertFalse(config["features"]["apps"])
        self.assertFalse(config["features"]["multi_agent"])
        self.assertFalse(config["features"]["codex_hooks"])
        self.assertEqual(config["skills"]["config"], [])
        self.assertEqual(config["shell_environment_policy"]["inherit"], "core")
        self.assertIn("OPENAI_API_KEY", config["shell_environment_policy"]["exclude"])
        self.assertIn("GESTALT_*", config["shell_environment_policy"]["exclude"])

        bridge = config["mcp_servers"]["gestalt"]
        self.assertTrue(bridge["url"].startswith("http://127.0.0.1:"), bridge)
        self.assertEqual(bridge["enabled_tools"], ["linear__issues", "github__pulls_list"])
        self.assertEqual(bridge["startup_timeout_sec"], 5)
        self.assertEqual(bridge["tool_timeout_sec"], 5)
        self.assertTrue(bridge["required"])
        self.assertNotIn("command", bridge)
        self.assertNotIn("args", bridge)
        self.assertNotIn("env", bridge)
        self.assertNotIn(_host_socket, repr(fake_server.called_arguments))
        self.assertNotIn("relay-token", repr(fake_server.called_arguments))
        self.assertNotIn("grant-codex", repr(fake_server.called_arguments))

        self.assertEqual(fake_server.params["command"], "codex")
        self.assertEqual(fake_server.params["args"], ["mcp-server"])
        self.assertIn("CODEX_HOME", fake_server.params["env"])
        self.assertEqual(fake_server.params["env"]["OPENAI_API_KEY"], "test-openai-key")
        self.assertNotIn("test-openai-key", repr(fake_server.called_arguments))

        self.assertEqual([request["page_token"] for request in host.list_requests], ["", "page-2"])
        self.assertEqual(host.list_requests[0]["run_grant"], "grant-codex")

    def test_enabled_tools_come_from_list_tools_not_tool_refs(self) -> None:
        _, provider_client = _configure_provider()
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-grant"))
        request = _turn_request(turn_id="turn-grant", session_id="session-grant")
        del request.tool_refs[:]
        ref = request.tool_refs.add()
        ref.plugin = "notion"
        ref.operation = "search"
        provider_client.CreateTurn(request)

        _wait_for_turn(provider_client, "turn-grant", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        config = _FakeCodexMCPServer.instances[0].called_arguments["config"]
        self.assertEqual(config["mcp_servers"]["gestalt"]["enabled_tools"], ["linear__issues", "github__pulls_list"])

    def test_session_and_turn_models_are_passed_only_when_set(self) -> None:
        _, provider_client = _configure_provider()
        provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(session_id="session-model", model="gpt-session")
        )
        provider_client.CreateTurn(_turn_request(turn_id="turn-session-model", session_id="session-model"))
        _wait_for_turn(provider_client, "turn-session-model", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(_FakeCodexMCPServer.instances[-1].called_arguments["model"], "gpt-session")

        provider_client.CreateTurn(
            _turn_request(turn_id="turn-request-model", session_id="session-model", model="gpt-turn")
        )
        _wait_for_turn(provider_client, "turn-request-model", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(_FakeCodexMCPServer.instances[-1].called_arguments["model"], "gpt-turn")

    def test_content_fallback_result_is_returned(self) -> None:
        _FakeCodexMCPServer.result_style = "content"
        _, provider_client = _configure_provider()
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-content"))
        provider_client.CreateTurn(_turn_request(turn_id="turn-content", session_id="session-content"))
        fetched = _wait_for_turn(provider_client, "turn-content", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.output_text, "Codex text part 1\nCodex text part 2")

    def test_create_turn_rejects_unsupported_tool_contract_inputs(self) -> None:
        _, provider_client = _configure_provider()
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-validation"))

        bad_source = _turn_request(turn_id="turn-bad-source", session_id="session-validation")
        bad_source.tool_source = 999
        _assert_invalid(provider_client, bad_source, "requires toolSource mcp_catalog")

        missing_grant = _turn_request(turn_id="turn-missing-grant", session_id="session-validation", run_grant="")
        _assert_invalid(provider_client, missing_grant, "run_grant is required")

        missing_refs = _turn_request(turn_id="turn-missing-refs", session_id="session-validation")
        del missing_refs.tool_refs[:]
        _assert_invalid(provider_client, missing_refs, "tool_refs are required")

        wildcard_ref = _turn_request(turn_id="turn-wildcard", session_id="session-validation")
        wildcard_ref.tool_refs[0].plugin = "*"
        _assert_invalid(provider_client, wildcard_ref, "wildcard tool_refs are not supported")

        response_schema = struct_pb2.Struct()
        response_schema.update({"type": "object"})
        bad_schema = _turn_request(
            turn_id="turn-response-schema", session_id="session-validation", response_schema=response_schema
        )
        _assert_invalid(provider_client, bad_schema, "response_schema is not supported")

        model_options = struct_pb2.Struct()
        model_options.update({"temperature": 0.2})
        bad_options = _turn_request(
            turn_id="turn-provider-options", session_id="session-validation", model_options=model_options
        )
        _assert_invalid(provider_client, bad_options, "model_options are not supported")

        resolved_tools = _turn_request(turn_id="turn-resolved-tools", session_id="session-validation")
        resolved_tools.tools.add(id="resolved-tool", name="legacy", description="legacy")
        _assert_invalid(provider_client, resolved_tools, "resolved tools are not supported")

    def test_configure_rejects_interactive_approval_policy(self) -> None:
        channel = grpc.insecure_channel(f"unix:{_runtime_socket}")
        lifecycle = runtime_pb2_grpc.ProviderLifecycleStub(channel)
        request = runtime_pb2.ConfigureProviderRequest(name="codex", protocol_version=_runtime.CURRENT_PROTOCOL_VERSION)
        request.config.update({"approvalPolicy": "on-request"})
        with self.assertRaises(grpc.RpcError) as raised:
            lifecycle.ConfigureProvider(request)
        self.assertIn("approvalPolicy must be never", cast(Any, raised.exception).details())

    def test_cancel_turn_cleans_up_mcp_server_and_terminal_status_wins(self) -> None:
        _FakeCodexMCPServer.mode = "cancel"
        _, provider_client = _configure_provider()
        runner = provider_module.provider._runner
        assert runner is not None
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-cancel"))
        provider_client.CreateTurn(_turn_request(turn_id="turn-cancel", session_id="session-cancel"))
        fake_server = _wait_for_fake_server()

        canceled = provider_client.CancelTurn(
            agent_pb2.CancelAgentProviderTurnRequest(turn_id="turn-cancel", reason="test cancellation")
        )
        self.assertEqual(canceled.status, agent_pb2.AGENT_EXECUTION_STATUS_CANCELED)
        fetched = _wait_for_turn(provider_client, "turn-cancel", agent_pb2.AGENT_EXECUTION_STATUS_CANCELED)
        self.assertEqual(fetched.status_message, "test cancellation")
        self.assertGreaterEqual(fake_server.cleanup_count, 1)

        time.sleep(0.1)
        fetched_again = provider_client.GetTurn(agent_pb2.GetAgentProviderTurnRequest(turn_id="turn-cancel"))
        self.assertEqual(fetched_again.status, agent_pb2.AGENT_EXECUTION_STATUS_CANCELED)
        self.assertEqual(runner._canceled_turns, set())

    def test_mcp_failure_marks_turn_failed_and_cleans_up(self) -> None:
        _FakeCodexMCPServer.mode = "failure"
        _, provider_client = _configure_provider()
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-failure"))
        provider_client.CreateTurn(_turn_request(turn_id="turn-failure", session_id="session-failure"))

        failed = _wait_for_turn(provider_client, "turn-failure", agent_pb2.AGENT_EXECUTION_STATUS_FAILED)
        self.assertIn("boom", failed.status_message)
        self.assertGreaterEqual(_FakeCodexMCPServer.instances[0].cleanup_count, 1)

    def test_timeout_marks_turn_failed_and_cleans_up(self) -> None:
        _FakeCodexMCPServer.mode = "hang"
        _, provider_client = _configure_provider(timeout_seconds=0.1)
        runner = provider_module.provider._runner
        assert runner is not None
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-timeout"))
        provider_client.CreateTurn(_turn_request(turn_id="turn-timeout", session_id="session-timeout"))

        failed = _wait_for_turn(provider_client, "turn-timeout", agent_pb2.AGENT_EXECUTION_STATUS_FAILED)
        self.assertIn("timed out", failed.status_message)
        self.assertGreaterEqual(_FakeCodexMCPServer.instances[0].cleanup_count, 1)
        self.assertEqual(runner._canceled_turns, set())

    def test_list_tools_contract_errors_cross_grpc_agent_host(self) -> None:
        host = _host_servicer
        assert host is not None
        for mode, message in (
            ("duplicate", "duplicate mcp_name"),
            ("unsafe", "unsafe mcp_name"),
            ("repeat-token", "repeated page token"),
            ("too-many", "more than"),
        ):
            with self.subTest(mode=mode):
                host.reset()
                host.mode = mode
                with self.assertRaisesRegex(ToolBridgeError, message):
                    list_tools(session_id="session-list", turn_id="turn-list", run_grant="grant-list")

    def test_list_tools_uses_agent_host_grpc_deadline(self) -> None:
        host = _host_servicer
        assert host is not None
        host.mode = "list-slow"

        started_at = time.monotonic()
        with self.assertRaisesRegex(ToolBridgeError, "DEADLINE_EXCEEDED"):
            list_tools(
                session_id="session-list-deadline",
                turn_id="turn-list-deadline",
                run_grant="grant-list-deadline",
                timeout_seconds=0.05,
            )
        self.assertLess(time.monotonic() - started_at, 0.5)

    def test_bridge_http_server_lists_and_executes_tools(self) -> None:
        host = _host_servicer
        assert host is not None
        asyncio.run(_exercise_bridge_http_server())

        self.assertEqual([request["page_token"] for request in host.list_requests], ["", "page-2"])
        self.assertEqual(host.execute_requests[0]["tool_call_id"], "mcp-1")
        self.assertEqual(host.execute_requests[0]["tool_id"], "tool-linear-issues")
        self.assertEqual(host.execute_requests[0]["run_grant"], "grant-bridge")
        self.assertEqual(host.execute_requests[0]["idempotency_key"], "agent/codex-mcp:turn-bridge:1:linear__issues")
        self.assertEqual(host.execute_requests[0]["arguments"], {"query": "AIT"})

    def test_bridge_execute_tool_uses_agent_host_grpc_deadline(self) -> None:
        host = _host_servicer
        assert host is not None
        host.mode = "execute-slow"

        asyncio.run(_exercise_bridge_execute_deadline())

    def test_normalize_codex_result_prefers_structured_content(self) -> None:
        result = mcp_types.CallToolResult(
            content=[mcp_types.TextContent(type="text", text="fallback")],
            structuredContent={"threadId": "thread-123", "content": "structured content"},
        )
        self.assertEqual(normalize_codex_result(result), "structured content")


def setUpModule() -> None:
    global _runtime_server, _host_server, _runtime_socket, _host_socket, _host_servicer
    global _previous_agent_host_socket, _previous_agent_host_token, _previous_openai_api_key

    _runtime_socket = _fresh_socket("codex-mcp-agent-runtime")
    _host_socket = _fresh_socket("codex-mcp-agent-host")
    _previous_agent_host_socket = os.environ.get(ENV_AGENT_HOST_SOCKET)
    _previous_agent_host_token = os.environ.get(ENV_AGENT_HOST_SOCKET_TOKEN)
    _previous_openai_api_key = os.environ.get("OPENAI_API_KEY")
    os.environ[ENV_AGENT_HOST_SOCKET] = _host_socket
    os.environ[ENV_AGENT_HOST_SOCKET_TOKEN] = "relay-token"
    os.environ.pop("OPENAI_API_KEY", None)

    _host_servicer = _FakeAgentHost()
    _host_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    agent_pb2_grpc.add_AgentHostServicer_to_server(_host_servicer, _host_server)
    _host_server.add_insecure_port(f"unix:{_host_socket}")
    _host_server.start()

    _runtime_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    adapter = _runtime._servable_target(provider_module.provider, runtime_kind=ProviderKind.AGENT)
    _runtime._register_services(server=_runtime_server, servable=adapter)
    _runtime_server.add_insecure_port(f"unix:{_runtime_socket}")
    _runtime_server.start()


def tearDownModule() -> None:
    if provider_module.provider is not None:
        provider_module.provider.close()
    for server in (_runtime_server, _host_server):
        if server is not None:
            server.stop(0)
    for path in (_runtime_socket, _host_socket):
        try:
            os.unlink(path)
        except OSError:
            pass
    _restore_env(ENV_AGENT_HOST_SOCKET, _previous_agent_host_socket)
    _restore_env(ENV_AGENT_HOST_SOCKET_TOKEN, _previous_agent_host_token)
    _restore_env("OPENAI_API_KEY", _previous_openai_api_key)


def _configure_provider(*, timeout_seconds: float = 5, default_model: str = "") -> tuple[Any, Any]:
    channel = grpc.insecure_channel(f"unix:{_runtime_socket}")
    lifecycle = runtime_pb2_grpc.ProviderLifecycleStub(channel)
    provider_client = agent_pb2_grpc.AgentProviderStub(channel)
    request = runtime_pb2.ConfigureProviderRequest(name="codex", protocol_version=_runtime.CURRENT_PROTOCOL_VERSION)
    request.config.update(
        {
            "timeoutSeconds": timeout_seconds,
            "approvalPolicy": "never",
            "sandbox": "read-only",
            "openaiApiKey": "test-openai-key",
        }
    )
    if default_model:
        request.config["defaultModel"] = default_model
    lifecycle.ConfigureProvider(request)
    assert provider_module.provider._runner is not None
    provider_module.provider._runner._server_factory = _FakeCodexMCPServer
    return lifecycle, provider_client


def _turn_request(
    *,
    turn_id: str,
    session_id: str,
    model: str = "",
    messages: list[Any] | None = None,
    run_grant: str = "grant-codex",
    execution_ref: str = "",
    response_schema: Any | None = None,
    model_options: Any | None = None,
) -> Any:
    request = agent_pb2.CreateAgentProviderTurnRequest(
        turn_id=turn_id,
        session_id=session_id,
        model=model,
        messages=messages or [agent_pb2.AgentMessage(role="user", text="List my Linear issues")],
        tool_source=agent_pb2.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG,
        run_grant=run_grant,
        execution_ref=execution_ref,
        created_by=agent_pb2.AgentActor(subject_id="user-123", subject_kind="human"),
    )
    linear = request.tool_refs.add()
    linear.plugin = "linear"
    linear.operation = "searchIssues"
    github = request.tool_refs.add()
    github.plugin = "github"
    github.operation = "pulls/list"
    if response_schema is not None:
        request.response_schema.CopyFrom(response_schema)
    if model_options is not None:
        request.model_options.CopyFrom(model_options)
    return request


async def _exercise_bridge_http_server() -> None:
    bridge_server = BridgeHTTPServer(
        BridgeContext(session_id="session-bridge", turn_id="turn-bridge", run_grant="grant-bridge")
    )
    bridge_server.start()
    bridge = MCPServerStreamableHttp(
        name="Gestalt Bridge", params={"url": bridge_server.url}, client_session_timeout_seconds=5
    )
    try:
        await bridge.connect()
        tools = await bridge.list_tools()
        self_names = [tool.name for tool in tools]
        assert self_names == ["linear__issues", "github__pulls_list"], self_names
        result = await bridge.call_tool("linear__issues", {"query": "AIT"})
        first_content = result.content[0]
        assert isinstance(first_content, mcp_types.TextContent), result
        assert first_content.text == '{"ok":true}', result
        assert not result.isError
    finally:
        await bridge.cleanup()
        bridge_server.stop()


async def _exercise_bridge_execute_deadline() -> None:
    bridge_server = BridgeHTTPServer(
        BridgeContext(
            session_id="session-bridge-deadline",
            turn_id="turn-bridge-deadline",
            run_grant="grant-bridge-deadline",
            timeout_seconds=0.05,
        )
    )
    bridge_server.start()
    bridge = MCPServerStreamableHttp(
        name="Gestalt Bridge", params={"url": bridge_server.url}, client_session_timeout_seconds=5
    )
    try:
        await bridge.connect()
        tools = await bridge.list_tools()
        self_names = [tool.name for tool in tools]
        assert self_names == ["linear__issues", "github__pulls_list"], self_names
        result = await bridge.call_tool("linear__issues", {"query": "AIT"})
        first_content = result.content[0]
        assert isinstance(first_content, mcp_types.TextContent), result
        assert result.isError
        assert "DEADLINE_EXCEEDED" in first_content.text, result
    finally:
        await bridge.cleanup()
        bridge_server.stop()


def _structured_result(content: str) -> mcp_types.CallToolResult:
    return mcp_types.CallToolResult(
        content=[mcp_types.TextContent(type="text", text="fallback")],
        structuredContent={"threadId": "thread-123", "content": content},
    )


def _add_tool(
    response: Any,
    *,
    tool_id: str,
    mcp_name: str,
    title: str = "Tool",
    description: str = "Tool description",
    input_schema: str = '{"type":"object"}',
) -> None:
    tool = response.tools.add()
    tool.id = tool_id
    tool.mcp_name = mcp_name
    tool.title = title
    tool.description = description
    tool.input_schema = input_schema
    setattr(tool.annotations, "read_only_hint", True)
    setattr(tool.ref, "plugin", "linear")
    setattr(tool.ref, "operation", "searchIssues")


def _assert_invalid(provider_client: Any, request: Any, message: str) -> None:
    try:
        provider_client.CreateTurn(request)
    except grpc.RpcError as exc:
        error = cast(Any, exc)
    else:
        raise AssertionError("CreateTurn unexpectedly succeeded")
    assert error.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert message in error.details()


def _wait_for_turn(provider_client: Any, turn_id: str, status: int) -> Any:
    deadline = time.time() + 5
    while time.time() < deadline:
        turn = provider_client.GetTurn(agent_pb2.GetAgentProviderTurnRequest(turn_id=turn_id))
        if turn.status == status:
            return turn
        if status != agent_pb2.AGENT_EXECUTION_STATUS_FAILED and turn.status == agent_pb2.AGENT_EXECUTION_STATUS_FAILED:
            raise AssertionError(f"turn failed: {turn.status_message}")
        time.sleep(0.05)
    raise AssertionError(f"turn {turn_id} did not reach status {status}")


def _wait_for_fake_server() -> _FakeCodexMCPServer:
    deadline = time.time() + 5
    while time.time() < deadline:
        if _FakeCodexMCPServer.instances:
            return _FakeCodexMCPServer.instances[0]
        time.sleep(0.05)
    raise AssertionError("fake Codex MCP server was not created")


def _restore_env(key: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(key, None)
    else:
        os.environ[key] = value


def _fresh_socket(prefix: str) -> str:
    root = tempfile.mkdtemp(prefix=prefix)
    path = os.path.join(root, "server.sock")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
        try:
            probe.bind(path)
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
    return path
