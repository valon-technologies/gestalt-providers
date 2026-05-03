from __future__ import annotations

import asyncio
import os
import socket
import tempfile
import time
import types as py_types
import unittest
from concurrent import futures
from typing import Any, cast

import grpc
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from mcp import types as mcp_types

import provider as provider_module
from gestalt import ENV_AGENT_HOST_SOCKET, ENV_AGENT_HOST_SOCKET_TOKEN, ProviderKind, _runtime
from gestalt._gen.v1 import agent_pb2 as _agent_pb2
from gestalt._gen.v1 import agent_pb2_grpc as _agent_pb2_grpc
from gestalt._gen.v1 import runtime_pb2 as _runtime_pb2
from gestalt._gen.v1 import runtime_pb2_grpc as _runtime_pb2_grpc

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


class _FakeAgentHost(agent_pb2_grpc.AgentHostServicer):
    def __init__(self) -> None:
        self.list_requests: list[dict[str, Any]] = []
        self.execute_requests: list[dict[str, Any]] = []

    def reset(self) -> None:
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
        response = agent_pb2.ListAgentToolsResponse()
        if request.page_token == "":
            tool = response.tools.add()
            tool.id = "tool-ashby-candidates"
            tool.mcp_name = "ashby__candidate_list"
            tool.title = "List Ashby candidates"
            tool.description = "List Ashby candidates"
            tool.input_schema = '{"type":"object"}'
            setattr(tool.annotations, "read_only_hint", True)
            setattr(tool.ref, "plugin", "ashby")
            setattr(tool.ref, "operation", "candidate.list")
            response.next_page_token = "page-2"
        elif request.page_token == "page-2":
            tool = response.tools.add()
            tool.id = "tool-linear-issues"
            tool.mcp_name = "linear__issues"
            tool.title = "Search Linear issues"
            tool.description = "Search Linear issues by text"
            tool.input_schema = '{"type":"object","properties":{"query":{"type":"string"}}}'
            setattr(tool.annotations, "read_only_hint", True)
            setattr(tool.ref, "plugin", "linear")
            setattr(tool.ref, "operation", "searchIssues")
            tool = response.tools.add()
            tool.id = "tool-github-pulls"
            tool.mcp_name = "github__pulls_list"
            tool.title = "List GitHub pull requests"
            tool.description = "List pull requests from GitHub"
            tool.input_schema = '{"type":"object"}'
            setattr(tool.ref, "plugin", "github")
            setattr(tool.ref, "operation", "pulls/list")
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
        return agent_pb2.ExecuteAgentToolResponse(status=200, body='{"ok":true}')


class _FakeClaudeSDKClient:
    mode = "success"
    instances: list["_FakeClaudeSDKClient"] = []

    def __init__(self, *, options: Any) -> None:
        self.options = options
        self.prompt = ""
        self.session_id = ""
        self.connected = False
        self.disconnected = False
        self.interrupted = False
        self.tool_result: Any | None = None
        self._interrupt_event: asyncio.Event | None = None
        self.instances.append(self)

    async def connect(self) -> None:
        self.connected = True
        self._interrupt_event = asyncio.Event()

    async def query(self, prompt: str, session_id: str = "default") -> None:
        self.prompt = prompt
        self.session_id = session_id

    async def receive_response(self) -> Any:
        if self.mode == "cancel":
            assert self._interrupt_event is not None
            await self._interrupt_event.wait()
            yield ResultMessage(
                subtype="interrupted",
                duration_ms=0,
                duration_api_ms=0,
                is_error=True,
                num_turns=1,
                session_id=self.session_id,
                result="interrupted",
            )
            return
        if self.mode == "failure":
            yield ResultMessage(
                subtype="error",
                duration_ms=0,
                duration_api_ms=0,
                is_error=True,
                num_turns=1,
                session_id=self.session_id,
                result="boom",
            )
            return

        visible_tools = await _visible_sdk_tools(self.options)
        assert visible_tools == ["ashby__candidate_list", "linear__issues", "github__pulls_list"], visible_tools
        self.tool_result = await _call_sdk_tool(self.options, name="linear__issues", arguments={"query": "AIT"})
        yield AssistantMessage(content=[TextBlock(text="assistant intermediate text")], model="fake-claude")
        yield ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id=self.session_id,
            result="Claude completed",
        )

    async def interrupt(self) -> None:
        self.interrupted = True
        if self._interrupt_event is not None:
            self._interrupt_event.set()

    async def disconnect(self) -> None:
        self.disconnected = True


class ClaudeProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        assert _host_servicer is not None
        _host_servicer.reset()
        _FakeClaudeSDKClient.mode = "success"
        _FakeClaudeSDKClient.instances.clear()

    def test_provider_completes_turn_through_agent_sdk_with_catalog_tools(self) -> None:
        host = _host_servicer
        assert host is not None
        lifecycle, provider_client = _configure_provider()
        capabilities = provider_client.GetCapabilities(agent_pb2.GetAgentProviderCapabilitiesRequest())
        self.assertEqual(list(capabilities.supported_tool_sources), [agent_pb2.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG])
        self.assertEqual(lifecycle.GetProviderIdentity(empty_pb2.Empty()).name, "claude")

        provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(
                session_id="session-claude",
                model="sonnet-session",
                created_by=agent_pb2.AgentActor(subject_id="user-123", subject_kind="human"),
            )
        )
        started = provider_client.CreateTurn(
            _turn_request(
                turn_id="turn-claude",
                session_id="session-claude",
                messages=[agent_pb2.AgentMessage(role="user", text="List my Linear issues")],
                execution_ref="exec-claude",
            )
        )
        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)

        fetched = _wait_for_turn(provider_client, "turn-claude", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.output_text, "Claude completed")

        self.assertEqual(len(_FakeClaudeSDKClient.instances), 1)
        fake_client = _FakeClaudeSDKClient.instances[0]
        self.assertTrue(fake_client.connected)
        self.assertTrue(fake_client.disconnected)
        self.assertEqual(fake_client.session_id, "turn-claude")
        self.assertIn("List my Linear issues", fake_client.prompt)
        self.assertIn('<message 1 role="user">', fake_client.prompt)
        self.assertEqual(fake_client.options.model, "sonnet-session")
        self.assertEqual(fake_client.options.tools, [])
        self.assertEqual(fake_client.options.allowed_tools, ["mcp__gestalt__*"])
        self.assertIsNotNone(fake_client.options.can_use_tool)
        self.assertEqual(set(fake_client.options.mcp_servers.keys()), {"gestalt"})
        self.assertEqual(fake_client.options.permission_mode, "dontAsk")
        self.assertEqual(fake_client.options.setting_sources, [])
        self.assertEqual(fake_client.options.skills, [])
        self.assertEqual(fake_client.options.plugins, [])
        self.assertEqual(fake_client.options.env["ANTHROPIC_API_KEY"], "test-anthropic-key")
        self.assertEqual(fake_client.options.env["ENABLE_TOOL_SEARCH"], "auto:5")
        self.assertIn("CLAUDE_CONFIG_DIR", fake_client.options.env)

        self.assertEqual([request["page_token"] for request in host.list_requests], ["", "page-2"])
        self.assertEqual(host.list_requests[0]["run_grant"], "grant-claude")
        self.assertEqual(host.execute_requests[0]["tool_call_id"], "sdk-1")
        self.assertEqual(host.execute_requests[0]["tool_id"], "tool-linear-issues")
        self.assertEqual(host.execute_requests[0]["run_grant"], "grant-claude")
        self.assertEqual(
            host.execute_requests[0]["idempotency_key"], "agent/claude-sdk:turn-claude:sdk-1:linear__issues"
        )
        self.assertEqual(host.execute_requests[0]["arguments"], {"query": "AIT"})
        tool_result = cast(Any, fake_client.tool_result)
        self.assertEqual(tool_result.content[0].text, '{"ok":true}')
        self.assertFalse(tool_result.isError)

    def test_sdk_mcp_bridge_exposes_full_catalog_for_native_tool_search(self) -> None:
        host = _host_servicer
        assert host is not None
        _configure_provider()
        runner = provider_module.provider._runner
        assert runner is not None
        options = runner._options(
            model="sonnet-session", session_id="session-claude", turn_id="turn-claude", run_grant="grant-claude"
        )

        first, second = asyncio.run(_list_tools_through_sdk_bridge(options))

        self.assertEqual(
            [tool["name"] for tool in first["result"]["tools"]],
            ["ashby__candidate_list", "linear__issues", "github__pulls_list"],
        )
        self.assertNotIn("nextCursor", first["result"])
        self.assertEqual([tool["name"] for tool in second["result"]["tools"]], ["linear__issues", "github__pulls_list"])
        self.assertNotIn("nextCursor", second["result"])
        self.assertEqual([request["page_token"] for request in host.list_requests], ["", "page-2", "page-2"])

    def test_create_turn_rejects_unsupported_tool_contract_inputs(self) -> None:
        _, provider_client = _configure_provider()
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-validation"))

        bad_source = _turn_request(turn_id="turn-bad-source", session_id="session-validation")
        bad_source.tool_source = 999
        _assert_invalid(provider_client, bad_source, "requires toolSource mcp_catalog")

        missing_grant = _turn_request(turn_id="turn-missing-grant", session_id="session-validation", run_grant="")
        _assert_invalid(provider_client, missing_grant, "run_grant is required")

        wildcard_ref = _turn_request(turn_id="turn-wildcard", session_id="session-validation")
        wildcard_ref.tool_refs[0].operation = "*"
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

    def test_create_turn_accepts_broad_catalog_tool_refs(self) -> None:
        _, provider_client = _configure_provider()
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-broad-refs"))

        empty_refs = _turn_request(turn_id="turn-empty-refs", session_id="session-broad-refs")
        del empty_refs.tool_refs[:]
        self.assertEqual(provider_client.CreateTurn(empty_refs).status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)

        plugin_only = _turn_request(turn_id="turn-plugin-only", session_id="session-broad-refs")
        del plugin_only.tool_refs[1:]
        plugin_only.tool_refs[0].operation = ""
        self.assertEqual(provider_client.CreateTurn(plugin_only).status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)

        global_ref = _turn_request(turn_id="turn-global-ref", session_id="session-broad-refs")
        del global_ref.tool_refs[:]
        global_ref.tool_refs.add(plugin="*")
        self.assertEqual(provider_client.CreateTurn(global_ref).status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)

        for turn_id in ("turn-empty-refs", "turn-plugin-only", "turn-global-ref"):
            _wait_for_turn(provider_client, turn_id, agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

    def test_create_turn_rejects_invalid_broad_catalog_tool_refs(self) -> None:
        _, provider_client = _configure_provider()
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-invalid-refs"))

        for field in ("operation", "connection", "instance"):
            request = _turn_request(turn_id=f"turn-wildcard-{field}", session_id="session-invalid-refs")
            del request.tool_refs[1:]
            setattr(request.tool_refs[0], field, "*")
            _assert_invalid(provider_client, request, "wildcard tool_refs are not supported")

        wildcard_system = _turn_request(turn_id="turn-wildcard-system", session_id="session-invalid-refs")
        del wildcard_system.tool_refs[:]
        wildcard_system.tool_refs.add(system="*", operation="run")
        _assert_invalid(provider_client, wildcard_system, "wildcard tool_refs are not supported")

        missing_plugin_or_system = _turn_request(turn_id="turn-missing-plugin", session_id="session-invalid-refs")
        del missing_plugin_or_system.tool_refs[:]
        missing_plugin_or_system.tool_refs.add(operation="issues")
        _assert_invalid(provider_client, missing_plugin_or_system, "tool_refs[1].plugin is required")

        system_without_operation = _turn_request(
            turn_id="turn-system-missing-operation", session_id="session-invalid-refs"
        )
        del system_without_operation.tool_refs[:]
        system_without_operation.tool_refs.add(system="workflow")
        _assert_invalid(provider_client, system_without_operation, "operation is required for system tool refs")

        for field in ("connection", "instance", "title", "description"):
            request = _turn_request(turn_id=f"turn-system-ref-{field}", session_id="session-invalid-refs")
            del request.tool_refs[:]
            ref = request.tool_refs.add(system="workflow", operation="schedules.list")
            setattr(ref, field, "value")
            _assert_invalid(provider_client, request, "system refs cannot include")

        for field in ("operation", "connection", "instance", "title", "description"):
            request = _turn_request(turn_id=f"turn-global-ref-{field}", session_id="session-invalid-refs")
            del request.tool_refs[:]
            ref = request.tool_refs.add(plugin="*")
            setattr(ref, field, "value")
            _assert_invalid(provider_client, request, "global search ref cannot include")

    def test_cancel_turn_interrupts_sdk_client_and_terminal_status_wins(self) -> None:
        _FakeClaudeSDKClient.mode = "cancel"
        _, provider_client = _configure_provider()
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-cancel"))
        provider_client.CreateTurn(_turn_request(turn_id="turn-cancel", session_id="session-cancel"))
        _wait_for_fake_client()

        canceled = provider_client.CancelTurn(
            agent_pb2.CancelAgentProviderTurnRequest(turn_id="turn-cancel", reason="test cancellation")
        )
        self.assertEqual(canceled.status, agent_pb2.AGENT_EXECUTION_STATUS_CANCELED)
        fetched = _wait_for_turn(provider_client, "turn-cancel", agent_pb2.AGENT_EXECUTION_STATUS_CANCELED)
        self.assertEqual(fetched.status_message, "test cancellation")
        self.assertTrue(_FakeClaudeSDKClient.instances[0].interrupted)

        time.sleep(0.1)
        fetched_again = provider_client.GetTurn(agent_pb2.GetAgentProviderTurnRequest(turn_id="turn-cancel"))
        self.assertEqual(fetched_again.status, agent_pb2.AGENT_EXECUTION_STATUS_CANCELED)

    def test_sdk_failure_marks_turn_failed(self) -> None:
        _FakeClaudeSDKClient.mode = "failure"
        _, provider_client = _configure_provider()
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-failure"))
        provider_client.CreateTurn(_turn_request(turn_id="turn-failure", session_id="session-failure"))

        failed = _wait_for_turn(provider_client, "turn-failure", agent_pb2.AGENT_EXECUTION_STATUS_FAILED)
        self.assertIn("boom", failed.status_message)


def setUpModule() -> None:
    global _runtime_server, _host_server, _runtime_socket, _host_socket, _host_servicer
    global _previous_agent_host_socket, _previous_agent_host_token

    _runtime_socket = _fresh_socket("claude-sdk-agent-runtime")
    _host_socket = _fresh_socket("claude-sdk-agent-host")
    _previous_agent_host_socket = os.environ.get(ENV_AGENT_HOST_SOCKET)
    _previous_agent_host_token = os.environ.get(ENV_AGENT_HOST_SOCKET_TOKEN)
    os.environ[ENV_AGENT_HOST_SOCKET] = _host_socket
    os.environ[ENV_AGENT_HOST_SOCKET_TOKEN] = "relay-token"

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


def _configure_provider() -> tuple[Any, Any]:
    channel = grpc.insecure_channel(f"unix:{_runtime_socket}")
    lifecycle = runtime_pb2_grpc.ProviderLifecycleStub(channel)
    provider_client = agent_pb2_grpc.AgentProviderStub(channel)
    request = runtime_pb2.ConfigureProviderRequest(name="claude", protocol_version=_runtime.CURRENT_PROTOCOL_VERSION)
    request.config.update(
        {
            "defaultModel": "sonnet-config",
            "timeoutSeconds": 5,
            "permissionMode": "dontAsk",
            "anthropicApiKey": "test-anthropic-key",
        }
    )
    lifecycle.ConfigureProvider(request)
    assert provider_module.provider._runner is not None
    provider_module.provider._runner._client_factory = _FakeClaudeSDKClient
    return lifecycle, provider_client


def _turn_request(
    *,
    turn_id: str,
    session_id: str,
    messages: list[Any] | None = None,
    run_grant: str = "grant-claude",
    execution_ref: str = "",
    response_schema: Any | None = None,
    model_options: Any | None = None,
) -> Any:
    request = agent_pb2.CreateAgentProviderTurnRequest(
        turn_id=turn_id,
        session_id=session_id,
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


async def _visible_sdk_tools(options: Any) -> list[str]:
    server = options.mcp_servers["gestalt"]["instance"]
    list_result = await server.request_handlers[mcp_types.ListToolsRequest](mcp_types.ListToolsRequest())
    return [tool.name for tool in list_result.root.tools]


async def _call_sdk_tool(options: Any, *, name: str, arguments: dict[str, Any]) -> Any:
    server = options.mcp_servers["gestalt"]["instance"]
    call_result = await server.request_handlers[mcp_types.CallToolRequest](
        mcp_types.CallToolRequest(params=mcp_types.CallToolRequestParams(name=name, arguments=arguments))
    )
    return call_result.root


async def _list_tools_through_sdk_bridge(options: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    from claude_agent_sdk._internal.query import Query

    bridge = py_types.SimpleNamespace(sdk_mcp_servers={"gestalt": options.mcp_servers["gestalt"]["instance"]})
    handle_request = cast(Any, Query._handle_sdk_mcp_request)
    first = await handle_request(bridge, "gestalt", {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
    second = await handle_request(
        bridge, "gestalt", {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {"cursor": "page-2"}}
    )
    return first, second


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


def _wait_for_fake_client() -> _FakeClaudeSDKClient:
    deadline = time.time() + 5
    while time.time() < deadline:
        if _FakeClaudeSDKClient.instances:
            return _FakeClaudeSDKClient.instances[0]
        time.sleep(0.05)
    raise AssertionError("fake Claude SDK client was not created")


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
