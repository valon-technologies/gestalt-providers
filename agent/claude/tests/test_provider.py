from __future__ import annotations

import io
import json
import os
import socket
import tempfile
import textwrap
import time
import unittest
from concurrent import futures
from pathlib import Path
from typing import Any, cast

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import struct_pb2 as _struct_pb2

import provider as provider_module
from gestalt import ENV_AGENT_HOST_SOCKET, ENV_AGENT_HOST_SOCKET_TOKEN, ProviderKind, _runtime
from gestalt.gen.v1 import agent_pb2 as _agent_pb2
from gestalt.gen.v1 import agent_pb2_grpc as _agent_pb2_grpc
from gestalt.gen.v1 import runtime_pb2 as _runtime_pb2
from gestalt.gen.v1 import runtime_pb2_grpc as _runtime_pb2_grpc
from internals.mcp_server import GestaltMCPServer

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
                "tool_grant": request.tool_grant,
            }
        )
        response = agent_pb2.ListAgentToolsResponse()
        if request.page_token == "":
            tool = response.tools.add()
            tool.id = "tool-linear-issues"
            tool.mcp_name = "linear__issues"
            tool.title = "List Linear issues"
            tool.description = "List issues from Linear"
            tool.input_schema = '{"type":"object","properties":{"query":{"type":"string"}}}'
            setattr(tool.annotations, "read_only_hint", True)
            setattr(tool.ref, "plugin", "linear")
            setattr(tool.ref, "operation", "issues")
            response.next_page_token = "page-2"
        elif request.page_token == "page-2":
            tool = response.tools.add()
            tool.id = "tool-github-repos"
            tool.mcp_name = "github__repos"
            tool.title = "List GitHub repos"
            tool.description = "List GitHub repositories"
            tool.input_schema = '{"type":"object"}'
            setattr(tool.ref, "plugin", "github")
            setattr(tool.ref, "operation", "repos/list-for-authenticated-user")
        return response

    def ExecuteTool(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        self.execute_requests.append(
            {
                "session_id": request.session_id,
                "turn_id": request.turn_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id,
                "tool_grant": request.tool_grant,
                "idempotency_key": request.idempotency_key,
            }
        )
        return agent_pb2.ExecuteAgentToolResponse(status=200, body='{"ok":true}')


class ClaudeProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        assert _host_servicer is not None
        _host_servicer.reset()

    def test_provider_completes_turn_by_launching_claude_code_with_mcp_config(self) -> None:
        host = _host_servicer
        assert host is not None
        lifecycle, provider_client = _configure_provider()
        capabilities = provider_client.GetCapabilities(agent_pb2.GetAgentProviderCapabilitiesRequest())
        self.assertFalse(capabilities.native_tool_search)
        self.assertEqual(list(capabilities.supported_tool_sources), [agent_pb2.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG])
        self.assertEqual(lifecycle.GetProviderIdentity(empty_pb2.Empty()).name, "claude")

        provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(
                session_id="session-claude",
                model="sonnet",
                created_by=agent_pb2.AgentActor(subject_id="user-123", subject_kind="human"),
            )
        )
        started = provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-claude",
                session_id="session-claude",
                model="sonnet",
                messages=[agent_pb2.AgentMessage(role="user", text="List my Linear issues")],
                tool_source=agent_pb2.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG,
                tool_grant="grant-claude",
                execution_ref="exec-claude",
                created_by=agent_pb2.AgentActor(subject_id="user-123", subject_kind="human"),
            )
        )
        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)

        fetched = _wait_for_turn(provider_client, "turn-claude", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.output_text, "Claude completed")

        fake_log = json.loads(Path(os.environ["CLAUDE_FAKE_LOG"]).read_text(encoding="utf-8"))
        self.assertIn("--mcp-config", fake_log["argv"])
        self.assertIn("--strict-mcp-config", fake_log["argv"])
        self.assertIn("--allowedTools", fake_log["argv"])
        self.assertIn("--input-format", fake_log["argv"])
        self.assertNotIn("user: List my Linear issues", fake_log["argv"])
        self.assertEqual(fake_log["stdin"], "user: List my Linear issues")
        self.assertEqual(
            fake_log["mcp_config"]["mcpServers"]["gestalt"]["env"]["GESTALT_CLAUDE_TOOL_GRANT"], "grant-claude"
        )
        self.assertEqual(fake_log["mcp_config"]["mcpServers"]["gestalt"]["env"][ENV_AGENT_HOST_SOCKET], _host_socket)
        self.assertEqual([request["page_token"] for request in host.list_requests], ["", "page-2"])
        self.assertEqual(host.execute_requests[0]["tool_call_id"], "mcp-3")
        self.assertEqual(host.execute_requests[0]["idempotency_key"], "agent/claude:turn-claude:mcp-3")
        self.assertEqual(fake_log["mcp_call"]["result"]["content"][0]["text"], '{"ok":true}')

    def test_mcp_server_lists_and_executes_turn_scoped_gestalt_tools(self) -> None:
        assert _host_servicer is not None
        with _patched_env(
            {
                "GESTALT_CLAUDE_SESSION_ID": "session-mcp",
                "GESTALT_CLAUDE_TURN_ID": "turn-mcp",
                "GESTALT_CLAUDE_TOOL_GRANT": "grant-mcp",
                ENV_AGENT_HOST_SOCKET: _host_socket,
                ENV_AGENT_HOST_SOCKET_TOKEN: "relay-token",
            }
        ):
            stdin = io.StringIO(
                "\n".join(
                    [
                        json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
                        json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}),
                        json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": 3,
                                "method": "tools/call",
                                "params": {"name": "linear__issues", "arguments": {"query": "AIT"}},
                            }
                        ),
                    ]
                )
                + "\n"
            )
            stdout = io.StringIO()
            GestaltMCPServer().serve(stdin=stdin, stdout=stdout)

        responses = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(responses[0]["result"]["capabilities"], {"tools": {}})
        tools = responses[1]["result"]["tools"]
        self.assertEqual(tools[0]["name"], "linear__issues")
        self.assertEqual(tools[1]["name"], "github__repos")
        self.assertEqual(tools[0]["annotations"], {"readOnlyHint": True})
        self.assertEqual(
            responses[2]["result"], {"content": [{"type": "text", "text": '{"ok":true}'}], "isError": False}
        )
        self.assertEqual([request["page_token"] for request in _host_servicer.list_requests], ["", "page-2"])
        self.assertEqual(_host_servicer.list_requests[0]["tool_grant"], "grant-mcp")
        self.assertEqual(_host_servicer.execute_requests[0]["tool_id"], "tool-linear-issues")
        self.assertEqual(_host_servicer.execute_requests[0]["tool_grant"], "grant-mcp")
        self.assertEqual(_host_servicer.execute_requests[0]["idempotency_key"], "agent/claude:turn-mcp:mcp-3")

    def test_cancel_turn_terminates_running_claude_code_process(self) -> None:
        lifecycle, provider_client = _configure_provider(fake_binary=_sleeping_claude_binary())
        self.assertEqual(lifecycle.GetProviderIdentity(empty_pb2.Empty()).name, "claude")
        provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(session_id="session-cancel", model="sonnet")
        )
        provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-cancel",
                session_id="session-cancel",
                model="sonnet",
                messages=[agent_pb2.AgentMessage(role="user", text="wait")],
                tool_source=agent_pb2.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG,
                tool_grant="grant-cancel",
            )
        )
        process_log = Path(os.environ["CLAUDE_FAKE_LOG"])
        _wait_for_file(process_log)
        pid = int(json.loads(process_log.read_text(encoding="utf-8"))["pid"])

        canceled = provider_client.CancelTurn(
            agent_pb2.CancelAgentProviderTurnRequest(turn_id="turn-cancel", reason="test cancellation")
        )
        self.assertEqual(canceled.status, agent_pb2.AGENT_EXECUTION_STATUS_CANCELED)
        _wait_for_process_exit(pid)

    def test_reconfigure_terminates_running_claude_code_process(self) -> None:
        lifecycle, provider_client = _configure_provider(fake_binary=_sleeping_claude_binary())
        provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(session_id="session-reconfigure", model="sonnet")
        )
        provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-reconfigure",
                session_id="session-reconfigure",
                model="sonnet",
                messages=[agent_pb2.AgentMessage(role="user", text="wait")],
                tool_source=agent_pb2.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG,
                tool_grant="grant-reconfigure",
            )
        )
        process_log = Path(os.environ["CLAUDE_FAKE_LOG"])
        _wait_for_file(process_log)
        pid = int(json.loads(process_log.read_text(encoding="utf-8"))["pid"])

        fake_binary, fake_log = _fake_claude_binary()
        os.environ["CLAUDE_FAKE_LOG"] = fake_log
        request = runtime_pb2.ConfigureProviderRequest(name="claude", protocol_version=_runtime.CURRENT_PROTOCOL_VERSION)
        request.config.update({"defaultModel": "sonnet", "claudeBinary": fake_binary, "timeoutSeconds": 5})
        lifecycle.ConfigureProvider(request)

        _wait_for_process_exit(pid)

    def test_duplicate_turn_id_in_another_session_is_rejected(self) -> None:
        _, provider_client = _configure_provider()
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-a", model="sonnet"))
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-b", model="sonnet"))
        provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-shared",
                session_id="session-a",
                model="sonnet",
                messages=[agent_pb2.AgentMessage(role="user", text="one")],
                tool_source=agent_pb2.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG,
                tool_grant="grant-a",
            )
        )
        with self.assertRaises(grpc.RpcError) as raised:
            provider_client.CreateTurn(
                agent_pb2.CreateAgentProviderTurnRequest(
                    turn_id="turn-shared",
                    session_id="session-b",
                    model="sonnet",
                    messages=[agent_pb2.AgentMessage(role="user", text="two")],
                    tool_source=agent_pb2.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG,
                    tool_grant="grant-b",
                )
            )
        self.assertEqual(cast(Any, raised.exception).code(), grpc.StatusCode.ALREADY_EXISTS)
        _wait_for_turn(provider_client, "turn-shared", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)


def setUpModule() -> None:
    global _runtime_server, _host_server, _runtime_socket, _host_socket, _host_servicer
    global _previous_agent_host_socket, _previous_agent_host_token

    _runtime_socket = _fresh_socket("claude-agent-runtime")
    _host_socket = _fresh_socket("claude-agent-host")
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


def _configure_provider(*, fake_binary: str | None = None) -> tuple[Any, Any]:
    fake_binary, fake_log = _fake_claude_binary() if fake_binary is None else (fake_binary, os.environ["CLAUDE_FAKE_LOG"])
    os.environ["CLAUDE_FAKE_LOG"] = fake_log
    channel = grpc.insecure_channel(f"unix:{_runtime_socket}")
    lifecycle = runtime_pb2_grpc.ProviderLifecycleStub(channel)
    provider_client = agent_pb2_grpc.AgentProviderStub(channel)
    request = runtime_pb2.ConfigureProviderRequest(name="claude", protocol_version=_runtime.CURRENT_PROTOCOL_VERSION)
    request.config.update({"defaultModel": "sonnet", "claudeBinary": fake_binary, "timeoutSeconds": 5})
    lifecycle.ConfigureProvider(request)
    return lifecycle, provider_client


def _fake_claude_binary() -> tuple[str, str]:
    temp_dir = tempfile.mkdtemp(prefix="claude-agent-test-")
    binary = Path(temp_dir) / "claude"
    log_path = str(Path(temp_dir) / "claude-log.json")
    binary.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            import json
            import os
            import subprocess
            import sys

            prompt = sys.stdin.read()
            config_path = sys.argv[sys.argv.index("--mcp-config") + 1]
            with open(config_path, encoding="utf-8") as file:
                mcp_config = json.load(file)
            server = mcp_config["mcpServers"]["gestalt"]
            env = os.environ.copy()
            env.update(server.get("env", {}))
            proc = subprocess.Popen(
                [server["command"], *server.get("args", [])],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env,
            )

            def rpc(payload):
                assert proc.stdin is not None
                assert proc.stdout is not None
                proc.stdin.write(json.dumps(payload) + "\\n")
                proc.stdin.flush()
                return json.loads(proc.stdout.readline())

            mcp_initialize = rpc({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
            mcp_tools = rpc({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
            mcp_call = rpc(
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {
                        "name": mcp_tools["result"]["tools"][0]["name"],
                        "arguments": {"query": "AIT"},
                    },
                }
            )
            assert proc.stdin is not None
            proc.stdin.close()
            proc.wait(timeout=5)
            with open(os.environ["CLAUDE_FAKE_LOG"], "w", encoding="utf-8") as file:
                json.dump(
                    {
                        "argv": sys.argv[1:],
                        "stdin": prompt,
                        "mcp_config": mcp_config,
                        "mcp_initialize": mcp_initialize,
                        "mcp_tools": mcp_tools,
                        "mcp_call": mcp_call,
                    },
                    file,
                )
            print(json.dumps({"result": "Claude completed"}))
            """
        ),
        encoding="utf-8",
    )
    binary.chmod(0o755)
    return str(binary), log_path


def _sleeping_claude_binary() -> str:
    temp_dir = tempfile.mkdtemp(prefix="claude-agent-test-")
    binary = Path(temp_dir) / "claude"
    log_path = str(Path(temp_dir) / "claude-log.json")
    os.environ["CLAUDE_FAKE_LOG"] = log_path
    binary.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            import json
            import os
            import time

            with open(os.environ["CLAUDE_FAKE_LOG"], "w", encoding="utf-8") as file:
                json.dump({"pid": os.getpid()}, file)
            time.sleep(60)
            print(json.dumps({"result": "should not complete"}))
            """
        ),
        encoding="utf-8",
    )
    binary.chmod(0o755)
    return str(binary)


def _wait_for_turn(provider_client: Any, turn_id: str, status: int) -> Any:
    deadline = time.time() + 5
    while time.time() < deadline:
        turn = provider_client.GetTurn(agent_pb2.GetAgentProviderTurnRequest(turn_id=turn_id))
        if turn.status == status:
            return turn
        if turn.status == agent_pb2.AGENT_EXECUTION_STATUS_FAILED:
            raise AssertionError(f"turn failed: {turn.status_message}")
        time.sleep(0.05)
    raise AssertionError(f"turn {turn_id} did not reach status {status}")


def _wait_for_file(path: Path) -> None:
    deadline = time.time() + 5
    while time.time() < deadline:
        if path.exists():
            return
        time.sleep(0.05)
    raise AssertionError(f"{path} was not created")


def _wait_for_process_exit(pid: int) -> None:
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except OSError:
            return
        time.sleep(0.05)
    raise AssertionError(f"process {pid} did not exit")


class _patched_env:
    def __init__(self, values: dict[str, str]) -> None:
        self._values = values
        self._previous: dict[str, str | None] = {}

    def __enter__(self) -> None:
        for key, value in self._values.items():
            self._previous[key] = os.environ.get(key)
            os.environ[key] = value

    def __exit__(self, *_args: Any) -> None:
        for key, value in self._previous.items():
            _restore_env(key, value)


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
