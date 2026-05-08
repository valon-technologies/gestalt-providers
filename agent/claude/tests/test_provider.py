from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
import tempfile
import time
import types as py_types
import unittest
from concurrent import futures
from datetime import UTC, datetime
from typing import Any, cast

import grpc
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from google.protobuf import json_format
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from mcp import types as mcp_types

import provider as provider_module
from gestalt import ENV_AGENT_HOST_SOCKET, ENV_AGENT_HOST_SOCKET_TOKEN, ProviderKind, _runtime, indexeddb_socket_env
from gestalt._gen.v1 import agent_pb2 as _agent_pb2
from gestalt._gen.v1 import agent_pb2_grpc as _agent_pb2_grpc
from gestalt._gen.v1 import runtime_pb2 as _runtime_pb2
from gestalt._gen.v1 import runtime_pb2_grpc as _runtime_pb2_grpc
from internals.mcp_bridge import GestaltMCPBridge
from internals.provider_io import prepared_workspace_to_dict
from internals.session_start import ADDITIONAL_CONTEXT_KEY, prepend_session_start_context, run_session_start_hooks
from tests.fake_indexeddb import FakeIndexedDB, datastore_pb2_grpc

agent_pb2: Any = cast(Any, _agent_pb2)
agent_pb2_grpc: Any = _agent_pb2_grpc
empty_pb2: Any = _empty_pb2
runtime_pb2: Any = _runtime_pb2
runtime_pb2_grpc: Any = _runtime_pb2_grpc
struct_pb2: Any = _struct_pb2

_runtime_server: grpc.Server | None = None
_host_server: grpc.Server | None = None
_indexeddb_server: grpc.Server | None = None
_runtime_socket = ""
_host_socket = ""
_indexeddb_socket = ""
_host_servicer: "_FakeAgentHost | None" = None
_indexeddb_servicer: "FakeIndexedDB | None" = None
_previous_agent_host_socket: str | None = None
_previous_agent_host_token: str | None = None
_previous_indexeddb_socket: str | None = None


class _FakeAgentHost(agent_pb2_grpc.AgentHostServicer):
    def __init__(self) -> None:
        self.list_requests: list[dict[str, Any]] = []
        self.execute_requests: list[dict[str, Any]] = []
        self.large_catalog = False
        self.include_reconnect_sentinel = False
        self.metadata_supported = False
        self.list_error = ""
        self.execute_error = ""

    def reset(self) -> None:
        self.list_requests.clear()
        self.execute_requests.clear()
        self.large_catalog = False
        self.include_reconnect_sentinel = False
        self.metadata_supported = False
        self.list_error = ""
        self.execute_error = ""

    def ListTools(self, request: Any, context: grpc.ServicerContext) -> Any:
        self.list_requests.append(
            {
                "session_id": request.session_id,
                "turn_id": request.turn_id,
                "page_size": request.page_size,
                "page_token": request.page_token,
                "run_grant": request.run_grant,
            }
        )
        if self.list_error:
            context.abort(grpc.StatusCode.UNKNOWN, self.list_error)
        response = agent_pb2.ListAgentToolsResponse()
        if self.large_catalog:
            if request.page_token:
                return response
            for index in range(60):
                plugin = "github" if index % 2 == 0 else "linear"
                tool = response.tools.add()
                tool.id = f"tool-{plugin}-{index}"
                tool.mcp_name = f"{plugin}__operation_{index}"
                tool.title = f"{plugin.title()} operation {index}"
                tool.description = f"{plugin.title()} catalog operation {index}"
                if hasattr(tool, "tags"):
                    self.metadata_supported = True
                    if plugin == "github":
                        tool.tags.extend(["pr", "prs"])
                        tool.search_text = "github pull request repository owner number"
                tool.input_schema = '{"type":"object","properties":{"query":{"type":"string"}}}'
                setattr(tool.ref, "plugin", plugin)
                setattr(tool.ref, "operation", f"operation{index}")
            return response
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
            if self.include_reconnect_sentinel:
                tool = response.tools.add()
                tool.id = "tool-linear-reconnect"
                tool.mcp_name = "linear__reconnect_required"
                tool.title = "linear reconnect required"
                tool.description = "linear credentials expired or refresh failed"
                tool.input_schema = '{"type":"object","properties":{},"additionalProperties":false}'
                setattr(tool.annotations, "read_only_hint", True)
                setattr(tool.ref, "plugin", "linear")
                return response
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
            if hasattr(tool, "tags"):
                self.metadata_supported = True
                tool.tags.extend(["pr", "prs"])
                tool.search_text = "github pull request repository owner number"
            tool.input_schema = '{"type":"object"}'
            setattr(tool.ref, "plugin", "github")
            setattr(tool.ref, "operation", "pulls/list")
        return response

    def ExecuteTool(self, request: Any, context: grpc.ServicerContext) -> Any:
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
        if self.execute_error:
            context.abort(grpc.StatusCode.UNKNOWN, self.execute_error)
        if request.tool_id == "tool-linear-reconnect":
            return agent_pb2.ExecuteAgentToolResponse(
                status=424, body='{"error":{"code":"reconnect_required","plugin":"linear"}}'
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


class _FakeProviderContext:
    def abort(self, code: grpc.StatusCode, details: str) -> None:
        raise grpc.RpcError(f"{code.name}: {details}")


class ClaudeProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        assert _host_servicer is not None
        assert _indexeddb_servicer is not None
        _host_servicer.reset()
        _indexeddb_servicer.reset()
        _FakeClaudeSDKClient.mode = "success"
        _FakeClaudeSDKClient.instances.clear()

    def test_session_start_hooks_capture_context_and_metadata(self) -> None:
        hook = py_types.SimpleNamespace(
            id="load-memory",
            type="command",
            command=[sys.executable, "-c", "print('session context')"],
            cwd="",
            timeout="5s",
            env={},
            output=py_types.SimpleNamespace(additional_context=True, metadata=True),
        )

        metadata = run_session_start_hooks(py_types.SimpleNamespace(hooks=[hook]), {"caller": "kept"})

        self.assertEqual(metadata["caller"], "kept")
        self.assertEqual(
            metadata["__gestalt.lifecycle.sessionStart.results.load-memory"]["stdout"], "session context\n"
        )
        self.assertEqual(metadata["__gestalt.lifecycle.sessionStart.results.load-memory"]["status"], "succeeded")
        self.assertFalse(metadata["__gestalt.lifecycle.sessionStart.results.load-memory"]["timedOut"])
        self.assertEqual(metadata["__gestalt.lifecycle.sessionStart.additionalContext"], "session context")
        messages = prepend_session_start_context([{"role": "user", "text": "hello"}], metadata)
        self.assertEqual(messages[0]["role"], "system")
        self.assertIn("session context", messages[0]["text"])

    def test_provider_runs_session_start_once_and_prepends_context(self) -> None:
        _, provider_client = _configure_provider()
        session_start = py_types.SimpleNamespace(
            hooks=[
                py_types.SimpleNamespace(
                    id="load-memory",
                    type="command",
                    command=[sys.executable, "-c", "print('session context from provider')"],
                    cwd="",
                    timeout="5s",
                    env={},
                    output=py_types.SimpleNamespace(additional_context=True, metadata=True),
                )
            ]
        )

        session = provider_module.provider.CreateSession(
            py_types.SimpleNamespace(
                session_id="session-start-provider",
                idempotency_key="session-start-idem",
                model="",
                metadata=None,
                session_start=session_start,
                client_ref="",
                created_by=py_types.SimpleNamespace(subject_id="user-123", subject_kind="human"),
            ),
            cast(grpc.ServicerContext, _FakeProviderContext()),
        )
        metadata = json_format.MessageToDict(session.metadata)
        self.assertEqual(
            metadata["__gestalt.lifecycle.sessionStart.results.load-memory"]["stdout"],
            "session context from provider\n",
        )

        replay = provider_module.provider.CreateSession(
            py_types.SimpleNamespace(
                session_id="session-start-provider-replay",
                idempotency_key="session-start-idem",
                model="",
                metadata=None,
                session_start=py_types.SimpleNamespace(
                    hooks=[
                        py_types.SimpleNamespace(
                            id="should-not-run",
                            type="command",
                            command=[sys.executable, "-c", "import sys; sys.exit(7)"],
                            cwd="",
                            timeout="",
                            env={},
                            output=py_types.SimpleNamespace(additional_context=False, metadata=False),
                        )
                    ]
                ),
                client_ref="",
                created_by=py_types.SimpleNamespace(subject_id="user-123", subject_kind="human"),
            ),
            cast(grpc.ServicerContext, _FakeProviderContext()),
        )
        self.assertEqual(replay.id, "session-start-provider")

        provider_client.CreateTurn(
            _turn_request(
                turn_id="turn-session-start-context",
                session_id="session-start-provider",
                messages=[agent_pb2.AgentMessage(role="user", text="hello")],
            )
        )
        _wait_for_turn(provider_client, "turn-session-start-context", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertIn("session context from provider", _FakeClaudeSDKClient.instances[-1].prompt)

    def test_provider_passes_plugins_and_allowed_tools_to_claude_sdk(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            docs_real = _make_claude_plugin(root, "docs")
            docs_link = os.path.join(root, "docs-link")
            os.symlink(docs_real, docs_link)
            workflows = _make_claude_plugin(root, "workflows")
            _, provider_client = _configure_provider(
                {
                    "skillDiscovery": "all",
                    "plugins": [docs_link, workflows],
                    "allowedTools": ["Skill", "Read", "Bash(git status:*)"],
                }
            )
            provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-plugins"))
            provider_client.CreateTurn(
                _turn_request(
                    turn_id="turn-plugins",
                    session_id="session-plugins",
                    messages=[agent_pb2.AgentMessage(role="user", text="hello")],
                )
            )
            _wait_for_turn(provider_client, "turn-plugins", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

            options = _FakeClaudeSDKClient.instances[-1].options
            self.assertEqual(getattr(options, "tools"), ["mcp__gestalt__*", "Skill", "Read", "Bash"])
            self.assertEqual(
                getattr(options, "allowed_tools"), ["mcp__gestalt__*", "Skill", "Read", "Bash(git status:*)"]
            )
            self.assertEqual(getattr(options, "setting_sources"), [])
            self.assertEqual(getattr(options, "skills"), "all")
            self.assertEqual(
                getattr(options, "plugins"),
                [
                    {"type": "local", "path": os.path.realpath(docs_real)},
                    {"type": "local", "path": os.path.realpath(workflows)},
                ],
            )
            self.assertEqual(getattr(options.env, "get")("CLAUDE_CODE_DISABLE_AUTO_MEMORY"), "1")
            self.assertTrue(callable(getattr(options, "stderr")))
            self.assertEqual(getattr(asyncio.run(options.can_use_tool("Skill", {}, None)), "behavior", ""), "allow")
            self.assertEqual(
                getattr(asyncio.run(options.can_use_tool("Skill(docs:search)", {}, None)), "behavior", ""), "allow"
            )
            self.assertEqual(getattr(asyncio.run(options.can_use_tool("Read", {}, None)), "behavior", ""), "allow")
            self.assertEqual(getattr(asyncio.run(options.can_use_tool("Write", {}, None)), "behavior", ""), "deny")
            self.assertEqual(
                getattr(
                    asyncio.run(options.can_use_tool("Bash", {"command": "git status --short"}, None)), "behavior", ""
                ),
                "allow",
            )
            self.assertEqual(
                getattr(asyncio.run(options.can_use_tool("Bash", {"command": "git statusx"}, None)), "behavior", ""),
                "deny",
            )
            self.assertEqual(
                getattr(
                    asyncio.run(options.can_use_tool("Bash", {"command": "git status --short && rm -rf /"}, None)),
                    "behavior",
                    "",
                ),
                "deny",
            )
            self.assertEqual(getattr(asyncio.run(options.can_use_tool("WebFetch", {}, None)), "behavior", ""), "deny")

    def test_skill_discovery_all_without_allowed_tool_does_not_enable_skill_tool(self) -> None:
        _, provider_client = _configure_provider({"skillDiscovery": "all"})
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-skill-denied"))
        provider_client.CreateTurn(_turn_request(turn_id="turn-skill-denied", session_id="session-skill-denied"))
        _wait_for_turn(provider_client, "turn-skill-denied", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        options = _FakeClaudeSDKClient.instances[-1].options
        self.assertEqual(options.skills, [])
        self.assertEqual(options.setting_sources, [])
        self.assertEqual(options.tools, ["mcp__gestalt__*"])
        self.assertEqual(options.allowed_tools, ["mcp__gestalt__*"])
        self.assertEqual(
            getattr(asyncio.run(options.can_use_tool("Skill(docs:search)", {}, None)), "behavior", ""), "deny"
        )

    def test_allowed_tools_can_enable_skill_without_plugins(self) -> None:
        _, provider_client = _configure_provider({"skillDiscovery": "all", "allowedTools": ["Skill"]})
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-skill-allowed"))
        provider_client.CreateTurn(_turn_request(turn_id="turn-skill-allowed", session_id="session-skill-allowed"))
        _wait_for_turn(provider_client, "turn-skill-allowed", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        options = _FakeClaudeSDKClient.instances[-1].options
        self.assertEqual(options.skills, "all")
        self.assertEqual(options.tools, ["mcp__gestalt__*", "Skill"])
        self.assertEqual(options.allowed_tools, ["mcp__gestalt__*", "Skill"])
        self.assertEqual(
            getattr(asyncio.run(options.can_use_tool("Skill(docs:search)", {}, None)), "behavior", ""), "allow"
        )

    def test_provider_rejects_reserved_session_start_metadata(self) -> None:
        _, provider_client = _configure_provider()
        metadata = struct_pb2.Struct()
        metadata.update({ADDITIONAL_CONTEXT_KEY: "spoofed"})

        with self.assertRaises(grpc.RpcError) as create_error:
            provider_client.CreateSession(
                agent_pb2.CreateAgentProviderSessionRequest(
                    session_id="reserved-session-start-create", metadata=metadata
                )
            )
        self.assertEqual(cast(Any, create_error.exception).code(), grpc.StatusCode.INVALID_ARGUMENT)

        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="reserved-session-start"))
        with self.assertRaises(grpc.RpcError) as update_error:
            provider_client.UpdateSession(
                agent_pb2.UpdateAgentProviderSessionRequest(session_id="reserved-session-start", metadata=metadata)
            )
        self.assertEqual(cast(Any, update_error.exception).code(), grpc.StatusCode.INVALID_ARGUMENT)

    def test_config_validation_rejects_unsafe_claude_code_configuration(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            valid = _make_claude_plugin(root, "docs")
            missing_manifest = os.path.join(root, "missing-manifest")
            os.makedirs(missing_manifest)
            missing_name_manifest = _make_claude_plugin(root, "missing-name", extra_manifest={"name": ""})
            manifest_executable_component = _make_claude_plugin(root, "with-mcp", extra_manifest={"mcpServers": {}})
            root_executable_component = _make_claude_plugin(root, "with-hooks")
            os.makedirs(os.path.join(root_executable_component, "hooks"))
            with open(os.path.join(root_executable_component, "hooks", "hooks.json"), "w", encoding="utf-8") as handle:
                json.dump({}, handle)
            metadata_manifest = _make_claude_plugin(
                root,
                "metadata",
                extra_manifest={
                    "version": "1.0.0",
                    "author": "Test Author",
                    "homepage": "https://example.test",
                    "repository": "https://example.test/repo",
                    "license": "MIT",
                },
            )
            default_skills_dir_manifest = _make_claude_plugin(root, "default-skills", include_manifest_skills=False)
            duplicate_docs = _make_claude_plugin(root, "duplicate-docs", manifest_name="docs")

            cases = [
                (
                    {"plugins": ["relative"]},
                    "path must be absolute",
                ),
                (
                    {"plugins": [{"path": valid}]},
                    r"plugins\[1\] must be a local plugin path",
                ),
                (
                    {"plugins": [valid, valid]},
                    "resolve to the same path",
                ),
                (
                    {"plugins": [valid, duplicate_docs]},
                    "use the same manifest name",
                ),
                (
                    {"plugins": [missing_manifest]},
                    "must include .claude-plugin/plugin.json",
                ),
                (
                    {"plugins": [missing_name_manifest]},
                    "manifest name is required",
                ),
                (
                    {"plugins": [manifest_executable_component]},
                    "unsupported components",
                ),
                (
                    {"plugins": [root_executable_component]},
                    "unsupported root components",
                ),
                ({"settingSources": ["workspace"]}, "settingSources entries must be one of"),
                ({"skillDiscovery": "named"}, "skillDiscovery must be one of"),
                (
                    {"allowedTools": ["WebFetch"]},
                    "unsupported Claude Code tool specifier",
                ),
                (
                    {"allowedTools": "Read"},
                    "allowedTools must be a list",
                ),
                (
                    {"pluginRegistry": {"docs": {"path": valid}}},
                    "unsupported Claude Code config fields: pluginRegistry",
                ),
                (
                    {"pluginSources": [{"type": "configured", "names": ["docs"]}]},
                    "unsupported Claude Code config fields: pluginSources",
                ),
                (
                    {"toolPermissions": {"allowedTools": ["Read"]}},
                    "unsupported Claude Code config fields: toolPermissions",
                ),
                (
                    {
                        "permissionMode": "bypassPermissions",
                        "allowedTools": ["Read"],
                    },
                    "bypassPermissions cannot be used",
                ),
            ]
            for config, message in cases:
                with self.subTest(message=message):
                    with self.assertRaisesRegex(ValueError, message):
                        provider_module.ClaudeCodeAgentProvider().configure("claude", _base_config(config))
            provider_module.ClaudeCodeAgentProvider().configure(
                "claude",
                _base_config(
                    {"plugins": [metadata_manifest, default_skills_dir_manifest]}
                ),
            )

    def test_provider_completes_turn_through_agent_sdk_with_catalog_tools(self) -> None:
        host = _host_servicer
        assert host is not None
        lifecycle, provider_client = _configure_provider()
        capabilities = provider_client.GetCapabilities(agent_pb2.GetAgentProviderCapabilitiesRequest())
        self.assertEqual(list(capabilities.supported_tool_sources), [agent_pb2.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG])
        if hasattr(capabilities, "supports_prepared_workspace"):
            self.assertTrue(capabilities.supports_prepared_workspace)
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
        events = provider_client.ListTurnEvents(agent_pb2.ListAgentProviderTurnEventsRequest(turn_id="turn-claude"))
        self.assertEqual(
            [event.type for event in events.events], ["turn.started", "assistant.message", "turn.completed"]
        )
        self.assertEqual([event.visibility for event in events.events], ["external", "external", "external"])

        self.assertEqual(len(_FakeClaudeSDKClient.instances), 1)
        fake_client = _FakeClaudeSDKClient.instances[0]
        self.assertTrue(fake_client.connected)
        self.assertTrue(fake_client.disconnected)
        self.assertEqual(fake_client.session_id, "turn-claude")
        self.assertIn("List my Linear issues", fake_client.prompt)
        self.assertIn('<message 1 role="user">', fake_client.prompt)
        self.assertEqual(fake_client.options.model, "sonnet-session")
        self.assertEqual(fake_client.options.tools, ["mcp__gestalt__*"])
        self.assertEqual(fake_client.options.allowed_tools, ["mcp__gestalt__*"])
        self.assertIsNotNone(fake_client.options.can_use_tool)
        self.assertEqual(
            getattr(asyncio.run(fake_client.options.can_use_tool("Read", {}, None)), "behavior", ""), "deny"
        )
        self.assertEqual(set(fake_client.options.mcp_servers.keys()), {"gestalt"})
        self.assertEqual(fake_client.options.permission_mode, "dontAsk")
        self.assertEqual(fake_client.options.setting_sources, [])
        self.assertEqual(fake_client.options.skills, [])
        self.assertEqual(fake_client.options.plugins, [])
        self.assertEqual(fake_client.options.env["ANTHROPIC_API_KEY"], "test-anthropic-key")
        self.assertEqual(fake_client.options.env["ENABLE_TOOL_SEARCH"], "auto:5")
        self.assertIn("CLAUDE_CONFIG_DIR", fake_client.options.env)
        self.assertIn("Gestalt MCP catalog tools", fake_client.options.system_prompt)
        self.assertIn("Linear", fake_client.options.system_prompt)
        self.assertIn("native tool search", fake_client.options.system_prompt)
        self.assertIn(
            "Do not infer tool availability from Claude Code built-in tools only", fake_client.options.system_prompt
        )

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

    def test_provider_launches_agent_sdk_from_prepared_workspace(self) -> None:
        if not hasattr(agent_pb2.CreateAgentProviderSessionRequest(), "prepared_workspace"):
            self.skipTest("installed gestalt-sdk does not expose prepared workspaces yet")
        _, provider_client = _configure_provider()
        request = agent_pb2.CreateAgentProviderSessionRequest(session_id="session-claude-workspace")
        request.prepared_workspace.root = "/sandbox/runtime/workspaces/session-claude-workspace"
        request.prepared_workspace.cwd = "/sandbox/runtime/workspaces/session-claude-workspace/repo"
        provider_client.CreateSession(request)
        provider_client.CreateTurn(
            _turn_request(
                turn_id="turn-claude-workspace",
                session_id="session-claude-workspace",
                messages=[agent_pb2.AgentMessage(role="user", text="inspect repo")],
            )
        )
        _wait_for_turn(provider_client, "turn-claude-workspace", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        self.assertEqual(
            _FakeClaudeSDKClient.instances[0].options.cwd, "/sandbox/runtime/workspaces/session-claude-workspace/repo"
        )

    def test_prepared_workspace_requires_root_and_cwd(self) -> None:
        with self.assertRaisesRegex(ValueError, "root and cwd are required"):
            prepared_workspace_to_dict(py_types.SimpleNamespace(root="/workspace", cwd=""))

    def test_indexeddb_persists_session_for_new_provider_instance(self) -> None:
        provider_a = provider_module.ClaudeCodeAgentProvider()
        server_a, socket_a, channel_a, lifecycle_a, client_a = _start_provider_runtime(provider_a)
        self.addCleanup(_stop_runtime, provider_a, server_a, socket_a, channel_a)
        _configure_lifecycle(lifecycle_a, provider_a)

        client_a.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(
                session_id="session-durable",
                model="sonnet-session",
                created_by=agent_pb2.AgentActor(subject_id="user-123", subject_kind="human"),
            )
        )
        _stop_runtime(provider_a, server_a, socket_a, channel_a)

        provider_b = provider_module.ClaudeCodeAgentProvider()
        server_b, socket_b, channel_b, lifecycle_b, client_b = _start_provider_runtime(provider_b)
        self.addCleanup(_stop_runtime, provider_b, server_b, socket_b, channel_b)
        _configure_lifecycle(lifecycle_b, provider_b)

        fetched_session = client_b.GetSession(agent_pb2.GetAgentProviderSessionRequest(session_id="session-durable"))
        self.assertEqual(fetched_session.id, "session-durable")
        started = client_b.CreateTurn(
            _turn_request(
                turn_id="turn-durable",
                session_id="session-durable",
                messages=[agent_pb2.AgentMessage(role="user", text="Continue after restart")],
            )
        )
        self.assertEqual(started.session_id, "session-durable")

        fetched_turn = client_b.GetTurn(agent_pb2.GetAgentProviderTurnRequest(turn_id="turn-durable"))
        self.assertEqual(fetched_turn.id, "turn-durable")
        _wait_for_turn(client_b, "turn-durable", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

    def test_list_paths_stream_projection_records_through_provider_rpc(self) -> None:
        indexeddb = _indexeddb_servicer
        assert indexeddb is not None
        _, provider_client = _configure_provider()
        store = provider_module.provider._store
        assert store is not None

        for suffix in ("a", "b"):
            metadata = struct_pb2.Struct()
            metadata.update({"large": "x" * 1024, "suffix": suffix})
            session_req = agent_pb2.CreateAgentProviderSessionRequest(
                session_id=f"session-stream-{suffix}",
                idempotency_key=f"session-idem-stream-{suffix}",
                metadata=metadata,
                created_by=agent_pb2.AgentActor(subject_id="user-123", subject_kind="human"),
            )
            if hasattr(session_req, "prepared_workspace"):
                session_req.prepared_workspace.root = f"/workspaces/session-stream-{suffix}"
                session_req.prepared_workspace.cwd = f"/workspaces/session-stream-{suffix}/repo"
            provider_client.CreateSession(session_req)
            provider_client.CreateTurn(
                _turn_request(
                    turn_id=f"turn-stream-{suffix}",
                    session_id=f"session-stream-{suffix}",
                    messages=[agent_pb2.AgentMessage(role="user", text=f"stream {suffix}")],
                )
            )
            _wait_for_turn(provider_client, f"turn-stream-{suffix}", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
            if suffix == "a":
                time.sleep(0.001)

        source_session_store = store._session_store_name
        source_turn_store = store._run_store_name
        session_projection_store = store._session_projection_store_name
        turn_projection_store = store._turn_projection_store_name
        before_counts = {
            (source_session_store, "get_all"): indexeddb.operation_count(
                store=source_session_store, operation="get_all"
            ),
            (source_turn_store, "get_all"): indexeddb.operation_count(store=source_turn_store, operation="get_all"),
            (session_projection_store, "open_cursor"): indexeddb.operation_count(
                store=session_projection_store, operation="open_cursor"
            ),
            (turn_projection_store, "open_cursor"): indexeddb.operation_count(
                store=turn_projection_store, operation="open_cursor"
            ),
        }
        before_session_projection_cursors = len(indexeddb.cursor_commands(store=session_projection_store))
        before_turn_projection_cursors = len(indexeddb.cursor_commands(store=turn_projection_store))

        summary_sessions = provider_client.ListSessions(
            agent_pb2.ListAgentProviderSessionsRequest(
                subject=agent_pb2.AgentSubjectContext(subject_id="user-123"), limit=1, summary_only=True
            )
        )
        full_sessions = provider_client.ListSessions(
            agent_pb2.ListAgentProviderSessionsRequest(
                subject=agent_pb2.AgentSubjectContext(subject_id="user-123"), limit=1
            )
        )
        summary_turns = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id="session-stream-a",
                subject=agent_pb2.AgentSubjectContext(subject_id="user-123"),
                limit=1,
                summary_only=True,
            )
        )
        full_turns = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id="session-stream-a", subject=agent_pb2.AgentSubjectContext(subject_id="user-123"), limit=1
            )
        )
        running_turns = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id="session-stream-a",
                subject=agent_pb2.AgentSubjectContext(subject_id="user-123"),
                status=agent_pb2.AGENT_EXECUTION_STATUS_RUNNING,
                limit=10,
                summary_only=True,
            )
        )
        succeeded_turns = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id="session-stream-a",
                subject=agent_pb2.AgentSubjectContext(subject_id="user-123"),
                status=agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED,
                limit=10,
                summary_only=True,
            )
        )
        bounded_source_session_get_all = (
            indexeddb.operation_count(store=source_session_store, operation="get_all")
            - before_counts[(source_session_store, "get_all")]
        )
        bounded_source_turn_get_all = (
            indexeddb.operation_count(store=source_turn_store, operation="get_all")
            - before_counts[(source_turn_store, "get_all")]
        )
        provider_client.UpdateSession(
            agent_pb2.UpdateAgentProviderSessionRequest(
                session_id="session-stream-b", state=agent_pb2.AGENT_SESSION_STATE_ARCHIVED
            )
        )
        active_sessions = provider_client.ListSessions(
            agent_pb2.ListAgentProviderSessionsRequest(
                subject=agent_pb2.AgentSubjectContext(subject_id="user-123"),
                state=agent_pb2.AGENT_SESSION_STATE_ACTIVE,
                limit=10,
                summary_only=True,
            )
        )
        archived_sessions = provider_client.ListSessions(
            agent_pb2.ListAgentProviderSessionsRequest(
                subject=agent_pb2.AgentSubjectContext(subject_id="user-123"),
                state=agent_pb2.AGENT_SESSION_STATE_ARCHIVED,
                limit=10,
                summary_only=True,
            )
        )

        self.assertEqual([session.id for session in summary_sessions.sessions], ["session-stream-b"])
        self.assertFalse(summary_sessions.sessions[0].HasField("metadata"))
        self.assertEqual([session.id for session in full_sessions.sessions], ["session-stream-b"])
        self.assertEqual(full_sessions.sessions[0].metadata.fields["suffix"].string_value, "b")
        self.assertEqual([turn.id for turn in summary_turns.turns], ["turn-stream-a"])
        self.assertEqual(len(summary_turns.turns[0].messages), 0)
        self.assertEqual(summary_turns.turns[0].output_text, "")
        self.assertEqual([turn.id for turn in full_turns.turns], ["turn-stream-a"])
        self.assertEqual(full_turns.turns[0].messages[0].text, "stream a")
        self.assertEqual(full_turns.turns[0].output_text, "Claude completed")
        self.assertEqual(list(running_turns.turns), [])
        self.assertEqual([turn.id for turn in succeeded_turns.turns], ["turn-stream-a"])
        self.assertEqual([session.id for session in active_sessions.sessions], ["session-stream-a"])
        self.assertEqual([session.id for session in archived_sessions.sessions], ["session-stream-b"])
        self.assertEqual(bounded_source_session_get_all, 0)
        self.assertEqual(bounded_source_turn_get_all, 0)
        self.assertGreater(
            indexeddb.operation_count(store=session_projection_store, operation="open_cursor")
            - before_counts[(session_projection_store, "open_cursor")],
            0,
        )
        self.assertGreater(
            indexeddb.operation_count(store=turn_projection_store, operation="open_cursor")
            - before_counts[(turn_projection_store, "open_cursor")],
            0,
        )
        session_projection_commands = indexeddb.cursor_commands(store=session_projection_store)[
            before_session_projection_cursors:
        ]
        turn_projection_commands = indexeddb.cursor_commands(store=turn_projection_store)[
            before_turn_projection_cursors:
        ]
        for commands in session_projection_commands[:2]:
            self.assertEqual(commands.count("next"), 1)
        for commands in turn_projection_commands[:2]:
            self.assertEqual(commands.count("next"), 1)

    def test_missing_indexeddb_socket_fails_first_store_rpc_with_failed_precondition(self) -> None:
        missing_socket = _fresh_socket("claude-agent-missing-indexeddb")
        previous_socket = os.environ.get(indexeddb_socket_env())
        os.environ[indexeddb_socket_env()] = missing_socket

        try:
            lifecycle, provider_client = _configure_provider()
            identity = lifecycle.GetProviderIdentity(empty_pb2.Empty())
            self.assertEqual(identity.name, "claude")

            with self.assertRaises(grpc.RpcError) as raised:
                provider_client.CreateSession(
                    agent_pb2.CreateAgentProviderSessionRequest(session_id="session-missing-indexeddb")
                )
        finally:
            if previous_socket is None:
                os.environ.pop(indexeddb_socket_env(), None)
            else:
                os.environ[indexeddb_socket_env()] = previous_socket
            if os.path.exists(missing_socket):
                os.remove(missing_socket)

        error = cast(Any, raised.exception)
        self.assertEqual(error.code(), grpc.StatusCode.FAILED_PRECONDITION)
        self.assertIn("IndexedDB host socket binding", error.details())

    def test_turn_idempotency_key_is_scoped_to_session(self) -> None:
        _, provider_client = _configure_provider()
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-idem-a"))
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-idem-b"))

        first = provider_client.CreateTurn(
            _turn_request(turn_id="turn-idem-a", session_id="session-idem-a", idempotency_key="repeatable")
        )
        replay = provider_client.CreateTurn(
            _turn_request(turn_id="turn-idem-a-replay", session_id="session-idem-a", idempotency_key="repeatable")
        )
        second_session = provider_client.CreateTurn(
            _turn_request(turn_id="turn-idem-b", session_id="session-idem-b", idempotency_key="repeatable")
        )

        self.assertEqual(first.id, "turn-idem-a")
        self.assertEqual(replay.id, "turn-idem-a")
        self.assertEqual(second_session.id, "turn-idem-b")
        _wait_for_turn(provider_client, "turn-idem-a", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        _wait_for_turn(provider_client, "turn-idem-b", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

    def test_session_idempotency_replays_after_indexeddb_add_conflict(self) -> None:
        indexeddb = _indexeddb_servicer
        assert indexeddb is not None
        _, provider_client = _configure_provider()
        store = provider_module.provider._store
        assert store is not None

        def seed_conflict(
            db: FakeIndexedDB, transaction_stores: dict[str, dict[str, Any]], store_name: str, request: Any
        ) -> None:
            del request
            if store_name != store._session_idempotency_store_name:
                return
            now = datetime.now(tz=UTC)
            db.put_record(
                store._session_store_name,
                {
                    "id": "session-race-winner",
                    "idempotency_key": "session-race",
                    "provider_name": "claude",
                    "model": "sonnet-config",
                    "client_ref": "",
                    "state": agent_pb2.AGENT_SESSION_STATE_ACTIVE,
                    "metadata": {},
                    "created_by": {"subject_id": "user-123", "subject_kind": "human"},
                    "created_at": now,
                    "updated_at": now,
                    "last_turn_at": None,
                },
                transaction_stores=transaction_stores,
            )
            db.put_record(
                store._session_idempotency_store_name,
                {
                    "id": "session-race",
                    "session_id": "session-race-winner",
                    "provider_name": "claude",
                    "created_at": now,
                },
                transaction_stores=transaction_stores,
            )

        indexeddb.inject_before_transaction_add(seed_conflict)
        replayed = provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(
                session_id="session-race-loser",
                idempotency_key="session-race",
                created_by=agent_pb2.AgentActor(subject_id="user-123", subject_kind="human"),
            )
        )

        self.assertEqual(replayed.id, "session-race-winner")

    def test_turn_idempotency_replays_after_indexeddb_add_conflict(self) -> None:
        indexeddb = _indexeddb_servicer
        assert indexeddb is not None
        _, provider_client = _configure_provider()
        store = provider_module.provider._store
        assert store is not None
        provider_client.CreateSession(agent_pb2.CreateAgentProviderSessionRequest(session_id="session-turn-race"))

        def seed_conflict(
            db: FakeIndexedDB, transaction_stores: dict[str, dict[str, Any]], store_name: str, request: Any
        ) -> None:
            del request
            if store_name != store._turn_idempotency_store_name:
                return
            now = datetime.now(tz=UTC)
            db.put_record(
                store._run_store_name,
                {
                    "id": "turn-race-winner",
                    "session_id": "session-turn-race",
                    "idempotency_key": "turn-race",
                    "provider_name": "claude",
                    "model": "sonnet-config",
                    "status": agent_pb2.AGENT_EXECUTION_STATUS_RUNNING,
                    "messages": [{"role": "user", "text": "winner"}],
                    "output_text": "",
                    "status_message": "",
                    "created_by": {"subject_id": "user-123", "subject_kind": "human"},
                    "created_at": now,
                    "started_at": now,
                    "completed_at": None,
                    "execution_ref": "turn-race-winner",
                },
                transaction_stores=transaction_stores,
            )
            db.put_record(
                store._turn_idempotency_store_name,
                {
                    "id": "session-turn-race\x1fturn-race",
                    "session_id": "session-turn-race",
                    "idempotency_key": "turn-race",
                    "turn_id": "turn-race-winner",
                    "provider_name": "claude",
                    "created_at": now,
                },
                transaction_stores=transaction_stores,
            )

        indexeddb.inject_before_transaction_add(seed_conflict)
        replayed = provider_client.CreateTurn(
            _turn_request(turn_id="turn-race-loser", session_id="session-turn-race", idempotency_key="turn-race")
        )

        self.assertEqual(replayed.id, "turn-race-winner")

    def test_sdk_mcp_bridge_exposes_direct_tools_for_small_grants(self) -> None:
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

    def test_sdk_mcp_bridge_exposes_direct_tools_for_large_grants(self) -> None:
        host = _host_servicer
        assert host is not None
        host.large_catalog = True
        _configure_provider()
        runner = provider_module.provider._runner
        assert runner is not None
        options = runner._options(
            model="sonnet-session", session_id="session-claude", turn_id="turn-claude", run_grant="grant-claude"
        )

        sdk_tools = asyncio.run(_sdk_tools(options))
        visible_tools = [tool.name for tool in sdk_tools]

        self.assertEqual(len(visible_tools), 60)
        self.assertEqual(visible_tools[0], "github__operation_0")
        self.assertEqual(visible_tools[-1], "linear__operation_59")
        self.assertEqual([request["page_token"] for request in host.list_requests], [""])
        if host.metadata_supported:
            self.assertIn("Search metadata:", sdk_tools[0].description)
            self.assertIn("pr", sdk_tools[0].description)
            self.assertIn("pull request", sdk_tools[0].description)
            bridged = asyncio.run(_list_tools_json_through_sdk_bridge(options))
            bridged_description = bridged["result"]["tools"][0]["description"]
            self.assertIn("Search metadata:", bridged_description)
            self.assertIn("pr", bridged_description)
            self.assertIn("pull request", bridged_description)

        execute_result = asyncio.run(_call_sdk_tool(options, name="github__operation_0", arguments={"query": "mine"}))

        self.assertEqual(execute_result.content[0].text, '{"ok":true}')
        self.assertEqual(host.execute_requests[-1]["tool_id"], "tool-github-0")
        self.assertEqual(host.execute_requests[-1]["arguments"], {"query": "mine"})

    def test_sdk_mcp_bridge_marks_unavailable_sentinel_call_as_error(self) -> None:
        host = _host_servicer
        assert host is not None
        host.include_reconnect_sentinel = True
        _configure_provider()
        runner = provider_module.provider._runner
        assert runner is not None
        options = runner._options(
            model="sonnet-session", session_id="session-claude", turn_id="turn-claude", run_grant="grant-claude"
        )

        sdk_tools = asyncio.run(_sdk_tools(options))
        self.assertEqual([tool.name for tool in sdk_tools], ["ashby__candidate_list", "linear__reconnect_required"])

        execute_result = asyncio.run(_call_sdk_tool(options, name="linear__reconnect_required", arguments={}))

        self.assertTrue(execute_result.isError)
        self.assertEqual(execute_result.content[0].text, '{"error":{"code":"reconnect_required","plugin":"linear"}}')
        self.assertEqual(host.execute_requests[-1]["tool_id"], "tool-linear-reconnect")

    def test_sdk_mcp_bridge_returns_tool_result_for_execute_rpc_error(self) -> None:
        host = _host_servicer
        assert host is not None
        host.execute_error = "integration reconnect required: token expired and refresh failed"
        _configure_provider()
        runner = provider_module.provider._runner
        assert runner is not None
        options = runner._options(
            model="sonnet-session", session_id="session-claude", turn_id="turn-claude", run_grant="grant-claude"
        )

        asyncio.run(_sdk_tools(options))
        tool_result = asyncio.run(_call_sdk_tool(options, name="linear__issues", arguments={"query": "AIT"}))

        self.assertTrue(tool_result.isError)
        self.assertIn("integration reconnect required", tool_result.content[0].text)
        self.assertEqual(host.execute_requests[0]["tool_id"], "tool-linear-issues")

    def test_sdk_mcp_bridge_exposes_tool_discovery_error_as_diagnostic_tool(self) -> None:
        host = _host_servicer
        assert host is not None
        host.list_error = "integration reconnect required: token expired and refresh failed"
        _configure_provider()
        runner = provider_module.provider._runner
        assert runner is not None
        options = runner._options(
            model="sonnet-session", session_id="session-claude", turn_id="turn-claude", run_grant="grant-claude"
        )

        server = options.mcp_servers["gestalt"]["instance"]
        list_result = asyncio.run(server.request_handlers[mcp_types.ListToolsRequest](mcp_types.ListToolsRequest()))
        tool = list_result.root.tools[0]
        call_result = asyncio.run(
            server.request_handlers[mcp_types.CallToolRequest](
                mcp_types.CallToolRequest(params=mcp_types.CallToolRequestParams(name=tool.name, arguments={}))
            )
        )

        self.assertEqual(tool.name, "gestalt__tools_unavailable")
        self.assertIn("integration reconnect required", tool.description)
        self.assertTrue(call_result.root.isError)
        self.assertIn("integration reconnect required", call_result.root.content[0].text)

    def test_sdk_mcp_bridge_returns_tool_result_for_lookup_error(self) -> None:
        host = _host_servicer
        assert host is not None
        host.list_error = "integration reconnect required: token expired and refresh failed"
        bridge = GestaltMCPBridge(session_id="session-claude", turn_id="turn-claude", run_grant="grant-claude")

        tool_result = asyncio.run(bridge.call_tool("linear__issues", {"query": "AIT"}))

        self.assertTrue(tool_result.isError)
        self.assertIn("integration reconnect required", cast(Any, tool_result.content[0]).text)
        self.assertEqual(host.execute_requests, [])

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
    global _runtime_server, _host_server, _indexeddb_server, _runtime_socket, _host_socket, _indexeddb_socket
    global _host_servicer, _indexeddb_servicer
    global _previous_agent_host_socket, _previous_agent_host_token, _previous_indexeddb_socket

    _runtime_socket = _fresh_socket("claude-sdk-agent-runtime")
    _host_socket = _fresh_socket("claude-sdk-agent-host")
    _indexeddb_socket = _fresh_socket("claude-sdk-agent-indexeddb")
    _previous_agent_host_socket = os.environ.get(ENV_AGENT_HOST_SOCKET)
    _previous_agent_host_token = os.environ.get(ENV_AGENT_HOST_SOCKET_TOKEN)
    _previous_indexeddb_socket = os.environ.get(indexeddb_socket_env())
    os.environ[ENV_AGENT_HOST_SOCKET] = _host_socket
    os.environ[ENV_AGENT_HOST_SOCKET_TOKEN] = "relay-token"
    os.environ[indexeddb_socket_env()] = _indexeddb_socket

    _host_servicer = _FakeAgentHost()
    _host_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    agent_pb2_grpc.add_AgentHostServicer_to_server(_host_servicer, _host_server)
    _host_server.add_insecure_port(f"unix:{_host_socket}")
    _host_server.start()

    _indexeddb_servicer = FakeIndexedDB()
    _indexeddb_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    datastore_pb2_grpc.add_IndexedDBServicer_to_server(_indexeddb_servicer, _indexeddb_server)
    _indexeddb_server.add_insecure_port(f"unix:{_indexeddb_socket}")
    _indexeddb_server.start()

    _runtime_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    adapter = _runtime._servable_target(provider_module.provider, runtime_kind=ProviderKind.AGENT)
    _runtime._register_services(server=_runtime_server, servable=adapter)
    _runtime_server.add_insecure_port(f"unix:{_runtime_socket}")
    _runtime_server.start()


def tearDownModule() -> None:
    if provider_module.provider is not None:
        provider_module.provider.close()
    for server in (_runtime_server, _host_server, _indexeddb_server):
        if server is not None:
            server.stop(0)
    for path in (_runtime_socket, _host_socket, _indexeddb_socket):
        try:
            os.unlink(path)
        except OSError:
            pass
    _restore_env(ENV_AGENT_HOST_SOCKET, _previous_agent_host_socket)
    _restore_env(ENV_AGENT_HOST_SOCKET_TOKEN, _previous_agent_host_token)
    _restore_env(indexeddb_socket_env(), _previous_indexeddb_socket)


def _configure_provider(config: dict[str, Any] | None = None) -> tuple[Any, Any]:
    channel = grpc.insecure_channel(f"unix:{_runtime_socket}")
    lifecycle = runtime_pb2_grpc.ProviderLifecycleStub(channel)
    provider_client = agent_pb2_grpc.AgentProviderStub(channel)
    _configure_lifecycle(lifecycle, provider_module.provider, config)
    return lifecycle, provider_client


def _configure_lifecycle(lifecycle: Any, provider_obj: Any, config: dict[str, Any] | None = None) -> None:
    request = runtime_pb2.ConfigureProviderRequest(name="claude", protocol_version=_runtime.CURRENT_PROTOCOL_VERSION)
    request.config.update(_base_config(config))
    lifecycle.ConfigureProvider(request)
    assert provider_obj._runner is not None
    provider_obj._runner._client_factory = _FakeClaudeSDKClient


def _base_config(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    config: dict[str, Any] = {
        "defaultModel": "sonnet-config",
        "timeoutSeconds": 5,
        "permissionMode": "dontAsk",
        "anthropicApiKey": "test-anthropic-key",
    }
    if overrides:
        config.update(overrides)
    return config


def _start_provider_runtime(provider_obj: Any) -> tuple[Any, str, Any, Any, Any]:
    runtime_socket = _fresh_socket("claude-sdk-agent-runtime-extra")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    adapter = _runtime._servable_target(provider_obj, runtime_kind=ProviderKind.AGENT)
    _runtime._register_services(server=server, servable=adapter)
    server.add_insecure_port(f"unix:{runtime_socket}")
    server.start()
    channel = grpc.insecure_channel(f"unix:{runtime_socket}")
    lifecycle = runtime_pb2_grpc.ProviderLifecycleStub(channel)
    provider_client = agent_pb2_grpc.AgentProviderStub(channel)
    return server, runtime_socket, channel, lifecycle, provider_client


def _stop_runtime(provider_obj: Any, server: Any, runtime_socket: str, channel: Any) -> None:
    provider_obj.close()
    channel.close()
    server.stop(0)
    try:
        os.unlink(runtime_socket)
    except OSError:
        pass


def _make_claude_plugin(
    root: str,
    name: str,
    *,
    manifest_name: str | None = None,
    extra_manifest: dict[str, Any] | None = None,
    include_manifest_skills: bool = True,
) -> str:
    path = os.path.join(root, name)
    os.makedirs(os.path.join(path, ".claude-plugin"))
    os.makedirs(os.path.join(path, "skills"))
    manifest = {"name": manifest_name or name, "description": f"{name} test plugin"}
    if include_manifest_skills:
        manifest["skills"] = "./skills"
    if extra_manifest:
        manifest.update(extra_manifest)
    with open(os.path.join(path, ".claude-plugin", "plugin.json"), "w", encoding="utf-8") as handle:
        json.dump(manifest, handle)
    return path


def _turn_request(
    *,
    turn_id: str,
    session_id: str,
    messages: list[Any] | None = None,
    run_grant: str = "grant-claude",
    execution_ref: str = "",
    idempotency_key: str = "",
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
        idempotency_key=idempotency_key,
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
    return [tool.name for tool in await _sdk_tools(options)]


async def _sdk_tools(options: Any) -> list[Any]:
    server = options.mcp_servers["gestalt"]["instance"]
    list_result = await server.request_handlers[mcp_types.ListToolsRequest](mcp_types.ListToolsRequest())
    return list(list_result.root.tools)


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


async def _list_tools_json_through_sdk_bridge(options: Any) -> dict[str, Any]:
    from claude_agent_sdk._internal.query import Query

    bridge = py_types.SimpleNamespace(sdk_mcp_servers={"gestalt": options.mcp_servers["gestalt"]["instance"]})
    handle_request = cast(Any, Query._handle_sdk_mcp_request)
    return await handle_request(bridge, "gestalt", {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})


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
