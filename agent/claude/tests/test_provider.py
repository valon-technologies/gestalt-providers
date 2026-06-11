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
from unittest import mock

import grpc
import gestalt
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, ToolResultBlock, ToolUseBlock
from google.protobuf import json_format
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from mcp import types as mcp_types

import provider as provider_module
from gestalt import ENV_HOST_SERVICE_SOCKET, ENV_HOST_SERVICE_TOKEN, ProviderKind, _runtime
from gestalt._gen.v1 import agent_pb2 as _agent_pb2
from gestalt._gen.v1 import agent_pb2_grpc as _agent_pb2_grpc
from gestalt._gen.v1 import app_pb2 as _app_pb2
from gestalt._gen.v1 import app_pb2_grpc as _app_pb2_grpc
from gestalt._gen.v1 import runtime_pb2 as _runtime_pb2
from gestalt._gen.v1 import runtime_pb2_grpc as _runtime_pb2_grpc
import internals.claude_runner as claude_runner_module
from internals.mcp_bridge import GestaltMCPBridge, _schema_from_json
from internals.session_start import ADDITIONAL_CONTEXT_KEY, prepend_session_start_context, run_session_start_hooks
from tests.fake_indexeddb import FakeIndexedDB, indexeddb_pb2_grpc

agent_pb2: Any = cast(Any, _agent_pb2)
agent_pb2_grpc: Any = _agent_pb2_grpc
empty_pb2: Any = _empty_pb2
app_pb2: Any = cast(Any, _app_pb2)
app_pb2_grpc: Any = _app_pb2_grpc
runtime_pb2: Any = _runtime_pb2
runtime_pb2_grpc: Any = _runtime_pb2_grpc
struct_pb2: Any = _struct_pb2
AGENT_TOOL_SOURCE_MODE_NONE = provider_module.AGENT_TOOL_SOURCE_MODE_NONE
AGENT_TOOL_SOURCE_MODE_CATALOG = provider_module.AGENT_TOOL_SOURCE_MODE_CATALOG


_runtime_server: grpc.Server | None = None
_host_server: grpc.Server | None = None
_runtime_socket = ""
_host_socket = ""
_host_servicer: "_FakeHostApp | None" = None
_indexeddb_servicer: "FakeIndexedDB | None" = None
_previous_host_service_socket: str | None = None
_previous_host_service_token: str | None = None
_claude_client_patch: Any = None


class _FakeHostApp(app_pb2_grpc.AppServicer):
    def __init__(self) -> None:
        self.invoke_requests: list[dict[str, Any]] = []
        self.execute_error = ""

    def reset(self) -> None:
        self.invoke_requests.clear()
        self.execute_error = ""

    def Invoke(self, request: Any, context: grpc.ServicerContext) -> Any:
        params = json_format.MessageToDict(request.params, preserving_proto_field_name=True) if request.HasField("params") else {}
        self.invoke_requests.append(
            {
                "app": request.app,
                "operation": request.operation,
                "params": params,
            }
        )
        if self.execute_error:
            context.abort(grpc.StatusCode.UNKNOWN, self.execute_error)
        return app_pb2.OperationResult(status=200, body=b'{"ok":true}')


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

    async def __aenter__(self) -> "_FakeClaudeSDKClient":
        await self.connect()
        return self

    async def __aexit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> bool:
        await self.disconnect()
        return False

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
        if self.mode == "cancelled_result":
            yield ResultMessage(
                subtype="cancelled",
                duration_ms=0,
                duration_api_ms=0,
                is_error=True,
                num_turns=1,
                session_id=self.session_id,
                result="cancelled by sdk",
            )
            return
        if self.mode == "no_result":
            yield AssistantMessage(content=[TextBlock(text="orphan response")], model="fake-claude")
            return
        if self.mode == "text_fallback":
            yield AssistantMessage(content=[TextBlock(text="fallback assistant text")], model="fake-claude")
            yield ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id=self.session_id,
                result="",
            )
            return
        if self.mode == "tool_fallback":
            yield AssistantMessage(
                content=[
                    ToolUseBlock(id="tool-1", name="linear__issues", input={"query": "AIT"}),
                    ToolResultBlock(tool_use_id="tool-1", content="{}", is_error=False),
                ],
                model="fake-claude",
            )
            yield ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id=self.session_id,
                result="",
            )
            return
        if self.mode == "structured_success":
            assert self.options.tools == []
            assert self.options.allowed_tools == []
            assert self.options.mcp_servers == {}
            yield ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id=self.session_id,
                result="graded",
                structured_output={"score": 1, "reasoning": "correct"},
            )
            return
        if self.mode == "structured_missing":
            yield ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id=self.session_id,
                result="missing",
            )
            return
        if self.mode == "structured_list":
            yield ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id=self.session_id,
                result="list",
                structured_output=[{"score": 1}],
            )
            return
        if self.mode == "catalog_structured_success":
            assert self.options.tools == ["mcp__gestalt__*"]
            assert self.options.allowed_tools == ["mcp__gestalt__*"]
            assert set(self.options.mcp_servers.keys()) == {"gestalt"}
            assert self.options.output_format["type"] == "json_schema"
            visible_tools = await _visible_sdk_tools(self.options)
            assert visible_tools == ["ashby__candidate_list", "linear__issues", "github__pulls_list"], visible_tools
            yield ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id=self.session_id,
                result="catalog structured",
                structured_output={"answer": "done"},
            )
            return

        if "gestalt" in self.options.mcp_servers:
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
        assert _indexeddb_servicer is not None
        _host_servicer.reset()
        _indexeddb_servicer.reset()
        _FakeClaudeSDKClient.mode = "success"
        _FakeClaudeSDKClient.instances.clear()

    def test_agent_tool_schema_projection_merges_provider_hostile_combinators(self) -> None:
        schema = _schema_from_json(
            json.dumps(
                {
                    "type": ["object", "null"],
                    "properties": {"root": {"type": "string"}},
                    "required": ["root"],
                    "allOf": [{"properties": {"from_all_of": {"type": "string"}}, "required": ["from_all_of"]}],
                    "anyOf": [{"properties": {"from_any_of": {"type": "string"}}, "required": ["from_any_of"]}],
                }
            )
        )

        self.assertEqual(schema["type"], "object")
        self.assertNotIn("allOf", schema)
        self.assertNotIn("anyOf", schema)
        self.assertEqual(set(schema["properties"]), {"root", "from_all_of", "from_any_of"})
        self.assertEqual(schema["required"], ["from_all_of", "root"])

    def test_agent_tool_schema_projection_falls_back_on_conflicts(self) -> None:
        schema = _schema_from_json(
            json.dumps(
                {
                    "type": "object",
                    "properties": {"same": {"type": "string"}},
                    "allOf": [{"properties": {"same": {"type": "integer"}}}],
                }
            )
        )

        self.assertEqual(schema, {"type": "object", "properties": {}, "additionalProperties": True})

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
        session_start = gestalt.AgentSessionStartConfig(
            hooks=[
                gestalt.AgentSessionStartHook(
                    id="load-memory",
                    type="command",
                    command=[sys.executable, "-c", "print('session context from provider')"],
                    timeout="5s",
                    output=gestalt.AgentSessionStartHookOutput(additional_context=True, metadata=True),
                )
            ]
        )

        session = provider_module.provider.create_session(
            gestalt.CreateAgentProviderSessionRequest(
                idempotency_key="session-start-idem",
                session_start=session_start,
                created_by_subject_id="user-123",
                subject=_sdk_subject("user-123"),
            )
        )
        self.assertTrue(session.id)
        metadata = session.metadata or {}
        self.assertEqual(
            metadata["__gestalt.lifecycle.sessionStart.results.load-memory"]["stdout"],
            "session context from provider\n",
        )

        replay_session_start = gestalt.AgentSessionStartConfig(
            hooks=[
                gestalt.AgentSessionStartHook(
                    id="should-not-run", type="command", command=[sys.executable, "-c", "import sys; sys.exit(7)"]
                )
            ]
        )

        replay = provider_module.provider.create_session(
            gestalt.CreateAgentProviderSessionRequest(
                idempotency_key="session-start-idem",
                session_start=replay_session_start,
                created_by_subject_id="user-123",
                subject=_sdk_subject("user-123"),
            )
        )
        self.assertEqual(replay.id, session.id)

        provider_client.CreateTurn(
            _turn_request(
                turn_id="turn-session-start-context",
                session_id=session.id,
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
            session = _create_owned_session(provider_client, tools=_catalog_tool_config())
            provider_client.CreateTurn(
                _turn_request(
                    turn_id="turn-plugins",
                    session_id=session.id,
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
        session = _create_owned_session(provider_client, tools=_catalog_tool_config())
        provider_client.CreateTurn(_turn_request(turn_id="turn-skill-denied", session_id=session.id))
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
        session = _create_owned_session(provider_client, tools=_catalog_tool_config())
        provider_client.CreateTurn(_turn_request(turn_id="turn-skill-allowed", session_id=session.id))
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
            provider_client.CreateSession(_owned_session_request(metadata=metadata))
        self.assertEqual(cast(Any, create_error.exception).code(), grpc.StatusCode.INVALID_ARGUMENT)

        session = _create_owned_session(provider_client)
        with self.assertRaises(grpc.RpcError) as update_error:
            provider_client.UpdateSession(
                agent_pb2.UpdateAgentProviderSessionRequest(session_id=session.id, metadata=metadata)
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
                ({"plugins": ["relative"]}, "path must be absolute"),
                ({"plugins": [{"path": valid}]}, r"plugins\[1\] must be a local plugin path"),
                ({"plugins": [valid, valid]}, "resolve to the same path"),
                ({"plugins": [valid, duplicate_docs]}, "use the same manifest name"),
                ({"plugins": [missing_manifest]}, "must include .claude-plugin/plugin.json"),
                ({"plugins": [missing_name_manifest]}, "manifest name is required"),
                ({"plugins": [manifest_executable_component]}, "unsupported components"),
                ({"plugins": [root_executable_component]}, "unsupported root components"),
                ({"settingSources": ["workspace"]}, "settingSources entries must be one of"),
                ({"skillDiscovery": "named"}, "skillDiscovery must be one of"),
                ({"allowedTools": ["WebFetch"]}, "unsupported Claude Code tool specifier"),
                ({"allowedTools": "Read"}, "allowedTools must be a list"),
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
                ({"permissionMode": "bypassPermissions", "allowedTools": ["Read"]}, "bypassPermissions cannot be used"),
            ]
            for config, message in cases:
                with self.subTest(message=message):
                    with self.assertRaisesRegex(ValueError, message):
                        provider_module.ClaudeCodeAgentProvider().configure("claude", _base_config(config))
            provider_module.ClaudeCodeAgentProvider().configure(
                "claude", _base_config({"plugins": [metadata_manifest, default_skills_dir_manifest]})
            )

    def test_provider_completes_turn_through_agent_sdk_with_catalog_tools(self) -> None:
        host = _host_servicer
        assert host is not None
        lifecycle, provider_client = _configure_provider()
        capabilities = provider_client.GetCapabilities(agent_pb2.GetAgentProviderCapabilitiesRequest())
        self.assertEqual(
            list(capabilities.supported_tool_sources),
            [AGENT_TOOL_SOURCE_MODE_NONE, agent_pb2.AGENT_TOOL_SOURCE_MODE_CATALOG],
        )
        if hasattr(capabilities, "supports_prepared_workspace"):
            self.assertTrue(capabilities.supports_prepared_workspace)
        self.assertEqual(lifecycle.GetProviderIdentity(empty_pb2.Empty()).name, "claude")

        created = provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(
                model="sonnet-session", created_by_subject_id="user-123", tools=_catalog_tool_config()
            )
        )
        started = provider_client.CreateTurn(
            _turn_request(
                turn_id="turn-claude",
                session_id=created.id,
                messages=[agent_pb2.AgentMessage(role="user", text="List my Linear issues")],
                execution_ref="exec-claude",
            )
        )
        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)

        fetched = _wait_for_turn(provider_client, "turn-claude", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.text.text, "Claude completed")
        events = provider_client.ListTurnEvents(
            agent_pb2.ListAgentProviderTurnEventsRequest(turn_id="turn-claude", subject=_subject_context("user-123"))
        )
        self.assertEqual(
            [event.type for event in events.events], ["turn.started", "assistant.message", "turn.completed"]
        )
        self.assertEqual([event.visibility for event in events.events], ["external", "external", "external"])
        message_events = [event for event in events.events if event.type == "assistant.message"]
        self.assertNotIn("value", message_events[0].data.fields)

        self.assertEqual(len(_FakeClaudeSDKClient.instances), 1)
        fake_client = _FakeClaudeSDKClient.instances[0]
        self.assertTrue(fake_client.connected)
        self.assertTrue(fake_client.disconnected)
        self.assertEqual(fake_client.session_id, created.id)
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
        self.assertIn("Gestalt catalog tools", fake_client.options.system_prompt)
        self.assertIn("Linear", fake_client.options.system_prompt)
        self.assertIn("native tool search", fake_client.options.system_prompt)
        self.assertIn(
            "Do not infer tool availability from Claude Code built-in tools only", fake_client.options.system_prompt
        )

        self.assertEqual(host.invoke_requests[0]["app"], "linear")
        self.assertEqual(host.invoke_requests[0]["operation"], "searchIssues")
        self.assertEqual(host.invoke_requests[0]["params"], {"query": "AIT"})
        tool_result = cast(Any, fake_client.tool_result)
        self.assertEqual(tool_result.content[0].text, '{"ok":true}')
        self.assertFalse(tool_result.isError)

    def test_provider_completes_structured_none_turn_without_tools(self) -> None:
        _FakeClaudeSDKClient.mode = "structured_success"
        _, provider_client = _configure_provider()
        session_req = agent_pb2.CreateAgentProviderSessionRequest(
            model="sonnet-session", created_by_subject_id="user-123", subject=_subject_context("user-123")
        )
        if hasattr(session_req, "prepared_workspace"):
            session_req.prepared_workspace.root = "/sandbox/runtime/workspaces/session-structured"
            session_req.prepared_workspace.cwd = "/sandbox/runtime/workspaces/session-structured/repo"
        created = provider_client.CreateSession(session_req)
        schema = struct_pb2.Struct()
        schema.update(
            {
                "type": "object",
                "properties": {"score": {"type": "number"}, "reasoning": {"type": "string"}},
                "required": ["score", "reasoning"],
            }
        )
        started = provider_client.CreateTurn(
            _turn_request(
                turn_id="turn-structured",
                session_id=created.id,
                messages=[agent_pb2.AgentMessage(role="user", text="Grade the answer")],
                output_schema=schema,
            )
        )
        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)

        fetched = _wait_for_turn(provider_client, "turn-structured", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.structured.text, "graded")
        self.assertEqual(fetched.structured.value.fields["score"].number_value, 1)
        self.assertEqual(fetched.structured.value.fields["reasoning"].string_value, "correct")
        summary = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id=created.id,
                turn_ids=["turn-structured"],
                subject=_subject_context("user-123"),
                summary_only=True,
            )
        )
        self.assertIsNone(summary.turns[0].WhichOneof("output"))

        fake_client = _FakeClaudeSDKClient.instances[0]
        self.assertEqual(fake_client.options.tools, [])
        self.assertEqual(fake_client.options.allowed_tools, [])
        self.assertEqual(fake_client.options.mcp_servers, {})
        self.assertIsNone(fake_client.options.can_use_tool)
        self.assertEqual(fake_client.options.setting_sources, [])
        self.assertEqual(fake_client.options.skills, [])
        self.assertEqual(fake_client.options.plugins, [])
        self.assertNotIn("ENABLE_TOOL_SEARCH", fake_client.options.env)
        self.assertEqual(fake_client.options.cwd, "/sandbox/runtime/workspaces/session-structured/repo")
        self.assertEqual(fake_client.options.output_format["type"], "json_schema")
        self.assertEqual(fake_client.options.output_format["schema"]["type"], "object")
        self.assertIsNone(fake_client.options.system_prompt)

        events = provider_client.ListTurnEvents(
            agent_pb2.ListAgentProviderTurnEventsRequest(
                turn_id="turn-structured", subject=_subject_context("user-123")
            )
        )
        message_events = [event for event in events.events if event.type == "assistant.message"]
        self.assertEqual(message_events[0].data.fields["value"].struct_value.fields["score"].number_value, 1)

    def test_provider_allows_catalog_tools_with_schema(self) -> None:
        _FakeClaudeSDKClient.mode = "catalog_structured_success"
        _, provider_client = _configure_provider()
        session = _create_owned_session(provider_client, tools=_catalog_tool_config())
        schema = struct_pb2.Struct()
        schema.update({"type": "object", "properties": {"answer": {"type": "string"}}})

        provider_client.CreateTurn(
            _turn_request(turn_id="turn-catalog-schema", session_id=session.id, output_schema=schema)
        )

        fetched = _wait_for_turn(provider_client, "turn-catalog-schema", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.structured.text, "catalog structured")
        self.assertEqual(fetched.structured.value.fields["answer"].string_value, "done")
        fake_client = _FakeClaudeSDKClient.instances[-1]
        self.assertEqual(fake_client.session_id, session.id)
        self.assertEqual(fake_client.options.output_format["schema"]["type"], "object")
        host = _host_servicer
        assert host is not None
        self.assertEqual(host.invoke_requests, [])

    def test_slack_sessions_are_company_readable_and_owner_writable(self) -> None:
        _, provider_client = _configure_provider()
        private = provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(created_by_subject_id="user-owner")
        )
        self.assertTrue(private.id)
        slack_metadata = struct_pb2.Struct()
        slack_metadata.update(_slack_session_metadata())
        shared = provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(
                metadata=slack_metadata, created_by_subject_id="service_account:slack-bot"
            )
        )
        self.assertTrue(shared.id)

        reader_subject = _subject_context("user-reader")
        visible = provider_client.ListSessions(
            agent_pb2.ListAgentProviderSessionsRequest(subject=reader_subject, limit=10, summary_only=True)
        )
        self.assertEqual([session.id for session in visible.sessions], [shared.id])
        fetched = provider_client.GetSession(
            agent_pb2.GetAgentProviderSessionRequest(session_id=shared.id, subject=reader_subject)
        )
        self.assertEqual(fetched.id, shared.id)
        with self.assertRaises(grpc.RpcError) as private_read:
            provider_client.GetSession(
                agent_pb2.GetAgentProviderSessionRequest(session_id=private.id, subject=reader_subject)
            )
        self.assertEqual(cast(Any, private_read.exception).code(), grpc.StatusCode.NOT_FOUND)

        with self.assertRaises(grpc.RpcError) as denied_update:
            provider_client.UpdateSession(
                agent_pb2.UpdateAgentProviderSessionRequest(
                    session_id=shared.id, subject=reader_subject, state=agent_pb2.AGENT_SESSION_STATE_ARCHIVED
                )
            )
        self.assertEqual(cast(Any, denied_update.exception).code(), grpc.StatusCode.PERMISSION_DENIED)
        owner_update = provider_client.UpdateSession(
            agent_pb2.UpdateAgentProviderSessionRequest(
                session_id=shared.id,
                subject=_subject_context("service_account:slack-bot"),
                state=agent_pb2.AGENT_SESSION_STATE_ARCHIVED,
            )
        )
        self.assertEqual(owner_update.state, agent_pb2.AGENT_SESSION_STATE_ARCHIVED)

    def test_slack_session_turn_reads_are_company_readable(self) -> None:
        _, provider_client = _configure_provider()
        slack_metadata = struct_pb2.Struct()
        slack_metadata.update(_slack_session_metadata())
        shared = provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(
                metadata=slack_metadata, created_by_subject_id="service_account:slack-bot"
            )
        )
        turn_request = _turn_request(
            turn_id="turn-slack",
            session_id=shared.id,
            messages=[agent_pb2.AgentMessage(role="user", text="Company-visible turn")],
        )
        turn_request.subject.id = "service_account:slack-bot"
        turn_request.created_by_subject_id = "service_account:slack-bot"
        provider_client.CreateTurn(turn_request)
        _wait_for_turn(provider_client, "turn-slack", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        reader_subject = _subject_context("user-reader")
        fetched = provider_client.GetTurn(
            agent_pb2.GetAgentProviderTurnRequest(turn_id="turn-slack", subject=reader_subject)
        )
        self.assertEqual(fetched.id, "turn-slack")
        listed = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id=shared.id, subject=reader_subject, limit=10, summary_only=True
            )
        )
        self.assertEqual([turn.id for turn in listed.turns], ["turn-slack"])
        events = provider_client.ListTurnEvents(
            agent_pb2.ListAgentProviderTurnEventsRequest(turn_id="turn-slack", subject=reader_subject)
        )
        self.assertGreaterEqual(len(events.events), 1)
        denied_turn = _turn_request(turn_id="turn-denied", session_id=shared.id)
        denied_turn.subject.id = "user-reader"
        denied_turn.created_by_subject_id = "user-reader"
        with self.assertRaises(grpc.RpcError) as denied_create:
            provider_client.CreateTurn(denied_turn)
        self.assertEqual(cast(Any, denied_create.exception).code(), grpc.StatusCode.PERMISSION_DENIED)

    def test_provider_fails_structured_turn_without_object_output(self) -> None:
        for mode, message in [
            ("structured_missing", "did not include structured output"),
            ("structured_list", "structured output must be a JSON object"),
        ]:
            with self.subTest(mode=mode):
                _FakeClaudeSDKClient.mode = mode
                _, provider_client = _configure_provider()
                session = _create_owned_session(provider_client)
                schema = struct_pb2.Struct()
                schema.update({"type": "object"})
                provider_client.CreateTurn(
                    _turn_request(turn_id=f"turn-{mode}", session_id=session.id, output_schema=schema)
                )
                failed = _wait_for_turn(provider_client, f"turn-{mode}", agent_pb2.AGENT_EXECUTION_STATUS_FAILED)
                self.assertIn(message, failed.status_message)

    def test_runner_uses_per_turn_timeout_override(self) -> None:
        _FakeClaudeSDKClient.mode = "structured_success"
        _configure_provider()
        runner = provider_module.provider._runner
        assert runner is not None
        captured_timeouts: list[float | None] = []

        async def fake_wait_for(awaitable: Any, timeout: float | None = None) -> Any:
            captured_timeouts.append(timeout)
            return await awaitable

        with mock.patch("internals.claude_runner.asyncio.wait_for", side_effect=fake_wait_for):
            result = runner.run_turn(
                session=_test_session("session-timeout", tool_source=AGENT_TOOL_SOURCE_MODE_NONE),
                turn_id="turn-timeout",
                model="sonnet-session",
                messages=[{"role": "user", "text": "hello"}],
                request_context=None,
                schema={"type": "object"},
                timeout_seconds=1.25,
            )

        self.assertEqual(captured_timeouts, [1.25])
        assert result.structured is not None
        self.assertEqual(result.structured.value, {"score": 1, "reasoning": "correct"})
        self.assertEqual(_FakeClaudeSDKClient.instances[-1].session_id, "session-timeout")

    def test_provider_uses_request_timeout_override(self) -> None:
        if "timeout_seconds" not in agent_pb2.CreateAgentProviderTurnRequest.DESCRIPTOR.fields_by_name:
            self.skipTest("installed Gestalt SDK does not expose CreateAgentProviderTurnRequest.timeout_seconds")
        _FakeClaudeSDKClient.mode = "structured_success"
        _, provider_client = _configure_provider()
        session = _create_owned_session(provider_client)
        captured_timeouts: list[float | None] = []
        schema = struct_pb2.Struct()
        schema.update({"type": "object"})

        async def fake_wait_for(awaitable: Any, timeout: float | None = None) -> Any:
            captured_timeouts.append(timeout)
            return await awaitable

        with mock.patch("internals.claude_runner.asyncio.wait_for", side_effect=fake_wait_for):
            provider_client.CreateTurn(
                _turn_request(
                    turn_id="turn-request-timeout", session_id=session.id, output_schema=schema, timeout_seconds=2
                )
            )

        fetched = _wait_for_turn(provider_client, "turn-request-timeout", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.structured.value.fields["score"].number_value, 1)
        self.assertEqual(captured_timeouts, [2.0])

    def test_provider_launches_agent_sdk_from_prepared_workspace(self) -> None:
        if not hasattr(agent_pb2.CreateAgentProviderSessionRequest(), "prepared_workspace"):
            self.skipTest("installed gestalt-sdk does not expose prepared workspaces yet")
        _, provider_client = _configure_provider()
        request = _owned_session_request()
        request.prepared_workspace.root = "/sandbox/runtime/workspaces/session-claude-workspace"
        request.prepared_workspace.cwd = "/sandbox/runtime/workspaces/session-claude-workspace/repo"
        created = provider_client.CreateSession(request)
        provider_client.CreateTurn(
            _turn_request(
                turn_id="turn-claude-workspace",
                session_id=created.id,
                messages=[agent_pb2.AgentMessage(role="user", text="inspect repo")],
            )
        )
        _wait_for_turn(provider_client, "turn-claude-workspace", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        self.assertEqual(
            _FakeClaudeSDKClient.instances[-1].options.cwd, "/sandbox/runtime/workspaces/session-claude-workspace/repo"
        )

    def test_prepared_workspace_requires_root_and_cwd(self) -> None:
        _configure_provider()
        with self.assertRaisesRegex(gestalt.Error, "root and cwd are required"):
            provider_module.provider.create_session(
                gestalt.CreateAgentProviderSessionRequest(
                    prepared_workspace=gestalt.AgentPreparedWorkspace(root="/workspace")
                )
            )

    def test_indexeddb_persists_session_for_new_provider_instance(self) -> None:
        provider_a = provider_module.ClaudeCodeAgentProvider()
        server_a, socket_a, channel_a, lifecycle_a, client_a = _start_provider_runtime(provider_a)
        self.addCleanup(_stop_runtime, provider_a, server_a, socket_a, channel_a)
        _configure_lifecycle(lifecycle_a, provider_a)

        created = client_a.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(model="sonnet-session", created_by_subject_id="user-123")
        )
        _stop_runtime(provider_a, server_a, socket_a, channel_a)

        provider_b = provider_module.ClaudeCodeAgentProvider()
        server_b, socket_b, channel_b, lifecycle_b, client_b = _start_provider_runtime(provider_b)
        self.addCleanup(_stop_runtime, provider_b, server_b, socket_b, channel_b)
        _configure_lifecycle(lifecycle_b, provider_b)

        fetched_session = client_b.GetSession(
            agent_pb2.GetAgentProviderSessionRequest(session_id=created.id, subject=_subject_context("user-123"))
        )
        self.assertEqual(fetched_session.id, created.id)
        started = client_b.CreateTurn(
            _turn_request(
                turn_id="turn-durable",
                session_id=created.id,
                messages=[agent_pb2.AgentMessage(role="user", text="Continue after restart")],
            )
        )
        self.assertEqual(started.session_id, created.id)

        fetched_turn = client_b.GetTurn(
            agent_pb2.GetAgentProviderTurnRequest(turn_id="turn-durable", subject=_subject_context("user-123"))
        )
        self.assertEqual(fetched_turn.id, "turn-durable")
        _wait_for_turn(client_b, "turn-durable", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

    def test_indexeddb_initializes_required_object_stores_without_backfill(self) -> None:
        indexeddb = _indexeddb_servicer
        assert indexeddb is not None
        _configure_provider()
        store = provider_module.provider._store
        assert store is not None

        store.initialize()

        self.assertEqual(
            indexeddb.created_stores(),
            [
                store._run_store_name,
                store._event_store_name,
                store._session_store_name,
                store._session_projection_store_name,
                store._turn_projection_store_name,
                store._session_idempotency_store_name,
                store._turn_idempotency_store_name,
            ],
        )
        self.assertEqual(indexeddb.operation_count(store=store._session_store_name, operation="get_all"), 0)
        self.assertEqual(indexeddb.operation_count(store=store._run_store_name, operation="get_all"), 0)
        self.assertEqual(
            indexeddb.operation_count(store=store._session_projection_store_name, operation="open_cursor"), 0
        )
        self.assertEqual(indexeddb.operation_count(store=store._turn_projection_store_name, operation="open_cursor"), 0)

    def test_provider_hydrates_existing_indexeddb_records(self) -> None:
        indexeddb = _indexeddb_servicer
        assert indexeddb is not None
        _, provider_client = _configure_provider()
        store = provider_module.provider._store
        assert store is not None
        created_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        updated_at = datetime(2026, 1, 2, 4, 5, 6, tzinfo=UTC)
        completed_at = datetime(2026, 1, 2, 4, 6, 7, tzinfo=UTC)

        indexeddb.put_record(
            store._session_store_name,
            {
                "id": "session-seeded",
                "idempotency_key": "session-seeded-idem",
                "provider_name": "claude",
                "model": "sonnet-seeded",
                "client_ref": "client-seeded",
                "state": agent_pb2.AGENT_SESSION_STATE_ACTIVE,
                "metadata": {"source": "seeded", "count": 1},
                "prepared_workspace": {"root": "/workspace/session-seeded", "cwd": "/workspace/session-seeded/repo"},
                "created_by_subject_id": "user-seeded",
                "created_at": created_at,
                "updated_at": updated_at,
                "last_turn_at": completed_at,
            },
        )
        indexeddb.put_record(
            store._run_store_name,
            {
                "id": "turn-seeded",
                "session_id": "session-seeded",
                "idempotency_key": "turn-seeded-idem",
                "provider_name": "claude",
                "model": "sonnet-seeded",
                "status": agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED,
                "messages": [{"role": "user", "text": "seeded message"}],
                "output": {"text": "seeded output"},
                "status_message": "",
                "created_by_subject_id": "user-seeded",
                "created_at": updated_at,
                "started_at": updated_at,
                "completed_at": completed_at,
                "execution_ref": "exec-seeded",
            },
        )

        fetched_session = provider_client.GetSession(
            agent_pb2.GetAgentProviderSessionRequest(
                session_id="session-seeded", subject=_subject_context("user-seeded")
            )
        )
        listed_sessions = provider_client.ListSessions(
            agent_pb2.ListAgentProviderSessionsRequest(subject=_subject_context("user-seeded"))
        )
        fetched_turn = provider_client.GetTurn(
            agent_pb2.GetAgentProviderTurnRequest(turn_id="turn-seeded", subject=_subject_context("user-seeded"))
        )
        listed_turns = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id="session-seeded", subject=_subject_context("user-seeded")
            )
        )

        self.assertEqual(fetched_session.id, "session-seeded")
        self.assertEqual(fetched_session.model, "sonnet-seeded")
        self.assertEqual(fetched_session.client_ref, "client-seeded")
        self.assertEqual(fetched_session.created_by_subject_id, "user-seeded")
        self.assertEqual(json_format.MessageToDict(fetched_session.metadata), {"source": "seeded", "count": 1.0})
        self.assertEqual([session.id for session in listed_sessions.sessions], ["session-seeded"])
        self.assertEqual(fetched_turn.id, "turn-seeded")
        self.assertEqual(fetched_turn.status, agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched_turn.messages[0].role, "user")
        self.assertEqual(fetched_turn.messages[0].text, "seeded message")
        self.assertEqual(fetched_turn.text.text, "seeded output")
        self.assertEqual(fetched_turn.execution_ref, "exec-seeded")
        self.assertEqual([turn.id for turn in listed_turns.turns], ["turn-seeded"])

    def test_list_paths_stream_projection_records_through_provider_rpc(self) -> None:
        indexeddb = _indexeddb_servicer
        assert indexeddb is not None
        _, provider_client = _configure_provider()
        store = provider_module.provider._store
        assert store is not None

        session_ids: dict[str, str] = {}
        for suffix in ("a", "b"):
            metadata = struct_pb2.Struct()
            metadata.update({"large": "x" * 1024, "suffix": suffix})
            session_req = agent_pb2.CreateAgentProviderSessionRequest(
                idempotency_key=f"session-idem-stream-{suffix}", metadata=metadata, created_by_subject_id="user-123"
            )
            if hasattr(session_req, "prepared_workspace"):
                session_req.prepared_workspace.root = f"/workspaces/session-stream-{suffix}"
                session_req.prepared_workspace.cwd = f"/workspaces/session-stream-{suffix}/repo"
            session_ids[suffix] = provider_client.CreateSession(session_req).id
            provider_client.CreateTurn(
                _turn_request(
                    turn_id=f"turn-stream-{suffix}",
                    session_id=session_ids[suffix],
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
            agent_pb2.ListAgentProviderSessionsRequest(subject=_subject_context("user-123"), limit=1, summary_only=True)
        )
        full_sessions = provider_client.ListSessions(
            agent_pb2.ListAgentProviderSessionsRequest(subject=_subject_context("user-123"), limit=1)
        )
        summary_turns = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id=session_ids["a"], subject=_subject_context("user-123"), limit=1, summary_only=True
            )
        )
        full_turns = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id=session_ids["a"], subject=_subject_context("user-123"), limit=1
            )
        )
        running_turns = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id=session_ids["a"],
                subject=_subject_context("user-123"),
                status=agent_pb2.AGENT_EXECUTION_STATUS_RUNNING,
                limit=10,
                summary_only=True,
            )
        )
        succeeded_turns = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id=session_ids["a"],
                subject=_subject_context("user-123"),
                status=agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED,
                limit=10,
                summary_only=True,
            )
        )
        exact_session_without_subject = provider_client.ListSessions(
            agent_pb2.ListAgentProviderSessionsRequest(session_ids=[session_ids["b"]], limit=10, summary_only=True)
        )
        subject_mismatch_exact_session = provider_client.ListSessions(
            agent_pb2.ListAgentProviderSessionsRequest(
                subject=_subject_context("user-456"), session_ids=[session_ids["b"]], limit=10, summary_only=True
            )
        )
        exact_turn_without_subject = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(turn_ids=["turn-stream-a"], limit=10, summary_only=True)
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
                session_id=session_ids["b"],
                state=agent_pb2.AGENT_SESSION_STATE_ARCHIVED,
                subject=_subject_context("user-123"),
            )
        )
        active_sessions = provider_client.ListSessions(
            agent_pb2.ListAgentProviderSessionsRequest(
                subject=_subject_context("user-123"),
                state=agent_pb2.AGENT_SESSION_STATE_ACTIVE,
                limit=10,
                summary_only=True,
            )
        )
        archived_sessions = provider_client.ListSessions(
            agent_pb2.ListAgentProviderSessionsRequest(
                subject=_subject_context("user-123"),
                state=agent_pb2.AGENT_SESSION_STATE_ARCHIVED,
                limit=10,
                summary_only=True,
            )
        )

        self.assertEqual([session.id for session in summary_sessions.sessions], [session_ids["b"]])
        self.assertFalse(summary_sessions.sessions[0].HasField("metadata"))
        self.assertEqual([session.id for session in full_sessions.sessions], [session_ids["b"]])
        self.assertEqual(full_sessions.sessions[0].metadata.fields["suffix"].string_value, "b")
        self.assertEqual([turn.id for turn in summary_turns.turns], ["turn-stream-a"])
        self.assertEqual(len(summary_turns.turns[0].messages), 0)
        self.assertIsNone(summary_turns.turns[0].WhichOneof("output"))
        self.assertEqual([turn.id for turn in full_turns.turns], ["turn-stream-a"])
        self.assertEqual(full_turns.turns[0].messages[0].text, "stream a")
        self.assertEqual(full_turns.turns[0].text.text, "Claude completed")
        self.assertEqual(list(running_turns.turns), [])
        self.assertEqual([turn.id for turn in succeeded_turns.turns], ["turn-stream-a"])
        self.assertEqual(list(exact_session_without_subject.sessions), [])
        self.assertEqual(list(subject_mismatch_exact_session.sessions), [])
        self.assertEqual(list(exact_turn_without_subject.turns), [])
        self.assertEqual([session.id for session in active_sessions.sessions], [session_ids["a"]])
        self.assertEqual([session.id for session in archived_sessions.sessions], [session_ids["b"]])
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
        previous_socket = os.environ.get(ENV_HOST_SERVICE_SOCKET)
        os.environ[ENV_HOST_SERVICE_SOCKET] = missing_socket

        try:
            lifecycle, provider_client = _configure_provider()
            identity = lifecycle.GetProviderIdentity(empty_pb2.Empty())
            self.assertEqual(identity.name, "claude")

            with self.assertRaises(grpc.RpcError) as raised:
                provider_client.CreateSession(_owned_session_request())
        finally:
            if previous_socket is None:
                os.environ.pop(ENV_HOST_SERVICE_SOCKET, None)
            else:
                os.environ[ENV_HOST_SERVICE_SOCKET] = previous_socket
            if os.path.exists(missing_socket):
                os.remove(missing_socket)

        error = cast(Any, raised.exception)
        self.assertEqual(error.code(), grpc.StatusCode.FAILED_PRECONDITION)
        self.assertIn("IndexedDB host socket binding", error.details())

    def test_turn_idempotency_key_is_scoped_to_session(self) -> None:
        _, provider_client = _configure_provider()
        session_a = _create_owned_session(provider_client, idempotency_key="idem-a")
        session_b = _create_owned_session(provider_client, idempotency_key="idem-b")

        first = provider_client.CreateTurn(
            _turn_request(turn_id="turn-idem-a", session_id=session_a.id, idempotency_key="repeatable")
        )
        replay = provider_client.CreateTurn(
            _turn_request(turn_id="turn-idem-a-replay", session_id=session_a.id, idempotency_key="repeatable")
        )
        second_session = provider_client.CreateTurn(
            _turn_request(turn_id="turn-idem-b", session_id=session_b.id, idempotency_key="repeatable")
        )

        self.assertEqual(first.id, "turn-idem-a")
        self.assertEqual(replay.id, "turn-idem-a")
        self.assertEqual(second_session.id, "turn-idem-b")
        _wait_for_turn(provider_client, "turn-idem-a", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        _wait_for_turn(provider_client, "turn-idem-b", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)


    def test_create_session_replays_same_subject_and_key(self) -> None:
        _, provider_client = _configure_provider()
        first = _create_owned_session(provider_client, idempotency_key="obligation-key")
        replay = _create_owned_session(provider_client, idempotency_key="obligation-key")
        self.assertEqual(replay.id, first.id)



    def test_create_session_same_key_different_subject_creates_distinct_sessions(self) -> None:
        _, provider_client = _configure_provider()
        first = _create_owned_session(provider_client, idempotency_key="shared-key")
        other = provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(
                idempotency_key="shared-key",
                created_by_subject_id="user-456",
                subject=_subject_context("user-456"),
            )
        )
        self.assertNotEqual(other.id, first.id)

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
                    "created_by_subject_id": "user-123",
                    "created_at": now,
                    "updated_at": now,
                    "last_turn_at": None,
                },
                transaction_stores=transaction_stores,
            )
            db.put_record(
                store._session_idempotency_store_name,
                {
                    "id": "user-123\x1fsession-race",
                    "session_id": "session-race-winner",
                    "provider_name": "claude",
                    "created_at": now,
                },
                transaction_stores=transaction_stores,
            )

        indexeddb.inject_before_transaction_add(seed_conflict)
        replayed = provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(
                idempotency_key="session-race",
                created_by_subject_id="user-123",
                subject=_subject_context("user-123"),
            )
        )

        self.assertEqual(replayed.id, "session-race-winner")

    def test_turn_idempotency_replays_after_indexeddb_add_conflict(self) -> None:
        indexeddb = _indexeddb_servicer
        assert indexeddb is not None
        _, provider_client = _configure_provider()
        store = provider_module.provider._store
        assert store is not None
        race_session = _create_owned_session(provider_client)
        race_session_id = race_session.id

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
                    "session_id": race_session_id,
                    "idempotency_key": "turn-race",
                    "provider_name": "claude",
                    "model": "sonnet-config",
                    "status": agent_pb2.AGENT_EXECUTION_STATUS_RUNNING,
                    "messages": [{"role": "user", "text": "winner"}],
                    "output": None,
                    "status_message": "",
                    "created_by_subject_id": "user-123",
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
                    "id": f"{race_session_id}\x1fturn-race",
                    "session_id": race_session_id,
                    "idempotency_key": "turn-race",
                    "turn_id": "turn-race-winner",
                    "provider_name": "claude",
                    "created_at": now,
                },
                transaction_stores=transaction_stores,
            )

        indexeddb.inject_before_transaction_add(seed_conflict)
        replayed = provider_client.CreateTurn(
            _turn_request(turn_id="turn-race-loser", session_id=race_session_id, idempotency_key="turn-race")
        )

        self.assertEqual(replayed.id, "turn-race-winner")

    def test_sdk_mcp_bridge_exposes_direct_tools_for_small_scopes(self) -> None:
        _configure_provider()
        runner = provider_module.provider._runner
        assert runner is not None
        options = _catalog_options(runner)

        listed = asyncio.run(_list_tools_json_through_sdk_bridge(options))

        self.assertEqual(
            [tool["name"] for tool in listed["result"]["tools"]],
            ["ashby__candidate_list", "linear__issues", "github__pulls_list"],
        )
        self.assertNotIn("nextCursor", listed["result"])

    def test_sdk_mcp_bridge_skips_non_app_catalog_tools(self) -> None:
        _configure_provider()
        runner = provider_module.provider._runner
        assert runner is not None
        config = _catalog_tool_config()
        system_tool = config.catalog.tools.add()
        system_tool.id = "tool-workflow-definitions-list"
        system_tool.mcp_name = "workflow__definitions_list"
        system_tool.title = "List workflow definitions"
        setattr(system_tool.ref, "system", "workflow")
        setattr(system_tool.ref, "operation", "definitions.list")
        options = _catalog_options(runner, listed_tools=list(config.catalog.tools))

        listed = asyncio.run(_list_tools_json_through_sdk_bridge(options))

        self.assertEqual(
            [tool["name"] for tool in listed["result"]["tools"]],
            ["ashby__candidate_list", "linear__issues", "github__pulls_list"],
        )

    def test_sdk_mcp_bridge_exposes_direct_tools_for_large_scopes(self) -> None:
        host = _host_servicer
        assert host is not None
        _configure_provider()
        runner = provider_module.provider._runner
        assert runner is not None
        options = _catalog_options(runner, listed_tools=list(_large_catalog_tool_config().catalog.tools))

        sdk_tools = asyncio.run(_sdk_tools(options))
        visible_tools = [tool.name for tool in sdk_tools]

        self.assertEqual(len(visible_tools), 60)
        self.assertEqual(visible_tools[0], "github__operation_0")
        self.assertEqual(visible_tools[-1], "linear__operation_59")
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
        self.assertEqual(host.invoke_requests[-1]["app"], "github")
        self.assertEqual(host.invoke_requests[-1]["operation"], "operation0")
        self.assertEqual(host.invoke_requests[-1]["params"], {"query": "mine"})

    def test_sdk_mcp_bridge_returns_tool_result_for_host_execution_error(self) -> None:
        host = _host_servicer
        assert host is not None
        host.execute_error = "integration reconnect required: token expired and refresh failed"
        _configure_provider()
        runner = provider_module.provider._runner
        assert runner is not None
        options = _catalog_options(runner)

        asyncio.run(_sdk_tools(options))
        tool_result = asyncio.run(_call_sdk_tool(options, name="linear__issues", arguments={"query": "AIT"}))

        self.assertTrue(tool_result.isError)
        self.assertIn("integration reconnect required", tool_result.content[0].text)
        self.assertEqual(host.invoke_requests[0]["app"], "linear")

    def test_sdk_mcp_bridge_exposes_tool_discovery_error_as_diagnostic_tool(self) -> None:
        _configure_provider()
        runner = provider_module.provider._runner
        assert runner is not None
        bad_config = _catalog_tool_config()
        duplicate = bad_config.catalog.tools.add()
        duplicate.CopyFrom(bad_config.catalog.tools[0])
        options = _catalog_options(runner, listed_tools=list(bad_config.catalog.tools))

        server = options.mcp_servers["gestalt"]["instance"]
        list_result = asyncio.run(server.request_handlers[mcp_types.ListToolsRequest](mcp_types.ListToolsRequest()))
        tool = list_result.root.tools[0]
        call_result = asyncio.run(
            server.request_handlers[mcp_types.CallToolRequest](
                mcp_types.CallToolRequest(params=mcp_types.CallToolRequestParams(name=tool.name, arguments={}))
            )
        )

        self.assertEqual(tool.name, "gestalt__tools_unavailable")
        self.assertIn("duplicate mcp_name", tool.description)
        self.assertTrue(call_result.root.isError)
        self.assertIn("duplicate mcp_name", call_result.root.content[0].text)

    def test_sdk_mcp_bridge_returns_tool_result_for_lookup_error(self) -> None:
        host = _host_servicer
        assert host is not None
        bridge = GestaltMCPBridge(
            turn_id="turn-claude",
            request_context=_request_context("user-123"),
            listed_tools=list(_catalog_tool_config().catalog.tools),
        )

        tool_result = asyncio.run(bridge.call_tool("missing__tool", {"query": "AIT"}))

        self.assertTrue(tool_result.isError)
        self.assertIn("not available in the current tool scope", cast(Any, tool_result.content[0]).text)
        self.assertEqual(host.invoke_requests, [])

    def test_create_turn_rejects_unsupported_tool_contract_inputs(self) -> None:
        _, provider_client = _configure_provider()
        session_with_tools = _create_owned_session(provider_client, tools=_catalog_tool_config())
        session_no_tools = _create_owned_session(provider_client)

        missing_context = _turn_request(
            turn_id="turn-missing-context", session_id=session_with_tools.id, include_context=False
        )
        _assert_invalid(provider_client, missing_context, "request context is required")

        empty_schema = _turn_request(
            turn_id="turn-empty-response-schema",
            session_id=session_no_tools.id,
            output_schema=struct_pb2.Struct(),
        )
        _assert_invalid(provider_client, empty_schema, "output.structured.schema")

        scalar_schema = struct_pb2.Struct()
        scalar_schema.update({"type": "array"})
        bad_schema = _turn_request(
            turn_id="turn-response-schema", session_id=session_no_tools.id, output_schema=scalar_schema
        )
        _assert_invalid(provider_client, bad_schema, "output.structured.schema.type must be")

        model_options = struct_pb2.Struct()
        model_options.update({"temperature": 0.2})
        bad_options = _turn_request(
            turn_id="turn-provider-options", session_id=session_with_tools.id, model_options=model_options
        )
        _assert_invalid(provider_client, bad_options, "model_options are not supported")

        none_without_schema = _turn_request(
            turn_id="turn-none-without-schema", session_id=session_no_tools.id
        )
        _FakeClaudeSDKClient.mode = "text_fallback"
        self.assertEqual(
            provider_client.CreateTurn(none_without_schema).status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING
        )
        fetched = _wait_for_turn(
            provider_client, "turn-none-without-schema", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED
        )
        self.assertEqual(fetched.text.text, "fallback assistant text")

    def test_cancel_turn_interrupts_sdk_client_and_terminal_status_wins(self) -> None:
        _FakeClaudeSDKClient.mode = "cancel"
        _, provider_client = _configure_provider()
        created = _create_owned_session(provider_client)
        provider_client.CreateTurn(_turn_request(turn_id="turn-cancel", session_id=created.id))
        _wait_for_fake_client()

        canceled = provider_client.CancelTurn(
            agent_pb2.CancelAgentProviderTurnRequest(
                turn_id="turn-cancel", reason="test cancellation", subject=_subject_context("user-123")
            )
        )
        self.assertEqual(canceled.status, agent_pb2.AGENT_EXECUTION_STATUS_CANCELED)
        fetched = _wait_for_turn(provider_client, "turn-cancel", agent_pb2.AGENT_EXECUTION_STATUS_CANCELED)
        self.assertEqual(fetched.status_message, "test cancellation")
        self.assertTrue(_FakeClaudeSDKClient.instances[0].interrupted)

        time.sleep(0.1)
        fetched_again = provider_client.GetTurn(
            agent_pb2.GetAgentProviderTurnRequest(turn_id="turn-cancel", subject=_subject_context("user-123"))
        )
        self.assertEqual(fetched_again.status, agent_pb2.AGENT_EXECUTION_STATUS_CANCELED)

    def test_sdk_failure_marks_turn_failed(self) -> None:
        _FakeClaudeSDKClient.mode = "failure"
        _, provider_client = _configure_provider()
        created = _create_owned_session(provider_client)
        provider_client.CreateTurn(_turn_request(turn_id="turn-failure", session_id=created.id))

        failed = _wait_for_turn(provider_client, "turn-failure", agent_pb2.AGENT_EXECUTION_STATUS_FAILED)
        self.assertIn("boom", failed.status_message)

    def test_sdk_empty_result_falls_back_to_assistant_text(self) -> None:
        _FakeClaudeSDKClient.mode = "text_fallback"
        _, provider_client = _configure_provider()
        created = _create_owned_session(provider_client)
        provider_client.CreateTurn(_turn_request(turn_id="turn-text-fallback", session_id=created.id))

        turn = _wait_for_turn(provider_client, "turn-text-fallback", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(turn.text.text, "fallback assistant text")

    def test_sdk_empty_result_falls_back_to_tool_json(self) -> None:
        _FakeClaudeSDKClient.mode = "tool_fallback"
        _, provider_client = _configure_provider()
        created = _create_owned_session(provider_client)
        provider_client.CreateTurn(_turn_request(turn_id="turn-tool-fallback", session_id=created.id))

        turn = _wait_for_turn(provider_client, "turn-tool-fallback", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        use_line, result_line = turn.text.text.splitlines()
        self.assertEqual(
            json.loads(use_line), {"tool_use": {"id": "tool-1", "input": {"query": "AIT"}, "name": "linear__issues"}}
        )
        self.assertEqual(
            json.loads(result_line), {"tool_result": {"content": "{}", "is_error": False, "tool_use_id": "tool-1"}}
        )

    def test_sdk_response_without_result_marks_turn_failed(self) -> None:
        _FakeClaudeSDKClient.mode = "no_result"
        _, provider_client = _configure_provider()
        created = _create_owned_session(provider_client)
        provider_client.CreateTurn(_turn_request(turn_id="turn-no-result", session_id=created.id))

        failed = _wait_for_turn(provider_client, "turn-no-result", agent_pb2.AGENT_EXECUTION_STATUS_FAILED)
        self.assertIn("ended without a result", failed.status_message)

    def test_sdk_cancelled_result_marks_turn_canceled(self) -> None:
        _FakeClaudeSDKClient.mode = "cancelled_result"
        _, provider_client = _configure_provider()
        created = provider_client.CreateSession(_owned_session_request())
        provider_client.CreateTurn(
            _turn_request(turn_id="turn-cancelled-result", session_id=created.id)
        )

        canceled = _wait_for_turn(provider_client, "turn-cancelled-result", agent_pb2.AGENT_EXECUTION_STATUS_CANCELED)
        self.assertEqual(canceled.status_message, "cancelled by sdk")


def setUpModule() -> None:
    global _runtime_server, _host_server, _runtime_socket, _host_socket
    global _host_servicer, _indexeddb_servicer
    global _previous_host_service_socket, _previous_host_service_token, _claude_client_patch

    _runtime_socket = _fresh_socket("claude-sdk-agent-runtime")
    _host_socket = _fresh_socket("claude-sdk-agent-host")
    _previous_host_service_socket = os.environ.get(ENV_HOST_SERVICE_SOCKET)
    _previous_host_service_token = os.environ.get(ENV_HOST_SERVICE_TOKEN)
    os.environ[ENV_HOST_SERVICE_SOCKET] = _host_socket
    os.environ[ENV_HOST_SERVICE_TOKEN] = "relay-token"
    _claude_client_patch = mock.patch.object(claude_runner_module, "ClaudeSDKClient", _FakeClaudeSDKClient)
    _claude_client_patch.start()

    _host_servicer = _FakeHostApp()
    _indexeddb_servicer = FakeIndexedDB()
    _host_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    app_pb2_grpc.add_AppServicer_to_server(_host_servicer, _host_server)
    indexeddb_pb2_grpc.add_IndexedDBServicer_to_server(_indexeddb_servicer, _host_server)
    _host_server.add_insecure_port(f"unix:{_host_socket}")
    _host_server.start()

    _runtime_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    adapter = _runtime._servable_target(provider_module.provider, runtime_kind=ProviderKind.AGENT)
    _runtime._register_services(server=_runtime_server, servable=adapter)
    _runtime_server.add_insecure_port(f"unix:{_runtime_socket}")
    _runtime_server.start()


def tearDownModule() -> None:
    global _claude_client_patch
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
    _restore_env(ENV_HOST_SERVICE_SOCKET, _previous_host_service_socket)
    _restore_env(ENV_HOST_SERVICE_TOKEN, _previous_host_service_token)
    if _claude_client_patch is not None:
        _claude_client_patch.stop()
        _claude_client_patch = None


def _configure_provider(config: dict[str, Any] | None = None) -> tuple[Any, Any]:
    channel = grpc.insecure_channel(f"unix:{_runtime_socket}")
    lifecycle = runtime_pb2_grpc.ProviderLifecycleStub(channel)
    provider_client = agent_pb2_grpc.AgentStub(channel)
    _configure_lifecycle(lifecycle, provider_module.provider, config)
    return lifecycle, provider_client


def _configure_lifecycle(lifecycle: Any, provider_obj: Any, config: dict[str, Any] | None = None) -> None:
    request = runtime_pb2.ConfigureProviderRequest(name="claude", protocol_version=_runtime.CURRENT_PROTOCOL_VERSION)
    request.config.update(_base_config(config))
    lifecycle.ConfigureProvider(request)
    assert provider_obj._runner is not None


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
    provider_client = agent_pb2_grpc.AgentStub(channel)
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
    execution_ref: str = "",
    idempotency_key: str = "",
    output_schema: Any | None = None,
    model_options: Any | None = None,
    timeout_seconds: int = 0,
    include_context: bool = True,
) -> Any:
    request = agent_pb2.CreateAgentProviderTurnRequest(
        turn_id=turn_id,
        session_id=session_id,
        messages=messages or [agent_pb2.AgentMessage(role="user", text="List my Linear issues")],
        execution_ref=execution_ref,
        idempotency_key=idempotency_key,
        created_by_subject_id="user-123",
        subject=_subject_context("user-123"),
    )
    if include_context:
        request.context.subject.CopyFrom(_subject_context("user-123"))
    if timeout_seconds:
        request.timeout_seconds = timeout_seconds
    if output_schema is None:
        request.output.text.SetInParent()
    else:
        request.output.structured.schema.CopyFrom(output_schema)
    if model_options is not None:
        request.model_options.CopyFrom(model_options)
    return request


def _owned_session_request(**kwargs: Any) -> Any:
    return agent_pb2.CreateAgentProviderSessionRequest(
        created_by_subject_id="user-123", subject=_subject_context("user-123"), **kwargs
    )


def _catalog_tool_config() -> Any:
    config = agent_pb2.AgentToolConfig()
    ashby = config.catalog.refs.add()
    ashby.app = "ashby"
    ashby.operation = "candidate.list"
    _add_tool(
        config.catalog,
        tool_id="tool-ashby-candidates",
        mcp_name="ashby__candidate_list",
        app="ashby",
        operation="candidate.list",
        title="List Ashby candidates",
        description="List Ashby candidates",
    )
    linear = config.catalog.refs.add()
    linear.app = "linear"
    linear.operation = "searchIssues"
    _add_tool(
        config.catalog,
        tool_id="tool-linear-issues",
        mcp_name="linear__issues",
        app="linear",
        operation="searchIssues",
        title="Search Linear issues",
        description="Search Linear issues by text",
        input_schema='{"type":"object","properties":{"query":{"type":"string"}}}',
    )
    github = config.catalog.refs.add()
    github.app = "github"
    github.operation = "pulls/list"
    _add_tool(
        config.catalog,
        tool_id="tool-github-pulls",
        mcp_name="github__pulls_list",
        app="github",
        operation="pulls/list",
        title="List GitHub pull requests",
        description="List pull requests from GitHub",
        tags=["pr", "prs"],
        search_text="github pull request repository owner number",
    )
    return config


def _large_catalog_tool_config() -> Any:
    config = agent_pb2.AgentToolConfig()
    for index in range(60):
        plugin = "github" if index % 2 == 0 else "linear"
        ref = config.catalog.refs.add()
        ref.app = plugin
        ref.operation = f"operation{index}"
        _add_tool(
            config.catalog,
            tool_id=f"tool-{plugin}-{index}",
            mcp_name=f"{plugin}__operation_{index}",
            app=plugin,
            operation=f"operation{index}",
            title=f"{plugin.title()} operation {index}",
            description=f"{plugin.title()} catalog operation {index}",
            input_schema='{"type":"object","properties":{"query":{"type":"string"}}}',
            tags=["pr", "prs"] if plugin == "github" else [],
            search_text="github pull request repository owner number" if plugin == "github" else "",
        )
    return config


def _create_owned_session(provider_client: Any, **kwargs: Any) -> Any:
    return provider_client.CreateSession(_owned_session_request(**kwargs))


def _add_tool(
    response: Any,
    *,
    tool_id: str,
    mcp_name: str,
    app: str,
    operation: str,
    title: str,
    description: str,
    input_schema: str = '{"type":"object"}',
    tags: list[str] | None = None,
    search_text: str = "",
) -> None:
    tool = response.tools.add()
    tool.id = tool_id
    tool.mcp_name = mcp_name
    tool.title = title
    tool.description = description
    tool.input_schema = input_schema
    tool.tags.extend(tags or [])
    tool.search_text = search_text
    setattr(tool.annotations, "read_only_hint", True)
    setattr(tool.ref, "app", app)
    setattr(tool.ref, "operation", operation)


def _subject_context(subject_id: str) -> Any:
    return app_pb2.SubjectContext(id=subject_id)


def _request_context(subject_id: str) -> Any:
    return app_pb2.RequestContext(subject=_subject_context(subject_id))


def _sdk_subject(subject_id: str) -> Any:
    return gestalt.Subject(id=subject_id)


def _catalog_options(runner: Any, *, listed_tools: list[Any] | None = None) -> Any:
    if listed_tools is None:
        listed_tools = list(_catalog_tool_config().catalog.tools)
    return runner._options(
        model="sonnet-session",
        session=_test_session("session-claude", listed_tools=listed_tools),
        turn_id="turn-claude",
        request_context=_request_context("user-123"),
    )


def _test_session(
    session_id: str,
    *,
    tool_source: int = AGENT_TOOL_SOURCE_MODE_CATALOG,
    listed_tools: list[Any] | None = None,
    metadata: dict[str, Any] | None = None,
    prepared_workspace: dict[str, str] | None = None,
) -> Any:
    return py_types.SimpleNamespace(
        session_id=session_id,
        tool_source=tool_source,
        listed_tools=list(listed_tools or []),
        metadata=dict(metadata or {}),
        prepared_workspace=prepared_workspace,
    )


def _slack_session_metadata() -> dict[str, Any]:
    return {
        "slack": {
            "team_id": "T123",
            "channel_id": "C789",
            "channel_type": "channel",
            "root_message_ts": "1712161829.000300",
            "session_ref": "slack:T123:C789:1712161829.000300",
        }
    }


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
    assert message in error.details(), error.details()


def _wait_for_turn(provider_client: Any, turn_id: str, status: int) -> Any:
    deadline = time.time() + 5
    while time.time() < deadline:
        turn = provider_client.GetTurn(
            agent_pb2.GetAgentProviderTurnRequest(turn_id=turn_id, subject=_subject_context("user-123"))
        )
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
