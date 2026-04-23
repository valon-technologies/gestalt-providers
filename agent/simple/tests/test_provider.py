import json
import os
import tempfile
import threading
import time
import unittest
from concurrent import futures
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import json_format
from google.protobuf import struct_pb2 as _struct_pb2

import provider as provider_module
from gestalt import ENV_AGENT_HOST_SOCKET, ProviderKind, _runtime, indexeddb_socket_env
from gestalt.gen.v1 import agent_pb2 as _agent_pb2
from gestalt.gen.v1 import agent_pb2_grpc as _agent_pb2_grpc
from gestalt.gen.v1 import datastore_pb2 as _datastore_pb2
from gestalt.gen.v1 import datastore_pb2_grpc as _datastore_pb2_grpc
from gestalt.gen.v1 import runtime_pb2 as _runtime_pb2
from gestalt.gen.v1 import runtime_pb2_grpc as _runtime_pb2_grpc

agent_pb2: Any = _agent_pb2
agent_pb2_grpc: Any = _agent_pb2_grpc
datastore_pb2: Any = _datastore_pb2
datastore_pb2_grpc: Any = _datastore_pb2_grpc
empty_pb2: Any = _empty_pb2
runtime_pb2: Any = _runtime_pb2
runtime_pb2_grpc: Any = _runtime_pb2_grpc
struct_pb2: Any = _struct_pb2

_runtime_server: grpc.Server | None = None
_host_server: grpc.Server | None = None
_indexeddb_server: grpc.Server | None = None
_runtime_socket: str = ""
_host_socket: str = ""
_indexeddb_socket: str = ""
_previous_agent_host_socket: str | None = None
_previous_indexeddb_socket: str | None = None
_host_servicer: "_FakeAgentHost | None" = None


class _FakeIndexedDB(datastore_pb2_grpc.IndexedDBServicer):
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stores: dict[str, dict[str, Any]] = {}

    def CreateObjectStore(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        with self._lock:
            self._stores.setdefault(request.name, {})
        return empty_pb2.Empty()

    def DeleteObjectStore(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        with self._lock:
            self._stores.pop(request.name, None)
        return empty_pb2.Empty()

    def Get(self, request: Any, context: grpc.ServicerContext) -> Any:
        with self._lock:
            record = self._stores.get(request.store, {}).get(request.id)
            if record is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "record not found")
            return datastore_pb2.RecordResponse(record=_copy_record(record))

    def GetKey(self, request: Any, context: grpc.ServicerContext) -> Any:
        with self._lock:
            if request.id not in self._stores.get(request.store, {}):
                context.abort(grpc.StatusCode.NOT_FOUND, "record not found")
            return datastore_pb2.KeyResponse(key=request.id)

    def Add(self, request: Any, context: grpc.ServicerContext) -> Any:
        record_id = _record_id(request.record)
        with self._lock:
            store = self._stores.setdefault(request.store, {})
            if record_id in store:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, "record already exists")
            store[record_id] = _copy_record(request.record)
        return empty_pb2.Empty()

    def Put(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        with self._lock:
            self._stores.setdefault(request.store, {})[_record_id(request.record)] = _copy_record(request.record)
        return empty_pb2.Empty()

    def Delete(self, request: Any, context: grpc.ServicerContext) -> Any:
        with self._lock:
            store = self._stores.get(request.store, {})
            if request.id not in store:
                context.abort(grpc.StatusCode.NOT_FOUND, "record not found")
            del store[request.id]
        return empty_pb2.Empty()

    def Clear(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        with self._lock:
            self._stores.setdefault(request.store, {}).clear()
        return empty_pb2.Empty()

    def GetAll(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        with self._lock:
            return datastore_pb2.RecordsResponse(
                records=[_copy_record(record) for record in self._stores.get(request.store, {}).values()]
            )

    def GetAllKeys(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        with self._lock:
            return datastore_pb2.KeysResponse(keys=list(self._stores.get(request.store, {}).keys()))

    def Count(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        with self._lock:
            return datastore_pb2.CountResponse(count=len(self._stores.get(request.store, {})))


def _copy_record(record: Any) -> Any:
    copied = datastore_pb2.Record()
    copied.CopyFrom(record)
    return copied


def _record_id(record: Any) -> str:
    record_id = str(record.fields["id"].string_value or "").strip()
    if not record_id:
        raise ValueError("record id is required")
    return record_id


class _FakeAgentHost(agent_pb2_grpc.AgentHostServicer):
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.wait_until_released = threading.Event()
        self.pause_on_lookup = False

    def ExecuteTool(self, request: Any, context: grpc.ServicerContext) -> Any:
        arguments = json_format.MessageToDict(request.arguments)
        self.requests.append(
            {
                "run_id": request.run_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id,
                "arguments": arguments,
            }
        )
        if self.pause_on_lookup:
            self.wait_until_released.wait(timeout=5)
        return agent_pb2.ExecuteAgentToolResponse(status=200, body=json.dumps({"echo": arguments}))


class _FakeOpenAIChatServer:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self._responses = list(responses)
        self.requests: list[dict[str, Any]] = []
        self._server = HTTPServer(("127.0.0.1", 0), self._handler_type())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        address = self._server.server_address
        return f"http://{address[0]}:{address[1]}"

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._thread.join(timeout=5)
        self._server.server_close()

    def _handler_type(self) -> type[BaseHTTPRequestHandler]:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                if self.path != "/v1/chat/completions":
                    self.send_response(404)
                    self.end_headers()
                    return
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                outer.requests.append(json.loads(body.decode("utf-8")))
                payload = outer._responses.pop(0)
                encoded = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def log_message(self, format: str, *args: Any) -> None:
                del format, args

        return Handler


class _FakeAnthropicMessagesServer:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self._responses = list(responses)
        self.requests: list[dict[str, Any]] = []
        self._server = HTTPServer(("127.0.0.1", 0), self._handler_type())
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        address = self._server.server_address
        return f"http://{address[0]}:{address[1]}"

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._thread.join(timeout=5)
        self._server.server_close()

    def _handler_type(self) -> type[BaseHTTPRequestHandler]:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                if self.path != "/v1/messages":
                    self.send_response(404)
                    self.end_headers()
                    return
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                outer.requests.append(json.loads(body.decode("utf-8")))
                payload = outer._responses.pop(0)
                encoded = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def log_message(self, format: str, *args: Any) -> None:
                del format, args

        return Handler


def _fresh_socket(name: str) -> str:
    socket_path = os.path.join(tempfile.gettempdir(), f"{name}-{os.getpid()}.sock")
    if os.path.exists(socket_path):
        os.remove(socket_path)
    return socket_path


def _configure_provider(*, run_store: str, idempotency_store: str, default_model: str = "fast") -> tuple[Any, Any]:
    channel = grpc.insecure_channel(f"unix:{_runtime_socket}")
    lifecycle = runtime_pb2_grpc.ProviderLifecycleStub(channel)
    provider_client = agent_pb2_grpc.AgentProviderStub(channel)
    request = runtime_pb2.ConfigureProviderRequest(name="simple", protocol_version=_runtime.CURRENT_PROTOCOL_VERSION)
    json_format.ParseDict(
        {
            "runStore": run_store,
            "idempotencyStore": idempotency_store,
            "defaultModel": default_model,
            "aliases": {"fast": "openai/fake-model"},
            "maxSteps": 4,
            "timeoutSeconds": 5,
            "systemPrompt": "Be concise.",
        },
        request.config,
    )
    lifecycle.ConfigureProvider(request)
    return lifecycle, provider_client


def setUpModule() -> None:
    global _runtime_server, _host_server, _indexeddb_server, _runtime_socket, _host_socket, _indexeddb_socket
    global _previous_agent_host_socket, _previous_indexeddb_socket, _host_servicer

    _runtime_socket = _fresh_socket("simple-agent-runtime")
    _host_socket = _fresh_socket("simple-agent-host")
    _indexeddb_socket = _fresh_socket("simple-agent-indexeddb")

    _previous_indexeddb_socket = os.environ.get(indexeddb_socket_env())
    os.environ[indexeddb_socket_env()] = _indexeddb_socket

    indexeddb = _FakeIndexedDB()
    _indexeddb_server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    datastore_pb2_grpc.add_IndexedDBServicer_to_server(indexeddb, _indexeddb_server)
    _indexeddb_server.add_insecure_port(f"unix:{_indexeddb_socket}")
    _indexeddb_server.start()

    provider = provider_module.SimpleAgentRuntimeProvider()

    _runtime_server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    adapter = _runtime._servable_target(provider, runtime_kind=ProviderKind.AGENT)
    _runtime._register_services(server=_runtime_server, servable=adapter)
    _runtime_server.add_insecure_port(f"unix:{_runtime_socket}")
    _runtime_server.start()

    host = _FakeAgentHost()
    _host_servicer = host

    _host_server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    agent_pb2_grpc.add_AgentHostServicer_to_server(host, _host_server)
    _host_server.add_insecure_port(f"unix:{_host_socket}")
    _host_server.start()

    _previous_agent_host_socket = os.environ.get(ENV_AGENT_HOST_SOCKET)
    os.environ[ENV_AGENT_HOST_SOCKET] = _host_socket


def tearDownModule() -> None:
    if _previous_agent_host_socket is None:
        os.environ.pop(ENV_AGENT_HOST_SOCKET, None)
    else:
        os.environ[ENV_AGENT_HOST_SOCKET] = _previous_agent_host_socket

    if _previous_indexeddb_socket is None:
        os.environ.pop(indexeddb_socket_env(), None)
    else:
        os.environ[indexeddb_socket_env()] = _previous_indexeddb_socket

    for server in (_runtime_server, _host_server, _indexeddb_server):
        if server is not None:
            server.stop(grace=0).wait()

    for path in (_runtime_socket, _host_socket, _indexeddb_socket):
        if path and os.path.exists(path):
            os.remove(path)


class SimpleAgentProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        assert _host_servicer is not None
        _host_servicer.requests.clear()
        _host_servicer.pause_on_lookup = False
        _host_servicer.wait_until_released.set()

    def test_start_run_completes_tool_loop_and_persists_run(self) -> None:
        lifecycle, provider_client = _configure_provider(
            run_store="run_success_runs", idempotency_store="run_success_idempotency"
        )
        identity = lifecycle.GetProviderIdentity(empty_pb2.Empty())

        fake_llm = _FakeOpenAIChatServer(
            responses=[
                {
                    "id": "chatcmpl-1",
                    "object": "chat.completion",
                    "created": 1710000000,
                    "model": "fake-model",
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call-1",
                                        "type": "function",
                                        "function": {"name": "person_lookup", "arguments": '{"query":"Ada Lovelace"}'},
                                    }
                                ],
                            },
                            "finish_reason": "tool_calls",
                        }
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 3, "total_tokens": 13},
                },
                {
                    "id": "chatcmpl-2",
                    "object": "chat.completion",
                    "created": 1710000001,
                    "model": "fake-model",
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": '{"summary":"Ada Lovelace is still relevant."}',
                            },
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 14, "completion_tokens": 6, "total_tokens": 20},
                },
            ]
        )
        fake_llm.start()
        self.addCleanup(fake_llm.close)

        provider_options = struct_pb2.Struct()
        provider_options.update({"base_url": f"{fake_llm.base_url}/v1", "api_key": "test-key", "timeout": 7})

        response_schema = struct_pb2.Struct()
        response_schema.update(
            {"type": "object", "properties": {"summary": {"type": "string"}}, "required": ["summary"]}
        )

        tool_parameters = struct_pb2.Struct()
        tool_parameters.update({"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]})

        started = provider_client.StartRun(
            agent_pb2.StartAgentProviderRunRequest(
                run_id="run-success",
                idempotency_key="idem-success",
                provider_name="simple",
                model="fast",
                messages=[agent_pb2.AgentMessage(role="user", text="Who is Ada Lovelace?")],
                tools=[
                    agent_pb2.ResolvedAgentTool(
                        id="lookup",
                        name="person_lookup",
                        description="Look up a historical figure",
                        parameters_schema=tool_parameters,
                    )
                ],
                response_schema=response_schema,
                provider_options=provider_options,
                execution_ref="exec-1",
                created_by=agent_pb2.AgentActor(
                    subject_id="user-123", subject_kind="human", display_name="Ada", auth_source="session"
                ),
            )
        )

        fetched = _wait_for_run(provider_client, "run-success", agent_pb2.AGENT_RUN_STATUS_SUCCEEDED)
        listed = provider_client.ListRuns(agent_pb2.ListAgentProviderRunsRequest())

        self.assertEqual(identity.kind, runtime_pb2.ProviderKind.PROVIDER_KIND_AGENT)
        self.assertEqual(identity.name, "simple")
        self.assertEqual(list(identity.warnings), [])

        self.assertEqual(started.status, agent_pb2.AGENT_RUN_STATUS_RUNNING)
        self.assertEqual(started.model, "openai/fake-model")
        self.assertEqual(started.execution_ref, "exec-1")
        self.assertEqual(started.created_by.subject_id, "user-123")

        self.assertEqual(fetched.status, agent_pb2.AGENT_RUN_STATUS_SUCCEEDED)
        self.assertEqual(fetched.model, "openai/fake-model")
        self.assertEqual(fetched.output_text, '{"summary":"Ada Lovelace is still relevant."}')
        self.assertEqual(fetched.structured_output.fields["summary"].string_value, "Ada Lovelace is still relevant.")
        self.assertEqual(fetched.execution_ref, "exec-1")
        self.assertEqual(fetched.created_by.subject_id, "user-123")
        self.assertEqual(len(fetched.messages), 2)
        self.assertEqual(fetched.messages[1].role, "assistant")
        self.assertEqual(fetched.id, "run-success")
        self.assertEqual(len(listed.runs), 1)

        assert _host_servicer is not None
        self.assertEqual(len(_host_servicer.requests), 1)
        self.assertEqual(
            _host_servicer.requests[0],
            {
                "run_id": "run-success",
                "tool_call_id": "call-1",
                "tool_id": "lookup",
                "arguments": {"query": "Ada Lovelace"},
            },
        )

        self.assertEqual(len(fake_llm.requests), 2)
        first_request = fake_llm.requests[0]
        second_request = fake_llm.requests[1]
        self.assertEqual(first_request["model"], "fake-model")
        self.assertEqual(first_request["messages"][0]["role"], "system")
        self.assertEqual(first_request["messages"][-1]["content"], "Who is Ada Lovelace?")
        self.assertEqual(second_request["messages"][-1]["role"], "tool")
        self.assertIn("Ada Lovelace", second_request["messages"][-1]["content"])
        self.assertNotIn("name", second_request["messages"][-1])

    def test_cancel_run_marks_active_run_canceled(self) -> None:
        assert _host_servicer is not None
        _, provider_client = _configure_provider(
            run_store="run_cancel_runs", idempotency_store="run_cancel_idempotency"
        )

        fake_llm = _FakeOpenAIChatServer(
            responses=[
                {
                    "id": "chatcmpl-10",
                    "object": "chat.completion",
                    "created": 1710000010,
                    "model": "fake-model",
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call-10",
                                        "type": "function",
                                        "function": {"name": "lookup", "arguments": '{"query":"Grace Hopper"}'},
                                    }
                                ],
                            },
                            "finish_reason": "tool_calls",
                        }
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 3, "total_tokens": 13},
                }
            ]
        )
        fake_llm.start()
        self.addCleanup(fake_llm.close)

        provider_options = struct_pb2.Struct()
        provider_options.update({"base_url": f"{fake_llm.base_url}/v1", "api_key": "test-key"})

        tool_parameters = struct_pb2.Struct()
        tool_parameters.update({"type": "object", "properties": {"query": {"type": "string"}}})

        _host_servicer.pause_on_lookup = True
        _host_servicer.wait_until_released.clear()

        started_holder: dict[str, Any] = {}

        def run_start() -> None:
            started_holder["run"] = provider_client.StartRun(
                agent_pb2.StartAgentProviderRunRequest(
                    run_id="run-cancel",
                    idempotency_key="idem-cancel",
                    provider_name="simple",
                    model="fast",
                    messages=[agent_pb2.AgentMessage(role="user", text="Who is Grace Hopper?")],
                    tools=[
                        agent_pb2.ResolvedAgentTool(
                            id="lookup", description="Look up a historical figure", parameters_schema=tool_parameters
                        )
                    ],
                    provider_options=provider_options,
                )
            )

        thread = threading.Thread(target=run_start, daemon=True)
        thread.start()

        for _ in range(50):
            if _host_servicer.requests:
                break
            time.sleep(0.05)

        canceled = provider_client.CancelRun(
            agent_pb2.CancelAgentProviderRunRequest(run_id="run-cancel", reason="user canceled")
        )
        _host_servicer.wait_until_released.set()
        thread.join(timeout=5)

        started = started_holder["run"]
        fetched = provider_client.GetRun(agent_pb2.GetAgentProviderRunRequest(run_id="run-cancel"))

        self.assertEqual(canceled.status, agent_pb2.AGENT_RUN_STATUS_CANCELED)
        self.assertEqual(canceled.status_message, "user canceled")
        self.assertEqual(started.status, agent_pb2.AGENT_RUN_STATUS_RUNNING)
        self.assertEqual(fetched.status, agent_pb2.AGENT_RUN_STATUS_CANCELED)
        self.assertEqual(len(fake_llm.requests), 1)

    def test_start_run_completes_anthropic_tool_loop_and_persists_run(self) -> None:
        _, provider_client = _configure_provider(
            run_store="run_anthropic_runs", idempotency_store="run_anthropic_idempotency"
        )

        fake_anthropic = _FakeAnthropicMessagesServer(
            responses=[
                {
                    "id": "msg-1",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-fake-model",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "toolu-1",
                            "name": "person_lookup",
                            "input": {"query": "Ada Lovelace"},
                        }
                    ],
                    "stop_reason": "tool_use",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                },
                {
                    "id": "msg-2",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-fake-model",
                    "content": [{"type": "text", "text": '{"summary":"Ada Lovelace is still relevant."}'}],
                    "stop_reason": "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 20, "output_tokens": 7},
                },
            ]
        )
        fake_anthropic.start()
        self.addCleanup(fake_anthropic.close)

        provider_options = struct_pb2.Struct()
        provider_options.update({"base_url": fake_anthropic.base_url, "api_key": "test-key", "max_tokens": 256})

        response_schema = struct_pb2.Struct()
        response_schema.update(
            {"type": "object", "properties": {"summary": {"type": "string"}}, "required": ["summary"]}
        )

        tool_parameters = struct_pb2.Struct()
        tool_parameters.update({"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]})

        started = provider_client.StartRun(
            agent_pb2.StartAgentProviderRunRequest(
                run_id="run-anthropic",
                idempotency_key="idem-anthropic",
                provider_name="simple",
                model="anthropic/claude-fake-model",
                messages=[agent_pb2.AgentMessage(role="user", text="Who is Ada Lovelace?")],
                tools=[
                    agent_pb2.ResolvedAgentTool(
                        id="lookup",
                        name="person_lookup",
                        description="Look up a historical figure",
                        parameters_schema=tool_parameters,
                    )
                ],
                response_schema=response_schema,
                provider_options=provider_options,
            )
        )

        fetched = _wait_for_run(provider_client, "run-anthropic", agent_pb2.AGENT_RUN_STATUS_SUCCEEDED)

        self.assertEqual(started.status, agent_pb2.AGENT_RUN_STATUS_RUNNING)
        self.assertEqual(started.model, "anthropic/claude-fake-model")
        self.assertEqual(fetched.status, agent_pb2.AGENT_RUN_STATUS_SUCCEEDED)
        self.assertEqual(fetched.model, "anthropic/claude-fake-model")
        self.assertEqual(fetched.output_text, '{"summary":"Ada Lovelace is still relevant."}')
        self.assertEqual(fetched.structured_output.fields["summary"].string_value, "Ada Lovelace is still relevant.")

        assert _host_servicer is not None
        self.assertEqual(
            _host_servicer.requests,
            [
                {
                    "run_id": "run-anthropic",
                    "tool_call_id": "toolu-1",
                    "tool_id": "lookup",
                    "arguments": {"query": "Ada Lovelace"},
                }
            ],
        )

        self.assertEqual(len(fake_anthropic.requests), 2)
        first_request = fake_anthropic.requests[0]
        second_request = fake_anthropic.requests[1]
        self.assertEqual(first_request["model"], "claude-fake-model")
        self.assertEqual(first_request["messages"][-1]["content"], "Who is Ada Lovelace?")
        self.assertEqual(first_request["tools"][0]["name"], "person_lookup")
        self.assertIn("Return only valid JSON", first_request["system"])
        self.assertEqual(second_request["messages"][1]["role"], "assistant")
        self.assertEqual(second_request["messages"][1]["content"][0]["type"], "tool_use")
        self.assertEqual(second_request["messages"][2]["role"], "user")
        self.assertEqual(second_request["messages"][2]["content"][0]["type"], "tool_result")

    def test_start_run_keeps_openai_compatible_prefixed_models_and_legacy_nested_overrides(self) -> None:
        _, provider_client = _configure_provider(
            run_store="run_compat_runs", idempotency_store="run_compat_idempotency"
        )

        fake_llm = _FakeOpenAIChatServer(
            responses=[
                {
                    "id": "chatcmpl-20",
                    "object": "chat.completion",
                    "created": 1710000020,
                    "model": "groq/fake-model",
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": "Prefixed OpenAI-compatible models still work.",
                            },
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 11, "completion_tokens": 4, "total_tokens": 15},
                }
            ]
        )
        fake_llm.start()
        self.addCleanup(fake_llm.close)

        provider_options = struct_pb2.Struct()
        provider_options.update(
            {
                "timeout": 7,
                "litellm": {"temperature": 0.2},
                "groq": {"base_url": f"{fake_llm.base_url}/v1", "api_key": "test-key", "top_p": 0.9},
            }
        )

        started = provider_client.StartRun(
            agent_pb2.StartAgentProviderRunRequest(
                run_id="run-compat",
                idempotency_key="idem-compat",
                provider_name="simple",
                model="groq/fake-model",
                messages=[agent_pb2.AgentMessage(role="user", text="Say something brief.")],
                provider_options=provider_options,
            )
        )

        fetched = _wait_for_run(provider_client, "run-compat", agent_pb2.AGENT_RUN_STATUS_SUCCEEDED)

        self.assertEqual(started.status, agent_pb2.AGENT_RUN_STATUS_RUNNING)
        self.assertEqual(started.model, "groq/fake-model")
        self.assertEqual(fetched.status, agent_pb2.AGENT_RUN_STATUS_SUCCEEDED)
        self.assertEqual(fetched.model, "groq/fake-model")
        self.assertEqual(fetched.output_text, "Prefixed OpenAI-compatible models still work.")

        self.assertEqual(len(fake_llm.requests), 1)
        request = fake_llm.requests[0]
        self.assertEqual(request["model"], "groq/fake-model")
        self.assertEqual(request["temperature"], 0.2)
        self.assertEqual(request["top_p"], 0.9)


def _wait_for_run(provider_client: Any, run_id: str, status: int, timeout_seconds: float = 5) -> Any:
    deadline = time.time() + timeout_seconds
    last = None
    while time.time() < deadline:
        last = provider_client.GetRun(agent_pb2.GetAgentProviderRunRequest(run_id=run_id))
        if last.status == status:
            return last
        time.sleep(0.05)
    raise AssertionError(f"run {run_id!r} did not reach status {status}; last={last}")


if __name__ == "__main__":
    unittest.main()
