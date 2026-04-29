import json
import os
import socket
import tempfile
import threading
import time
import unittest
from concurrent import futures
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, cast
from unittest import mock

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
from internals.config import SimpleAgentConfig
from internals.store import SimpleRunStore, StoredTurnCheckpoint

agent_pb2: Any = cast(Any, _agent_pb2)
agent_pb2_grpc: Any = _agent_pb2_grpc
datastore_pb2: Any = _datastore_pb2
datastore_pb2_grpc: Any = _datastore_pb2_grpc
empty_pb2: Any = _empty_pb2
runtime_pb2: Any = _runtime_pb2
runtime_pb2_grpc: Any = _runtime_pb2_grpc
struct_pb2: Any = _struct_pb2

_SIMPLE_CONFIG = SimpleAgentConfig.from_dict(name="simple", raw_config={})
_SIMPLE_RUN_STORE = _SIMPLE_CONFIG.run_store
_SIMPLE_IDEMPOTENCY_STORE = _SIMPLE_CONFIG.idempotency_store
_TOOL_REQUEST_HAS_IDEMPOTENCY_KEY = "idempotency_key" in agent_pb2.ExecuteAgentToolRequest.DESCRIPTOR.fields_by_name
_BUSY_DETAILS = "rpc error: code = Internal desc = database is locked (5) (SQLITE_BUSY)"

_runtime_server: grpc.Server | None = None
_host_server: grpc.Server | None = None
_indexeddb_server: grpc.Server | None = None
_runtime_socket: str = ""
_host_socket: str = ""
_indexeddb_socket: str = ""
_previous_agent_host_socket: str | None = None
_previous_indexeddb_socket: str | None = None
_host_servicer: "_FakeAgentHost | None" = None
_indexeddb_servicer: "_FakeIndexedDB | None" = None


class _FakeIndexedDB(datastore_pb2_grpc.IndexedDBServicer):
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stores: dict[str, dict[str, Any]] = {}
        self._busy_failures: dict[tuple[str, str], int] = {}

    def fail_next_busy(self, *, store: str, operation: str, count: int = 1) -> None:
        with self._lock:
            self._busy_failures[(store, operation)] = count

    def reset(self) -> None:
        with self._lock:
            self._stores.clear()
            self._busy_failures.clear()

    def _maybe_fail_busy(self, *, store: str, operation: str, context: grpc.ServicerContext) -> None:
        if not self._take_busy_failure(store=store, operation=operation):
            return
        context.abort(grpc.StatusCode.INTERNAL, _BUSY_DETAILS)

    def _take_busy_failure(self, *, store: str, operation: str) -> bool:
        key = (store, operation)
        remaining = self._busy_failures.get(key, 0)
        if remaining <= 0:
            return False
        if remaining == 1:
            self._busy_failures.pop(key, None)
        else:
            self._busy_failures[key] = remaining - 1
        return True

    def CreateObjectStore(self, request: Any, context: grpc.ServicerContext) -> Any:
        with self._lock:
            self._maybe_fail_busy(store=request.name, operation="create_object_store", context=context)
            self._stores.setdefault(request.name, {})
        return empty_pb2.Empty()

    def DeleteObjectStore(self, request: Any, context: grpc.ServicerContext) -> Any:
        with self._lock:
            self._maybe_fail_busy(store=request.name, operation="delete_object_store", context=context)
            self._stores.pop(request.name, None)
        return empty_pb2.Empty()

    def Get(self, request: Any, context: grpc.ServicerContext) -> Any:
        with self._lock:
            self._maybe_fail_busy(store=request.store, operation="get", context=context)
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
            self._maybe_fail_busy(store=request.store, operation="add", context=context)
            store = self._stores.setdefault(request.store, {})
            if record_id in store:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, "record already exists")
            store[record_id] = _copy_record(request.record)
        return empty_pb2.Empty()

    def Put(self, request: Any, context: grpc.ServicerContext) -> Any:
        with self._lock:
            self._maybe_fail_busy(store=request.store, operation="put", context=context)
            self._stores.setdefault(request.store, {})[_record_id(request.record)] = _copy_record(request.record)
        return empty_pb2.Empty()

    def Delete(self, request: Any, context: grpc.ServicerContext) -> Any:
        with self._lock:
            self._maybe_fail_busy(store=request.store, operation="delete", context=context)
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
        with self._lock:
            self._maybe_fail_busy(store=request.store, operation="get_all", context=context)
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

    def Transaction(self, request_iterator: Any, context: grpc.ServicerContext) -> Any:
        try:
            first = next(request_iterator)
        except StopIteration:
            return
        if first.WhichOneof("msg") != "begin":
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "first transaction message must be begin")
        mode = int(first.begin.mode)
        scoped_stores = set(first.begin.stores)
        with self._lock:
            working = _copy_stores(self._stores)
            yield datastore_pb2.TransactionServerMessage(begin=datastore_pb2.TransactionBeginResponse())
            for message in request_iterator:
                kind = message.WhichOneof("msg")
                if kind == "operation":
                    response = self._apply_transaction_operation(
                        stores=working,
                        operation=message.operation,
                        scoped_stores=scoped_stores,
                        readwrite=mode == datastore_pb2.TRANSACTION_READWRITE,
                    )
                    yield datastore_pb2.TransactionServerMessage(operation=response)
                    if response.HasField("error") and response.error.code:
                        return
                    continue
                if kind == "commit":
                    self._stores = working
                    yield datastore_pb2.TransactionServerMessage(commit=datastore_pb2.TransactionCommitResponse())
                    return
                if kind == "abort":
                    yield datastore_pb2.TransactionServerMessage(abort=datastore_pb2.TransactionAbortResponse())
                    return
                response = datastore_pb2.TransactionAbortResponse()
                _set_status(response.error, grpc.StatusCode.INVALID_ARGUMENT, "unknown transaction message")
                yield datastore_pb2.TransactionServerMessage(abort=response)
                return

    def _apply_transaction_operation(
        self, *, stores: dict[str, dict[str, Any]], operation: Any, scoped_stores: set[str], readwrite: bool
    ) -> Any:
        request_id = int(operation.request_id)
        kind = operation.WhichOneof("operation")
        if not kind:
            return _transaction_error(request_id, grpc.StatusCode.INVALID_ARGUMENT, "transaction operation is required")
        request = getattr(operation, kind)
        store_name = str(getattr(request, "store", "") or "")
        if not store_name and hasattr(request, "name"):
            store_name = str(request.name or "")
        if scoped_stores and store_name not in scoped_stores:
            return _transaction_error(
                request_id, grpc.StatusCode.FAILED_PRECONDITION, "object store is outside transaction scope"
            )
        if kind in {"add", "put", "delete", "clear", "delete_range", "index_delete"} and not readwrite:
            return _transaction_error(request_id, grpc.StatusCode.FAILED_PRECONDITION, "transaction is readonly")
        if self._take_busy_failure(store=store_name, operation=kind):
            return _transaction_error(request_id, grpc.StatusCode.INTERNAL, _BUSY_DETAILS)

        store = stores.setdefault(store_name, {})
        if kind == "get":
            record = store.get(request.id)
            if record is None:
                return _transaction_error(request_id, grpc.StatusCode.NOT_FOUND, "record not found")
            return datastore_pb2.TransactionOperationResponse(
                request_id=request_id, record=datastore_pb2.RecordResponse(record=_copy_record(record))
            )
        if kind == "get_key":
            if request.id not in store:
                return _transaction_error(request_id, grpc.StatusCode.NOT_FOUND, "record not found")
            return datastore_pb2.TransactionOperationResponse(
                request_id=request_id, key=datastore_pb2.KeyResponse(key=request.id)
            )
        if kind == "add":
            record_id = _record_id(request.record)
            if record_id in store:
                return _transaction_error(request_id, grpc.StatusCode.ALREADY_EXISTS, "record already exists")
            store[record_id] = _copy_record(request.record)
            return _transaction_empty(request_id)
        if kind == "put":
            store[_record_id(request.record)] = _copy_record(request.record)
            return _transaction_empty(request_id)
        if kind == "delete":
            if request.id not in store:
                return _transaction_error(request_id, grpc.StatusCode.NOT_FOUND, "record not found")
            del store[request.id]
            return _transaction_empty(request_id)
        if kind == "clear":
            store.clear()
            return _transaction_empty(request_id)
        if kind == "get_all":
            return datastore_pb2.TransactionOperationResponse(
                request_id=request_id,
                records=datastore_pb2.RecordsResponse(records=[_copy_record(record) for record in store.values()]),
            )
        if kind == "get_all_keys":
            return datastore_pb2.TransactionOperationResponse(
                request_id=request_id, keys=datastore_pb2.KeysResponse(keys=list(store.keys()))
            )
        if kind == "count":
            return datastore_pb2.TransactionOperationResponse(
                request_id=request_id, count=datastore_pb2.CountResponse(count=len(store))
            )
        return _transaction_error(
            request_id, grpc.StatusCode.INVALID_ARGUMENT, f"unsupported transaction operation {kind}"
        )


def _copy_record(record: Any) -> Any:
    copied = datastore_pb2.Record()
    copied.CopyFrom(record)
    return copied


def _copy_stores(stores: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        name: {record_id: _copy_record(record) for record_id, record in store.items()} for name, store in stores.items()
    }


def _record_id(record: Any) -> str:
    record_id = str(record.fields["id"].string_value or "").strip()
    if not record_id:
        raise ValueError("record id is required")
    return record_id


def _transaction_error(request_id: int, code: Any, message: str) -> Any:
    response = datastore_pb2.TransactionOperationResponse(request_id=request_id)
    _set_status(response.error, code, message)
    return response


def _transaction_empty(request_id: int) -> Any:
    return datastore_pb2.TransactionOperationResponse(request_id=request_id, empty=empty_pb2.Empty())


def _set_status(status: Any, code: Any, message: str) -> None:
    status.code = int(code.value[0])
    status.message = message


class _FakeAgentHost(agent_pb2_grpc.AgentHostServicer):
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.search_requests: list[dict[str, Any]] = []
        self.tools: list[Any] = []
        self.load_ref_tools: list[Any] = []
        self.candidates: list[Any] = []
        self.has_more = False
        self.tool_responses: list[Any] = []
        self.wait_until_released = threading.Event()
        self.pause_on_lookup = False

    def SearchTools(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        search_request = {
            "session_id": request.session_id,
            "turn_id": request.turn_id,
            "query": request.query,
            "max_results": request.max_results,
        }
        if _proto_message_has_field(request, "candidate_limit"):
            search_request["candidate_limit"] = request.candidate_limit
        if _proto_message_has_field(request, "load_refs"):
            search_request["load_refs"] = [_tool_ref_to_dict(ref) for ref in request.load_refs]
        self.search_requests.append(search_request)
        tools = self.load_ref_tools if search_request.get("load_refs") else self.tools
        response = agent_pb2.SearchAgentToolsResponse(tools=list(tools))
        if _proto_message_has_field(response, "candidates") and search_request.get("candidate_limit", 0) > 0:
            response.candidates.extend(self.candidates)
        if _proto_message_has_field(response, "has_more"):
            response.has_more = self.has_more and len(response.candidates) > 0
        return response

    def ExecuteTool(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        arguments = json_format.MessageToDict(request.arguments)
        self.requests.append(
            {
                "session_id": request.session_id,
                "turn_id": request.turn_id,
                "tool_call_id": request.tool_call_id,
                "tool_id": request.tool_id,
                "arguments": arguments,
                "idempotency_key": getattr(request, "idempotency_key", ""),
            }
        )
        if self.pause_on_lookup:
            self.wait_until_released.wait(timeout=5)
        if self.tool_responses:
            return self.tool_responses.pop(0)
        return agent_pb2.ExecuteAgentToolResponse(status=200, body=json.dumps({"echo": arguments}))


def _fake_resolved_tool(*, name: str) -> Any:
    tool_parameters = struct_pb2.Struct()
    tool_parameters.update({"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]})
    kwargs = {
        "id": "lookup",
        "name": name,
        "description": "Look up a historical figure",
        "parameters_schema": tool_parameters,
    }
    if hasattr(agent_pb2, "BoundAgentToolTarget"):
        kwargs["target"] = agent_pb2.BoundAgentToolTarget(plugin="people", operation="lookup")
    return agent_pb2.ResolvedAgentTool(**kwargs)


def _expected_tool_idempotency_key(*, provider_name: str = "simple", turn_id: str, tool_call_id: str) -> str:
    if not _TOOL_REQUEST_HAS_IDEMPOTENCY_KEY:
        return ""
    return f"agent/simple:{provider_name}:{turn_id}:{tool_call_id}"


def _fake_slack_reply_tool() -> Any:
    tool_parameters = struct_pb2.Struct()
    tool_parameters.update(
        {
            "type": "object",
            "properties": {"reply_ref": {"type": "string"}, "text": {"type": "string"}},
            "required": ["reply_ref", "text"],
        }
    )
    kwargs = {
        "id": "slack/events.reply?credentialMode=none",
        "name": "slack_events_reply",
        "description": "Reply to a Slack event",
        "parameters_schema": tool_parameters,
    }
    if hasattr(agent_pb2, "BoundAgentToolTarget"):
        kwargs["target"] = agent_pb2.BoundAgentToolTarget(plugin="slack", operation="events.reply")
    return agent_pb2.ResolvedAgentTool(**kwargs)


def _fake_tool_candidate(*, system: str = "people", plugin: str = "people", operation: str = "search_more") -> Any:
    if not hasattr(agent_pb2, "AgentToolCandidate"):
        raise unittest.SkipTest("adaptive tool search proto fields are not available")
    ref = agent_pb2.AgentToolRef(plugin=plugin, operation=operation)
    if _proto_message_has_field(ref, "system"):
        setattr(ref, "system", system)
    if _proto_message_has_field(ref, "credential_mode"):
        setattr(ref, "credential_mode", "user")
    return agent_pb2.AgentToolCandidate(
        ref=ref,
        id=f"{plugin}/{operation}",
        name="Search more people",
        description="Search more historical records",
        parameters=["query"],
    )


def _proto_message_has_field(message: Any, field_name: str) -> bool:
    descriptor = getattr(message, "DESCRIPTOR", None)
    fields_by_name = getattr(descriptor, "fields_by_name", {})
    return field_name in fields_by_name


def _supports_adaptive_tool_search() -> bool:
    return _proto_message_has_field(
        agent_pb2.SearchAgentToolsRequest(), "candidate_limit"
    ) and _proto_message_has_field(agent_pb2.SearchAgentToolsResponse(), "candidates")


def _tool_ref_to_dict(ref: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    for field in ("system", "plugin", "operation", "connection", "instance"):
        value = str(getattr(ref, field, "") or "").strip()
        if value:
            out[field] = value
    return out


def _expected_search_request(
    *, session_id: str, turn_id: str, query: str, max_results: int, candidate_limit: int | None = None
) -> dict[str, Any]:
    request: dict[str, Any] = {"session_id": session_id, "turn_id": turn_id, "query": query, "max_results": max_results}
    if candidate_limit is not None and _proto_message_has_field(agent_pb2.SearchAgentToolsRequest(), "candidate_limit"):
        request["candidate_limit"] = candidate_limit
    if _proto_message_has_field(agent_pb2.SearchAgentToolsRequest(), "load_refs"):
        request["load_refs"] = []
    return request


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


class _FakeOpenAIResponsesServer:
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
                if self.path != "/v1/responses":
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


def _configure_provider(
    *,
    default_model: str = "fast",
    provider_options: dict[str, Any] | None = None,
    resume: dict[str, Any] | None = None,
) -> tuple[Any, Any]:
    channel = grpc.insecure_channel(f"unix:{_runtime_socket}")
    lifecycle = runtime_pb2_grpc.ProviderLifecycleStub(channel)
    provider_client = agent_pb2_grpc.AgentProviderStub(channel)
    _configure_runtime(lifecycle, default_model=default_model, provider_options=provider_options, resume=resume)
    return lifecycle, provider_client


def _configure_runtime(
    lifecycle: Any,
    *,
    default_model: str = "fast",
    provider_options: dict[str, Any] | None = None,
    resume: dict[str, Any] | None = None,
) -> None:
    request = runtime_pb2.ConfigureProviderRequest(name="simple", protocol_version=_runtime.CURRENT_PROTOCOL_VERSION)
    config: dict[str, Any] = {
        "defaultModel": default_model,
        "aliases": {"fast": "openai/fake-model"},
        "maxSteps": 4,
        "timeoutSeconds": 5,
        "systemPrompt": "Be concise.",
    }
    if provider_options is not None:
        config["providerOptions"] = provider_options
    if resume is not None:
        config["resume"] = resume
    json_format.ParseDict(config, request.config)
    lifecycle.ConfigureProvider(request)


def _create_session(
    provider_client: Any,
    *,
    session_id: str,
    idempotency_key: str,
    model: str = "fast",
    client_ref: str = "",
    metadata: dict[str, Any] | None = None,
    created_by: Any | None = None,
) -> Any:
    request = agent_pb2.CreateAgentProviderSessionRequest(
        session_id=session_id, idempotency_key=idempotency_key, model=model, client_ref=client_ref
    )
    if metadata:
        request.metadata.update(metadata)
    if created_by is not None:
        request.created_by.CopyFrom(created_by)
    return provider_client.CreateSession(request)


def _direct_store() -> SimpleRunStore:
    return SimpleRunStore(run_store=_SIMPLE_RUN_STORE, idempotency_store=_SIMPLE_IDEMPOTENCY_STORE)


def _resume_seed(
    *,
    messages: list[dict[str, Any]],
    provider_options: dict[str, Any],
    conversation: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "phase": "model_next",
        "messages": messages,
        "conversation": conversation or [{"role": "user", "content": messages[0]["text"]}],
        "response_schema": {},
        "provider_options": provider_options,
        "tool_specs": [
            {
                "type": "function",
                "function": {
                    "name": "gestalt_search_tools",
                    "description": "Search the authorized Gestalt integration tool catalog for tools relevant to the task.",
                    "parameters": {
                        "type": "object",
                        "properties": {"query": {"type": "string"}},
                        "required": ["query"],
                    },
                },
            }
        ],
        "function_name_to_tool_id": {"gestalt_search_tools": "__gestalt_search_tools__"},
        "loaded_tool_ids": [],
        "slack_reply_ref": "",
        "step_index": 0,
        "pending_tool_call": None,
        "repaired_arguments": None,
    }


def setUpModule() -> None:
    global _runtime_server, _host_server, _indexeddb_server, _runtime_socket, _host_socket, _indexeddb_socket
    global _previous_agent_host_socket, _previous_indexeddb_socket, _host_servicer
    global _indexeddb_servicer

    _runtime_socket = _fresh_socket("simple-agent-runtime")
    _host_socket = _fresh_socket("simple-agent-host")
    _indexeddb_socket = _fresh_socket("simple-agent-indexeddb")

    _previous_indexeddb_socket = os.environ.get(indexeddb_socket_env())
    os.environ[indexeddb_socket_env()] = _indexeddb_socket

    indexeddb = _FakeIndexedDB()
    _indexeddb_servicer = indexeddb
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
        assert _indexeddb_servicer is not None
        _host_servicer.requests.clear()
        _host_servicer.search_requests.clear()
        _host_servicer.tools = [_fake_resolved_tool(name="person_lookup")]
        _host_servicer.load_ref_tools.clear()
        _host_servicer.candidates.clear()
        _host_servicer.has_more = False
        _host_servicer.tool_responses.clear()
        _host_servicer.pause_on_lookup = False
        _host_servicer.wait_until_released.set()
        _indexeddb_servicer.reset()

    def test_configure_provider_defers_indexeddb_connection_until_agent_rpc(self) -> None:
        missing_socket = _fresh_socket("simple-agent-missing-indexeddb")
        previous_socket = os.environ.get(indexeddb_socket_env())
        os.environ[indexeddb_socket_env()] = missing_socket

        channel = grpc.insecure_channel(f"unix:{_runtime_socket}")
        self.addCleanup(channel.close)
        lifecycle = runtime_pb2_grpc.ProviderLifecycleStub(channel)

        try:
            _configure_runtime(lifecycle)
            identity = lifecycle.GetProviderIdentity(empty_pb2.Empty())
        finally:
            if previous_socket is None:
                os.environ.pop(indexeddb_socket_env(), None)
            else:
                os.environ[indexeddb_socket_env()] = previous_socket
            if os.path.exists(missing_socket):
                os.remove(missing_socket)

        self.assertEqual(identity.name, "simple")
        self.assertEqual(list(identity.warnings), [])

    def test_configure_resumes_running_turn_from_atomic_checkpoint(self) -> None:
        fake_llm = _FakeOpenAIChatServer(
            responses=[
                {
                    "id": "chatcmpl-resume-seed",
                    "object": "chat.completion",
                    "created": 1710000100,
                    "model": "fake-model",
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": "seed resume complete"},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 4, "completion_tokens": 3, "total_tokens": 7},
                }
            ]
        )
        fake_llm.start()
        self.addCleanup(fake_llm.close)

        provider_options = {"base_url": f"{fake_llm.base_url}/v1", "api_key": "test-key"}
        messages = [{"role": "user", "text": "Resume the seeded turn"}]
        store = _direct_store()
        self.addCleanup(store.close)
        store.create_session(
            session_id="session-seed-resume",
            idempotency_key="session-idem-seed-resume",
            provider_name="simple",
            model="openai/fake-model",
            client_ref="",
            metadata={},
            created_by={},
        )
        store.begin_turn(
            turn_id="turn-seed-resume",
            session_id="session-seed-resume",
            idempotency_key="idem-seed-resume",
            provider_name="simple",
            model="openai/fake-model",
            messages=messages,
            created_by={},
            execution_ref="",
            resume_seed=_resume_seed(messages=messages, provider_options=provider_options),
        )
        self.assertIsNotNone(store.get_turn_checkpoint("turn-seed-resume"))

        _, provider_client = _configure_provider()
        fetched = _wait_for_turn(provider_client, "turn-seed-resume", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        events = provider_client.ListTurnEvents(
            agent_pb2.ListAgentProviderTurnEventsRequest(turn_id="turn-seed-resume")
        )

        self.assertEqual(fetched.output_text, "seed resume complete")
        self.assertEqual(len(fake_llm.requests), 1)
        self.assertEqual(
            [event.type for event in events.events], ["turn.started", "assistant.completed", "turn.completed"]
        )

    def test_configure_fails_uncertain_inflight_tool_without_replay(self) -> None:
        assert _host_servicer is not None
        messages = [{"role": "user", "text": "Use the tool"}]
        store = _direct_store()
        self.addCleanup(store.close)
        store.create_session(
            session_id="session-inflight",
            idempotency_key="session-idem-inflight",
            provider_name="simple",
            model="openai/fake-model",
            client_ref="",
            metadata={},
            created_by={},
        )
        run, _ = store.begin_turn(
            turn_id="turn-inflight",
            session_id="session-inflight",
            idempotency_key="idem-inflight",
            provider_name="simple",
            model="openai/fake-model",
            messages=messages,
            created_by={},
            execution_ref="",
            resume_seed=_resume_seed(messages=messages, provider_options={}),
        )
        store.put_turn_checkpoint(
            StoredTurnCheckpoint(
                turn_id=run.run_id,
                schema_version=1,
                provider_name=run.provider_name,
                session_id=run.session_ref,
                model=run.model,
                phase="tool_inflight",
                messages=messages,
                conversation=[{"role": "user", "content": "Use the tool"}],
                response_schema={},
                provider_options={},
                tool_specs=[],
                function_name_to_tool_id={},
                loaded_tool_ids=[],
                slack_reply_ref="",
                step_index=1,
                pending_tool_call={
                    "tool_call_id": "call-inflight",
                    "tool_name": "person_lookup",
                    "resolved_tool_id": "lookup",
                    "arguments": {"query": "Ada"},
                },
                repaired_arguments=None,
                attempt=0,
                lease_owner="",
                lease_expires_at=None,
                updated_at=datetime.now(tz=UTC),
            )
        )

        _, provider_client = _configure_provider()
        fetched = _wait_for_turn(provider_client, "turn-inflight", agent_pb2.AGENT_EXECUTION_STATUS_FAILED)
        events = provider_client.ListTurnEvents(agent_pb2.ListAgentProviderTurnEventsRequest(turn_id="turn-inflight"))

        self.assertEqual(_host_servicer.requests, [])
        self.assertIn("refusing to replay without a durable completed result", fetched.status_message)
        self.assertEqual([event.type for event in events.events], ["turn.started", "turn.failed"])

    def test_configure_resumes_repaired_slack_arguments_without_repair_model_call(self) -> None:
        assert _host_servicer is not None
        fake_llm = _FakeOpenAIChatServer(
            responses=[
                {
                    "id": "chatcmpl-after-repaired-tool",
                    "object": "chat.completion",
                    "created": 1710000101,
                    "model": "fake-model",
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": '{"posted":true}'},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 8, "completion_tokens": 4, "total_tokens": 12},
                }
            ]
        )
        fake_llm.start()
        self.addCleanup(fake_llm.close)
        provider_options = {"base_url": f"{fake_llm.base_url}/v1", "api_key": "test-key"}
        messages = [{"role": "user", "text": "reply_ref: slack-reply-ref\nSend the reply"}]
        store = _direct_store()
        self.addCleanup(store.close)
        store.create_session(
            session_id="session-repaired-slack",
            idempotency_key="session-idem-repaired-slack",
            provider_name="simple",
            model="openai/fake-model",
            client_ref="",
            metadata={},
            created_by={},
        )
        run, _ = store.begin_turn(
            turn_id="turn-repaired-slack",
            session_id="session-repaired-slack",
            idempotency_key="idem-repaired-slack",
            provider_name="simple",
            model="openai/fake-model",
            messages=messages,
            created_by={},
            execution_ref="",
            resume_seed=_resume_seed(messages=messages, provider_options=provider_options),
        )
        repaired_arguments = {"reply_ref": "slack-reply-ref", "text": "Persisted repaired reply"}
        store.put_turn_checkpoint(
            StoredTurnCheckpoint(
                turn_id=run.run_id,
                schema_version=1,
                provider_name=run.provider_name,
                session_id=run.session_ref,
                model=run.model,
                phase="tool_ready",
                messages=messages,
                conversation=[
                    {"role": "user", "content": "reply_ref: slack-reply-ref\nSend the reply"},
                    {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "call-repaired-slack",
                                "type": "function",
                                "function": {
                                    "name": "slack_events_reply",
                                    "arguments": json.dumps(repaired_arguments, separators=(",", ":")),
                                },
                            }
                        ],
                    },
                ],
                response_schema={
                    "type": "object",
                    "properties": {"posted": {"type": "boolean"}},
                    "required": ["posted"],
                },
                provider_options=provider_options,
                tool_specs=[
                    {
                        "type": "function",
                        "function": {
                            "name": "slack_events_reply",
                            "description": "Reply to a Slack event",
                            "parameters": {
                                "type": "object",
                                "properties": {"reply_ref": {"type": "string"}, "text": {"type": "string"}},
                                "required": ["reply_ref", "text"],
                            },
                        },
                    }
                ],
                function_name_to_tool_id={"slack_events_reply": "slack/events.reply?credentialMode=none"},
                loaded_tool_ids=["slack/events.reply?credentialMode=none"],
                slack_reply_ref="slack-reply-ref",
                step_index=1,
                pending_tool_call={
                    "tool_call_id": "call-repaired-slack",
                    "tool_name": "slack_events_reply",
                    "resolved_tool_id": "slack/events.reply?credentialMode=none",
                    "arguments": {"reply_ref": "slack-reply-ref"},
                },
                repaired_arguments=repaired_arguments,
                attempt=0,
                lease_owner="",
                lease_expires_at=None,
                updated_at=datetime.now(tz=UTC),
            )
        )

        _, provider_client = _configure_provider()
        fetched = _wait_for_turn(provider_client, "turn-repaired-slack", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        self.assertEqual(fetched.output_text, '{"posted":true}')
        self.assertEqual(len(fake_llm.requests), 1)
        self.assertEqual(
            _host_servicer.requests,
            [
                {
                    "session_id": "session-repaired-slack",
                    "turn_id": "turn-repaired-slack",
                    "tool_call_id": "call-repaired-slack",
                    "tool_id": "slack/events.reply?credentialMode=none",
                    "arguments": repaired_arguments,
                    "idempotency_key": _expected_tool_idempotency_key(
                        turn_id="turn-repaired-slack", tool_call_id="call-repaired-slack"
                    ),
                }
            ],
        )

    def test_list_turn_events_synthesizes_missing_terminal_event_types(self) -> None:
        store = _direct_store()
        self.addCleanup(store.close)
        store.create_session(
            session_id="session-partial-terminal-events",
            idempotency_key="session-idem-partial-terminal-events",
            provider_name="simple",
            model="openai/fake-model",
            client_ref="",
            metadata={},
            created_by={},
        )
        messages = [{"role": "user", "text": "finish"}]
        run, _ = store.begin_turn(
            turn_id="turn-partial-terminal-events",
            session_id="session-partial-terminal-events",
            idempotency_key="idem-partial-terminal-events",
            provider_name="simple",
            model="openai/fake-model",
            messages=messages,
            created_by={},
            execution_ref="",
            resume_seed=_resume_seed(messages=messages, provider_options={}),
        )
        store.append_turn_event_once(
            event_key="turn-partial-terminal-events:turn.started",
            turn_id="turn-partial-terminal-events",
            event_type="turn.started",
            source="simple",
            data={"session_id": "session-partial-terminal-events", "model": "openai/fake-model"},
        )
        store.mark_turn_succeeded(
            turn_id=run.run_id,
            messages=[*messages, {"role": "assistant", "text": "done"}],
            output_text="done",
            structured_output=None,
        )
        store.append_turn_event_once(
            event_key="turn-partial-terminal-events:assistant.completed",
            turn_id="turn-partial-terminal-events",
            event_type="assistant.completed",
            source="simple",
            data={"text": "done"},
        )

        _, provider_client = _configure_provider()
        events = provider_client.ListTurnEvents(
            agent_pb2.ListAgentProviderTurnEventsRequest(turn_id="turn-partial-terminal-events")
        )

        self.assertEqual(
            [event.type for event in events.events], ["turn.started", "assistant.completed", "turn.completed"]
        )
        self.assertEqual([event.seq for event in events.events], [1, 2, 3])

    def test_turn_checkpoint_lease_claim_is_exclusive(self) -> None:
        messages = [{"role": "user", "text": "lease"}]
        store = _direct_store()
        self.addCleanup(store.close)
        store.create_session(
            session_id="session-lease",
            idempotency_key="session-idem-lease",
            provider_name="simple",
            model="openai/fake-model",
            client_ref="",
            metadata={},
            created_by={},
        )
        store.begin_turn(
            turn_id="turn-lease",
            session_id="session-lease",
            idempotency_key="idem-lease",
            provider_name="simple",
            model="openai/fake-model",
            messages=messages,
            created_by={},
            execution_ref="",
            resume_seed=_resume_seed(messages=messages, provider_options={}),
        )

        self.assertTrue(store.claim_turn_lease("turn-lease", owner="worker-a", lease_seconds=30))
        self.assertFalse(store.claim_turn_lease("turn-lease", owner="worker-b", lease_seconds=30))
        checkpoint = store.get_turn_checkpoint("turn-lease")
        self.assertIsNotNone(checkpoint)
        assert checkpoint is not None
        self.assertEqual(checkpoint.lease_owner, "worker-a")
        with self.assertRaisesRegex(RuntimeError, "lease owner mismatch"):
            store.put_turn_checkpoint(checkpoint, lease_owner="worker-b")
        store.put_tool_result(
            turn_id="turn-lease",
            tool_call_id="call-active",
            result={"status": 200, "body": "active"},
            lease_owner="worker-a",
        )
        active_result = store.get_tool_result(turn_id="turn-lease", tool_call_id="call-active")
        self.assertIsNotNone(active_result)
        assert active_result is not None
        self.assertEqual(active_result["body"], "active")

        store.release_turn_lease("turn-lease", owner="worker-a")
        self.assertTrue(store.claim_turn_lease("turn-lease", owner="worker-b", lease_seconds=30))
        checkpoint = store.get_turn_checkpoint("turn-lease")
        self.assertIsNotNone(checkpoint)
        assert checkpoint is not None
        checkpoint.lease_expires_at = datetime.now(tz=UTC) - timedelta(seconds=1)
        store.put_turn_checkpoint(checkpoint)
        with self.assertRaisesRegex(RuntimeError, "lease expired"):
            store.put_tool_result(
                turn_id="turn-lease",
                tool_call_id="call-expired",
                result={"status": 200, "body": "expired"},
                lease_owner="worker-b",
            )
        self.assertIsNone(store.get_tool_result(turn_id="turn-lease", tool_call_id="call-expired"))

    def test_capabilities_report_resume_disabled(self) -> None:
        _, provider_client = _configure_provider(resume={"enabled": False})
        capabilities = provider_client.GetCapabilities(agent_pb2.GetAgentProviderCapabilitiesRequest())
        self.assertFalse(capabilities.resumable_turns)

    def test_create_turn_completes_tool_loop_and_persists_turn(self) -> None:
        lifecycle, provider_client = _configure_provider()
        identity = lifecycle.GetProviderIdentity(empty_pb2.Empty())
        actor = agent_pb2.AgentActor(
            subject_id="user-123", subject_kind="human", display_name="Ada", auth_source="session"
        )
        created_session = _create_session(
            provider_client,
            session_id="session-success",
            idempotency_key="session-idem-success",
            client_ref="cli-session-success",
            metadata={"heavy": "metadata"},
            created_by=actor,
        )

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
                                        "id": "call-search-1",
                                        "type": "function",
                                        "function": {
                                            "name": "gestalt_search_tools",
                                            "arguments": '{"query":"historical figure lookup","max_results":5}',
                                        },
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
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call-1",
                                        "type": "function",
                                        "function": {"name": "person_lookup", "arguments": '{"query":"Ada Lovelace"}'},
                                    },
                                    {
                                        "id": "call-2",
                                        "type": "function",
                                        "function": {
                                            "name": "person_lookup",
                                            "arguments": '{"query":"Analytical Engine"}',
                                        },
                                    },
                                ],
                            },
                            "finish_reason": "tool_calls",
                        }
                    ],
                    "usage": {"prompt_tokens": 14, "completion_tokens": 4, "total_tokens": 18},
                },
                {
                    "id": "chatcmpl-3",
                    "object": "chat.completion",
                    "created": 1710000002,
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
        message_metadata = struct_pb2.Struct()
        message_metadata.update({"source": "slack", "thread": "thread-123"})

        started = provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-success",
                session_id="session-success",
                idempotency_key="idem-success",
                model="fast",
                messages=[
                    agent_pb2.AgentMessage(
                        role="user",
                        parts=[
                            agent_pb2.AgentMessagePart(
                                type=agent_pb2.AGENT_MESSAGE_PART_TYPE_TEXT, text="Who is Ada Lovelace?"
                            )
                        ],
                        metadata=message_metadata,
                    )
                ],
                response_schema=response_schema,
                provider_options=provider_options,
                execution_ref="exec-1",
                created_by=actor,
            )
        )

        fetched = _wait_for_turn(provider_client, "turn-success", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        listed_sessions = provider_client.ListSessions(agent_pb2.ListAgentProviderSessionsRequest())
        listed_turns = provider_client.ListTurns(agent_pb2.ListAgentProviderTurnsRequest(session_id="session-success"))
        summary_sessions = provider_client.ListSessions(
            agent_pb2.ListAgentProviderSessionsRequest(
                subject=agent_pb2.AgentSubjectContext(subject_id="user-123"),
                session_ids=["missing-session", "session-success"],
                state=agent_pb2.AGENT_SESSION_STATE_ACTIVE,
                limit=1,
                summary_only=True,
            )
        )
        summary_turns = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id="session-success",
                subject=agent_pb2.AgentSubjectContext(subject_id="user-123"),
                turn_ids=["missing-turn", "turn-success"],
                status=agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED,
                limit=1,
                summary_only=True,
            )
        )
        filtered_turns = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id="session-success",
                subject=agent_pb2.AgentSubjectContext(subject_id="user-123"),
                status=agent_pb2.AGENT_EXECUTION_STATUS_FAILED,
                summary_only=True,
            )
        )
        subject_mismatch_turns = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                session_id="session-success",
                subject=agent_pb2.AgentSubjectContext(subject_id="user-456"),
                summary_only=True,
            )
        )
        empty_session_turns = provider_client.ListTurns(agent_pb2.ListAgentProviderTurnsRequest(summary_only=True))
        exact_turn_without_session = provider_client.ListTurns(
            agent_pb2.ListAgentProviderTurnsRequest(
                subject=agent_pb2.AgentSubjectContext(subject_id="user-123"),
                turn_ids=["turn-success"],
                summary_only=True,
            )
        )
        _create_session(
            provider_client,
            session_id="session-other-user",
            idempotency_key="session-idem-other-user",
            created_by=agent_pb2.AgentActor(
                subject_id="user-456", subject_kind="human", display_name="Grace", auth_source="session"
            ),
            metadata={"heavy": "other"},
        )
        subject_filtered_sessions = provider_client.ListSessions(
            agent_pb2.ListAgentProviderSessionsRequest(
                subject=agent_pb2.AgentSubjectContext(subject_id="user-123"),
                session_ids=["session-other-user", "session-success"],
                limit=10,
                summary_only=True,
            )
        )
        with self.assertRaises(grpc.RpcError) as negative_session_limit:
            provider_client.ListSessions(agent_pb2.ListAgentProviderSessionsRequest(limit=-1, summary_only=True))
        with self.assertRaises(grpc.RpcError) as negative_turn_limit:
            provider_client.ListTurns(
                agent_pb2.ListAgentProviderTurnsRequest(session_id="session-success", limit=-1, summary_only=True)
            )
        fetched_session = provider_client.GetSession(
            agent_pb2.GetAgentProviderSessionRequest(session_id="session-success")
        )
        listed_events = provider_client.ListTurnEvents(
            agent_pb2.ListAgentProviderTurnEventsRequest(turn_id="turn-success")
        )
        paged_events = provider_client.ListTurnEvents(
            agent_pb2.ListAgentProviderTurnEventsRequest(turn_id="turn-success", after_seq=2, limit=2)
        )
        capabilities = provider_client.GetCapabilities(agent_pb2.GetAgentProviderCapabilitiesRequest())

        self.assertEqual(identity.kind, runtime_pb2.ProviderKind.PROVIDER_KIND_AGENT)
        self.assertEqual(identity.name, "simple")
        self.assertEqual(list(identity.warnings), [])
        self.assertTrue(capabilities.native_tool_search)
        self.assertTrue(capabilities.resumable_turns)
        self.assertTrue(capabilities.bounded_list_hydration)

        self.assertEqual(created_session.id, "session-success")
        self.assertEqual(created_session.model, "openai/fake-model")
        self.assertEqual(created_session.client_ref, "cli-session-success")
        self.assertEqual(created_session.state, agent_pb2.AGENT_SESSION_STATE_ACTIVE)
        self.assertEqual(created_session.metadata.fields["heavy"].string_value, "metadata")
        self.assertEqual([session.id for session in listed_sessions.sessions], ["session-success"])
        self.assertEqual(listed_sessions.sessions[0].metadata.fields["heavy"].string_value, "metadata")
        self.assertEqual([session.id for session in summary_sessions.sessions], ["session-success"])
        self.assertFalse(summary_sessions.sessions[0].HasField("metadata"))
        self.assertEqual(fetched_session.id, "session-success")
        self.assertEqual(fetched_session.last_turn_at.seconds > 0 or fetched_session.last_turn_at.nanos > 0, True)

        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)
        self.assertEqual(started.model, "openai/fake-model")
        self.assertEqual(started.execution_ref, "exec-1")
        self.assertEqual(started.created_by.subject_id, "user-123")
        self.assertEqual(started.session_id, "session-success")
        self.assertEqual(len(started.messages), 1)
        self.assertEqual(started.messages[0].role, "user")
        self.assertEqual(started.messages[0].text, "")
        self.assertEqual(len(started.messages[0].parts), 1)
        self.assertEqual(started.messages[0].parts[0].type, agent_pb2.AGENT_MESSAGE_PART_TYPE_TEXT)
        self.assertEqual(started.messages[0].parts[0].text, "Who is Ada Lovelace?")
        self.assertEqual(started.messages[0].metadata.fields["source"].string_value, "slack")
        self.assertEqual(started.messages[0].metadata.fields["thread"].string_value, "thread-123")

        self.assertEqual(fetched.status, agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.model, "openai/fake-model")
        self.assertEqual(fetched.output_text, '{"summary":"Ada Lovelace is still relevant."}')
        self.assertEqual(fetched.structured_output.fields["summary"].string_value, "Ada Lovelace is still relevant.")
        self.assertEqual(fetched.execution_ref, "exec-1")
        self.assertEqual(fetched.created_by.subject_id, "user-123")
        self.assertEqual(len(fetched.messages), 2)
        self.assertEqual(fetched.messages[0].role, "user")
        self.assertEqual(fetched.messages[0].text, "")
        self.assertEqual(len(fetched.messages[0].parts), 1)
        self.assertEqual(fetched.messages[0].parts[0].type, agent_pb2.AGENT_MESSAGE_PART_TYPE_TEXT)
        self.assertEqual(fetched.messages[0].parts[0].text, "Who is Ada Lovelace?")
        self.assertEqual(fetched.messages[0].metadata.fields["source"].string_value, "slack")
        self.assertEqual(fetched.messages[0].metadata.fields["thread"].string_value, "thread-123")
        self.assertEqual(fetched.messages[1].role, "assistant")
        self.assertEqual(fetched.messages[1].text, '{"summary":"Ada Lovelace is still relevant."}')
        self.assertEqual(len(fetched.messages[1].parts), 1)
        self.assertEqual(fetched.messages[1].parts[0].type, agent_pb2.AGENT_MESSAGE_PART_TYPE_TEXT)
        self.assertEqual(fetched.messages[1].parts[0].text, '{"summary":"Ada Lovelace is still relevant."}')
        self.assertEqual(fetched.id, "turn-success")
        self.assertEqual(len(listed_turns.turns), 1)
        self.assertEqual([turn.id for turn in summary_turns.turns], ["turn-success"])
        self.assertEqual(len(summary_turns.turns[0].messages), 0)
        self.assertEqual(summary_turns.turns[0].output_text, "")
        self.assertFalse(summary_turns.turns[0].HasField("structured_output"))
        self.assertEqual(len(filtered_turns.turns), 0)
        self.assertEqual(len(subject_mismatch_turns.turns), 0)
        self.assertEqual(len(empty_session_turns.turns), 0)
        self.assertEqual([turn.id for turn in exact_turn_without_session.turns], ["turn-success"])
        self.assertEqual([session.id for session in subject_filtered_sessions.sessions], ["session-success"])
        self.assertEqual(_rpc_error_code(negative_session_limit.exception), grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(_rpc_error_details(negative_session_limit.exception), "limit must be non-negative")
        self.assertEqual(_rpc_error_code(negative_turn_limit.exception), grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(_rpc_error_details(negative_turn_limit.exception), "limit must be non-negative")

        assert _host_servicer is not None
        self.assertEqual(
            _host_servicer.search_requests,
            [
                _expected_search_request(
                    session_id="session-success",
                    turn_id="turn-success",
                    query="historical figure lookup",
                    max_results=5,
                    candidate_limit=10,
                )
            ],
        )
        self.assertEqual(len(_host_servicer.requests), 2)
        self.assertEqual(
            _host_servicer.requests[0],
            {
                "session_id": "session-success",
                "turn_id": "turn-success",
                "tool_call_id": "call-1",
                "tool_id": "lookup",
                "arguments": {"query": "Ada Lovelace"},
                "idempotency_key": _expected_tool_idempotency_key(turn_id="turn-success", tool_call_id="call-1"),
            },
        )
        self.assertEqual(
            _host_servicer.requests[1],
            {
                "session_id": "session-success",
                "turn_id": "turn-success",
                "tool_call_id": "call-2",
                "tool_id": "lookup",
                "arguments": {"query": "Analytical Engine"},
                "idempotency_key": _expected_tool_idempotency_key(turn_id="turn-success", tool_call_id="call-2"),
            },
        )

        self.assertEqual(len(fake_llm.requests), 3)
        first_request = fake_llm.requests[0]
        second_request = fake_llm.requests[1]
        third_request = fake_llm.requests[2]
        self.assertEqual(first_request["model"], "fake-model")
        self.assertEqual(first_request["messages"][0]["role"], "system")
        self.assertEqual(first_request["messages"][-1]["content"], "Who is Ada Lovelace?")
        self.assertEqual([tool["function"]["name"] for tool in first_request["tools"]], ["gestalt_search_tools"])
        if not _supports_adaptive_tool_search():
            properties = first_request["tools"][0]["function"]["parameters"]["properties"]
            self.assertNotIn("candidate_limit", properties)
            self.assertNotIn("load_refs", properties)
        self.assertEqual(second_request["messages"][-1]["role"], "tool")
        self.assertIn("person_lookup", second_request["messages"][-1]["content"])
        self.assertNotIn('"id"', second_request["messages"][-1]["content"])
        self.assertNotIn('"target"', second_request["messages"][-1]["content"])
        self.assertIn("person_lookup", [tool["function"]["name"] for tool in second_request["tools"]])
        self.assertEqual([message["role"] for message in third_request["messages"][-2:]], ["tool", "tool"])
        self.assertIn("Ada Lovelace", third_request["messages"][-2]["content"])
        self.assertIn("Analytical Engine", third_request["messages"][-1]["content"])
        self.assertNotIn("name", third_request["messages"][-1])
        self.assertEqual(
            [event.type for event in listed_events.events],
            [
                "turn.started",
                "tool.started",
                "tool.completed",
                "tool.started",
                "tool.completed",
                "tool.started",
                "tool.completed",
                "assistant.completed",
                "turn.completed",
            ],
        )
        self.assertEqual([event.seq for event in listed_events.events], [1, 2, 3, 4, 5, 6, 7, 8, 9])
        self.assertEqual(listed_events.events[0].data.fields["session_id"].string_value, "session-success")
        self.assertEqual(listed_events.events[1].data.fields["tool_id"].string_value, "__gestalt_search_tools__")
        self.assertEqual(listed_events.events[3].data.fields["tool_id"].string_value, "lookup")
        self.assertEqual(listed_events.events[4].data.fields["status"].number_value, 200)
        self.assertEqual(listed_events.events[5].data.fields["tool_id"].string_value, "lookup")
        self.assertEqual(listed_events.events[6].data.fields["status"].number_value, 200)
        self.assertEqual([event.type for event in paged_events.events], ["tool.completed", "tool.started"])

    @unittest.skipUnless(_supports_adaptive_tool_search(), "adaptive tool search proto fields are not available")
    def test_create_turn_searches_candidates_and_loads_refs(self) -> None:
        assert _host_servicer is not None
        _, provider_client = _configure_provider()
        _create_session(
            provider_client, session_id="session-adaptive-search", idempotency_key="session-idem-adaptive-search"
        )
        _host_servicer.tools = []
        _host_servicer.load_ref_tools = [_fake_resolved_tool(name="person_lookup")]
        _host_servicer.candidates = [_fake_tool_candidate()]
        _host_servicer.has_more = True

        fake_llm = _FakeOpenAIChatServer(
            responses=[
                {
                    "id": "chatcmpl-adaptive-1",
                    "object": "chat.completion",
                    "created": 1710000100,
                    "model": "fake-model",
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call-search-candidates",
                                        "type": "function",
                                        "function": {
                                            "name": "gestalt_search_tools",
                                            "arguments": '{"query":"historical records"}',
                                        },
                                    }
                                ],
                            },
                            "finish_reason": "tool_calls",
                        }
                    ],
                },
                {
                    "id": "chatcmpl-adaptive-2",
                    "object": "chat.completion",
                    "created": 1710000101,
                    "model": "fake-model",
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": None,
                                "tool_calls": [
                                    {
                                        "id": "call-load-ref",
                                        "type": "function",
                                        "function": {
                                            "name": "gestalt_search_tools",
                                            "arguments": (
                                                '{"load_refs":[{"system":"people","plugin":"people",'
                                                '"operation":"search_more"}]}'
                                            ),
                                        },
                                    }
                                ],
                            },
                            "finish_reason": "tool_calls",
                        }
                    ],
                },
                {
                    "id": "chatcmpl-adaptive-3",
                    "object": "chat.completion",
                    "created": 1710000102,
                    "model": "fake-model",
                    "choices": [
                        {"index": 0, "message": {"role": "assistant", "content": "done"}, "finish_reason": "stop"}
                    ],
                },
            ]
        )
        fake_llm.start()
        self.addCleanup(fake_llm.close)

        provider_options = struct_pb2.Struct()
        provider_options.update({"base_url": f"{fake_llm.base_url}/v1", "api_key": "test-key", "timeout": 7})

        provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-adaptive-search",
                session_id="session-adaptive-search",
                idempotency_key="idem-adaptive-search",
                model="fast",
                messages=[agent_pb2.AgentMessage(role="user", text="Find historical records.")],
                provider_options=provider_options,
            )
        )
        fetched = _wait_for_turn(provider_client, "turn-adaptive-search", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        self.assertEqual(fetched.output_text, "done")
        expected_load_ref = {"system": "people", "plugin": "people", "operation": "search_more"}
        if _proto_message_has_field(agent_pb2.AgentToolRef(), "credential_mode"):
            expected_load_ref["credential_mode"] = "user"
        self.assertEqual(
            _host_servicer.search_requests,
            [
                {
                    "session_id": "session-adaptive-search",
                    "turn_id": "turn-adaptive-search",
                    "query": "historical records",
                    "max_results": 3,
                    "candidate_limit": 10,
                    "load_refs": [],
                },
                {
                    "session_id": "session-adaptive-search",
                    "turn_id": "turn-adaptive-search",
                    "query": "",
                    "max_results": 0,
                    "candidate_limit": 0,
                    "load_refs": [expected_load_ref],
                },
            ],
        )
        first_tool_result = fake_llm.requests[1]["messages"][-1]["content"]
        second_tool_result = fake_llm.requests[2]["messages"][-1]["content"]
        self.assertIn('"candidates":[', first_tool_result)
        self.assertIn('"has_more":true', first_tool_result)
        self.assertNotIn("person_lookup", first_tool_result)
        self.assertEqual([tool["function"]["name"] for tool in fake_llm.requests[1]["tools"]], ["gestalt_search_tools"])
        self.assertIn("load_refs", fake_llm.requests[1]["tools"][0]["function"]["parameters"]["properties"])
        self.assertIn("person_lookup", second_tool_result)
        self.assertIn("person_lookup", [tool["function"]["name"] for tool in fake_llm.requests[2]["tools"]])

    def test_create_turn_backfills_missing_text_tool_argument_from_assistant_text(self) -> None:
        assert _host_servicer is not None
        _, provider_client = _configure_provider()
        _create_session(
            provider_client, session_id="session-backfill-text", idempotency_key="session-idem-backfill-text"
        )

        slack_reply_tool = _fake_slack_reply_tool()

        fake_llm = _FakeOpenAIChatServer(
            responses=[
                {
                    "id": "chatcmpl-backfill-1",
                    "object": "chat.completion",
                    "created": 1710000100,
                    "model": "fake-model",
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": "Here are your open pull requests.",
                                "tool_calls": [
                                    {
                                        "id": "call-reply-1",
                                        "type": "function",
                                        "function": {
                                            "name": "slack_events_reply",
                                            "arguments": '{"reply_ref":"reply-ref-1"}',
                                        },
                                    }
                                ],
                            },
                            "finish_reason": "tool_calls",
                        }
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16},
                },
                {
                    "id": "chatcmpl-backfill-2",
                    "object": "chat.completion",
                    "created": 1710000101,
                    "model": "fake-model",
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": "Posted to Slack."},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 14, "completion_tokens": 4, "total_tokens": 18},
                },
            ]
        )
        fake_llm.start()
        self.addCleanup(fake_llm.close)

        provider_options = struct_pb2.Struct()
        provider_options.update({"base_url": f"{fake_llm.base_url}/v1", "api_key": "test-key"})

        provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-backfill-text",
                session_id="session-backfill-text",
                idempotency_key="idem-backfill-text",
                model="fast",
                messages=[agent_pb2.AgentMessage(role="user", text="List my PRs in Slack")],
                provider_options=provider_options,
                tools=[slack_reply_tool],
            )
        )

        fetched = _wait_for_turn(provider_client, "turn-backfill-text", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        self.assertEqual(fetched.output_text, "Posted to Slack.")
        self.assertEqual(len(_host_servicer.requests), 1)
        self.assertEqual(
            _host_servicer.requests[0]["arguments"],
            {"reply_ref": "reply-ref-1", "text": "Here are your open pull requests."},
        )

    def test_create_turn_injects_slack_reply_ref_from_workflow_signal_batch(self) -> None:
        assert _host_servicer is not None
        _, provider_client = _configure_provider()
        _create_session(
            provider_client,
            session_id="session-slack-reply-ref",
            idempotency_key="session-idem-slack-reply-ref",
            model="anthropic/claude-fake-model",
        )

        slack_reply_tool = _fake_slack_reply_tool()
        signal_batch = json.dumps(
            {"signals": [{"payload": {"reply_ref": "reply-ref-from-signal"}}]}, separators=(",", ":")
        )

        fake_anthropic = _FakeAnthropicMessagesServer(
            responses=[
                {
                    "id": "msg-slack-reply",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-fake-model",
                    "content": [
                        {
                            "type": "tool_use",
                            "id": "toolu-slack-reply",
                            "name": "slack_events_reply",
                            "input": {"text": "Here are your open pull requests."},
                        }
                    ],
                    "stop_reason": "tool_use",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                },
                {
                    "id": "msg-slack-done",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-fake-model",
                    "content": [{"type": "text", "text": "Posted to Slack."}],
                    "stop_reason": "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 20, "output_tokens": 5},
                },
            ]
        )
        fake_anthropic.start()
        self.addCleanup(fake_anthropic.close)

        provider_options = struct_pb2.Struct()
        provider_options.update({"base_url": fake_anthropic.base_url, "api_key": "test-key"})

        provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-slack-reply-ref",
                session_id="session-slack-reply-ref",
                idempotency_key="idem-slack-reply-ref",
                model="anthropic/claude-fake-model",
                messages=[agent_pb2.AgentMessage(role="user", text=f"Workflow signal batch:\n{signal_batch}")],
                provider_options=provider_options,
                tools=[slack_reply_tool],
            )
        )

        fetched = _wait_for_turn(provider_client, "turn-slack-reply-ref", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        self.assertEqual(fetched.output_text, "Posted to Slack.")
        self.assertEqual(len(_host_servicer.requests), 1)
        self.assertEqual(
            _host_servicer.requests[0]["arguments"],
            {"reply_ref": "reply-ref-from-signal", "text": "Here are your open pull requests."},
        )
        self.assertEqual(len(fake_anthropic.requests), 2)
        reply_tool_schema = next(
            tool["input_schema"] for tool in fake_anthropic.requests[0]["tools"] if tool["name"] == "slack_events_reply"
        )
        self.assertEqual(reply_tool_schema["required"], ["text"])
        self.assertEqual(set(reply_tool_schema["properties"]), {"text"})

    def test_create_turn_repairs_missing_slack_reply_text_before_validation(self) -> None:
        assert _host_servicer is not None
        _, provider_client = _configure_provider()
        _create_session(
            provider_client,
            session_id="session-repair-slack-reply",
            idempotency_key="session-idem-repair-slack-reply",
            model="anthropic/claude-fake-model",
        )

        signal_batch = json.dumps(
            {"signals": [{"payload": {"reply_ref": "reply-ref-from-signal"}}]}, separators=(",", ":")
        )

        fake_anthropic = _FakeAnthropicMessagesServer(
            responses=[
                {
                    "id": "msg-slack-reply-missing-text",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-fake-model",
                    "content": [{"type": "tool_use", "id": "toolu-reply", "name": "slack_events_reply", "input": {}}],
                    "stop_reason": "tool_use",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                },
                {
                    "id": "msg-slack-reply-text-repair",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-fake-model",
                    "content": [{"type": "text", "text": "Here are your open Linear tickets."}],
                    "stop_reason": "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 20, "output_tokens": 6},
                },
                {
                    "id": "msg-slack-reply-posted",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-fake-model",
                    "content": [{"type": "text", "text": '{"posted":true}'}],
                    "stop_reason": "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 24, "output_tokens": 4},
                },
            ]
        )
        fake_anthropic.start()
        self.addCleanup(fake_anthropic.close)

        response_schema = struct_pb2.Struct()
        response_schema.update(
            {"type": "object", "properties": {"posted": {"type": "boolean"}}, "required": ["posted"]}
        )

        provider_options = struct_pb2.Struct()
        provider_options.update(
            {"base_url": fake_anthropic.base_url, "api_key": "test-key", "tool_choice": {"type": "auto"}}
        )

        provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-repair-slack-reply",
                session_id="session-repair-slack-reply",
                idempotency_key="idem-repair-slack-reply",
                model="anthropic/claude-fake-model",
                messages=[agent_pb2.AgentMessage(role="user", text=f"Workflow signal batch:\n{signal_batch}")],
                tools=[_fake_slack_reply_tool()],
                provider_options=provider_options,
                response_schema=response_schema,
            )
        )

        fetched = _wait_for_turn(provider_client, "turn-repair-slack-reply", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        self.assertEqual(fetched.output_text, '{"posted":true}')
        self.assertTrue(fetched.structured_output.fields["posted"].bool_value)
        self.assertEqual(
            _host_servicer.requests,
            [
                {
                    "session_id": "session-repair-slack-reply",
                    "turn_id": "turn-repair-slack-reply",
                    "tool_call_id": "toolu-reply",
                    "tool_id": "slack/events.reply?credentialMode=none",
                    "arguments": {"reply_ref": "reply-ref-from-signal", "text": "Here are your open Linear tickets."},
                    "idempotency_key": _expected_tool_idempotency_key(
                        turn_id="turn-repair-slack-reply", tool_call_id="toolu-reply"
                    ),
                }
            ],
        )

        self.assertEqual(len(fake_anthropic.requests), 3)
        self.assertIn("Return only valid JSON", fake_anthropic.requests[0]["system"])
        repair_request = fake_anthropic.requests[1]
        self.assertNotIn("tools", repair_request)
        self.assertNotIn("tool_choice", repair_request)
        self.assertNotIn("Return only valid JSON", repair_request["system"])
        self.assertIn("Return only the Slack message body", repair_request["messages"][-1]["content"])

        final_request = fake_anthropic.requests[2]
        repaired_tool_use = final_request["messages"][1]["content"][0]
        self.assertEqual(repaired_tool_use["type"], "tool_use")
        self.assertEqual(repaired_tool_use["input"], {"text": "Here are your open Linear tickets."})

    def test_create_turn_repairs_missing_slack_reply_text_for_openai_responses(self) -> None:
        assert _host_servicer is not None
        _, provider_client = _configure_provider(default_model="openai/gpt-5.5")
        _create_session(
            provider_client,
            session_id="session-repair-slack-reply-openai",
            idempotency_key="session-idem-repair-slack-reply-openai",
            model="openai/gpt-5.5",
        )

        signal_batch = json.dumps(
            {"signals": [{"payload": {"reply_ref": "reply-ref-from-signal"}}]}, separators=(",", ":")
        )

        fake_llm = _FakeOpenAIResponsesServer(
            responses=[
                {
                    "id": "resp-slack-reply-missing-text",
                    "object": "response",
                    "created_at": 1710000062,
                    "model": "gpt-5.5",
                    "status": "completed",
                    "output": [
                        {"id": "rs-reply", "type": "reasoning", "summary": [], "status": "completed"},
                        {
                            "id": "fc-reply",
                            "type": "function_call",
                            "call_id": "call-reply",
                            "name": "slack_events_reply",
                            "arguments": "{}",
                            "status": "completed",
                        },
                    ],
                    "parallel_tool_calls": True,
                    "tool_choice": "auto",
                    "tools": [],
                },
                {
                    "id": "resp-slack-reply-text-repair",
                    "object": "response",
                    "created_at": 1710000063,
                    "model": "gpt-5.5",
                    "status": "completed",
                    "output": [
                        {
                            "id": "msg-repair",
                            "type": "message",
                            "role": "assistant",
                            "status": "completed",
                            "content": [
                                {"type": "output_text", "text": "Here are your open pull requests.", "annotations": []}
                            ],
                        }
                    ],
                    "parallel_tool_calls": True,
                    "tool_choice": "auto",
                    "tools": [],
                },
                {
                    "id": "resp-slack-reply-posted",
                    "object": "response",
                    "created_at": 1710000064,
                    "model": "gpt-5.5",
                    "status": "completed",
                    "output": [
                        {
                            "id": "msg-posted",
                            "type": "message",
                            "role": "assistant",
                            "status": "completed",
                            "content": [{"type": "output_text", "text": "Posted to Slack.", "annotations": []}],
                        }
                    ],
                    "parallel_tool_calls": True,
                    "tool_choice": "auto",
                    "tools": [],
                },
            ]
        )
        fake_llm.start()
        self.addCleanup(fake_llm.close)

        provider_options = struct_pb2.Struct()
        provider_options.update(
            {
                "openai": {
                    "base_url": f"{fake_llm.base_url}/v1",
                    "api_key": "test-key",
                    "reasoning_effort": "xhigh",
                    "tool_choice": {"type": "function", "function": {"name": "slack_events_reply"}},
                }
            }
        )

        provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-repair-slack-reply-openai",
                session_id="session-repair-slack-reply-openai",
                idempotency_key="idem-repair-slack-reply-openai",
                model="openai/gpt-5.5",
                messages=[agent_pb2.AgentMessage(role="user", text=f"Workflow signal batch:\n{signal_batch}")],
                tools=[_fake_slack_reply_tool()],
                provider_options=provider_options,
            )
        )

        fetched = _wait_for_turn(
            provider_client, "turn-repair-slack-reply-openai", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED
        )

        self.assertEqual(fetched.output_text, "Posted to Slack.")
        self.assertEqual(
            _host_servicer.requests,
            [
                {
                    "session_id": "session-repair-slack-reply-openai",
                    "turn_id": "turn-repair-slack-reply-openai",
                    "tool_call_id": "call-reply",
                    "tool_id": "slack/events.reply?credentialMode=none",
                    "arguments": {"reply_ref": "reply-ref-from-signal", "text": "Here are your open pull requests."},
                    "idempotency_key": _expected_tool_idempotency_key(
                        turn_id="turn-repair-slack-reply-openai", tool_call_id="call-reply"
                    ),
                }
            ],
        )

        self.assertEqual(len(fake_llm.requests), 3)
        first_request = fake_llm.requests[0]
        self.assertEqual(first_request["tool_choice"], {"type": "function", "name": "slack_events_reply"})
        self.assertEqual(first_request["reasoning"], {"effort": "xhigh"})

        repair_request = fake_llm.requests[1]
        self.assertNotIn("tools", repair_request)
        self.assertNotIn("tool_choice", repair_request)
        self.assertEqual(repair_request["reasoning"], {"effort": "xhigh"})
        self.assertIn("Return only the Slack message body", repair_request["input"][-1]["content"])

        final_request = fake_llm.requests[2]
        self.assertIn(
            {"id": "rs-reply", "type": "reasoning", "summary": [], "status": "completed"}, final_request["input"]
        )
        replayed_call = next(item for item in final_request["input"] if item.get("type") == "function_call")
        self.assertEqual(json.loads(replayed_call["arguments"]), {"text": "Here are your open pull requests."})

    def test_create_turn_retries_sustained_indexeddb_busy_on_completion(self) -> None:
        assert _indexeddb_servicer is not None

        _, provider_client = _configure_provider()
        _create_session(provider_client, session_id="session-busy-retry", idempotency_key="session-idem-busy-retry")

        fake_llm = _FakeOpenAIChatServer(
            responses=[
                {
                    "id": "chatcmpl-busy-1",
                    "object": "chat.completion",
                    "created": 1710000030,
                    "model": "fake-model",
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": "retry survived"},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 8, "completion_tokens": 4, "total_tokens": 12},
                }
            ]
        )
        fake_llm.start()
        self.addCleanup(fake_llm.close)

        _indexeddb_servicer.fail_next_busy(store=_SIMPLE_RUN_STORE, operation="put", count=12)

        provider_options = struct_pb2.Struct()
        provider_options.update({"base_url": f"{fake_llm.base_url}/v1", "api_key": "test-key"})

        started = provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-busy-retry",
                session_id="session-busy-retry",
                idempotency_key="idem-busy-retry",
                model="fast",
                messages=[agent_pb2.AgentMessage(role="user", text="Retry through SQLITE_BUSY")],
                provider_options=provider_options,
            )
        )

        fetched = _wait_for_turn(provider_client, "turn-busy-retry", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        events = provider_client.ListTurnEvents(agent_pb2.ListAgentProviderTurnEventsRequest(turn_id="turn-busy-retry"))

        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)
        self.assertEqual(fetched.status, agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.output_text, "retry survived")
        self.assertEqual(
            [event.type for event in events.events], ["turn.started", "assistant.completed", "turn.completed"]
        )

    def test_text_only_turn_does_not_require_agent_host_socket(self) -> None:
        assert _host_servicer is not None

        previous_socket = os.environ.pop(ENV_AGENT_HOST_SOCKET, None)
        try:
            _, provider_client = _configure_provider()
            _create_session(provider_client, session_id="session-no-host", idempotency_key="session-idem-no-host")

            fake_llm = _FakeOpenAIChatServer(
                responses=[
                    {
                        "id": "chatcmpl-no-host",
                        "object": "chat.completion",
                        "created": 1710000035,
                        "model": "fake-model",
                        "choices": [
                            {
                                "index": 0,
                                "message": {"role": "assistant", "content": "text-only response"},
                                "finish_reason": "stop",
                            }
                        ],
                        "usage": {"prompt_tokens": 8, "completion_tokens": 4, "total_tokens": 12},
                    }
                ]
            )
            fake_llm.start()
            self.addCleanup(fake_llm.close)

            provider_options = struct_pb2.Struct()
            provider_options.update({"base_url": f"{fake_llm.base_url}/v1", "api_key": "test-key"})

            started = provider_client.CreateTurn(
                agent_pb2.CreateAgentProviderTurnRequest(
                    turn_id="turn-no-host",
                    session_id="session-no-host",
                    idempotency_key="idem-no-host",
                    model="fast",
                    messages=[agent_pb2.AgentMessage(role="user", text="Say something brief.")],
                    provider_options=provider_options,
                )
            )
            fetched = _wait_for_turn(provider_client, "turn-no-host", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        finally:
            if previous_socket is None:
                os.environ.pop(ENV_AGENT_HOST_SOCKET, None)
            else:
                os.environ[ENV_AGENT_HOST_SOCKET] = previous_socket

        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)
        self.assertEqual(fetched.output_text, "text-only response")
        self.assertEqual(_host_servicer.search_requests, [])
        self.assertEqual(_host_servicer.requests, [])

    def test_create_turn_completes_over_tcp_runtime_socket(self) -> None:
        provider = provider_module.SimpleAgentRuntimeProvider()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            host, port = sock.getsockname()
        address = f"{host}:{port}"
        server_holder: dict[str, grpc.Server] = {}
        ready = threading.Event()
        failures: list[BaseException] = []

        def capture_shutdown(server: grpc.Server, _close_provider: Any) -> None:
            server_holder["server"] = server
            ready.set()

        def run_server() -> None:
            try:
                with mock.patch.object(_runtime, "_register_shutdown_handlers", side_effect=capture_shutdown):
                    with mock.patch.dict(os.environ, {_runtime.ENV_PROVIDER_SOCKET: f"tcp://{address}"}, clear=False):
                        _runtime.serve(provider, runtime_kind=ProviderKind.AGENT)
            except BaseException as exc:  # pragma: no cover - surfaced via assertions
                failures.append(exc)
                ready.set()

        thread = threading.Thread(target=run_server, daemon=True)
        thread.start()
        self.assertTrue(ready.wait(timeout=5))
        self.assertEqual(failures, [])
        self.assertIn("server", server_holder)

        channel = grpc.insecure_channel(address)
        self.addCleanup(channel.close)
        grpc.channel_ready_future(channel).result(timeout=5)
        lifecycle = runtime_pb2_grpc.ProviderLifecycleStub(channel)
        provider_client = agent_pb2_grpc.AgentProviderStub(channel)

        try:
            _configure_runtime(lifecycle)
            created_session = _create_session(
                provider_client, session_id="session-tcp", idempotency_key="session-idem-tcp"
            )

            fake_llm = _FakeOpenAIChatServer(
                responses=[
                    {
                        "id": "chatcmpl-tcp-1",
                        "object": "chat.completion",
                        "created": 1710000040,
                        "model": "fake-model",
                        "choices": [
                            {
                                "index": 0,
                                "message": {"role": "assistant", "content": "pong over tcp"},
                                "finish_reason": "stop",
                            }
                        ],
                        "usage": {"prompt_tokens": 8, "completion_tokens": 4, "total_tokens": 12},
                    }
                ]
            )
            fake_llm.start()
            self.addCleanup(fake_llm.close)

            provider_options = struct_pb2.Struct()
            provider_options.update({"base_url": f"{fake_llm.base_url}/v1", "api_key": "test-key"})

            started = provider_client.CreateTurn(
                agent_pb2.CreateAgentProviderTurnRequest(
                    turn_id="turn-tcp",
                    session_id="session-tcp",
                    idempotency_key="idem-tcp",
                    model="fast",
                    messages=[agent_pb2.AgentMessage(role="user", text="Ping over tcp")],
                    provider_options=provider_options,
                ),
                timeout=5,
            )
            fetched = _wait_for_turn(provider_client, "turn-tcp", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        finally:
            if "server" in server_holder:
                server_holder["server"].stop(grace=0).wait()
            thread.join(timeout=5)

        self.assertFalse(thread.is_alive())
        self.assertEqual(created_session.model, "openai/fake-model")
        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)
        self.assertEqual(started.model, "openai/fake-model")
        self.assertEqual(fetched.status, agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.output_text, "pong over tcp")
        self.assertEqual(fetched.model, "openai/fake-model")

    def test_create_turn_merges_config_provider_options_before_request_options(self) -> None:
        fake_llm = _FakeOpenAIChatServer(
            responses=[
                {
                    "id": "chatcmpl-config-options",
                    "object": "chat.completion",
                    "created": 1710000050,
                    "model": "fake-model",
                    "choices": [
                        {
                            "index": 0,
                            "message": {"role": "assistant", "content": "config options applied"},
                            "finish_reason": "stop",
                        }
                    ],
                    "usage": {"prompt_tokens": 8, "completion_tokens": 4, "total_tokens": 12},
                }
            ]
        )
        fake_llm.start()
        self.addCleanup(fake_llm.close)

        _, provider_client = _configure_provider(
            provider_options={
                "litellm": {"presence_penalty": 1.5, "top_p": 0.1},
                "top_p": 0.8,
                "openai": {
                    "base_url": f"{fake_llm.base_url}/v1",
                    "api_key": "config-key",
                    "reasoning_effort": "xhigh",
                    "temperature": 0.1,
                },
            }
        )
        _create_session(
            provider_client, session_id="session-config-options", idempotency_key="session-idem-config-options"
        )

        provider_options = struct_pb2.Struct()
        provider_options.update({"max_completion_tokens": 64, "openai": {"temperature": 0.7}})

        started = provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-config-options",
                session_id="session-config-options",
                idempotency_key="idem-config-options",
                model="fast",
                messages=[agent_pb2.AgentMessage(role="user", text="Apply config options.")],
                provider_options=provider_options,
            )
        )
        fetched = _wait_for_turn(provider_client, "turn-config-options", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)
        self.assertEqual(fetched.status, agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.output_text, "config options applied")

        self.assertEqual(len(fake_llm.requests), 1)
        request = fake_llm.requests[0]
        self.assertEqual(request["model"], "fake-model")
        self.assertEqual(request["reasoning_effort"], "xhigh")
        self.assertEqual(request["top_p"], 0.8)
        self.assertEqual(request["temperature"], 0.7)
        self.assertEqual(request["max_completion_tokens"], 64)
        self.assertNotIn("litellm", request)
        self.assertNotIn("presence_penalty", request)

    def test_create_turn_uses_openai_responses_for_gpt5_tool_reasoning_options(self) -> None:
        fake_llm = _FakeOpenAIResponsesServer(
            responses=[
                {
                    "id": "resp-tool-search",
                    "object": "response",
                    "created_at": 1710000060,
                    "model": "gpt-5.5",
                    "status": "completed",
                    "output": [
                        {"id": "rs-tool-search", "type": "reasoning", "summary": [], "status": "completed"},
                        {
                            "id": "fc-search",
                            "type": "function_call",
                            "call_id": "call-search-responses",
                            "name": "gestalt_search_tools",
                            "arguments": '{"query":"person lookup"}',
                            "status": "completed",
                        },
                    ],
                    "parallel_tool_calls": True,
                    "tool_choice": "auto",
                    "tools": [],
                },
                {
                    "id": "resp-final",
                    "object": "response",
                    "created_at": 1710000061,
                    "model": "gpt-5.5",
                    "status": "completed",
                    "output": [
                        {
                            "id": "msg-final",
                            "type": "message",
                            "role": "assistant",
                            "status": "completed",
                            "content": [{"type": "output_text", "text": "Responses route works.", "annotations": []}],
                        }
                    ],
                    "parallel_tool_calls": True,
                    "tool_choice": "auto",
                    "tools": [],
                },
            ]
        )
        fake_llm.start()
        self.addCleanup(fake_llm.close)

        _, provider_client = _configure_provider(
            default_model="openai/gpt-5.5",
            provider_options={
                "openai": {
                    "base_url": f"{fake_llm.base_url}/v1",
                    "api_key": "test-key",
                    "reasoning_effort": "xhigh",
                    "reasoning": {"summary": "auto"},
                    "max_completion_tokens": 64,
                    "tool_choice": {"type": "function", "function": {"name": "gestalt_search_tools"}},
                }
            },
        )
        _create_session(
            provider_client,
            session_id="session-openai-responses",
            idempotency_key="session-idem-openai-responses",
            model="openai/gpt-5.5",
        )

        started = provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-openai-responses",
                session_id="session-openai-responses",
                idempotency_key="idem-openai-responses",
                messages=[agent_pb2.AgentMessage(role="user", text="Use a tool.")],
            )
        )
        fetched = _wait_for_turn(provider_client, "turn-openai-responses", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)
        self.assertEqual(fetched.status, agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.output_text, "Responses route works.")

        self.assertEqual(len(fake_llm.requests), 2)
        first_request = fake_llm.requests[0]
        self.assertEqual(first_request["model"], "gpt-5.5")
        self.assertEqual(first_request["reasoning"], {"summary": "auto", "effort": "xhigh"})
        self.assertEqual(first_request["max_output_tokens"], 64)
        self.assertEqual(first_request["tool_choice"], {"type": "function", "name": "gestalt_search_tools"})
        self.assertNotIn("reasoning_effort", first_request)
        self.assertNotIn("max_completion_tokens", first_request)
        self.assertEqual(first_request["tools"][0]["type"], "function")
        self.assertEqual(first_request["tools"][0]["name"], "gestalt_search_tools")
        self.assertNotIn("function", first_request["tools"][0])

        second_input = fake_llm.requests[1]["input"]
        self.assertIn({"id": "rs-tool-search", "type": "reasoning", "summary": [], "status": "completed"}, second_input)
        self.assertTrue(
            any(item.get("type") == "function_call" and item.get("id") == "fc-search" for item in second_input)
        )
        self.assertTrue(any(item.get("type") == "function_call_output" for item in second_input))
        self.assertIn("person_lookup", [tool["name"] for tool in fake_llm.requests[1]["tools"]])

    def test_cancel_turn_marks_active_turn_canceled(self) -> None:
        assert _host_servicer is not None
        _, provider_client = _configure_provider()
        _create_session(provider_client, session_id="session-cancel", idempotency_key="session-idem-cancel")

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
                                        "id": "call-search-10",
                                        "type": "function",
                                        "function": {
                                            "name": "gestalt_search_tools",
                                            "arguments": '{"query":"person lookup"}',
                                        },
                                    }
                                ],
                            },
                            "finish_reason": "tool_calls",
                        }
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 3, "total_tokens": 13},
                },
                {
                    "id": "chatcmpl-11",
                    "object": "chat.completion",
                    "created": 1710000011,
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
                    "usage": {"prompt_tokens": 12, "completion_tokens": 3, "total_tokens": 15},
                },
            ]
        )
        fake_llm.start()
        self.addCleanup(fake_llm.close)

        provider_options = struct_pb2.Struct()
        provider_options.update({"base_url": f"{fake_llm.base_url}/v1", "api_key": "test-key"})

        _host_servicer.tools = [_fake_resolved_tool(name="")]
        _host_servicer.pause_on_lookup = True
        _host_servicer.wait_until_released.clear()

        started_holder: dict[str, Any] = {}

        def run_start() -> None:
            started_holder["run"] = provider_client.CreateTurn(
                agent_pb2.CreateAgentProviderTurnRequest(
                    turn_id="turn-cancel",
                    session_id="session-cancel",
                    idempotency_key="idem-cancel",
                    model="fast",
                    messages=[agent_pb2.AgentMessage(role="user", text="Who is Grace Hopper?")],
                    provider_options=provider_options,
                )
            )

        thread = threading.Thread(target=run_start, daemon=True)
        thread.start()

        for _ in range(50):
            if _host_servicer.requests:
                break
            time.sleep(0.05)

        canceled = provider_client.CancelTurn(
            agent_pb2.CancelAgentProviderTurnRequest(turn_id="turn-cancel", reason="user canceled")
        )
        _host_servicer.wait_until_released.set()
        thread.join(timeout=5)

        started = started_holder["run"]
        fetched = provider_client.GetTurn(agent_pb2.GetAgentProviderTurnRequest(turn_id="turn-cancel"))
        events = provider_client.ListTurnEvents(agent_pb2.ListAgentProviderTurnEventsRequest(turn_id="turn-cancel"))

        self.assertEqual(canceled.status, agent_pb2.AGENT_EXECUTION_STATUS_CANCELED)
        self.assertEqual(canceled.status_message, "user canceled")
        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)
        self.assertEqual(fetched.status, agent_pb2.AGENT_EXECUTION_STATUS_CANCELED)
        self.assertEqual(len(_host_servicer.search_requests), 1)
        self.assertEqual(len(fake_llm.requests), 2)
        self.assertEqual(
            [event.type for event in events.events],
            ["turn.started", "tool.started", "tool.completed", "tool.started", "turn.canceled"],
        )
        self.assertEqual(events.events[-1].data.fields["reason"].string_value, "user canceled")

    def test_create_session_rejects_empty_session_id(self) -> None:
        _, provider_client = _configure_provider()

        with self.assertRaises(grpc.RpcError) as exc:
            provider_client.CreateSession(
                agent_pb2.CreateAgentProviderSessionRequest(idempotency_key="session-idem-empty", model="fast")
            )

        self.assertEqual(_rpc_error_code(exc.exception), grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(_rpc_error_details(exc.exception), "session_id is required")

    def test_update_session_can_clear_metadata(self) -> None:
        _, provider_client = _configure_provider()
        session_metadata = struct_pb2.Struct()
        session_metadata.update({"mode": "sticky"})

        created = provider_client.CreateSession(
            agent_pb2.CreateAgentProviderSessionRequest(
                session_id="session-update",
                idempotency_key="session-idem-update",
                model="fast",
                metadata=session_metadata,
            )
        )
        self.assertEqual(created.metadata.fields["mode"].string_value, "sticky")

        cleared = provider_client.UpdateSession(
            agent_pb2.UpdateAgentProviderSessionRequest(session_id="session-update", metadata=struct_pb2.Struct())
        )
        fetched = provider_client.GetSession(agent_pb2.GetAgentProviderSessionRequest(session_id="session-update"))

        self.assertEqual(dict(cleared.metadata.fields), {})
        self.assertEqual(dict(fetched.metadata.fields), {})

    def test_create_turn_rejects_cross_session_conflicts(self) -> None:
        _, provider_client = _configure_provider()
        _create_session(provider_client, session_id="session-a", idempotency_key="session-idem-a")
        _create_session(provider_client, session_id="session-b", idempotency_key="session-idem-b")

        fake_llm = _FakeOpenAIChatServer(
            responses=[
                {
                    "id": "chatcmpl-30",
                    "object": "chat.completion",
                    "created": 1710000030,
                    "model": "fake-model",
                    "choices": [
                        {"index": 0, "message": {"role": "assistant", "content": "Done."}, "finish_reason": "stop"}
                    ],
                    "usage": {"prompt_tokens": 8, "completion_tokens": 2, "total_tokens": 10},
                }
            ]
        )
        fake_llm.start()
        self.addCleanup(fake_llm.close)

        provider_options = struct_pb2.Struct()
        provider_options.update({"base_url": f"{fake_llm.base_url}/v1", "api_key": "test-key"})

        started = provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-shared",
                session_id="session-a",
                idempotency_key="idem-shared",
                model="fast",
                messages=[agent_pb2.AgentMessage(role="user", text="Say done.")],
                provider_options=provider_options,
            )
        )
        self.assertEqual(started.session_id, "session-a")
        _wait_for_turn(provider_client, "turn-shared", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        with self.assertRaises(grpc.RpcError) as turn_id_exc:
            provider_client.CreateTurn(
                agent_pb2.CreateAgentProviderTurnRequest(
                    turn_id="turn-shared",
                    session_id="session-b",
                    idempotency_key="idem-other",
                    model="fast",
                    messages=[agent_pb2.AgentMessage(role="user", text="Say done.")],
                    provider_options=provider_options,
                )
            )
        self.assertEqual(_rpc_error_code(turn_id_exc.exception), grpc.StatusCode.ALREADY_EXISTS)
        self.assertIn("session 'session-a'", _rpc_error_details(turn_id_exc.exception))

        with self.assertRaises(grpc.RpcError) as idempotency_exc:
            provider_client.CreateTurn(
                agent_pb2.CreateAgentProviderTurnRequest(
                    turn_id="turn-other",
                    session_id="session-b",
                    idempotency_key="idem-shared",
                    model="fast",
                    messages=[agent_pb2.AgentMessage(role="user", text="Say done.")],
                    provider_options=provider_options,
                )
            )
        self.assertEqual(_rpc_error_code(idempotency_exc.exception), grpc.StatusCode.ALREADY_EXISTS)
        self.assertIn("session 'session-a'", _rpc_error_details(idempotency_exc.exception))

    def test_create_turn_completes_anthropic_tool_loop_and_persists_turn(self) -> None:
        _, provider_client = _configure_provider(
            provider_options={"anthropic": {"thinking": {"type": "adaptive"}, "output_config": {"effort": "medium"}}}
        )
        _create_session(
            provider_client,
            session_id="session-anthropic",
            idempotency_key="session-idem-anthropic",
            model="anthropic/claude-fake-model",
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
                            "type": "thinking",
                            "thinking": "Search for the relevant lookup tool.",
                            "signature": "sig-search",
                        },
                        {
                            "type": "tool_use",
                            "id": "toolu-search-1",
                            "name": "gestalt_search_tools",
                            "input": {"query": "historical figure lookup", "max_results": 5},
                        },
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
                    "content": [
                        {
                            "type": "thinking",
                            "thinking": "Use person_lookup for Ada Lovelace.",
                            "signature": "sig-lookup",
                        },
                        {
                            "type": "tool_use",
                            "id": "toolu-1",
                            "name": "person_lookup",
                            "input": {"query": "Ada Lovelace"},
                        },
                    ],
                    "stop_reason": "tool_use",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 20, "output_tokens": 5},
                },
                {
                    "id": "msg-3",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-fake-model",
                    "content": [{"type": "text", "text": '{"summary":"Ada Lovelace is still relevant."}'}],
                    "stop_reason": "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 30, "output_tokens": 7},
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

        started = provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-anthropic",
                session_id="session-anthropic",
                idempotency_key="idem-anthropic",
                model="anthropic/claude-fake-model",
                messages=[agent_pb2.AgentMessage(role="user", text="Who is Ada Lovelace?")],
                response_schema=response_schema,
                provider_options=provider_options,
            )
        )

        fetched = _wait_for_turn(provider_client, "turn-anthropic", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)
        self.assertEqual(started.model, "anthropic/claude-fake-model")
        self.assertEqual(fetched.status, agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.model, "anthropic/claude-fake-model")
        self.assertEqual(fetched.output_text, '{"summary":"Ada Lovelace is still relevant."}')
        self.assertEqual(fetched.structured_output.fields["summary"].string_value, "Ada Lovelace is still relevant.")

        assert _host_servicer is not None
        self.assertEqual(
            _host_servicer.search_requests,
            [
                _expected_search_request(
                    session_id="session-anthropic",
                    turn_id="turn-anthropic",
                    query="historical figure lookup",
                    max_results=5,
                    candidate_limit=10,
                )
            ],
        )
        self.assertEqual(
            _host_servicer.requests,
            [
                {
                    "session_id": "session-anthropic",
                    "turn_id": "turn-anthropic",
                    "tool_call_id": "toolu-1",
                    "tool_id": "lookup",
                    "arguments": {"query": "Ada Lovelace"},
                    "idempotency_key": _expected_tool_idempotency_key(turn_id="turn-anthropic", tool_call_id="toolu-1"),
                }
            ],
        )

        self.assertEqual(len(fake_anthropic.requests), 3)
        first_request = fake_anthropic.requests[0]
        second_request = fake_anthropic.requests[1]
        third_request = fake_anthropic.requests[2]
        self.assertEqual(first_request["model"], "claude-fake-model")
        self.assertEqual(first_request["messages"][-1]["content"], "Who is Ada Lovelace?")
        self.assertEqual([tool["name"] for tool in first_request["tools"]], ["gestalt_search_tools"])
        self.assertEqual(first_request["thinking"], {"type": "adaptive"})
        self.assertEqual(first_request["output_config"], {"effort": "medium"})
        self.assertIn("Return only valid JSON", first_request["system"])
        self.assertIn("person_lookup", [tool["name"] for tool in second_request["tools"]])
        self.assertEqual(second_request["messages"][1]["role"], "assistant")
        self.assertEqual(second_request["messages"][1]["content"][0]["type"], "thinking")
        self.assertEqual(second_request["messages"][1]["content"][0]["signature"], "sig-search")
        self.assertEqual(second_request["messages"][1]["content"][1]["type"], "tool_use")
        self.assertEqual(second_request["messages"][2]["role"], "user")
        self.assertEqual(second_request["messages"][2]["content"][0]["type"], "tool_result")
        self.assertEqual(third_request["messages"][3]["role"], "assistant")
        self.assertEqual(third_request["messages"][3]["content"][0]["type"], "thinking")
        self.assertEqual(third_request["messages"][3]["content"][0]["signature"], "sig-lookup")
        self.assertEqual(third_request["messages"][3]["content"][1]["name"], "person_lookup")
        self.assertEqual(third_request["messages"][4]["content"][0]["type"], "tool_result")

    def test_create_turn_returns_anthropic_tool_error_for_invalid_arguments(self) -> None:
        _, provider_client = _configure_provider()
        _create_session(
            provider_client,
            session_id="session-anthropic-tool-error",
            idempotency_key="session-idem-anthropic-tool-error",
            model="anthropic/claude-fake-model",
        )

        fake_anthropic = _FakeAnthropicMessagesServer(
            responses=[
                {
                    "id": "msg-invalid-tool",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-fake-model",
                    "content": [{"type": "tool_use", "id": "toolu-invalid", "name": "person_lookup", "input": {}}],
                    "stop_reason": "tool_use",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 10, "output_tokens": 5},
                },
                {
                    "id": "msg-recovered",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-fake-model",
                    "content": [{"type": "text", "text": "I need a query before using that tool."}],
                    "stop_reason": "end_turn",
                    "stop_sequence": None,
                    "usage": {"input_tokens": 20, "output_tokens": 5},
                },
            ]
        )
        fake_anthropic.start()
        self.addCleanup(fake_anthropic.close)

        provider_options = struct_pb2.Struct()
        provider_options.update({"base_url": fake_anthropic.base_url, "api_key": "test-key"})

        started = provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-anthropic-tool-error",
                session_id="session-anthropic-tool-error",
                idempotency_key="idem-anthropic-tool-error",
                model="anthropic/claude-fake-model",
                messages=[agent_pb2.AgentMessage(role="user", text="Look up a person.")],
                tools=[_fake_resolved_tool(name="person_lookup")],
                provider_options=provider_options,
            )
        )

        fetched = _wait_for_turn(
            provider_client, "turn-anthropic-tool-error", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED
        )

        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)
        self.assertEqual(fetched.output_text, "I need a query before using that tool.")
        assert _host_servicer is not None
        self.assertEqual(_host_servicer.requests, [])
        self.assertEqual(len(fake_anthropic.requests), 2)
        tool_result = fake_anthropic.requests[1]["messages"][2]["content"][0]
        self.assertEqual(tool_result["type"], "tool_result")
        self.assertEqual(tool_result["tool_use_id"], "toolu-invalid")
        self.assertEqual(tool_result["is_error"], True)
        self.assertIn("query", tool_result["content"])
        self.assertIn("required", tool_result["content"])

    def test_create_turn_keeps_openai_compatible_prefixed_models_and_provider_overrides(self) -> None:
        _, provider_client = _configure_provider()
        _create_session(
            provider_client, session_id="session-compat", idempotency_key="session-idem-compat", model="groq/fake-model"
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
                "litellm": {"frequency_penalty": 1.1, "temperature": 0.9},
                "temperature": 0.2,
                "groq": {"base_url": f"{fake_llm.base_url}/v1", "api_key": "test-key", "top_p": 0.9},
            }
        )

        started = provider_client.CreateTurn(
            agent_pb2.CreateAgentProviderTurnRequest(
                turn_id="turn-compat",
                session_id="session-compat",
                idempotency_key="idem-compat",
                model="groq/fake-model",
                messages=[agent_pb2.AgentMessage(role="user", text="Say something brief.")],
                provider_options=provider_options,
            )
        )

        fetched = _wait_for_turn(provider_client, "turn-compat", agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)

        self.assertEqual(started.status, agent_pb2.AGENT_EXECUTION_STATUS_RUNNING)
        self.assertEqual(started.model, "groq/fake-model")
        self.assertEqual(fetched.status, agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED)
        self.assertEqual(fetched.model, "groq/fake-model")
        self.assertEqual(fetched.output_text, "Prefixed OpenAI-compatible models still work.")

        self.assertEqual(len(fake_llm.requests), 1)
        request = fake_llm.requests[0]
        self.assertEqual(request["model"], "groq/fake-model")
        self.assertEqual(request["temperature"], 0.2)
        self.assertEqual(request["top_p"], 0.9)
        self.assertNotIn("litellm", request)
        self.assertNotIn("frequency_penalty", request)


def _wait_for_turn(provider_client: Any, turn_id: str, status: int, timeout_seconds: float = 5) -> Any:
    deadline = time.time() + timeout_seconds
    last = None
    while time.time() < deadline:
        last = provider_client.GetTurn(agent_pb2.GetAgentProviderTurnRequest(turn_id=turn_id))
        if last.status == status:
            return last
        time.sleep(0.05)
    raise AssertionError(f"turn {turn_id!r} did not reach status {status}; last={last}")


def _rpc_error_code(exc: grpc.RpcError) -> grpc.StatusCode:
    return cast(Any, exc).code()


def _rpc_error_details(exc: grpc.RpcError) -> str:
    return str(cast(Any, exc).details())


if __name__ == "__main__":
    unittest.main()
