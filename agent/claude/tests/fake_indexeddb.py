from __future__ import annotations

import threading
from datetime import datetime
from typing import Any, cast

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import json_format
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from gestalt._gen.v1 import indexeddb_pb2 as _indexeddb_pb2
from gestalt._gen.v1 import indexeddb_pb2_grpc as _indexeddb_pb2_grpc

indexeddb_pb2: Any = cast(Any, _indexeddb_pb2)
indexeddb_pb2_grpc: Any = _indexeddb_pb2_grpc
empty_pb2: Any = _empty_pb2
struct_pb2: Any = _struct_pb2
timestamp_pb2: Any = _timestamp_pb2


class FakeIndexedDB(indexeddb_pb2_grpc.IndexedDBServicer):
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._stores: dict[str, dict[str, Any]] = {}
        self._before_transaction_add: Any | None = None
        self._operation_counts: dict[tuple[str, str], int] = {}
        self._cursor_commands: dict[str, list[list[str]]] = {}
        self._created_stores: list[str] = []

    def reset(self) -> None:
        with self._lock:
            self._stores.clear()
            self._before_transaction_add = None
            self._operation_counts.clear()
            self._cursor_commands.clear()
            self._created_stores.clear()

    def operation_count(self, *, store: str, operation: str) -> int:
        with self._lock:
            return self._operation_counts.get((store, operation), 0)

    def cursor_commands(self, *, store: str) -> list[list[str]]:
        with self._lock:
            return [list(commands) for commands in self._cursor_commands.get(store, [])]

    def created_stores(self) -> list[str]:
        with self._lock:
            return list(self._created_stores)

    def put_record(
        self, store: str, record: dict[str, Any], *, transaction_stores: dict[str, dict[str, Any]] | None = None
    ) -> None:
        proto_record = _record_from_dict(record)
        if transaction_stores is not None:
            transaction_stores.setdefault(store, {})[_record_id(proto_record)] = _copy_record(proto_record)
        with self._lock:
            self._stores.setdefault(store, {})[_record_id(proto_record)] = proto_record

    def inject_before_transaction_add(self, hook: Any) -> None:
        with self._lock:
            self._before_transaction_add = hook

    def _record_operation(self, *, store: str, operation: str) -> None:
        self._operation_counts[(store, operation)] = self._operation_counts.get((store, operation), 0) + 1

    def _start_cursor_commands(self, *, store: str) -> list[str]:
        commands: list[str] = []
        with self._lock:
            self._cursor_commands.setdefault(store, []).append(commands)
        return commands

    def _record_cursor_command(self, commands: list[str], command: str) -> None:
        with self._lock:
            commands.append(command)

    def CreateObjectStore(self, request: Any, context: grpc.ServicerContext) -> Any:
        with self._lock:
            if request.name in self._stores:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, "object store already exists")
            self._stores.setdefault(request.name, {})
            self._created_stores.append(request.name)
        return empty_pb2.Empty()

    def Get(self, request: Any, context: grpc.ServicerContext) -> Any:
        with self._lock:
            self._record_operation(store=request.store, operation="get")
            record = self._stores.get(request.store, {}).get(request.id)
            if record is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "record not found")
            return indexeddb_pb2.RecordResponse(record=_copy_record(record))

    def Add(self, request: Any, context: grpc.ServicerContext) -> Any:
        record_id = _record_id(request.record)
        with self._lock:
            self._record_operation(store=request.store, operation="add")
            store = self._stores.setdefault(request.store, {})
            if record_id in store:
                context.abort(grpc.StatusCode.ALREADY_EXISTS, "record already exists")
            store[record_id] = _copy_record(request.record)
        return empty_pb2.Empty()

    def Put(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        with self._lock:
            self._record_operation(store=request.store, operation="put")
            self._stores.setdefault(request.store, {})[_record_id(request.record)] = _copy_record(request.record)
        return empty_pb2.Empty()

    def Delete(self, request: Any, context: grpc.ServicerContext) -> Any:
        with self._lock:
            self._record_operation(store=request.store, operation="delete")
            store = self._stores.get(request.store, {})
            if request.id not in store:
                context.abort(grpc.StatusCode.NOT_FOUND, "record not found")
            del store[request.id]
        return empty_pb2.Empty()

    def Clear(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        with self._lock:
            self._record_operation(store=request.store, operation="clear")
            self._stores.setdefault(request.store, {}).clear()
        return empty_pb2.Empty()

    def GetAll(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        with self._lock:
            self._record_operation(store=request.store, operation="get_all")
            return indexeddb_pb2.RecordsResponse(
                records=[
                    _copy_record(record)
                    for record in _records_for_request_range(self._stores.get(request.store, {}), request)
                ]
            )

    def OpenCursor(self, request_iterator: Any, context: grpc.ServicerContext) -> Any:
        try:
            first = next(request_iterator)
        except StopIteration:
            return
        if first.WhichOneof("msg") != "open":
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "first cursor message must be open")
        open_req = first.open
        with self._lock:
            self._record_operation(store=open_req.store, operation="open_cursor")
            records = _records_for_request_range(self._stores.get(open_req.store, {}), open_req)
        command_log = self._start_cursor_commands(store=open_req.store)
        yield indexeddb_pb2.CursorResponse(done=False)

        cursor_index = -1
        for message in request_iterator:
            if message.WhichOneof("msg") != "command":
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "cursor command is required")
            command = message.command
            kind = command.WhichOneof("command")
            self._record_cursor_command(command_log, str(kind))
            if kind == "next":
                cursor_index += 1
            elif kind == "advance":
                cursor_index += max(1, int(command.advance or 0))
            elif kind == "continue_to_key":
                target = _cursor_target_key(command.continue_to_key)
                cursor_index += 1
                while cursor_index < len(records) and _record_id(records[cursor_index]) < target:
                    cursor_index += 1
            elif kind == "close":
                return
            else:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "unsupported cursor command")

            if cursor_index >= len(records):
                yield indexeddb_pb2.CursorResponse(done=True)
                continue
            record = records[cursor_index]
            record_id = _record_id(record)
            yield indexeddb_pb2.CursorResponse(
                entry=indexeddb_pb2.CursorEntry(
                    key=indexeddb_pb2.KeyValue(scalar=indexeddb_pb2.TypedValue(string_value=record_id)),
                    primary_key=record_id,
                    record=_copy_record(record),
                )
            )

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
            yield indexeddb_pb2.TransactionServerMessage(begin=indexeddb_pb2.TransactionBeginResponse())
            for message in request_iterator:
                kind = message.WhichOneof("msg")
                if kind == "operation":
                    response = self._apply_transaction_operation(
                        stores=working,
                        operation=message.operation,
                        scoped_stores=scoped_stores,
                        readwrite=mode == indexeddb_pb2.TRANSACTION_READWRITE,
                    )
                    yield indexeddb_pb2.TransactionServerMessage(operation=response)
                    if response.HasField("error") and response.error.code:
                        return
                    continue
                if kind == "commit":
                    self._stores = working
                    yield indexeddb_pb2.TransactionServerMessage(commit=indexeddb_pb2.TransactionCommitResponse())
                    return
                if kind == "abort":
                    yield indexeddb_pb2.TransactionServerMessage(abort=indexeddb_pb2.TransactionAbortResponse())
                    return
                response = indexeddb_pb2.TransactionAbortResponse()
                _set_status(response.error, grpc.StatusCode.INVALID_ARGUMENT, "unknown transaction message")
                yield indexeddb_pb2.TransactionServerMessage(abort=response)
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
        self._record_operation(store=store_name, operation=kind)
        if scoped_stores and store_name not in scoped_stores:
            return _transaction_error(
                request_id, grpc.StatusCode.FAILED_PRECONDITION, "object store is outside transaction scope"
            )
        if kind in {"add", "put", "delete", "clear"} and not readwrite:
            return _transaction_error(request_id, grpc.StatusCode.FAILED_PRECONDITION, "transaction is readonly")

        store = stores.setdefault(store_name, {})
        if kind == "get":
            record = store.get(request.id)
            if record is None:
                return _transaction_error(request_id, grpc.StatusCode.NOT_FOUND, "record not found")
            return indexeddb_pb2.TransactionOperationResponse(
                request_id=request_id, record=indexeddb_pb2.RecordResponse(record=_copy_record(record))
            )
        if kind == "add":
            record_id = _record_id(request.record)
            if self._before_transaction_add is not None:
                hook = self._before_transaction_add
                self._before_transaction_add = None
                hook(self, stores, store_name, request)
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
            return indexeddb_pb2.TransactionOperationResponse(
                request_id=request_id,
                records=indexeddb_pb2.RecordsResponse(
                    records=[_copy_record(record) for record in _records_for_request_range(store, request)]
                ),
            )
        return _transaction_error(
            request_id, grpc.StatusCode.INVALID_ARGUMENT, f"unsupported transaction operation {kind}"
        )


def _copy_record(record: Any) -> Any:
    copied = indexeddb_pb2.Record()
    copied.CopyFrom(record)
    return copied


def _record_from_dict(record: dict[str, Any]) -> Any:
    out = indexeddb_pb2.Record()
    for key, value in record.items():
        out.fields[str(key)].CopyFrom(_typed_value_from_python(value))
    return out


def _typed_value_from_python(value: Any) -> Any:
    typed = indexeddb_pb2.TypedValue()
    if value is None:
        setattr(typed, "null_value", 0)
    elif isinstance(value, bool):
        typed.bool_value = value
    elif isinstance(value, int):
        typed.int_value = value
    elif isinstance(value, float):
        typed.float_value = value
    elif isinstance(value, str):
        typed.string_value = value
    elif isinstance(value, datetime):
        stamp = timestamp_pb2.Timestamp()
        stamp.FromDatetime(value)
        typed.time_value.CopyFrom(stamp)
    else:
        json_value = struct_pb2.Value()
        json_value.CopyFrom(json_format.ParseDict(value, struct_pb2.Value()))
        typed.json_value.CopyFrom(json_value)
    return typed


def _copy_stores(stores: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        name: {record_id: _copy_record(record) for record_id, record in store.items()} for name, store in stores.items()
    }


def _record_id(record: Any) -> str:
    record_id = str(record.fields["id"].string_value or "").strip()
    if not record_id:
        raise ValueError("record id is required")
    return record_id


def _records_for_request_range(store: dict[str, Any], request: Any) -> list[Any]:
    records = sorted(store.values(), key=_record_id)
    if not _proto_message_has_field(request, "query") or not request.HasField("query"):
        return records
    query = request.query
    selector = query.WhichOneof("query")
    if selector == "range":
        return [record for record in records if _record_id_matches_range(_record_id(record), query.range)]
    if selector == "key":
        target = str(_typed_value_to_python(query.key.scalar) or "")
        return [record for record in records if _record_id(record) == target]
    return records


def _record_id_matches_range(record_id: str, key_range: Any) -> bool:
    if key_range.HasField("lower"):
        lower = str(_typed_value_to_python(key_range.lower.scalar) or "")
        if record_id < lower or (key_range.lower_open and record_id == lower):
            return False
    if key_range.HasField("upper"):
        upper = str(_typed_value_to_python(key_range.upper.scalar) or "")
        if record_id > upper or (key_range.upper_open and record_id == upper):
            return False
    return True


def _cursor_target_key(target: Any) -> str:
    if not target.HasField("key"):
        return ""
    return str(_typed_value_to_python(target.key.scalar) or "")


def _typed_value_to_python(value: Any) -> Any:
    kind = value.WhichOneof("kind")
    if kind == "string_value":
        return value.string_value
    if kind == "int_value":
        return int(value.int_value)
    if kind == "float_value":
        return float(value.float_value)
    if kind == "bool_value":
        return bool(value.bool_value)
    return None


def _transaction_error(request_id: int, code: Any, message: str) -> Any:
    response = indexeddb_pb2.TransactionOperationResponse(request_id=request_id)
    _set_status(response.error, code, message)
    return response


def _transaction_empty(request_id: int) -> Any:
    return indexeddb_pb2.TransactionOperationResponse(request_id=request_id, empty=empty_pb2.Empty())


def _set_status(status: Any, code: Any, message: str) -> None:
    status.code = int(code.value[0])
    status.message = message


def _proto_message_has_field(message: Any, field_name: str) -> bool:
    return any(field.name == field_name for field in message.DESCRIPTOR.fields)
