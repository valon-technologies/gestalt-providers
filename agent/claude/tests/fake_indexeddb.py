from __future__ import annotations

import threading
from datetime import datetime
from typing import Any, cast

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import json_format
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2

from gestalt._gen.v1 import datastore_pb2 as _datastore_pb2
from gestalt._gen.v1 import datastore_pb2_grpc as _datastore_pb2_grpc

datastore_pb2: Any = cast(Any, _datastore_pb2)
datastore_pb2_grpc: Any = _datastore_pb2_grpc
empty_pb2: Any = _empty_pb2
struct_pb2: Any = _struct_pb2
timestamp_pb2: Any = _timestamp_pb2


class FakeIndexedDB(datastore_pb2_grpc.IndexedDBServicer):
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._stores: dict[str, dict[str, Any]] = {}
        self._before_transaction_add: Any | None = None

    def reset(self) -> None:
        with self._lock:
            self._stores.clear()
            self._before_transaction_add = None

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

    def CreateObjectStore(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        with self._lock:
            self._stores.setdefault(request.name, {})
        return empty_pb2.Empty()

    def Get(self, request: Any, context: grpc.ServicerContext) -> Any:
        with self._lock:
            record = self._stores.get(request.store, {}).get(request.id)
            if record is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "record not found")
            return datastore_pb2.RecordResponse(record=_copy_record(record))

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

    def GetAll(self, request: Any, context: grpc.ServicerContext) -> Any:
        del context
        with self._lock:
            return datastore_pb2.RecordsResponse(
                records=[
                    _copy_record(record)
                    for record in _records_for_request_range(self._stores.get(request.store, {}), request)
                ]
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
            return datastore_pb2.TransactionOperationResponse(
                request_id=request_id, record=datastore_pb2.RecordResponse(record=_copy_record(record))
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
        if kind == "get_all":
            return datastore_pb2.TransactionOperationResponse(
                request_id=request_id,
                records=datastore_pb2.RecordsResponse(
                    records=[_copy_record(record) for record in _records_for_request_range(store, request)]
                ),
            )
        return _transaction_error(
            request_id, grpc.StatusCode.INVALID_ARGUMENT, f"unsupported transaction operation {kind}"
        )


def _copy_record(record: Any) -> Any:
    copied = datastore_pb2.Record()
    copied.CopyFrom(record)
    return copied


def _record_from_dict(record: dict[str, Any]) -> Any:
    out = datastore_pb2.Record()
    for key, value in record.items():
        out.fields[str(key)].CopyFrom(_typed_value_from_python(value))
    return out


def _typed_value_from_python(value: Any) -> Any:
    typed = datastore_pb2.TypedValue()
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
    if not _proto_message_has_field(request, "range") or not request.HasField("range"):
        return records
    key_range = request.range
    return [record for record in records if _record_id_matches_range(_record_id(record), key_range)]


def _record_id_matches_range(record_id: str, key_range: Any) -> bool:
    if key_range.HasField("lower"):
        lower = str(_typed_value_to_python(key_range.lower) or "")
        if record_id < lower or (key_range.lower_open and record_id == lower):
            return False
    if key_range.HasField("upper"):
        upper = str(_typed_value_to_python(key_range.upper) or "")
        if record_id > upper or (key_range.upper_open and record_id == upper):
            return False
    return True


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
    response = datastore_pb2.TransactionOperationResponse(request_id=request_id)
    _set_status(response.error, code, message)
    return response


def _transaction_empty(request_id: int) -> Any:
    return datastore_pb2.TransactionOperationResponse(request_id=request_id, empty=empty_pb2.Empty())


def _set_status(status: Any, code: Any, message: str) -> None:
    status.code = int(code.value[0])
    status.message = message


def _proto_message_has_field(message: Any, field_name: str) -> bool:
    return any(field.name == field_name for field in message.DESCRIPTOR.fields)
