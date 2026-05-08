from __future__ import annotations

import time
from typing import Any

import gestalt
import grpc

BUSY_RETRY_INITIAL_DELAY_SECONDS = 0.02
BUSY_RETRY_MAX_DELAY_SECONDS = 0.25


class StoreUnavailableError(RuntimeError):
    pass


def _call_with_busy_retry(operation: Any) -> Any:
    delay = BUSY_RETRY_INITIAL_DELAY_SECONDS
    while True:
        try:
            return operation()
        except (grpc.RpcError, gestalt.TransactionError) as exc:
            if _is_busy_error(exc):
                time.sleep(delay)
                delay = min(delay * 2, BUSY_RETRY_MAX_DELAY_SECONDS)
                continue
            if _is_unavailable_error(exc):
                raise StoreUnavailableError(_indexeddb_unavailable_message(_error_details(exc))) from exc
            raise


def _is_busy_error(exc: BaseException) -> bool:
    code_fn = getattr(exc, "code", None)
    code = code_fn() if callable(code_fn) else None
    details = _error_details(exc).lower()
    if code is not None and code != grpc.StatusCode.INTERNAL:
        return False
    return "database is locked" in details or "sqlite_busy" in details or "sql_busy" in details


def _is_unavailable_error(exc: BaseException) -> bool:
    code_fn = getattr(exc, "code", None)
    code = code_fn() if callable(code_fn) else None
    if code == grpc.StatusCode.UNAVAILABLE:
        return True
    details = _error_details(exc).lower()
    return "failed to connect" in details or "connection refused" in details or "no such file" in details


def _error_details(exc: BaseException) -> str:
    details_fn = getattr(exc, "details", None)
    if callable(details_fn):
        return str(details_fn() or exc)
    return str(exc)


def _indexeddb_unavailable_message(details: str) -> str:
    env_name = gestalt.indexeddb_socket_env()
    detail = details.strip()
    suffix = f": {detail}" if detail else ""
    return f"agent/claude requires an IndexedDB host socket binding via {env_name}{suffix}"
