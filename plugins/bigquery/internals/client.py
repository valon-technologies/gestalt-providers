from __future__ import annotations

import base64
import datetime as dt
import decimal
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Final, Protocol, TypeAlias

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference, QueryJobConfig, SchemaField
from google.oauth2.credentials import Credentials

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | list["JSONValue"] | dict[str, "JSONValue"]
QueryRow: TypeAlias = dict[str, JSONValue]


class BigQueryRow(Protocol):
    def items(self) -> Iterable[tuple[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class QueryRequest:
    access_token: str
    project_id: str
    sql: str
    dataset: str | None = None
    max_results: int = 500
    timeout_seconds: int = 60
    use_legacy_sql: bool = False

    @property
    def row_limit(self) -> int:
        return max(0, self.max_results)

    @property
    def timeout(self) -> int | None:
        if self.timeout_seconds <= 0:
            return None
        return self.timeout_seconds


@dataclass(frozen=True, slots=True)
class QueryExecutionResult:
    schema: tuple[SchemaField, ...]
    rows: tuple[QueryRow, ...]
    total_rows: int


class QueryExecutor(Protocol):
    def execute(self, request: QueryRequest) -> QueryExecutionResult: ...


class GoogleBigQueryQueryExecutor:
    def execute(self, request: QueryRequest) -> QueryExecutionResult:
        query_timeout = request.timeout
        with bigquery.Client(
            project=request.project_id,
            credentials=Credentials(token=request.access_token),
        ) as client:
            job = client.query(
                request.sql,
                job_config=query_job_config(request),
                timeout=query_timeout,
                project=request.project_id,
            )
            iterator = job.result(timeout=query_timeout)
            return QueryExecutionResult(
                schema=tuple(iterator.schema),
                rows=query_rows(iterator, limit=request.row_limit),
                total_rows=int(iterator.total_rows or 0),
            )


DEFAULT_QUERY_EXECUTOR: Final[QueryExecutor] = GoogleBigQueryQueryExecutor()


def execute_query(
    request: QueryRequest, executor: QueryExecutor = DEFAULT_QUERY_EXECUTOR
) -> QueryExecutionResult:
    return executor.execute(request)


def query_job_config(request: QueryRequest) -> QueryJobConfig:
    return QueryJobConfig(
        use_legacy_sql=request.use_legacy_sql,
        default_dataset=default_dataset(request.project_id, request.dataset),
    )


def query_rows(rows: Iterable[BigQueryRow], *, limit: int) -> tuple[QueryRow, ...]:
    collected: list[QueryRow] = []
    for index, row in enumerate(rows):
        if index >= limit:
            break
        collected.append(sanitize_row(row.items()))
    return tuple(collected)


def sanitize_row(items: Iterable[tuple[str, Any]]) -> QueryRow:
    return {key: sanitize_value(value) for key, value in items}


def sanitize_value(value: Any) -> JSONValue:
    if isinstance(value, decimal.Decimal):
        return format(value, "f")
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        if math.isnan(value):
            return "NaN"
        if value > 0:
            return "Infinity"
        return "-Infinity"
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, Mapping):
        return {str(key): sanitize_value(item) for key, item in value.items()}
    row_items = _row_items(value)
    if row_items is not None:
        return sanitize_row(row_items)
    if isinstance(value, (list, tuple)):
        return [sanitize_value(item) for item in value]
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, (dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _row_items(value: Any) -> Iterable[tuple[str, Any]] | None:
    items = getattr(value, "items", None)
    if not callable(items):
        return None
    return items()


def default_dataset(project_id: str, dataset: str | None) -> DatasetReference | None:
    if not dataset:
        return None
    return DatasetReference.from_string(dataset, default_project=project_id)


def google_api_status(err: GoogleAPICallError) -> HTTPStatus:
    code = getattr(err, "code", None)
    if isinstance(code, HTTPStatus):
        status = code
    elif isinstance(code, int):
        try:
            status = HTTPStatus(code)
        except ValueError:
            return HTTPStatus.INTERNAL_SERVER_ERROR
    else:
        return HTTPStatus.INTERNAL_SERVER_ERROR
    if status < HTTPStatus.BAD_REQUEST:
        return HTTPStatus.INTERNAL_SERVER_ERROR
    return status


def google_api_message(err: GoogleAPICallError) -> str:
    message = getattr(err, "message", "")
    if isinstance(message, str) and message:
        return message
    text = str(err).strip()
    if text:
        return text
    return "BigQuery request failed"
