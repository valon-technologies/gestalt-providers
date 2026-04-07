import datetime as dt
import decimal
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, SchemaField
from google.oauth2.credentials import Credentials


@dataclass
class QueryExecutionResult:
    schema: list[SchemaField]
    rows: list[dict[str, Any]]
    total_rows: int


def query_operation(
    *,
    access_token: str,
    project_id: str,
    dataset: str | None,
    query: str,
    max_results: int,
    timeout_seconds: int,
    use_legacy_sql: bool,
) -> QueryExecutionResult:
    max_results = max(0, max_results)
    query_timeout = timeout_seconds if timeout_seconds > 0 else None
    with bigquery.Client(project=project_id, credentials=Credentials(token=access_token)) as client:
        job = client.query(
            query,
            job_config=QueryJobConfig(
                use_legacy_sql=use_legacy_sql,
                default_dataset=default_dataset(project_id, dataset),
            ),
            timeout=query_timeout,
            project=project_id,
        )
        iterator = job.result(timeout=query_timeout)
        rows: list[dict[str, Any]] = []
        for index, row in enumerate(iterator):
            if index >= max_results:
                break
            rows.append(sanitize_row(dict(row.items())))

        return QueryExecutionResult(schema=list(iterator.schema), rows=rows, total_rows=int(iterator.total_rows or 0))


def sanitize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: sanitize_value(value) for key, value in row.items()}


def sanitize_value(value: Any) -> Any:
    if isinstance(value, decimal.Decimal):
        return format(value, "f")
    if isinstance(value, dict):
        return {key: sanitize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_value(item) for item in value]
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, (dt.date, dt.time)):
        return value.isoformat()
    return value


def default_dataset(project_id: str, dataset: str | None) -> str | None:
    if not dataset:
        return None
    if "." in dataset:
        return dataset
    return f"{project_id}.{dataset}"


def google_api_status(err: GoogleAPICallError) -> HTTPStatus:
    code = getattr(err, "code", None)
    if isinstance(code, HTTPStatus):
        status = code
    elif isinstance(code, int):
        status = HTTPStatus(code)
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
