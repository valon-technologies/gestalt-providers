import datetime as dt
import decimal
from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, SchemaField
from google.oauth2.credentials import Credentials

from .models import QueryInput, QueryOutput, QuerySchemaField

QueryResult: TypeAlias = QueryOutput | gestalt.Response[dict[str, str]]


def query_operation(input: QueryInput, req: gestalt.Request) -> QueryResult:
    if not input.project_id:
        return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": "project_id is required"})
    if not input.query:
        return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": "query is required"})

    max_results = max(0, input.max_results)
    timeout_seconds = input.timeout_seconds if input.timeout_seconds > 0 else None
    try:
        with bigquery.Client(project=input.project_id, credentials=Credentials(token=req.token)) as client:
            job = client.query(
                input.query,
                job_config=QueryJobConfig(use_legacy_sql=input.use_legacy_sql),
                timeout=timeout_seconds,
                project=input.project_id,
            )
            iterator = job.result(timeout=timeout_seconds)
            rows: list[dict[str, Any]] = []
            for index, row in enumerate(iterator):
                if index >= max_results:
                    break
                rows.append(sanitize_row(dict(row.items())))

            return QueryOutput(
                schema=convert_schema(iterator.schema),
                rows=rows,
                total_rows=int(iterator.total_rows or 0),
                job_complete=True,
            )
    except GoogleAPICallError as err:
        return gestalt.Response(
            status=google_api_status(err),
            body={"error": google_api_message(err)},
        )


def convert_schema(schema: list[SchemaField]) -> list[QuerySchemaField]:
    return [
        QuerySchemaField(
            name=field.name,
            type=field.field_type,
            mode=field.mode or "NULLABLE",
        )
        for field in schema
    ]


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
