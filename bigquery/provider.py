import datetime as dt
import decimal
from http import HTTPStatus
from typing import Any

import gestalt
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, SchemaField
from google.oauth2.credentials import Credentials


class QueryInput(gestalt.Model):
    project_id: str = gestalt.field(description="GCP project ID")
    query: str = gestalt.field(description="SQL query to execute")
    max_results: int = gestalt.field(
        description="Maximum number of rows to return",
        default=500,
        required=False,
    )
    timeout_seconds: int = gestalt.field(
        description="Query timeout in seconds",
        default=60,
        required=False,
    )
    use_legacy_sql: bool = gestalt.field(
        description="Use legacy SQL syntax",
        default=False,
        required=False,
    )


class QuerySchemaField(gestalt.Model):
    name: str
    type: str
    mode: str


class QueryOutput(gestalt.Model):
    schema: list[QuerySchemaField]
    rows: list[dict[str, Any]]
    total_rows: int
    job_complete: bool


@gestalt.operation(description="Execute a BigQuery SQL query")
def query(input: QueryInput, req: gestalt.Request) -> QueryOutput | gestalt.Response[dict[str, str]]:
    if not input.project_id:
        return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": "project_id is required"})
    if not input.query:
        return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": "query is required"})

    max_results = max(0, input.max_results)
    timeout_seconds = input.timeout_seconds if input.timeout_seconds > 0 else None
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
