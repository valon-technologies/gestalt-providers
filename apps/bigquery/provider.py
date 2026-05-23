from __future__ import annotations

from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.bigquery import SchemaField

from internals.client import (
    QueryRequest,
    execute_query,
    google_api_message,
    google_api_status,
)

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]


class QueryInput(gestalt.Model):
    project_id: str = gestalt.field(description="GCP project ID")
    query: str = gestalt.field(description="SQL query to execute")
    dataset: str | None = gestalt.field(
        description="Optional default dataset for unqualified table names",
        default=None,
        required=False,
    )
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


QueryResult: TypeAlias = QueryOutput | ErrorResponse


@gestalt.operation(description="Execute a BigQuery SQL query", tags=["bq", "sql"])
def query(input: QueryInput, req: gestalt.Request) -> QueryResult:
    try:
        result = execute_query(
            QueryRequest(
                access_token=req.token,
                project_id=_require_trimmed_text(input.project_id, "project_id"),
                dataset=_optional_trimmed_text(input.dataset),
                sql=_require_trimmed_text(input.query, "query"),
                max_results=input.max_results,
                timeout_seconds=input.timeout_seconds,
                use_legacy_sql=input.use_legacy_sql,
            )
        )
        return QueryOutput(
            schema=convert_schema(result.schema),
            rows=list(result.rows),
            total_rows=result.total_rows,
            job_complete=True,
        )
    except GoogleAPICallError as err:
        return gestalt.Response(
            status=google_api_status(err),
            body={"error": google_api_message(err)},
        )
    except ValueError as err:
        return gestalt.Response(
            status=HTTPStatus.BAD_REQUEST,
            body={"error": str(err)},
        )


def convert_schema(schema: tuple[SchemaField, ...]) -> list[QuerySchemaField]:
    return [
        QuerySchemaField(
            name=field.name,
            type=field.field_type,
            mode=field.mode or "NULLABLE",
        )
        for field in schema
    ]


def _require_trimmed_text(value: str, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _optional_trimmed_text(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip() or None
