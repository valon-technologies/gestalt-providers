from http import HTTPStatus
from typing import Any

import gestalt
from google.api_core.exceptions import GoogleAPICallError
from google.cloud.bigquery import SchemaField

from internals import google_api_message, google_api_status, query_operation


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

QueryResult = QueryOutput | gestalt.Response[dict[str, str]]


@gestalt.operation(description="Execute a BigQuery SQL query")
def query(input: QueryInput, req: gestalt.Request) -> QueryResult:
    project_id = input.project_id.strip()
    if not project_id:
        return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": "project_id is required"})

    dataset = input.dataset.strip() if input.dataset else None
    query_text = input.query.strip()
    if not query_text:
        return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": "query is required"})

    try:
        result = query_operation(
            access_token=req.token,
            project_id=project_id,
            dataset=dataset or None,
            query=query_text,
            max_results=input.max_results,
            timeout_seconds=input.timeout_seconds,
            use_legacy_sql=input.use_legacy_sql,
        )
        return QueryOutput(
            schema=convert_schema(result.schema),
            rows=result.rows,
            total_rows=result.total_rows,
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
