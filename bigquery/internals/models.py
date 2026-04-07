from typing import Any

import gestalt


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
