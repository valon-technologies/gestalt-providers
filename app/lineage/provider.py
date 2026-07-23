from __future__ import annotations

from enum import StrEnum
from http import HTTPStatus
from typing import TypeAlias

import gestalt
from google.api_core.exceptions import GoogleAPICallError
from google.auth.exceptions import DefaultCredentialsError

from internals.lineage import LineageSnapshot, traverse

app = gestalt.App("lineage")

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]


class Direction(StrEnum):
    UPSTREAM = "upstream"
    DOWNSTREAM = "downstream"


class GetColumnLineageInput(gestalt.Model):
    tenant: str = gestalt.field(description="Lineage tenant")
    model: str = gestalt.field(description="dbt model or source name")
    column: str = gestalt.field(description="Column name")
    direction: str = gestalt.field(description='"upstream" or "downstream"')
    max_depth: int = gestalt.field(
        description="Maximum traversal depth",
        default=20,
        required=False,
    )


class ColumnLineageNode(gestalt.Model):
    model: str
    column: str
    depth: int


class GetColumnLineageOutput(gestalt.Model):
    tenant: str
    model: str
    column: str
    direction: str
    generated_at: str | None
    results: list[ColumnLineageNode]


GetColumnLineageResult: TypeAlias = GetColumnLineageOutput | ErrorResponse


@app.operation(
    id="get_column_lineage",
    method="POST",
    description="Get upstream or downstream lineage for a dbt column.",
)
def get_column_lineage(
    input: GetColumnLineageInput, _req: gestalt.Request
) -> GetColumnLineageResult:
    try:
        tenant = _required_text(input.tenant, "tenant")
        model = _required_text(input.model, "model")
        column = _required_text(input.column, "column")
        direction = Direction(input.direction.strip().lower())
        if input.max_depth < 1:
            raise ValueError("max_depth must be positive")

        snapshot = LineageSnapshot.load(tenant)
        results = traverse(
            snapshot.edges,
            model=model,
            column=column,
            direction=direction.value,
            max_depth=input.max_depth,
        )
        return GetColumnLineageOutput(
            tenant=tenant,
            model=model,
            column=column,
            direction=direction.value,
            generated_at=snapshot.generated_at,
            results=[
                ColumnLineageNode(
                    model=result.model,
                    column=result.column,
                    depth=result.depth,
                )
                for result in results
            ],
        )
    except ValueError as err:
        return gestalt.Response(
            status=HTTPStatus.BAD_REQUEST,
            body={"error": str(err)},
        )
    except GoogleAPICallError as err:
        return gestalt.Response(
            status=_google_api_status(err),
            body={"error": str(err)},
        )
    except DefaultCredentialsError as err:
        return gestalt.Response(
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
            body={"error": f"Failed to query lineage: {err}"},
        )


def _required_text(value: str, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _google_api_status(err: GoogleAPICallError) -> HTTPStatus:
    code = getattr(err, "code", None)
    if isinstance(code, HTTPStatus):
        return code
    if isinstance(code, int):
        try:
            return HTTPStatus(code)
        except ValueError:
            pass
    return HTTPStatus.INTERNAL_SERVER_ERROR
