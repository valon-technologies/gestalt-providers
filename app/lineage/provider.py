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
    tenant: str = gestalt.field(
        description=(
            "Lineage scope: 1 or valon_mortgage for Valon Mortgage; "
            "9 or service_mac for ServiceMac; 1,9 or valon_analytics "
            "for combined multi-tenant lineage. Spaces in 1, 9 are accepted."
        )
    )
    model: str = gestalt.field(description="dbt model or source name")
    column: str = gestalt.field(description="Column name")
    direction: str = gestalt.field(
        description=(
            '"upstream" finds source columns; "downstream" finds columns '
            "that depend on this column"
        )
    )
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
    description=(
        "Get dbt column lineage for Valon Mortgage, ServiceMac, or the "
        "combined analytics environment."
    ),
)
def get_column_lineage(
    input: GetColumnLineageInput, _req: gestalt.Request
) -> GetColumnLineageResult:
    try:
        tenant = _normalize_tenant(input.tenant)
        model = _required_text(input.model, "model")
        column = _required_text(input.column, "column")
        direction = Direction(input.direction.strip().lower())
        if input.max_depth < 1:
            raise ValueError("max_depth must be positive")

        snapshot = LineageSnapshot.load(tenant)
        if not snapshot.edges:
            return gestalt.Response(
                status=HTTPStatus.NOT_FOUND,
                body={
                    "error": f"No column lineage data found for tenant: {tenant}"
                },
            )
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


def _normalize_tenant(value: str) -> str:
    tenant = _required_text(value, "tenant")
    normalized_tenant = ",".join(part.strip() for part in tenant.split(","))
    tenant_mapping = {
        "1": "valon_mortgage",
        "9": "service_mac",
        "1,9": "valon_analytics",
        "valon_mortgage": "valon_mortgage",
        "service_mac": "service_mac",
        "valon_analytics": "valon_analytics",
    }
    try:
        return tenant_mapping[normalized_tenant]
    except KeyError as err:
        raise ValueError(
            "tenant must be 1 or valon_mortgage, 9 or service_mac, "
            "or 1,9 or valon_analytics"
        ) from err


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
