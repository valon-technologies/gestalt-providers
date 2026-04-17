from __future__ import annotations

from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

from internals import HexAPIError, export_project, import_project, run_cell, run_draft

ErrorResponse: TypeAlias = gestalt.Response[dict[str, Any]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse


class ProjectExportInput(gestalt.Model):
    project_id: str = gestalt.field(description="Hex project ID")
    version: str | int | float = gestalt.field(
        description='Project version to export: "draft" or a published version number',
        default="draft",
        required=False,
    )


class ProjectImportInput(gestalt.Model):
    content: str = gestalt.field(description="Raw Hex project YAML content")


class ProjectRunDraftInput(gestalt.Model):
    project_id: str = gestalt.field(description="Hex project ID")
    use_cached_sql_results: bool | None = gestalt.field(
        description="Reuse cached SQL results when supported",
        default=None,
        required=False,
    )


class CellRunInput(gestalt.Model):
    cell_id: str = gestalt.field(description="Hex cell ID")
    dry_run: bool = gestalt.field(
        description="Validate the request without executing the cell",
        default=False,
        required=False,
    )


@gestalt.operation(
    id="project.export",
    method="POST",
    description="Export a Hex project as YAML via Hex's CLI-only API endpoint",
)
def project_export(input: ProjectExportInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    project_id = input.project_id.strip()
    if not project_id:
        return _bad_request("project_id is required")

    version = str(input.version).strip()
    if not version:
        return _bad_request("version must be 'draft' or a version number")

    try:
        return export_project(req.token, project_id, version)
    except ValueError as err:
        return _bad_request(str(err))
    except HexAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="project.import",
    method="POST",
    description="Import or update a Hex project from raw YAML via Hex's CLI-only API endpoint",
)
def project_import(input: ProjectImportInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    if not input.content.strip():
        return _bad_request("content is required")

    try:
        return import_project(req.token, input.content)
    except HexAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="project.runDraft",
    method="POST",
    description="Run the draft notebook version of a Hex project via Hex's CLI-only API endpoint",
)
def project_run_draft(input: ProjectRunDraftInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    project_id = input.project_id.strip()
    if not project_id:
        return _bad_request("project_id is required")

    try:
        return run_draft(req.token, project_id, input.use_cached_sql_results)
    except HexAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="cell.run",
    method="POST",
    description="Run a Hex cell and its dependencies via Hex's CLI-only API endpoint",
)
def cell_run(input: CellRunInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    cell_id = input.cell_id.strip()
    if not cell_id:
        return _bad_request("cell_id is required")

    try:
        return run_cell(req.token, cell_id, input.dry_run)
    except HexAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except RuntimeError as err:
        return _server_error(str(err))


def _validate_token(req: gestalt.Request) -> ErrorResponse | None:
    if not req.token.strip():
        return gestalt.Response(status=HTTPStatus.UNAUTHORIZED, body={"error": "token is required"})
    return None


def _bad_request(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _server_error(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message})
