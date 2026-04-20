from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

from internals import (
    HexAPIError,
    create_context_version,
    export_project,
    get_suggestion,
    import_project,
    list_suggestions,
    publish_context_version,
    run_cell,
    run_draft,
    update_context_version,
)

ErrorResponse: TypeAlias = gestalt.Response[dict[str, Any]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse


class ProjectExportInput(gestalt.Model):
    project_id: str = gestalt.field(description="Hex project ID")
    version: str = gestalt.field(
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


class SuggestionsListInput(gestalt.Model):
    limit: int = gestalt.field(description="Maximum number of suggestions to return", default=100, required=False)
    after: str = gestalt.field(description="Pagination cursor to fetch results after", default="", required=False)
    before: str = gestalt.field(description="Pagination cursor to fetch results before", default="", required=False)
    sort_by: str = gestalt.field(
        description="Optional sort field: CREATED_DATE, EVIDENCE_COUNT, or LAST_SOURCE_ADDED_DATE",
        default="",
        required=False,
    )
    sort_direction: str = gestalt.field(
        description="Optional sort direction: ASC or DESC",
        default="",
        required=False,
    )
    status: str = gestalt.field(
        description="Optional status filter: OPEN, IN_PROGRESS, or RESOLVED",
        default="",
        required=False,
    )


class SuggestionsGetInput(gestalt.Model):
    suggestion_id: str = gestalt.field(description="Hex suggestion ID")


class ContextVersionCreateInput(gestalt.Model):
    external_source: dict[str, Any] = gestalt.field(
        description="Hex externalSource payload for creating a context version",
    )


class ContextVersionUpdateInput(gestalt.Model):
    context_version_id: str = gestalt.field(description="Hex context version ID")
    operation: dict[str, Any] = gestalt.field(
        description="Hex operation payload for updating a context version",
    )


class ContextVersionPublishInput(gestalt.Model):
    context_version_id: str = gestalt.field(description="Hex context version ID")
    update_latest_version: bool = gestalt.field(
        description="Whether to update the latest context version with the draft changes",
        default=False,
        required=False,
    )
    title: str = gestalt.field(
        description="Optional short human-readable title for the published version",
        default="",
        required=False,
    )
    description: str = gestalt.field(
        description="Optional long human-readable description for the published version",
        default="",
        required=False,
    )


@gestalt.operation(
    id="projects.export",
    method="POST",
    description="Export a Hex project as YAML via Hex's CLI-only API endpoint",
)
def project_export(input: ProjectExportInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    try:
        return export_project(
            req.token,
            _require_trimmed_text(input.project_id, "project_id"),
            _require_trimmed_text(input.version, "version"),
        )
    except ValueError as err:
        return _bad_request(str(err))
    except HexAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="projects.import",
    method="POST",
    description="Import or update a Hex project from raw YAML via Hex's CLI-only API endpoint",
)
def project_import(input: ProjectImportInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    try:
        return import_project(req.token, _require_text(input.content, "content"))
    except ValueError as err:
        return _bad_request(str(err))
    except HexAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="projects.runDraft",
    method="POST",
    description="Run the draft notebook version of a Hex project via Hex's CLI-only API endpoint",
)
def project_run_draft(input: ProjectRunDraftInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    try:
        return run_draft(
            req.token,
            _require_trimmed_text(input.project_id, "project_id"),
            input.use_cached_sql_results,
        )
    except ValueError as err:
        return _bad_request(str(err))
    except HexAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="cells.run",
    method="POST",
    description="Run a Hex cell and its dependencies via Hex's CLI-only API endpoint",
)
def cell_run(input: CellRunInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    try:
        return run_cell(req.token, _require_trimmed_text(input.cell_id, "cell_id"), input.dry_run)
    except ValueError as err:
        return _bad_request(str(err))
    except HexAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="suggestions.list",
    method="POST",
    description="List Hex Context Studio suggestions via Hex's CLI-only API endpoint",
)
def suggestions_list(input: SuggestionsListInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    try:
        if input.limit <= 0:
            raise ValueError("limit must be greater than 0")

        return list_suggestions(
            req.token,
            limit=input.limit,
            after=input.after.strip() or None,
            before=input.before.strip() or None,
            sort_by=_normalize_enum(
                input.sort_by,
                "sort_by",
                {"CREATED_DATE", "EVIDENCE_COUNT", "LAST_SOURCE_ADDED_DATE"},
            ),
            sort_direction=_normalize_enum(input.sort_direction, "sort_direction", {"ASC", "DESC"}),
            status=_normalize_enum(input.status, "status", {"OPEN", "IN_PROGRESS", "RESOLVED"}),
        )
    except ValueError as err:
        return _bad_request(str(err))
    except HexAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="suggestions.get",
    method="POST",
    description="Get a Hex Context Studio suggestion via Hex's CLI-only API endpoint",
)
def suggestions_get(input: SuggestionsGetInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    try:
        return get_suggestion(req.token, _require_trimmed_text(input.suggestion_id, "suggestion_id"))
    except ValueError as err:
        return _bad_request(str(err))
    except HexAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="contextVersions.create",
    method="POST",
    description="Create a Hex Context Studio context version via Hex's CLI-only API endpoint",
)
def context_version_create(input: ContextVersionCreateInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    try:
        return create_context_version(req.token, input.external_source)
    except HexAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="contextVersions.update",
    method="POST",
    description="Update a Hex Context Studio context version via Hex's CLI-only API endpoint",
)
def context_version_update(input: ContextVersionUpdateInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    try:
        return update_context_version(
            req.token,
            _require_trimmed_text(input.context_version_id, "context_version_id"),
            input.operation,
        )
    except ValueError as err:
        return _bad_request(str(err))
    except HexAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="contextVersions.publish",
    method="POST",
    description="Publish a Hex Context Studio context version via Hex's CLI-only API endpoint",
)
def context_version_publish(input: ContextVersionPublishInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    try:
        return publish_context_version(
            req.token,
            _require_trimmed_text(input.context_version_id, "context_version_id"),
            update_latest_version=input.update_latest_version,
            title=input.title.strip() or None,
            description=input.description.strip() or None,
        )
    except ValueError as err:
        return _bad_request(str(err))
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


def _require_text(value: str, field_name: str) -> str:
    if not value.strip():
        raise ValueError(f"{field_name} is required")
    return value


def _require_trimmed_text(value: str, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    return normalized


def _normalize_enum(value: str, field_name: str, allowed: set[str]) -> str | None:
    normalized = value.strip()
    if not normalized:
        return None

    normalized = normalized.upper()
    if normalized not in allowed:
        raise ValueError(f"{field_name} must be one of: {', '.join(sorted(allowed))}")
    return normalized
