from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .client import (
    DEFAULT_HEX_CLIENT,
    HexAPIClient,
    JsonObject,
    JsonPayload,
    encode_path_component,
)


class HexSuggestionSortBy(StrEnum):
    CREATED_DATE = "CREATED_DATE"
    EVIDENCE_COUNT = "EVIDENCE_COUNT"
    LAST_SOURCE_ADDED_DATE = "LAST_SOURCE_ADDED_DATE"


class HexSortDirection(StrEnum):
    ASC = "ASC"
    DESC = "DESC"


class HexSuggestionStatus(StrEnum):
    OPEN = "OPEN"
    IN_PROGRESS = "IN_PROGRESS"
    RESOLVED = "RESOLVED"


@dataclass(frozen=True, slots=True)
class ProjectExportRequest:
    project_id: str
    version: str


@dataclass(frozen=True, slots=True)
class ProjectRunDraftRequest:
    project_id: str
    use_cached_sql_results: bool | None = None


@dataclass(frozen=True, slots=True)
class CellRunRequest:
    cell_id: str
    dry_run: bool = False


@dataclass(frozen=True, slots=True)
class SuggestionListRequest:
    limit: int
    after: str | None = None
    before: str | None = None
    sort_by: HexSuggestionSortBy | None = None
    sort_direction: HexSortDirection | None = None
    status: HexSuggestionStatus | None = None


@dataclass(frozen=True, slots=True)
class ContextVersionUpdateRequest:
    context_version_id: str
    operation: JsonPayload


@dataclass(frozen=True, slots=True)
class ContextVersionPublishRequest:
    context_version_id: str
    update_latest_version: bool
    title: str | None = None
    description: str | None = None


def export_project(
    token: str,
    request: ProjectExportRequest,
    *,
    client: HexAPIClient = DEFAULT_HEX_CLIENT,
) -> JsonObject:
    return client.post_json(
        "/projects/export",
        {
            "projectId": request.project_id,
            "version": _normalize_export_version(request.version),
        },
        token,
    )


def import_project(
    token: str, content: str, *, client: HexAPIClient = DEFAULT_HEX_CLIENT
) -> JsonObject:
    return client.post_json("/projects/import", {"content": content}, token)


def run_draft(
    token: str,
    request: ProjectRunDraftRequest,
    *,
    client: HexAPIClient = DEFAULT_HEX_CLIENT,
) -> JsonObject:
    payload: dict[str, Any] = {}
    if request.use_cached_sql_results is not None:
        payload["useCachedSqlResults"] = request.use_cached_sql_results

    return client.post_json(
        f"/projects/{encode_path_component(request.project_id)}/notebook/run",
        payload,
        token,
    )


def run_cell(
    token: str,
    request: CellRunRequest,
    *,
    client: HexAPIClient = DEFAULT_HEX_CLIENT,
) -> JsonObject:
    return client.post_json(
        f"/cells/{encode_path_component(request.cell_id)}/run",
        {"dryRun": request.dry_run},
        token,
    )


def list_suggestions(
    token: str,
    request: SuggestionListRequest,
    *,
    client: HexAPIClient = DEFAULT_HEX_CLIENT,
) -> JsonObject:
    return client.get_json(
        "/suggestions",
        token,
        {
            "limit": request.limit,
            "after": request.after,
            "before": request.before,
            "sortBy": request.sort_by,
            "sortDirection": request.sort_direction,
            "status": request.status,
        },
    )


def get_suggestion(
    token: str, suggestion_id: str, *, client: HexAPIClient = DEFAULT_HEX_CLIENT
) -> JsonObject:
    return client.get_json(f"/suggestions/{encode_path_component(suggestion_id)}", token)


def create_context_version(
    token: str,
    external_source: JsonPayload,
    *,
    client: HexAPIClient = DEFAULT_HEX_CLIENT,
) -> JsonObject:
    return client.post_json(
        "/context/version", {"externalSource": external_source}, token
    )


def update_context_version(
    token: str,
    request: ContextVersionUpdateRequest,
    *,
    client: HexAPIClient = DEFAULT_HEX_CLIENT,
) -> JsonObject:
    return client.post_json(
        f"/context/version/{encode_path_component(request.context_version_id)}",
        {"operation": request.operation},
        token,
    )


def publish_context_version(
    token: str,
    request: ContextVersionPublishRequest,
    *,
    client: HexAPIClient = DEFAULT_HEX_CLIENT,
) -> JsonObject:
    payload: dict[str, Any] = {"updateLatestVersion": request.update_latest_version}
    if request.title is not None:
        payload["title"] = request.title
    if request.description is not None:
        payload["description"] = request.description

    return client.post_json(
        f"/context/version/{encode_path_component(request.context_version_id)}/publish",
        payload,
        token,
    )


def _normalize_export_version(version: str) -> str | int | float:
    normalized = version.strip()
    if normalized.lower() == "draft":
        return "draft"

    try:
        if any(char in normalized for char in (".", "e", "E")):
            return float(normalized)
        return int(normalized)
    except ValueError as exc:
        raise ValueError("version must be 'draft' or a version number") from exc
