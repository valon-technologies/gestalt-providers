from typing import Any

from .client import encode_path_component, get_json, post_json


def export_project(token: str, project_id: str, version: str) -> dict[str, Any]:
    return post_json(
        "/projects/export",
        {
            "projectId": project_id,
            "version": _normalize_export_version(version),
        },
        token,
    )


def import_project(token: str, content: str) -> dict[str, Any]:
    return post_json("/projects/import", {"content": content}, token)


def run_draft(token: str, project_id: str, use_cached_sql_results: bool | None) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if use_cached_sql_results is not None:
        payload["useCachedSqlResults"] = use_cached_sql_results

    return post_json(
        f"/projects/{encode_path_component(project_id)}/notebook/run",
        payload,
        token,
    )


def run_cell(token: str, cell_id: str, dry_run: bool) -> dict[str, Any]:
    return post_json(
        f"/cells/{encode_path_component(cell_id)}/run",
        {"dryRun": dry_run},
        token,
    )


def list_suggestions(
    token: str,
    *,
    limit: int,
    after: str | None = None,
    before: str | None = None,
    sort_by: str | None = None,
    sort_direction: str | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    return get_json(
        "/suggestions",
        token,
        {
            "limit": limit,
            "after": after,
            "before": before,
            "sortBy": sort_by,
            "sortDirection": sort_direction,
            "status": status,
        },
    )


def get_suggestion(token: str, suggestion_id: str) -> dict[str, Any]:
    return get_json(f"/suggestions/{encode_path_component(suggestion_id)}", token)


def create_context_version(token: str, external_source: dict[str, Any]) -> dict[str, Any]:
    return post_json("/context/version", {"externalSource": external_source}, token)


def update_context_version(
    token: str,
    context_version_id: str,
    operation: dict[str, Any],
) -> dict[str, Any]:
    return post_json(
        f"/context/version/{encode_path_component(context_version_id)}",
        {"operation": operation},
        token,
    )


def publish_context_version(
    token: str,
    context_version_id: str,
    *,
    update_latest_version: bool,
    title: str | None = None,
    description: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"updateLatestVersion": update_latest_version}
    if title is not None:
        payload["title"] = title
    if description is not None:
        payload["description"] = description

    return post_json(
        f"/context/version/{encode_path_component(context_version_id)}/publish",
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
