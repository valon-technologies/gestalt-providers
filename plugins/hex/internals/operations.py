from __future__ import annotations

from typing import Any

from .client import encode_path_component, post_json


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
    body: dict[str, Any] = {}
    if use_cached_sql_results is not None:
        body["useCachedSqlResults"] = use_cached_sql_results

    return post_json(
        f"/projects/{encode_path_component(project_id)}/notebook/run",
        body,
        token,
    )


def run_cell(token: str, cell_id: str, dry_run: bool) -> dict[str, Any]:
    return post_json(
        f"/cells/{encode_path_component(cell_id)}/run",
        {"dryRun": dry_run},
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
