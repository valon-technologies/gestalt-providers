from __future__ import annotations

import datetime as dt
from typing import Any

from . import cache_store
from .config import get_github_config
from .helpers import int_field, map_field, str_field

_EVENT_OBJECT_KEYS = {
    "pull_request": "pull_request",
    "pull_request_review": "review",
    "pull_request_review_comment": "comment",
    "check_run": "check_run",
    "check_suite": "check_suite",
    "workflow_run": "workflow_run",
    "issues": "issue",
    "issue_comment": "comment",
    "deployment": "deployment",
    "deployment_status": "deployment_status",
}

_ENTITY_TYPES = {
    "pull_request": "pull_request",
    "pull_request_review": "pull_request_review",
    "pull_request_review_comment": "pull_request_review_comment",
    "check_run": "check_run",
    "check_suite": "check_suite",
    "workflow_run": "workflow_run",
    "issues": "issue",
    "issue_comment": "issue_comment",
    "deployment": "deployment",
    "deployment_status": "deployment_status",
}

_DOMAINS = {
    "pull_request": {"pull_request"},
    "pull_request_review": {"pull_request"},
    "pull_request_review_comment": {"pull_request", "issue_comment"},
    "check_run": {"check_run"},
    "check_suite": {"check_run"},
    "workflow_run": {"workflow_run"},
    "issues": {"pull_request"},
    "issue_comment": {"issue_comment", "pull_request"},
    "deployment": {"deployment"},
    "deployment_status": {"deployment"},
}

_PROJECTED_FIELDS = {
    "id",
    "node_id",
    "number",
    "state",
    "status",
    "conclusion",
    "name",
    "head_sha",
    "head_branch",
    "sha",
    "ref",
    "environment",
    "created_at",
    "updated_at",
    "submitted_at",
    "completed_at",
    "started_at",
    "html_url",
    "url",
    "pull_requests",
}


def ingest_webhook_event(
    event_type: str,
    payload: dict[str, Any],
    summary: dict[str, Any],
) -> bool:
    config = get_github_config()
    if not config.cache_enabled:
        return False
    object_key = _EVENT_OBJECT_KEYS.get(event_type)
    entity_type = _ENTITY_TYPES.get(event_type)
    domains = _DOMAINS.get(event_type)
    if object_key is None or entity_type is None or domains is None:
        return False
    entity = map_field(payload, object_key)
    if not entity:
        return False
    repository = str_field(summary, "repository")
    installation_id = int_field(summary, "installation_id")
    if not repository or installation_id <= 0:
        return False
    entity_id = _entity_id(event_type, entity, payload, summary)
    if not entity_id:
        return False
    updated_at = _entity_version(entity, entity_id)
    scope = cache_store.cache_scope(
        config.provider_name,
        config.api_base_url,
        config.app_id,
        installation_id,
    )
    action = str_field(payload, "action").lower()
    projected = {
        key: value for key, value in entity.items() if key in _PROJECTED_FIELDS
    }
    projected["action"] = action
    if event_type == "deployment_status":
        deployment = map_field(payload, "deployment")
        deployment_id = int_field(deployment, "id")
        if deployment_id > 0:
            projected["deployment_id"] = deployment_id
    updated, _ = cache_store.put_entity_and_increment(
        scope,
        repository,
        entity_type,
        entity_id,
        projected,
        updated_at=updated_at,
        deleted=action == "deleted",
        domains=domains,
    )
    return updated


def _entity_id(
    event_type: str,
    entity: dict[str, Any],
    payload: dict[str, Any],
    summary: dict[str, Any],
) -> str:
    entity_id = int_field(entity, "id")
    if entity_id > 0:
        return str(entity_id)
    if event_type in {"pull_request", "issues"}:
        number = int_field(entity, "number") or int_field(summary, "number")
        return str(number) if number > 0 else ""
    if event_type == "deployment_status":
        deployment = map_field(payload, "deployment")
        deployment_id = int_field(deployment, "id")
        return f"deployment:{deployment_id}" if deployment_id > 0 else ""
    return ""


def _entity_version(entity: dict[str, Any], entity_id: str) -> str:
    for key in (
        "updated_at",
        "completed_at",
        "submitted_at",
        "started_at",
        "created_at",
    ):
        value = str_field(entity, key)
        if value:
            return value
    numeric_id = int_field(entity, "id")
    if numeric_id > 0:
        return f"id:{numeric_id:020d}"
    return (
        f"observed:{dt.datetime.now(dt.timezone.utc).isoformat()}:"
        f"{entity_id}"
    )
