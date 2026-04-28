from __future__ import annotations

import hashlib
import json
from typing import Any

from gestalt.gen.v1 import workflow_pb2 as _workflow_pb2

from .config import get_github_config
from .constants import GITHUB_WORKFLOW_EVENT_TYPE
from .helpers import int_field, map_field, nested_str, str_field
from .webhook import event_summary, installation_id_from_payload

workflow_pb2: Any = _workflow_pb2

MAX_WORKFLOW_EVENT_PAYLOAD_CHARS = 60000
SENSITIVE_FIELD_FRAGMENTS = (
    "authorization",
    "client_secret",
    "private_key",
    "secret",
    "signature",
    "token",
)


def build_workflow_event(payload: dict[str, Any]) -> Any:
    installation_id = installation_id_from_payload(payload)
    summary = event_summary(payload, installation_id)
    payload_digest = _payload_digest(payload)
    delivery_id = _github_delivery_id(payload)
    event_id = f"github:{delivery_id}" if delivery_id else f"github:{payload_digest}"
    safe_payload, truncated = _workflow_payload(payload, summary, payload_digest)

    event = workflow_pb2.WorkflowEvent(
        id=event_id,
        source="github",
        spec_version="1.0",
        type=GITHUB_WORKFLOW_EVENT_TYPE,
        subject=_workflow_subject(summary, installation_id),
        datacontenttype="application/json",
    )
    event.time.GetCurrentTime()
    event.data.update(
        {
            "delivery_id": delivery_id or event_id,
            "github_event": summary.get("event_type", ""),
            "github_action": summary.get("action", ""),
            "installation": _installation_data(payload),
            "repository": _repository_data(payload),
            "sender": _sender_data(payload),
            "summary": summary,
            "payload": safe_payload,
            "payload_sha256": payload_digest,
            "payload_truncated": truncated,
        }
    )
    return event


def workflow_payload_from_context(workflow: dict[str, Any]) -> dict[str, Any]:
    event = _workflow_event_from_context(workflow)
    if not event:
        raise ValueError("workflow trigger event is missing")
    if (
        event.get("source") != "github"
        or event.get("type") != GITHUB_WORKFLOW_EVENT_TYPE
    ):
        raise ValueError("workflow trigger event is not a GitHub webhook event")

    data = event.get("data")
    if not isinstance(data, dict):
        raise ValueError("workflow trigger event data is missing")

    payload = data.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("workflow trigger event data.payload is missing")
    return payload


def _workflow_event_from_context(workflow: dict[str, Any]) -> dict[str, Any]:
    trigger = workflow.get("trigger")
    if not isinstance(trigger, dict):
        return {}
    event = trigger.get("event")
    if isinstance(event, dict):
        return event
    return {}


def _workflow_subject(summary: dict[str, Any], installation_id: int) -> str:
    repository = str(summary.get("repository", "")).strip()
    if repository:
        return repository
    if installation_id > 0:
        return f"installation:{installation_id}"
    app_id = get_github_config().app_id
    if app_id:
        return f"app:{app_id}"
    return "github"


def _workflow_payload(
    payload: dict[str, Any],
    summary: dict[str, Any],
    payload_digest: str,
) -> tuple[dict[str, Any], bool]:
    redacted = _redact_value(payload)
    encoded = _canonical_json(redacted)
    if len(encoded) <= MAX_WORKFLOW_EVENT_PAYLOAD_CHARS:
        return redacted, False

    compact = _compact_payload(payload)
    compact["_gestalt_payload_truncated"] = True
    compact["_gestalt_payload_sha256"] = payload_digest
    compact["_gestalt_payload_preview_json"] = (
        encoded[:MAX_WORKFLOW_EVENT_PAYLOAD_CHARS] + "\n...<truncated>"
    )
    compact["_gestalt_summary"] = summary
    return compact, True


def _payload_digest(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _github_delivery_id(payload: dict[str, Any]) -> str:
    headers = map_field(payload, "headers")
    for key, value in headers.items():
        if str(key).lower() == "x-github-delivery" and isinstance(value, str):
            return value.strip()
    return ""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, nested in value.items():
            key_text = str(key)
            normalized = key_text.lower()
            if any(fragment in normalized for fragment in SENSITIVE_FIELD_FRAGMENTS):
                out[key_text] = "[redacted]"
            else:
                out[key_text] = _redact_value(nested)
        return out
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    return value


def _compact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key in (
        "action",
        "ref",
        "before",
        "after",
        "installation",
        "repository",
        "sender",
    ):
        if key in payload:
            compact[key] = _redact_value(payload[key])
    for key in (
        "pull_request",
        "issue",
        "comment",
        "review",
        "check_run",
        "check_suite",
        "workflow_run",
    ):
        nested = map_field(payload, key)
        if nested:
            compact[key] = _compact_nested_object(nested)
    return compact


def _compact_nested_object(value: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key in (
        "id",
        "node_id",
        "number",
        "name",
        "title",
        "state",
        "status",
        "conclusion",
        "html_url",
        "url",
        "head",
        "base",
    ):
        if key in value:
            compact[key] = _redact_value(value[key])
    return compact


def _installation_data(payload: dict[str, Any]) -> dict[str, Any]:
    installation = map_field(payload, "installation")
    account = map_field(installation, "account")
    return {
        key: value
        for key, value in {
            "id": int_field(installation, "id"),
            "app_id": int_field(installation, "app_id"),
            "app_slug": str_field(installation, "app_slug"),
            "target_type": str_field(installation, "target_type"),
            "account_login": str_field(account, "login"),
            "account_id": int_field(account, "id"),
            "account_type": str_field(account, "type"),
        }.items()
        if value not in ("", 0)
    }


def _repository_data(payload: dict[str, Any]) -> dict[str, Any]:
    repository = map_field(payload, "repository")
    return {
        key: value
        for key, value in {
            "id": int_field(repository, "id"),
            "name": str_field(repository, "name"),
            "full_name": str_field(repository, "full_name"),
            "owner": nested_str(repository, "owner", "login"),
            "default_branch": str_field(repository, "default_branch"),
            "html_url": str_field(repository, "html_url"),
        }.items()
        if value not in ("", 0)
    }


def _sender_data(payload: dict[str, Any]) -> dict[str, Any]:
    sender = map_field(payload, "sender")
    return {
        key: value
        for key, value in {
            "login": str_field(sender, "login"),
            "id": int_field(sender, "id"),
            "type": str_field(sender, "type"),
        }.items()
        if value not in ("", 0)
    }
