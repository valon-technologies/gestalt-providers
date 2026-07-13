from __future__ import annotations

from typing import Any

import gestalt

SLACK_V2_EVENT_SOURCE = "slack_v2"
SLACK_V2_EVENT_TYPE = "slack_v2.event.received"


def slack_app_id_from_payload(payload: dict[str, Any]) -> str:
    return str(payload.get("api_app_id") or "").strip()


def slack_event_id_from_payload(payload: dict[str, Any]) -> str:
    return str(payload.get("event_id") or "").strip()


def workflow_event_id(*, app_id: str, payload: dict[str, Any]) -> str:
    _ = app_id
    event_id = slack_event_id_from_payload(payload)
    if not event_id:
        raise ValueError("event_id is required")
    return f"slack_v2:{event_id}"


def workflow_event_data(*, app_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "app_id": app_id,
        "slack": {
            "api_app_id": app_id,
            "team_id": str(payload.get("team_id") or "").strip(),
            "enterprise_id": str(payload.get("enterprise_id") or "").strip(),
            "type": str(payload.get("type") or "").strip(),
            "event_id": slack_event_id_from_payload(payload),
        },
        "raw": payload,
    }


def build_workflow_deliver_event_request(
    *,
    app_id: str,
    workflow_event_subject: str,
    payload: dict[str, Any],
) -> gestalt.WorkflowDeliverEvent:
    return gestalt.WorkflowDeliverEvent(
        provider="local",
        event=gestalt.WorkflowEvent(
            id=workflow_event_id(app_id=app_id, payload=payload),
            source=SLACK_V2_EVENT_SOURCE,
            spec_version="1.0",
            type=SLACK_V2_EVENT_TYPE,
            subject=workflow_event_subject,
            datacontenttype="application/json",
            data=workflow_event_data(app_id=app_id, payload=payload),
        ),
    )
