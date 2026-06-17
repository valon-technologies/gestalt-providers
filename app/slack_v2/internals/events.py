from __future__ import annotations

import hashlib
import json
from typing import Any

import gestalt

SLACK_V2_EVENT_SOURCE = "slack_v2"
SLACK_V2_EVENT_TYPE = "slack_v2.event.received"


def slack_app_id_from_payload(payload: dict[str, Any]) -> str:
    return str(payload.get("api_app_id") or "").strip()


def is_url_verification(payload: dict[str, Any]) -> bool:
    return str(payload.get("type") or "").strip() == "url_verification"


def url_verification_challenge(payload: dict[str, Any]) -> dict[str, str]:
    return {"challenge": str(payload.get("challenge") or "")}


def payload_digest(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def workflow_event_id(*, app_id: str, payload: dict[str, Any]) -> str:
    nested_event = payload.get("event")
    if isinstance(nested_event, dict):
        event_id = str(nested_event.get("event_id") or "").strip()
        if event_id:
            return f"slack_v2:{event_id}"
    return f"slack_v2:{app_id}:{payload_digest(payload)}"


def first_event_match(
    definition: gestalt.WorkflowDefinition | None,
) -> gestalt.WorkflowEventMatch | None:
    if definition is None:
        return None
    for activation in definition.activations:
        if activation.paused:
            continue
        event = activation.event
        if not isinstance(event, dict):
            continue
        match = event.get("match")
        if not isinstance(match, dict):
            continue
        event_type = str(match.get("type") or "").strip()
        if not event_type:
            continue
        return gestalt.WorkflowEventMatch(
            type=event_type,
            source=str(match.get("source") or "").strip(),
            subject=str(match.get("subject") or "").strip(),
        )
    return None


def workflow_event_match(
    *,
    definition: gestalt.WorkflowDefinition | None,
    workflow_definition_id: str,
) -> gestalt.WorkflowEventMatch:
    matched = first_event_match(definition)
    if matched is not None:
        return matched
    return gestalt.WorkflowEventMatch(
        type=SLACK_V2_EVENT_TYPE,
        source=SLACK_V2_EVENT_SOURCE,
        subject=workflow_definition_id,
    )


def workflow_event_data(*, app_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "app_id": app_id,
        "slack": {
            "api_app_id": app_id,
            "team_id": str(payload.get("team_id") or "").strip(),
            "enterprise_id": str(payload.get("enterprise_id") or "").strip(),
            "type": str(payload.get("type") or "").strip(),
            "event_id": _nested_event_id(payload),
        },
        "raw": payload,
    }


def build_workflow_deliver_event_request(
    *,
    app_id: str,
    workflow_definition_id: str,
    payload: dict[str, Any],
    definition: gestalt.WorkflowDefinition | None,
) -> gestalt.WorkflowDeliverEvent:
    event_match = workflow_event_match(
        definition=definition,
        workflow_definition_id=workflow_definition_id,
    )
    provider_name = ""
    if definition is not None:
        provider_name = str(definition.provider_name or "").strip()
    return gestalt.WorkflowDeliverEvent(
        provider_name=provider_name,
        event=gestalt.WorkflowEvent(
            id=workflow_event_id(app_id=app_id, payload=payload),
            source=event_match.source or SLACK_V2_EVENT_SOURCE,
            spec_version="1.0",
            type=event_match.type,
            subject=event_match.subject or workflow_definition_id,
            datacontenttype="application/json",
            data=workflow_event_data(app_id=app_id, payload=payload),
        ),
    )


def _nested_event_id(payload: dict[str, Any]) -> str:
    nested_event = payload.get("event")
    if not isinstance(nested_event, dict):
        return ""
    return str(nested_event.get("event_id") or "").strip()
