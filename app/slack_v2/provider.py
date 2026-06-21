from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import logging
from http import HTTPStatus
import time
from typing import Any, cast

import gestalt

from internals.events import (
    build_workflow_deliver_event_request,
    slack_app_id_from_payload,
    slack_event_id_from_payload,
)
from internals.store import (
    get_default_signing_secret as load_default_signing_secret,
    get_default_workflow_event_subject as load_default_workflow_event_subject,
    get_debug_payload as load_debug_payload,
    list_debug_payload_ids as load_debug_payload_ids,
    save_debug_payload,
    save_slack_event_registration,
)

logger = logging.getLogger(__name__)

app = gestalt.App("slack_v2")


class GetWorkflowEventSubjectForAppInput(gestalt.Model):
    app_id: str = gestalt.field(description="Slack app ID.")


class DebugRecordSmokeRunInput(gestalt.Model):
    payload: dict[str, Any] = gestalt.field(
        description="Original Slack event payload.",
    )


class DebugGetSmokeRunPayloadInput(gestalt.Model):
    event_id: str = gestalt.field(description="Slack event ID.")


class RegisterSlackEventInput(gestalt.Model):
    app_id: str = gestalt.field(description="Slack app ID.")
    client_id: str = gestalt.field(description="Slack OAuth client ID.")
    client_secret: str = gestalt.field(description="Slack OAuth client secret.")
    signing_secret: str = gestalt.field(
        description="Slack signing secret for request verification."
    )
    display_name: str = gestalt.field(
        description="Human-readable name for the Slack bot."
    )
    bot_token: str = gestalt.field(
        default="",
        description="Slack bot user OAuth token (xoxb-...) for Web API calls.",
    )
    workflow_event_subject: str = gestalt.field(
        default="",
        description="Workflow event subject to publish for Slack events.",
    )


@app.operation(
    id="register_slack_event",
    method="POST",
    description="Register a Slack event handler configuration.",
)
def register_slack_event(
    input: RegisterSlackEventInput, _req: gestalt.Request
) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    app_id = input.app_id.strip()
    if not app_id:
        return gestalt.Response(
            status=HTTPStatus.BAD_REQUEST, body={"error": "app_id is required"}
        )
    workflow_event_subject = input.workflow_event_subject.strip()
    if not workflow_event_subject:
        return gestalt.Response(
            status=HTTPStatus.BAD_REQUEST,
            body={"error": "workflow_event_subject is required"},
        )

    save_slack_event_registration(
        app_id=app_id,
        client_id=input.client_id,
        client_secret=input.client_secret,
        signing_secret=input.signing_secret,
        display_name=input.display_name,
        bot_token=input.bot_token.strip(),
        workflow_event_subject=workflow_event_subject,
    )
    return {
        "ok": True,
        "app_id": app_id,
        "display_name": input.display_name,
        "workflow_event_subject": workflow_event_subject,
    }


@app.operation(
    id="get_workflow_event_subject_for_app",
    method="POST",
    description="Return the workflow event subject registered for a Slack app.",
)
def get_workflow_event_subject_for_app(
    input: GetWorkflowEventSubjectForAppInput, _req: gestalt.Request
) -> dict[str, str] | gestalt.Response[dict[str, str]]:
    app_id = input.app_id.strip()
    if not app_id:
        return gestalt.Response(
            status=HTTPStatus.BAD_REQUEST, body={"error": "app_id is required"}
        )

    try:
        workflow_event_subject = load_default_workflow_event_subject()
    except gestalt.NotFoundError:
        return gestalt.Response(
            status=HTTPStatus.NOT_FOUND,
            body={"error": "default Slack event registration not found"},
        )

    return {
        "app_id": app_id,
        "workflow_event_subject": workflow_event_subject,
    }


@app.operation(
    id="debug_record_smoke_run",
    method="POST",
    description="Debug endpoint that stores a Slack payload when the smoke workflow runs.",
)
def debug_record_smoke_run(
    input: DebugRecordSmokeRunInput, _req: gestalt.Request
) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    event_id = slack_event_id_from_payload(input.payload)
    if not event_id:
        return gestalt.Response(
            status=HTTPStatus.BAD_REQUEST,
            body={"error": "event_id is required"},
        )

    save_debug_payload(event_id=event_id, payload=input.payload)
    return {"ok": True, "stored": True, "id": event_id}


@app.operation(
    id="debug_get_smoke_run_payload",
    method="POST",
    description="Return a stored Slack smoke workflow debug payload by event ID.",
)
def debug_get_smoke_run_payload(
    input: DebugGetSmokeRunPayloadInput, _req: gestalt.Request
) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    event_id = input.event_id.strip()
    if not event_id:
        return gestalt.Response(
            status=HTTPStatus.BAD_REQUEST,
            body={"error": "event_id is required"},
        )

    try:
        return load_debug_payload(event_id=event_id)
    except gestalt.NotFoundError:
        return gestalt.Response(
            status=HTTPStatus.NOT_FOUND,
            body={"error": f"debug payload not found for event_id {event_id!r}"},
        )


@app.operation(
    id="debug_list_smoke_run_payload_ids",
    method="POST",
    description="List stored Slack smoke workflow debug payload event IDs.",
)
def debug_list_smoke_run_payload_ids(
    _input: dict[str, Any], _req: gestalt.Request
) -> dict[str, list[str]]:
    return {"ids": load_debug_payload_ids()}


@app.operation(
    id="handle_slack_event",
    method="POST",
    description="Handle a Slack event by publishing the registered workflow event subject.",
)
def handle_slack_event(
    input: dict[str, Any], req: gestalt.Request
) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    if not _verify_slack_signature(input, req):
        return gestalt.Response(
            status=HTTPStatus.UNAUTHORIZED,
            body={"error": "invalid Slack signature"},
        )

    if _is_url_verification(input):
        return {"challenge": str(input.get("challenge") or "")}

    app_id = slack_app_id_from_payload(input)
    if not app_id:
        return gestalt.Response(
            status=HTTPStatus.BAD_REQUEST,
            body={"error": "api_app_id is required"},
        )
    event_id = str(input.get("event_id") or "").strip()
    if not event_id:
        return gestalt.Response(
            status=HTTPStatus.BAD_REQUEST,
            body={"error": "event_id is required"},
        )

    try:
        workflow_event_subject = load_default_workflow_event_subject()
    except gestalt.NotFoundError:
        return gestalt.Response(
            status=HTTPStatus.NOT_FOUND,
            body={"error": "default Slack event registration not found"},
        )

    try:
        with req.workflows() as workflows:
            workflow_request = build_workflow_deliver_event_request(
                app_id=app_id,
                workflow_event_subject=workflow_event_subject,
                payload=input,
            )
            workflows.deliver_event(workflow_request)
    except Exception as err:
        logger.exception(
            "failed to deliver Slack v2 workflow event",
            extra={
                "slack_app_id": app_id,
                "workflow_event_subject": workflow_event_subject,
            },
        )
        return gestalt.Response(
            status=HTTPStatus.INTERNAL_SERVER_ERROR,
            body={"error": f"failed to deliver workflow event: {err}"},
        )

    event = workflow_request.event
    return {
        "ok": True,
        "delivered": True,
        "app_id": app_id,
        "workflow_event_subject": workflow_event_subject,
        "workflow_event_id": event.id if event is not None else "",
        "workflow_provider": workflow_request.provider_name,
    }


def _is_url_verification(payload: dict[str, Any]) -> bool:
    return str(payload.get("type") or "").strip() == "url_verification"


def _verify_slack_signature(payload: dict[str, Any], req: gestalt.Request) -> bool:
    timestamp = _slack_request_header(req, "X-Slack-Request-Timestamp")
    signature = _slack_request_header(req, "X-Slack-Signature")
    body = _slack_request_body(payload, req)
    if not timestamp or not signature:
        return False
    if not _slack_request_timestamp_is_fresh(timestamp):
        return False

    secrets = _slack_signing_secrets_for_payload(payload)
    if not secrets:
        return False

    return any(
        hmac.compare_digest(
            signature,
            "v0="
            + hmac.new(
                signing_secret.encode("utf-8"),
                b"v0:" + timestamp.encode("utf-8") + b":" + body,
                hashlib.sha256,
            ).hexdigest(),
        )
        for signing_secret in secrets
    )


def _slack_signing_secrets_for_payload(payload: dict[str, Any]) -> list[str]:
    try:
        return [load_default_signing_secret()]
    except gestalt.NotFoundError:
        return []


def _slack_request_timestamp_is_fresh(timestamp: str) -> bool:
    try:
        request_time = int(timestamp)
    except ValueError:
        return False
    return abs(time.time() - request_time) <= 60 * 5


def _slack_request_header(req: gestalt.Request, name: str) -> str:
    value = _slack_workflow_header(req.workflow, name)
    if value:
        return value

    context = req.context
    headers = getattr(context, "headers", None)
    value = _slack_header_value(headers, name)
    if value:
        return value

    workflow = getattr(context, "workflow", None)
    return _slack_workflow_header(workflow, name)


def _slack_workflow_header(workflow: object, name: str) -> str:
    if not isinstance(workflow, dict):
        return ""
    workflow_map = cast(dict[str, Any], workflow)
    http_context = workflow_map.get("http")
    if not isinstance(http_context, dict):
        return ""
    return _slack_header_value(http_context.get("headers"), name)


def _slack_header_value(headers: object, name: str) -> str:
    if not isinstance(headers, dict):
        return ""
    wanted = name.lower()
    for key, value in headers.items():
        if str(key).lower() != wanted:
            continue
        if isinstance(value, list):
            return str(value[-1] if value else "").strip()
        return str(value).strip()
    return ""


def _slack_request_body(payload: dict[str, Any], req: gestalt.Request) -> bytes:
    raw_body = _slack_raw_request_body(req)
    if raw_body:
        return raw_body
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def _slack_raw_request_body(req: gestalt.Request) -> bytes:
    raw_body = _slack_workflow_raw_body(req.workflow)
    if raw_body:
        return raw_body

    context = req.context
    context_raw_body = getattr(context, "raw_body", None)
    if isinstance(context_raw_body, bytes) and context_raw_body:
        return context_raw_body

    workflow = getattr(context, "workflow", None)
    return _slack_workflow_raw_body(workflow)


def _slack_workflow_raw_body(workflow: object) -> bytes:
    if not isinstance(workflow, dict):
        return b""
    workflow_map = cast(dict[str, Any], workflow)
    http_context = workflow_map.get("http")
    if not isinstance(http_context, dict):
        return b""
    raw_body_base64 = http_context.get("rawBodyBase64")
    if not isinstance(raw_body_base64, str) or not raw_body_base64:
        return b""
    try:
        return base64.b64decode(raw_body_base64, validate=True)
    except binascii.Error, ValueError:
        return b""
