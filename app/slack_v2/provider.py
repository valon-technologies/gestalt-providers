from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Any

import gestalt

from internals.events import (
    build_workflow_deliver_event_request,
    slack_app_id_from_payload,
)
from internals.store import (
    get_workflow_event_subject_for_app as load_workflow_event_subject_for_app,
    save_slack_event_registration,
)
from internals.smoke_metrics import record_smoke_run

logger = logging.getLogger(__name__)

app = gestalt.App("slack_v2")


class GetWorkflowEventSubjectForAppInput(gestalt.Model):
    app_id: str = gestalt.field(description="Slack app ID.")


class DebugRecordSmokeRunInput(gestalt.Model):
    app_id: str = gestalt.field(
        default="",
        description="Slack app ID from the triggering event, when available.",
    )


class RegisterSlackEventInput(gestalt.Model):
    app_id: str = gestalt.field(description="Slack app ID.")
    client_id: str = gestalt.field(description="Slack OAuth client ID.")
    client_secret: str = gestalt.field(description="Slack OAuth client secret.")
    signing_secret: str = gestalt.field(description="Slack signing secret for request verification.")
    display_name: str = gestalt.field(description="Human-readable name for the Slack bot.")
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
        workflow_event_subject = load_workflow_event_subject_for_app(app_id=app_id)
    except gestalt.NotFoundError:
        return gestalt.Response(
            status=HTTPStatus.NOT_FOUND,
            body={"error": f"registration not found for app_id {app_id!r}"},
        )

    return {
        "app_id": app_id,
        "workflow_event_subject": workflow_event_subject,
    }


@app.operation(
    id="debug_record_smoke_run",
    method="POST",
    description="Debug endpoint that records an OTel metric when the smoke workflow runs.",
)
def debug_record_smoke_run(
    input: DebugRecordSmokeRunInput, _req: gestalt.Request
) -> dict[str, bool]:
    record_smoke_run(app_id=input.app_id)
    return {"ok": True, "recorded": True}


@app.operation(
    id="handle_slack_event",
    method="POST",
    description="Handle a Slack event by publishing the registered workflow event subject.",
)
def handle_slack_event(
    input: dict[str, Any], req: gestalt.Request
) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    app_id = slack_app_id_from_payload(input)
    if not app_id:
        return gestalt.Response(
            status=HTTPStatus.BAD_REQUEST,
            body={"error": "api_app_id is required"},
        )

    try:
        workflow_event_subject = load_workflow_event_subject_for_app(app_id=app_id)
    except gestalt.NotFoundError:
        return gestalt.Response(
            status=HTTPStatus.NOT_FOUND,
            body={"error": f"registration not found for app_id {app_id!r}"},
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
