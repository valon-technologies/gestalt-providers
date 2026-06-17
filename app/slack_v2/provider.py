from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Any

import gestalt

from internals.events import (
    build_workflow_deliver_event_request,
    is_url_verification,
    slack_app_id_from_payload,
    url_verification_challenge,
)
from internals.store import (
    get_workflow_definition_id_for_app as load_workflow_definition_id_for_app,
    save_slack_event_registration,
)

logger = logging.getLogger(__name__)

app = gestalt.App("slack_v2")


class GetWorkflowDefinitionIdForAppInput(gestalt.Model):
    app_id: str = gestalt.field(description="Slack app ID.")


class RegisterSlackEventInput(gestalt.Model):
    app_id: str = gestalt.field(description="Slack app ID.")
    client_id: str = gestalt.field(description="Slack OAuth client ID.")
    client_secret: str = gestalt.field(description="Slack OAuth client secret.")
    signing_secret: str = gestalt.field(description="Slack signing secret for request verification.")
    display_name: str = gestalt.field(description="Human-readable name for the Slack bot.")
    workflow_definition_id: str = gestalt.field(
        description="Gestalt workflow definition ID to invoke for Slack events."
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

    save_slack_event_registration(
        app_id=app_id,
        client_id=input.client_id,
        client_secret=input.client_secret,
        signing_secret=input.signing_secret,
        display_name=input.display_name,
        workflow_definition_id=input.workflow_definition_id,
    )
    return {
        "ok": True,
        "app_id": app_id,
        "display_name": input.display_name,
        "workflow_definition_id": input.workflow_definition_id,
    }


@app.operation(
    id="get_workflow_definition_id_for_app",
    method="POST",
    description="Return the workflow definition ID registered for a Slack app.",
)
def get_workflow_definition_id_for_app(
    input: GetWorkflowDefinitionIdForAppInput, _req: gestalt.Request
) -> dict[str, str] | gestalt.Response[dict[str, str]]:
    app_id = input.app_id.strip()
    if not app_id:
        return gestalt.Response(
            status=HTTPStatus.BAD_REQUEST, body={"error": "app_id is required"}
        )

    try:
        workflow_definition_id = load_workflow_definition_id_for_app(app_id=app_id)
    except gestalt.NotFoundError:
        return gestalt.Response(
            status=HTTPStatus.NOT_FOUND,
            body={"error": f"registration not found for app_id {app_id!r}"},
        )

    return {
        "app_id": app_id,
        "workflow_definition_id": workflow_definition_id,
    }


@app.operation(
    id="handle_slack_event",
    method="POST",
    description="Handle a Slack event by delivering a workflow event for the registered definition.",
)
def handle_slack_event(
    input: dict[str, Any], req: gestalt.Request
) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    if is_url_verification(input):
        return url_verification_challenge(input)

    app_id = slack_app_id_from_payload(input)
    if not app_id:
        return gestalt.Response(
            status=HTTPStatus.BAD_REQUEST,
            body={"error": "api_app_id is required"},
        )

    try:
        workflow_definition_id = load_workflow_definition_id_for_app(app_id=app_id)
    except gestalt.NotFoundError:
        return gestalt.Response(
            status=HTTPStatus.NOT_FOUND,
            body={"error": f"registration not found for app_id {app_id!r}"},
        )

    definition: gestalt.WorkflowDefinition | None = None
    workflow_request: gestalt.WorkflowDeliverEvent
    try:
        with req.workflows() as workflows:
            try:
                definition = workflows.get_definition(
                    gestalt.WorkflowGetDefinition(definition_id=workflow_definition_id)
                )
            except Exception:
                logger.exception(
                    "failed to load workflow definition for Slack app",
                    extra={
                        "slack_app_id": app_id,
                        "workflow_definition_id": workflow_definition_id,
                    },
                )
            workflow_request = build_workflow_deliver_event_request(
                app_id=app_id,
                workflow_definition_id=workflow_definition_id,
                payload=input,
                definition=definition,
            )
            workflows.deliver_event(workflow_request)
    except Exception as err:
        logger.exception(
            "failed to deliver Slack v2 workflow event",
            extra={
                "slack_app_id": app_id,
                "workflow_definition_id": workflow_definition_id,
                "workflow_provider": workflow_request.provider_name,
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
        "workflow_definition_id": workflow_definition_id,
        "workflow_event_id": event.id if event is not None else "",
        "workflow_provider": workflow_request.provider_name,
    }
