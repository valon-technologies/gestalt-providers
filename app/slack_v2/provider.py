from __future__ import annotations

from http import HTTPStatus
from typing import Any

import gestalt

from internals.store import save_slack_event_registration

app = gestalt.App("slack_v2")


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
    id="handle_slack_event",
    method="POST",
    description="Handle a Slack event.",
)
def handle_slack_event(_input: dict[str, Any], _req: gestalt.Request) -> str:
    return "hello world"
