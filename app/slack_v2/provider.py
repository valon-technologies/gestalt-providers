from __future__ import annotations

from typing import Any

import gestalt

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
def register_slack_event(_input: RegisterSlackEventInput, _req: gestalt.Request) -> dict[str, Any]:
    return {}


@app.operation(
    id="handle_slack_event",
    method="POST",
    description="Handle a Slack event.",
)
def handle_slack_event(_input: dict[str, Any], _req: gestalt.Request) -> str:
    return "hello world"
