from __future__ import annotations

from typing import Any

import gestalt

app = gestalt.App("slack_v2")


@app.operation(
    id="HandleSlackEvent",
    method="POST",
    description="Handle a Slack event.",
)
def handle_slack_event(_input: dict[str, Any], _req: gestalt.Request) -> str:
    return "hello world"
