from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import gestalt

from .helpers import map_field
from .models import SlackReplyRef
from .reply_ref import decode_reply_ref

SLACK_AGENT_WORKFLOW_EVENT_TYPE = "slack.agent.event.received"
SLACK_INTERACTION_WORKFLOW_EVENT_TYPE = "slack.agent.interaction.received"


@dataclass(frozen=True, slots=True)
class SlackWorkflowReplyBinding:
    reply_ref: str
    route_id: str
    team_id: str
    channel_id: str
    message_ts: str
    reply_thread_ts: str

    def matches_reply_ref(self, raw_reply_ref: str, ref: SlackReplyRef) -> bool:
        if self.reply_ref != raw_reply_ref.strip():
            return False
        if not ref.team_id or not ref.channel_id or not ref.message_ts:
            return False
        if self.team_id != ref.team_id:
            return False
        if self.channel_id != ref.channel_id:
            return False
        if self.message_ts != ref.message_ts:
            return False
        if self.reply_thread_ts != ref.reply_thread_ts:
            return False
        if ref.route_id and self.route_id != ref.route_id:
            return False
        return True


def authorize_reply_ref(
    reply_ref: str, req: gestalt.Request, *, signing_key: bytes
) -> SlackReplyRef:
    subject_id = req.subject.id.strip()
    ref = decode_reply_ref(reply_ref, signing_key=signing_key)
    if ref.subject_id == subject_id:
        return ref

    binding = workflow_reply_binding(req)
    if binding is not None and binding.matches_reply_ref(reply_ref, ref):
        return ref

    raise ValueError("reply_ref does not belong to this subject")


def workflow_reply_binding(
    req: gestalt.Request,
) -> SlackWorkflowReplyBinding | None:
    workflow = req.workflow_run_context()
    if workflow.trigger.kind != "event" or not isinstance(workflow.trigger.event, dict):
        return None
    return parse_slack_workflow_reply_binding(workflow.trigger.event)


def parse_slack_workflow_reply_binding(
    event: dict[str, Any],
) -> SlackWorkflowReplyBinding | None:
    if str(event.get("source") or "").strip() != "slack":
        return None

    data = map_field(event, "data")
    slack = map_field(data, "slack")
    binding = SlackWorkflowReplyBinding(
        reply_ref=str(data.get("reply_ref") or "").strip(),
        route_id=str(data.get("routeId") or "").strip(),
        team_id=str(slack.get("team_id") or "").strip(),
        channel_id=str(slack.get("channel_id") or "").strip(),
        message_ts=str(slack.get("message_ts") or "").strip(),
        reply_thread_ts=str(slack.get("reply_thread_ts") or "").strip(),
    )
    if (
        not binding.reply_ref
        or not binding.team_id
        or not binding.channel_id
        or not binding.message_ts
    ):
        return None
    return binding
