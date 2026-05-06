from __future__ import annotations

from typing import Any

from .config import GitHubWebhookPolicy, effective_policy_operations


def policy_base_metadata(policy: GitHubWebhookPolicy) -> dict[str, Any]:
    action: dict[str, Any] = {
        "allow_code_review_comments": policy.allow_code_review_comments,
        "allow_self_fix": policy.allow_self_fix,
        "self_fix_mode": policy.self_fix_mode,
    }
    if policy.action_preference_subject:
        action["preference_subject"] = policy.action_preference_subject
    metadata: dict[str, Any] = {
        "id": policy.id,
        "mode": policy.action_mode,
        "tool_refs": list(effective_policy_operations(policy)),
        "trigger": {
            "frequency": policy.trigger.frequency,
            "include_drafts": policy.trigger.include_drafts,
            "manual_commands": list(policy.trigger.manual_commands),
            "manual_command_match": policy.trigger.manual_command_match,
        },
        "dedupe": {"scope": policy.dedupe.scope},
        "action": action,
        "comments": {
            "timeline_policy": policy.comments.timeline_policy,
            "inline_policy": policy.comments.inline_policy,
            "suppress_stale_head": policy.comments.suppress_stale_head,
        },
    }
    if policy.action_preferences is not None:
        metadata["action_preferences"] = policy.action_preferences
    return metadata
