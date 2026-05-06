from __future__ import annotations

from typing import Any

from .config import (
    GitHubAppConfig,
    GitHubWebhookPolicy,
    WEBHOOK_MANUAL_COMMAND_EXACT,
    WEBHOOK_TRIGGER_MANUAL_ONLY,
)
from .helpers import map_field, nested_str, str_field
from .webhook import github_event_header, github_event_type


def webhook_event_type_for_policy(payload: dict[str, Any]) -> str:
    return (github_event_header(payload) or github_event_type(payload)).lower()


def select_webhook_policy(
    config: GitHubAppConfig, payload: dict[str, Any], summary: dict[str, Any]
) -> GitHubWebhookPolicy | None:
    for policy in config.webhook_policies:
        if webhook_policy_matches(policy, payload, summary):
            return policy
    return None


def webhook_policy_matches(
    policy: GitHubWebhookPolicy, payload: dict[str, Any], summary: dict[str, Any]
) -> bool:
    match = policy.match
    event_type = str(summary.get("event_type", "")).lower()
    event_object = map_field(payload, event_type)
    if not _matches(match.events, [event_type]):
        return False
    if not _matches(match.actions, [str(summary.get("action", "")).lower()]):
        return False
    if not _matches(match.statuses, [str_field(event_object, "status").lower()]):
        return False
    if not _matches(match.conclusions, [str_field(event_object, "conclusion").lower()]):
        return False
    if not _matches(match.repositories, [str(summary.get("repository", ""))]):
        return False
    if not _matches(match.branches, branch_candidates(payload, summary, event_object)):
        return False
    if not _matches(match.check_names, check_name_candidates(payload, event_type)):
        return False
    if not _matches(match.workflow_names, workflow_name_candidates(payload)):
        return False
    if not _policy_allows_draft(policy, payload, event_type):
        return False
    if not _manual_command_matches(policy, payload, event_type):
        return False
    return True


def _policy_allows_draft(
    policy: GitHubWebhookPolicy, payload: dict[str, Any], event_type: str
) -> bool:
    if policy.trigger.include_drafts or event_type != "pull_request":
        return True
    pull_request = map_field(payload, "pull_request")
    return pull_request.get("draft") is not True


def _manual_command_matches(
    policy: GitHubWebhookPolicy, payload: dict[str, Any], event_type: str
) -> bool:
    if policy.trigger.frequency != WEBHOOK_TRIGGER_MANUAL_ONLY:
        return True
    if event_type not in ("issue_comment", "pull_request_review_comment"):
        return False
    body = str_field(map_field(payload, "comment"), "body")
    if policy.trigger.manual_command_match == WEBHOOK_MANUAL_COMMAND_EXACT:
        normalized_body = normalize_manual_command(body)
        return any(
            normalize_manual_command(command) == normalized_body
            for command in policy.trigger.manual_commands
        )
    return any(command in body for command in policy.trigger.manual_commands)


def normalize_manual_command(value: str) -> str:
    return " ".join(value.strip().casefold().split())


def branch_candidates(
    payload: dict[str, Any], summary: dict[str, Any], event_object: dict[str, Any]
) -> list[str]:
    candidates: list[str] = []
    pull_request = map_field(payload, "pull_request")
    for value in (
        str(summary.get("head_ref", "")),
        str(summary.get("base_ref", "")),
        nested_str(pull_request, "head", "ref"),
        nested_str(pull_request, "base", "ref"),
        str_field(event_object, "head_branch"),
        branch_from_ref(str_field(payload, "ref")),
        branch_from_ref(str_field(payload, "base_ref")),
    ):
        if value and value not in candidates:
            candidates.append(value)
    return candidates


def branch_from_ref(ref: str) -> str:
    if ref.startswith("refs/heads/"):
        return ref.removeprefix("refs/heads/")
    return ref


def check_name_candidates(payload: dict[str, Any], event_type: str) -> list[str]:
    if event_type != "check_run":
        return []
    name = str_field(map_field(payload, "check_run"), "name")
    return [name] if name else []


def workflow_name_candidates(payload: dict[str, Any]) -> list[str]:
    name = str_field(map_field(payload, "workflow_run"), "name")
    return [name] if name else []


def _matches(configured: tuple[str, ...], candidates: list[str]) -> bool:
    if not configured:
        return True
    return any(candidate in configured for candidate in candidates if candidate)
