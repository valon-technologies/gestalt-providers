from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .client import bot_identity
from .config import get_github_config
from .constants import (
    GITHUB_INSTALLATION_SUBJECT_PREFIX,
    GITHUB_REPOSITORY_SUBJECT_SEPARATOR,
)
from .errors import GitHubAPIError, GitHubConfigError
from .helpers import int_field, map_field, nested_str, str_field


@dataclass(frozen=True, slots=True)
class GitHubWebhookSubject:
    id: str
    kind: str
    display_name: str
    auth_source: str


def webhook_subject_from_payload(
    payload: dict[str, Any],
) -> GitHubWebhookSubject | None:
    installation_id = installation_id_from_payload(payload)
    if installation_id <= 0:
        return None

    repo = repository_full_name(payload)
    subject_id = f"{GITHUB_INSTALLATION_SUBJECT_PREFIX}{installation_id}"
    display_name = f"GitHub App installation {installation_id}"
    if repo:
        subject_id = f"{subject_id}{GITHUB_REPOSITORY_SUBJECT_SEPARATOR}{repo}"
        display_name = f"{display_name} ({repo})"
    return GitHubWebhookSubject(
        id=subject_id,
        kind="workload",
        display_name=display_name,
        auth_source="github_app_webhook",
    )


def event_summary(payload: dict[str, Any], installation_id: int) -> dict[str, Any]:
    repository = map_field(payload, "repository")
    sender = map_field(payload, "sender")
    pull_request = map_field(payload, "pull_request")
    issue = map_field(payload, "issue")
    summary: dict[str, Any] = {
        "installation_id": installation_id,
        "event_type": github_event_type(payload),
        "action": str_field(payload, "action"),
        "repository": repository_full_name(payload),
        "repository_owner": nested_str(repository, "owner", "login"),
        "repository_name": str_field(repository, "name"),
        "sender": str_field(sender, "login"),
    }
    number = int_field(pull_request, "number") or int_field(issue, "number")
    if number > 0:
        summary["number"] = number
    if str_field(pull_request, "head", "ref"):
        summary["head_ref"] = nested_str(pull_request, "head", "ref")
    if str_field(pull_request, "base", "ref"):
        summary["base_ref"] = nested_str(pull_request, "base", "ref")
    return {key: value for key, value in summary.items() if value not in ("", 0)}


def installation_id_from_payload(payload: dict[str, Any]) -> int:
    installation = map_field(payload, "installation")
    return int_field(installation, "id")


def repository_full_name(payload: dict[str, Any]) -> str:
    repository = map_field(payload, "repository")
    full_name = str_field(repository, "full_name")
    if full_name:
        return full_name
    owner = nested_str(repository, "owner", "login")
    name = str_field(repository, "name")
    if owner and name:
        return f"{owner}/{name}"
    return ""


def webhook_ignored_reason(payload: dict[str, Any]) -> str:
    if is_ping_event(payload):
        return "ping"
    if installation_id_from_payload(payload) <= 0:
        return "missing_installation"

    event_type = github_event_type(payload)
    if not event_type:
        return "unknown_event_type"
    config = get_github_config()
    if event_type not in config.webhook_events:
        return f"unsupported_event_type:{event_type}"
    if config.ignore_bot_sender:
        try:
            if is_configured_bot_sender(payload):
                return "configured_bot_sender"
        except (GitHubAPIError, GitHubConfigError):
            if is_bot_sender(payload):
                return "unresolved_bot_sender"
    return ""


def is_ping_event(payload: dict[str, Any]) -> bool:
    return bool(payload.get("zen")) and isinstance(payload.get("hook"), dict)


def github_event_type(payload: dict[str, Any]) -> str:
    if "check_run" in payload:
        return "check_run"
    if "check_suite" in payload:
        return "check_suite"
    if "workflow_run" in payload:
        return "workflow_run"
    if "pull_request" in payload and "review" in payload:
        return "pull_request_review"
    if "pull_request" in payload and "comment" in payload:
        return "pull_request_review_comment"
    if "pull_request" in payload:
        return "pull_request"
    if "issue" in payload and "comment" in payload:
        return "issue_comment"
    if "issue" in payload:
        return "issues"
    if "ref" in payload and ("commits" in payload or "head_commit" in payload):
        return "push"
    if "repository" in payload and str_field(payload, "action"):
        return str_field(payload, "action")
    return ""


def is_configured_bot_sender(payload: dict[str, Any]) -> bool:
    sender_login = nested_str(map_field(payload, "sender"), "login").lower()
    if not sender_login or not is_bot_login(sender_login):
        return False
    identity = bot_identity()
    return bool(identity.login and sender_login == identity.login.lower())


def is_bot_sender(payload: dict[str, Any]) -> bool:
    sender_login = nested_str(map_field(payload, "sender"), "login").lower()
    return is_bot_login(sender_login)


def is_bot_login(login: str) -> bool:
    return login.endswith("[bot]") or login.endswith("-bot") or login.endswith("_bot")
