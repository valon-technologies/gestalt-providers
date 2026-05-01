from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .client import bot_identity
from .config import get_github_config
from .constants import (
    GITHUB_INSTALLATION_SUBJECT_PREFIX,
    GITHUB_REPOSITORY_SUBJECT_SEPARATOR,
    MAX_GITHUB_TITLE_CHARS,
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
        kind="service_account",
        display_name=display_name,
        auth_source="github_app_webhook",
    )


def event_summary(
    payload: dict[str, Any], installation_id: int, event_type: str = ""
) -> dict[str, Any]:
    repository = map_field(payload, "repository")
    sender = map_field(payload, "sender")
    pull_request = map_field(payload, "pull_request")
    issue = map_field(payload, "issue")
    comment = map_field(payload, "comment")
    review = map_field(payload, "review")
    summary: dict[str, Any] = {
        "installation_id": installation_id,
        "event_type": event_type or github_event_type(payload),
        "action": str_field(payload, "action"),
        "repository": repository_full_name(payload),
        "repository_owner": nested_str(repository, "owner", "login"),
        "repository_name": str_field(repository, "name"),
        "sender": str_field(sender, "login"),
        "delivery_id": github_delivery_id(payload),
    }
    number = int_field(pull_request, "number") or int_field(issue, "number")
    if number > 0:
        summary["number"] = number
    _add_ci_event_summary(payload, summary)
    if str_field(pull_request, "head", "ref"):
        summary["head_ref"] = nested_str(pull_request, "head", "ref")
    if str_field(pull_request, "base", "ref"):
        summary["base_ref"] = nested_str(pull_request, "base", "ref")
    subject = pull_request or issue
    if str_field(subject, "title"):
        summary["title"] = bounded_text(
            str_field(subject, "title"), MAX_GITHUB_TITLE_CHARS
        )
    if str_field(subject, "state"):
        summary["state"] = str_field(subject, "state")
    if str_field(subject, "html_url"):
        summary["html_url"] = str_field(subject, "html_url")
    if comment:
        comment_id = int_field(comment, "id")
        if comment_id > 0:
            summary["comment_id"] = comment_id
        if str_field(comment, "html_url"):
            summary["comment_url"] = str_field(comment, "html_url")
    if review:
        review_id = int_field(review, "id")
        if review_id > 0:
            summary["review_id"] = review_id
        if str_field(review, "state"):
            summary["review_state"] = str_field(review, "state")
        if str_field(review, "html_url"):
            summary["review_url"] = str_field(review, "html_url")
    for key in ("ref", "base_ref", "before", "after", "compare", "ref_type"):
        if str_field(payload, key):
            summary[key] = str_field(payload, key)
    for key in ("created", "deleted", "forced"):
        value = payload.get(key)
        if isinstance(value, bool):
            summary[key] = value
    return {key: value for key, value in summary.items() if value not in ("", 0)}


def _add_ci_event_summary(payload: dict[str, Any], summary: dict[str, Any]) -> None:
    event_type = str(summary.get("event_type", "")).strip()
    if event_type not in ("check_run", "check_suite", "workflow_run"):
        return
    summary["payload_sha256"] = payload_digest(payload)
    event_object = map_field(payload, event_type)
    event_id = int_field(event_object, "id")
    if event_id > 0:
        summary[f"{event_type}_id"] = event_id
    if str_field(event_object, "status"):
        summary["status"] = str_field(event_object, "status")
    if str_field(event_object, "conclusion"):
        summary["conclusion"] = str_field(event_object, "conclusion")
    if str_field(event_object, "name"):
        summary["name"] = str_field(event_object, "name")
    if str_field(event_object, "head_branch"):
        summary["head_ref"] = str_field(event_object, "head_branch")
    if str_field(event_object, "head_sha"):
        summary["head_sha"] = str_field(event_object, "head_sha")
    pr_numbers = pull_request_numbers(event_object)
    if pr_numbers:
        summary["pull_request_numbers"] = pr_numbers
    if len(pr_numbers) == 1:
        summary["number"] = pr_numbers[0]


def pull_request_numbers(value: dict[str, Any]) -> list[int]:
    pull_requests = value.get("pull_requests")
    if not isinstance(pull_requests, list):
        return []
    numbers: list[int] = []
    seen: set[int] = set()
    for item in pull_requests:
        if not isinstance(item, dict):
            continue
        number = int_field(item, "number")
        if number <= 0 or number in seen:
            continue
        seen.add(number)
        numbers.append(number)
    return numbers


def github_delivery_id(payload: dict[str, Any]) -> str:
    headers = map_field(payload, "headers")
    for key, value in headers.items():
        if str(key).lower() == "x-github-delivery" and isinstance(value, str):
            return value.strip()
    return ""


def github_event_header(payload: dict[str, Any]) -> str:
    headers = map_field(payload, "headers")
    for key, value in headers.items():
        if str(key).lower() == "x-github-event" and isinstance(value, str):
            return value.strip()
    return ""


def payload_digest(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    ).hexdigest()


def bounded_text(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    return value[:max_chars] + "\n...<truncated>"


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


def webhook_ignored_reason(
    payload: dict[str, Any],
    *,
    event_type: str = "",
    enforce_event_allowlist: bool = True,
) -> str:
    if is_ping_event(payload):
        return "ping"
    if installation_id_from_payload(payload) <= 0:
        return "missing_installation"

    event_type = event_type or github_event_type(payload)
    if not event_type:
        return "unknown_event_type"
    config = get_github_config()
    if enforce_event_allowlist and event_type not in config.webhook_events:
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
