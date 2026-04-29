from __future__ import annotations

import hashlib
import json
from typing import Any

from .constants import (
    MAX_GITHUB_BODY_CHARS,
    MAX_GITHUB_TITLE_CHARS,
)
from .helpers import int_field, map_field, nested_str, str_field
from .webhook import bounded_text


def workflow_signal_data(
    payload: dict[str, Any],
    summary: dict[str, Any],
) -> dict[str, Any]:
    payload_digest = _payload_digest(payload)
    delivery_id = _github_delivery_id(payload)
    return {
        "delivery_id": delivery_id or f"github:{payload_digest}",
        "github_event": summary.get("event_type", ""),
        "github_action": summary.get("action", ""),
        "installation": _installation_data(payload),
        "repository": _repository_data(payload),
        "sender": _sender_data(payload),
        "summary": summary,
        "agent_request": _agent_request(payload, summary),
        "payload_sha256": payload_digest,
        "payload_omitted": True,
    }


def _payload_digest(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _github_delivery_id(payload: dict[str, Any]) -> str:
    headers = map_field(payload, "headers")
    for key, value in headers.items():
        if str(key).lower() == "x-github-delivery" and isinstance(value, str):
            return value.strip()
    return ""


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _agent_request(payload: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    request: dict[str, Any] = {}
    subject = _subject_data(payload, summary)
    if subject:
        request["subject"] = subject
    for key, value in {
        "pull_request": _pull_request_data(payload),
        "issue": _issue_data(payload),
        "comment": _comment_data(payload),
        "review": _review_data(payload),
    }.items():
        if value:
            request[key] = value
    request.update(_ref_data(payload))
    return request


def _subject_data(payload: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    repository = str(summary.get("repository", "")).strip()
    subject: dict[str, Any] = {}
    if repository:
        subject["repository"] = repository
    number = summary.get("number")
    if isinstance(number, (int, float)) and int(number) > 0:
        subject["number"] = int(number)
    url = (
        str_field(map_field(payload, "pull_request"), "html_url")
        or str_field(map_field(payload, "issue"), "html_url")
        or str_field(map_field(payload, "comment"), "html_url")
        or str_field(map_field(payload, "review"), "html_url")
        or str_field(map_field(payload, "repository"), "html_url")
    )
    if url:
        subject["html_url"] = url
    return subject


def _pull_request_data(payload: dict[str, Any]) -> dict[str, Any]:
    pull_request = map_field(payload, "pull_request")
    if not pull_request:
        return {}
    data = {
        "number": _positive_int(pull_request, "number"),
        "title": _bounded_field(pull_request, "title", MAX_GITHUB_TITLE_CHARS),
        "state": str_field(pull_request, "state"),
        "html_url": str_field(pull_request, "html_url"),
        "head_ref": nested_str(pull_request, "head", "ref"),
        "head_sha": nested_str(pull_request, "head", "sha"),
        "base_ref": nested_str(pull_request, "base", "ref"),
        "base_sha": nested_str(pull_request, "base", "sha"),
    }
    return _compact_dict(data)


def _issue_data(payload: dict[str, Any]) -> dict[str, Any]:
    issue = map_field(payload, "issue")
    if not issue:
        return {}
    data = {
        "number": _positive_int(issue, "number"),
        "title": _bounded_field(issue, "title", MAX_GITHUB_TITLE_CHARS),
        "state": str_field(issue, "state"),
        "html_url": str_field(issue, "html_url"),
    }
    return _compact_dict(data)


def _comment_data(payload: dict[str, Any]) -> dict[str, Any]:
    comment = map_field(payload, "comment")
    if not comment:
        return {}
    data = {
        "id": _positive_int(comment, "id"),
        "html_url": str_field(comment, "html_url"),
        "body": _bounded_field(comment, "body", MAX_GITHUB_BODY_CHARS),
        "user": nested_str(comment, "user", "login"),
    }
    return _compact_dict(data)


def _review_data(payload: dict[str, Any]) -> dict[str, Any]:
    review = map_field(payload, "review")
    if not review:
        return {}
    data = {
        "id": _positive_int(review, "id"),
        "state": str_field(review, "state"),
        "html_url": str_field(review, "html_url"),
        "body": _bounded_field(review, "body", MAX_GITHUB_BODY_CHARS),
        "user": nested_str(review, "user", "login"),
    }
    return _compact_dict(data)


def _ref_data(payload: dict[str, Any]) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for key in ("ref", "base_ref", "before", "after", "compare", "ref_type"):
        value = str_field(payload, key)
        if value:
            data[key] = value
    for key in ("created", "deleted", "forced"):
        value = payload.get(key)
        if isinstance(value, bool):
            data[key] = value
    head_commit = map_field(payload, "head_commit")
    if head_commit:
        commit = _compact_dict(
            {
                "id": str_field(head_commit, "id"),
                "message": _bounded_field(
                    head_commit, "message", MAX_GITHUB_TITLE_CHARS
                ),
                "url": str_field(head_commit, "url"),
            }
        )
        if commit:
            data["head_commit"] = commit
    return data


def _bounded_field(value: dict[str, Any], key: str, max_chars: int) -> str:
    text = str_field(value, key)
    if not text:
        return ""
    return bounded_text(text, max_chars)


def _positive_int(value: dict[str, Any], key: str) -> int:
    number = int_field(value, key)
    if number <= 0:
        return 0
    return number


def _compact_dict(value: dict[str, Any]) -> dict[str, Any]:
    return {
        key: nested
        for key, nested in value.items()
        if nested not in ("", 0, None, {}, [])
    }


def _installation_data(payload: dict[str, Any]) -> dict[str, Any]:
    installation = map_field(payload, "installation")
    account = map_field(installation, "account")
    return {
        key: value
        for key, value in {
            "id": int_field(installation, "id"),
            "app_id": int_field(installation, "app_id"),
            "app_slug": str_field(installation, "app_slug"),
            "target_type": str_field(installation, "target_type"),
            "account_login": str_field(account, "login"),
            "account_id": int_field(account, "id"),
            "account_type": str_field(account, "type"),
        }.items()
        if value not in ("", 0)
    }


def _repository_data(payload: dict[str, Any]) -> dict[str, Any]:
    repository = map_field(payload, "repository")
    return {
        key: value
        for key, value in {
            "id": int_field(repository, "id"),
            "name": str_field(repository, "name"),
            "full_name": str_field(repository, "full_name"),
            "owner": nested_str(repository, "owner", "login"),
            "default_branch": str_field(repository, "default_branch"),
            "html_url": str_field(repository, "html_url"),
        }.items()
        if value not in ("", 0)
    }


def _sender_data(payload: dict[str, Any]) -> dict[str, Any]:
    sender = map_field(payload, "sender")
    return {
        key: value
        for key, value in {
            "login": str_field(sender, "login"),
            "id": int_field(sender, "id"),
            "type": str_field(sender, "type"),
        }.items()
        if value not in ("", 0)
    }
