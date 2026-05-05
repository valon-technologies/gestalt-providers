from __future__ import annotations

import base64
import logging
from dataclasses import dataclass
from typing import Any

import gestalt

from .config import (
    GitHubWebhookPolicy,
    WEBHOOK_PREFERENCE_SUBJECT_COMMENT_AUTHOR,
    WEBHOOK_PREFERENCE_SUBJECT_PULL_REQUEST_AUTHOR,
    WEBHOOK_PREFERENCE_SUBJECT_SENDER,
)
from .constants import GITHUB_EXTERNAL_IDENTITY_TYPE
from .helpers import int_field, map_field, nested_str

logger = logging.getLogger(__name__)

EXTERNAL_IDENTITY_RESOURCE_TYPE = "external_identity"
EXTERNAL_IDENTITY_ASSUME_ACTION = "assume"


@dataclass(frozen=True, slots=True)
class GitHubPullRequestContext:
    pull_request: dict[str, Any]

    @property
    def head_sha(self) -> str:
        return nested_str(self.pull_request, "head", "sha")

    @property
    def author_user_id(self) -> str:
        user_id = int_field(map_field(self.pull_request, "user"), "id")
        return f"user:{user_id}" if user_id > 0 else ""


@dataclass(frozen=True, slots=True)
class GitHubPreferenceIdentity:
    preference_subject: str
    repository: str = ""
    external_identity_type: str = ""
    external_subject_id: str = ""
    subject_id: str = ""

    def metadata(self) -> dict[str, str]:
        data = {"preference_subject": self.preference_subject}
        if self.repository:
            data["repository"] = self.repository
        if self.external_identity_type:
            data["external_identity_type"] = self.external_identity_type
        if self.external_subject_id:
            data["external_subject_id"] = self.external_subject_id
        if self.subject_id:
            data["subject_id"] = self.subject_id
        return data


def action_preference_subject(
    policy: GitHubWebhookPolicy, payload: dict[str, Any], summary: dict[str, Any]
) -> str:
    configured = policy.action_preference_subject.strip()
    if configured:
        return configured
    if map_field(payload, "pull_request") or _single_pull_request_number(
        summary.get("pull_request_numbers")
    ):
        return WEBHOOK_PREFERENCE_SUBJECT_PULL_REQUEST_AUTHOR
    return WEBHOOK_PREFERENCE_SUBJECT_SENDER


def needs_pull_request_context_for_preferences(
    policy: GitHubWebhookPolicy,
    payload: dict[str, Any],
    summary: dict[str, Any],
    *,
    preferences_enabled: bool,
) -> bool:
    if not preferences_enabled:
        return False
    subject = action_preference_subject(policy, payload, summary)
    if subject != WEBHOOK_PREFERENCE_SUBJECT_PULL_REQUEST_AUTHOR:
        return False
    if _pull_request_author_identity(payload):
        return False
    return _single_pull_request_number(summary.get("pull_request_numbers")) > 0


def preference_identity_from_webhook(
    payload: dict[str, Any],
    summary: dict[str, Any],
    policy: GitHubWebhookPolicy,
    *,
    pull_request_context: GitHubPullRequestContext | None,
    authorization: Any | None = None,
) -> GitHubPreferenceIdentity:
    subject = action_preference_subject(policy, payload, summary)
    external_subject_id = ""
    if subject == WEBHOOK_PREFERENCE_SUBJECT_PULL_REQUEST_AUTHOR:
        external_subject_id = _pull_request_author_identity(payload)
        if not external_subject_id and pull_request_context is not None:
            external_subject_id = pull_request_context.author_user_id
    elif subject == WEBHOOK_PREFERENCE_SUBJECT_COMMENT_AUTHOR:
        external_subject_id = _comment_author_identity(payload)
    elif subject == WEBHOOK_PREFERENCE_SUBJECT_SENDER:
        external_subject_id = _sender_identity(payload)

    if not external_subject_id:
        return GitHubPreferenceIdentity(
            preference_subject=subject,
            repository=str(summary.get("repository", "") or "").strip(),
        )

    subject_id = resolve_subject_id_for_external_identity(
        authorization,
        identity_type=GITHUB_EXTERNAL_IDENTITY_TYPE,
        identity_id=external_subject_id,
    )
    return GitHubPreferenceIdentity(
        preference_subject=subject,
        repository=str(summary.get("repository", "") or "").strip(),
        external_identity_type=GITHUB_EXTERNAL_IDENTITY_TYPE,
        external_subject_id=external_subject_id,
        subject_id=subject_id,
    )


def caller_preference_identity(req: Any, identity_kind: str) -> GitHubPreferenceIdentity:
    normalized = identity_kind.strip()
    external_identity = getattr(req, "agent_external_identity", None)
    external_identity_type = str(getattr(external_identity, "type", "") or "").strip()
    external_subject_id = str(getattr(external_identity, "id", "") or "").strip()
    agent_subject = getattr(req, "agent_subject", None)
    subject_id = _human_subject_id(agent_subject)
    if not normalized:
        if external_identity_type == GITHUB_EXTERNAL_IDENTITY_TYPE and external_subject_id:
            normalized = "external_subject_id"
        elif subject_id:
            normalized = "subject_id"
        else:
            raise ValueError("a GitHub external identity or human subject is required")

    if normalized == "external_subject_id":
        if external_identity_type != GITHUB_EXTERNAL_IDENTITY_TYPE or not external_subject_id:
            raise ValueError(
                "identityKind external_subject_id requires a linked GitHub agent external identity"
            )
        return GitHubPreferenceIdentity(
            preference_subject="caller",
            repository="",
            external_identity_type=external_identity_type,
            external_subject_id=external_subject_id,
            subject_id=subject_id,
        )
    if normalized == "subject_id":
        if not subject_id:
            raise ValueError("identityKind subject_id requires a human agent subject")
        return GitHubPreferenceIdentity(
            preference_subject="caller",
            repository="",
            subject_id=subject_id,
            external_identity_type=(
                external_identity_type
                if external_identity_type == GITHUB_EXTERNAL_IDENTITY_TYPE
                else ""
            ),
            external_subject_id=(
                external_subject_id
                if external_identity_type == GITHUB_EXTERNAL_IDENTITY_TYPE
                else ""
            ),
        )
    raise ValueError("identityKind must be external_subject_id or subject_id")


def resolve_subject_id_for_external_identity(
    authorization: Any | None, *, identity_type: str, identity_id: str
) -> str:
    if authorization is None or not identity_type or not identity_id:
        return ""
    try:
        response = authorization.search_subjects(
            gestalt.SubjectSearchRequest(
                resource=gestalt.AuthorizationResource(
                    type=EXTERNAL_IDENTITY_RESOURCE_TYPE,
                    id=external_identity_resource_id(identity_type, identity_id),
                ),
                action=gestalt.AuthorizationAction(
                    name=EXTERNAL_IDENTITY_ASSUME_ACTION
                ),
                page_size=10,
            )
        )
    except Exception as err:
        logger.warning("GitHub action preference subject lookup failed: %s", err)
        return ""
    subjects = _dedupe_resolved_subjects(getattr(response, "subjects", []) or [])
    if len(subjects) != 1:
        if len(subjects) > 1:
            logger.warning(
                "GitHub action preference external identity resolved multiple subjects"
            )
        return ""
    return str(getattr(subjects[0], "id", "") or "").strip()


def external_identity_resource_id(identity_type: str, identity_id: str) -> str:
    raw = f"{identity_type.strip()}\x00{identity_id.strip()}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _pull_request_author_identity(payload: dict[str, Any]) -> str:
    return _github_user_external_id(map_field(map_field(payload, "pull_request"), "user"))


def _comment_author_identity(payload: dict[str, Any]) -> str:
    comment_identity = _github_user_external_id(map_field(map_field(payload, "comment"), "user"))
    if comment_identity:
        return comment_identity
    return _github_user_external_id(map_field(map_field(payload, "review"), "user"))


def _sender_identity(payload: dict[str, Any]) -> str:
    return _github_user_external_id(map_field(payload, "sender"))


def _github_user_external_id(user: dict[str, Any]) -> str:
    user_id = int_field(user, "id")
    return f"user:{user_id}" if user_id > 0 else ""


def _human_subject_id(subject: Any) -> str:
    subject_id = str(getattr(subject, "id", "") or "").strip()
    kind = str(getattr(subject, "kind", "") or "").strip()
    if not subject_id or subject_id.startswith("service_account:"):
        return ""
    if kind and kind not in {"human", "user", "subject"}:
        return ""
    return subject_id


def _dedupe_resolved_subjects(subjects: list[Any]) -> list[Any]:
    unique: dict[tuple[str, str], Any] = {}
    for subject in subjects:
        subject_id = str(getattr(subject, "id", "") or "").strip()
        if not subject_id:
            continue
        subject_type = str(getattr(subject, "type", "") or "").strip()
        key = (subject_type, subject_id)
        unique[key] = subject
    return list(unique.values())


def _single_pull_request_number(value: Any) -> int:
    if not isinstance(value, list) or len(value) != 1:
        return 0
    number = value[0]
    if isinstance(number, bool) or not isinstance(number, (int, float)):
        return 0
    return int(number) if int(number) > 0 else 0
