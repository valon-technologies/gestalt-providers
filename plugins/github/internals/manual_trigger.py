from __future__ import annotations

import re
from collections.abc import Sequence

from .client import bot_identity_or_none
from .config import (
    GitHubWebhookTrigger,
    WEBHOOK_MANUAL_COMMAND_CONTAINS,
    WEBHOOK_MANUAL_COMMAND_EXACT,
)


def manual_trigger_body_matches(body: str, trigger: GitHubWebhookTrigger) -> bool:
    if trigger.require_app_mention:
        return app_mention_body_matches(body)
    return manual_command_body_matches(
        body,
        trigger.manual_commands,
        match_mode=trigger.manual_command_match,
    )


def app_mention_body_matches(body: str) -> bool:
    identity = bot_identity_or_none()
    if identity is None:
        return False
    mentions = app_mention_names(
        slug=identity.slug,
        login=identity.login,
    )
    if not mentions:
        return False
    normalized_body = body.casefold()
    return any(
        _mention_pattern(mention).search(normalized_body) for mention in mentions
    )


def app_mention_names(*, slug: str, login: str) -> tuple[str, ...]:
    names: list[str] = []
    for raw in (slug, login, login.removesuffix("[bot]")):
        value = raw.strip().lstrip("@").casefold()
        if value and value not in names:
            names.append(value)
    return tuple(names)


def _mention_pattern(name: str) -> re.Pattern[str]:
    return re.compile(rf"(?<![A-Za-z0-9_-])@{re.escape(name)}(?![A-Za-z0-9_-])")


def manual_command_body_matches(
    body: str,
    commands: Sequence[str],
    *,
    match_mode: str = WEBHOOK_MANUAL_COMMAND_CONTAINS,
) -> bool:
    if match_mode == WEBHOOK_MANUAL_COMMAND_EXACT:
        normalized_body = normalize_manual_command(body)
        return any(
            normalize_manual_command(command) == normalized_body for command in commands
        )
    return any(command in body for command in commands)


def normalize_manual_command(value: str) -> str:
    return " ".join(value.strip().casefold().split())
