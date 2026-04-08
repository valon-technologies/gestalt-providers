from __future__ import annotations

import re
from http import HTTPStatus
from typing import Any, TypeAlias, cast
from urllib.parse import urlencode

import gestalt

from internals import get_json, slack_base_url

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse

SLACK_MESSAGE_URL_PATTERN = re.compile(r"https?://[^/]+\.slack\.com/archives/([A-Z0-9]+)/p(\d{10})(\d+)")
USER_MENTION_PATTERN = re.compile(r"<@(U[A-Z0-9]+)>")


class GetMessageInput(gestalt.Model):
    url: str = gestalt.field(description="Slack message URL", default="", required=False)
    channel: str = gestalt.field(description="Channel ID", default="", required=False)
    ts: str = gestalt.field(description="Message timestamp", default="", required=False)


class FindUserMentionsInput(gestalt.Model):
    channel: str = gestalt.field(description="Channel ID to scan")
    user_id: str = gestalt.field(description="Optional user ID to filter mentions to", default="", required=False)
    limit: int = gestalt.field(description="Number of messages to scan", default=100, required=False)
    oldest: str = gestalt.field(
        description="Only include messages after this Unix timestamp",
        default="",
        required=False,
    )
    latest: str = gestalt.field(
        description="Only include messages before this Unix timestamp",
        default="",
        required=False,
    )
    include_bots: bool = gestalt.field(
        description="Include bot messages in the scan",
        default=False,
        required=False,
    )


class GetThreadParticipantsInput(gestalt.Model):
    channel: str = gestalt.field(description="Channel ID containing the thread")
    ts: str = gestalt.field(description="Parent message timestamp")
    include_user_info: bool = gestalt.field(
        description="Fetch user profile details for participants",
        default=False,
        required=False,
    )
    include_bots: bool = gestalt.field(
        description="Include bot users in the participant list",
        default=True,
        required=False,
    )


@gestalt.operation(
    id="conversations.getMessage",
    method="POST",
    description="Fetch a single message by Slack URL or channel and timestamp",
)
def conversations_get_message(input: GetMessageInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error

    channel = input.channel
    ts = input.ts

    if input.url:
        match = SLACK_MESSAGE_URL_PATTERN.search(input.url)
        if match is None:
            return _server_error(f"invalid Slack message URL: {input.url}")
        channel = match.group(1)
        ts = f"{match.group(2)}.{match.group(3)}"

    if not channel or not ts:
        return _server_error("either url or both channel and ts are required")

    try:
        data = _slack_get(
            "conversations.history",
            {
                "channel": channel,
                "oldest": ts,
                "latest": ts,
                "inclusive": "true",
                "limit": "1",
            },
            req.token,
        )
    except RuntimeError as err:
        return _server_error(str(err))

    messages = _map_slice(data.get("messages"))
    if not messages:
        return _server_error(f"no message found at timestamp {ts}")

    return {"data": {"message": messages[0]}}


@gestalt.operation(
    id="conversations.findUserMentions",
    method="POST",
    description="Find Slack user mentions in channel messages",
)
def conversations_find_user_mentions(input: FindUserMentionsInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.channel:
        return _server_error("channel is required")

    query = {
        "channel": input.channel,
        "limit": str(input.limit),
    }
    if input.oldest:
        query["oldest"] = input.oldest
    if input.latest:
        query["latest"] = input.latest

    try:
        data = _slack_get("conversations.history", query, req.token)
    except RuntimeError as err:
        return _server_error(str(err))

    messages = _map_slice(data.get("messages"))
    mentions: list[dict[str, str]] = []
    mentioned_user_ids: set[str] = set()

    for message in messages:
        if not input.include_bots and _string_field(message, "bot_id"):
            continue

        text = _string_field(message, "text")
        for user_id in USER_MENTION_PATTERN.findall(text):
            if input.user_id and user_id != input.user_id:
                continue
            mentioned_user_ids.add(user_id)
            mentions.append(
                {
                    "user_id": user_id,
                    "message_ts": _string_field(message, "ts"),
                    "mentioned_by": _string_field(message, "user"),
                    "text": text,
                    "channel": input.channel,
                }
            )

    return {
        "data": {
            "mentions": mentions,
            "mentioned_user_ids": sorted(mentioned_user_ids),
            "total_mentions": len(mentions),
            "messages_scanned": len(messages),
        }
    }


@gestalt.operation(
    id="conversations.getThreadParticipants",
    method="POST",
    description="Get unique participants in a Slack thread",
)
def conversations_get_thread_participants(input: GetThreadParticipantsInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.channel:
        return _server_error("channel is required")
    if not input.ts:
        return _server_error("ts is required")

    try:
        data = _slack_get(
            "conversations.replies",
            {
                "channel": input.channel,
                "ts": input.ts,
                "limit": "1000",
            },
            req.token,
        )
    except RuntimeError as err:
        return _server_error(str(err))

    messages = _map_slice(data.get("messages"))
    thread_starter = _string_field(messages[0], "user") if messages else ""

    participants_by_user: dict[str, dict[str, Any]] = {}
    for message in messages:
        user_id = _string_field(message, "user")
        if not user_id:
            continue
        if not input.include_bots and _string_field(message, "bot_id"):
            continue

        participant = participants_by_user.get(user_id)
        if participant is None:
            participant = {
                "user_id": user_id,
                "message_count": 0,
                "first_reply_ts": _string_field(message, "ts"),
                "is_thread_starter": user_id == thread_starter,
            }
            participants_by_user[user_id] = participant
        participant["message_count"] = int(participant["message_count"]) + 1

    participants = list(participants_by_user.values())
    participants.sort(key=lambda participant: (str(participant["first_reply_ts"]), str(participant["user_id"])))

    if input.include_user_info:
        for participant in participants:
            try:
                user_data = _slack_get("users.info", {"user": str(participant["user_id"])}, req.token)
            except RuntimeError:
                continue
            user = _map_field(user_data, "user")
            profile = _map_field(user, "profile")
            participant["display_name"] = _string_field(profile, "display_name")
            participant["real_name"] = _string_field(user, "real_name")
            is_bot = _bool_field(user, "is_bot")
            if is_bot is not None:
                participant["is_bot"] = is_bot

    total_replies = len(messages) - 1 if messages else 0
    return {
        "data": {
            "participants": participants,
            "participant_count": len(participants),
            "total_replies": total_replies,
        }
    }


def _slack_get(endpoint: str, query: dict[str, str], token: str) -> dict[str, Any]:
    url = f"{slack_base_url()}/{endpoint.lstrip('/')}"
    if query:
        url = f"{url}?{urlencode(query)}"
    return get_json(url, token)


def _validate_token(req: gestalt.Request) -> ErrorResponse | None:
    if not req.token:
        return _server_error("token is required")
    return None


def _server_error(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message})


def _map_slice(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    items: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            items.append(cast(dict[str, Any], item))
    return items


def _map_field(data: object, key: str) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    value = cast(dict[str, Any], data).get(key)
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    return {}


def _string_field(data: object, key: str) -> str:
    if not isinstance(data, dict):
        return ""
    value = cast(dict[str, Any], data).get(key)
    if isinstance(value, str):
        return value
    if isinstance(value, int | float):
        return str(value)
    return ""


def _bool_field(data: object, key: str) -> bool | None:
    if not isinstance(data, dict):
        return None
    value = cast(dict[str, Any], data).get(key)
    if isinstance(value, bool):
        return value
    return None
