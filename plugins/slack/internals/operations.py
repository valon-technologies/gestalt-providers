import re
from typing import Any

from .client import slack_get
from .helpers import bool_field, map_field, map_slice, string_field

SLACK_MESSAGE_URL_PATTERN = re.compile(r"https?://[^/]+\.slack\.com/archives/([A-Z0-9]+)/p(\d{10})(\d+)")
USER_MENTION_PATTERN = re.compile(r"<@(U[A-Z0-9]+)>")


def parse_message_url(url: str) -> tuple[str, str] | None:
    match = SLACK_MESSAGE_URL_PATTERN.search(url)
    if match is None:
        return None
    channel = match.group(1)
    ts = f"{match.group(2)}.{match.group(3)}"
    return channel, ts


def get_message(token: str, channel: str, ts: str) -> dict[str, Any]:
    data = slack_get(
        "conversations.history",
        {
            "channel": channel,
            "oldest": ts,
            "latest": ts,
            "inclusive": "true",
            "limit": "1",
        },
        token,
    )
    messages = map_slice(data.get("messages"))
    if not messages:
        raise RuntimeError(f"no message found at timestamp {ts}")
    return {"data": {"message": messages[0]}}


def find_user_mentions(
    token: str,
    channel: str,
    user_id: str,
    limit: int,
    oldest: str,
    latest: str,
    include_bots: bool,
) -> dict[str, Any]:
    query: dict[str, str] = {
        "channel": channel,
        "limit": str(limit),
    }
    if oldest:
        query["oldest"] = oldest
    if latest:
        query["latest"] = latest

    data = slack_get("conversations.history", query, token)
    messages = map_slice(data.get("messages"))
    mentions: list[dict[str, str]] = []
    mentioned_user_ids: set[str] = set()

    for message in messages:
        if not include_bots and string_field(message, "bot_id"):
            continue

        text = string_field(message, "text")
        for found_user_id in USER_MENTION_PATTERN.findall(text):
            if user_id and found_user_id != user_id:
                continue
            mentioned_user_ids.add(found_user_id)
            mentions.append(
                {
                    "user_id": found_user_id,
                    "message_ts": string_field(message, "ts"),
                    "mentioned_by": string_field(message, "user"),
                    "text": text,
                    "channel": channel,
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


def get_thread_participants(
    token: str,
    channel: str,
    ts: str,
    include_user_info: bool,
    include_bots: bool,
) -> dict[str, Any]:
    data = slack_get(
        "conversations.replies",
        {
            "channel": channel,
            "ts": ts,
            "limit": "1000",
        },
        token,
    )
    messages = map_slice(data.get("messages"))
    thread_starter = string_field(messages[0], "user") if messages else ""

    participants_by_user: dict[str, dict[str, Any]] = {}
    for message in messages:
        uid = string_field(message, "user")
        if not uid:
            continue
        if not include_bots and string_field(message, "bot_id"):
            continue

        participant = participants_by_user.get(uid)
        if participant is None:
            participant = {
                "user_id": uid,
                "message_count": 0,
                "first_reply_ts": string_field(message, "ts"),
                "is_thread_starter": uid == thread_starter,
            }
            participants_by_user[uid] = participant
        participant["message_count"] = int(participant["message_count"]) + 1

    participants = list(participants_by_user.values())
    participants.sort(key=lambda participant: (str(participant["first_reply_ts"]), str(participant["user_id"])))

    if include_user_info:
        for participant in participants:
            try:
                user_data = slack_get("users.info", {"user": str(participant["user_id"])}, token)
            except RuntimeError:
                continue
            user = map_field(user_data, "user")
            profile = map_field(user, "profile")
            participant["display_name"] = string_field(profile, "display_name")
            participant["real_name"] = string_field(user, "real_name")
            is_bot = bool_field(user, "is_bot")
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
