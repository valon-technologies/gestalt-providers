import base64
import binascii
import json
import re
from http import HTTPStatus
from typing import Any, cast

from .client import (
    SlackAPIError,
    SlackClientError,
    get_bytes,
    slack_get,
    slack_post,
    slack_post_form,
    upload_bytes_to_slack_url,
)
from .helpers import bool_field, map_field, map_slice, string_field

SLACK_MESSAGE_URL_PATTERN = re.compile(
    r"https?://[^/]+\.slack\.com/archives/([A-Z0-9]+)/p(\d{10})(\d+)"
)
USER_MENTION_PATTERN = re.compile(r"<@(U[A-Z0-9]+)>")
DEFAULT_FILE_MAX_BYTES = 200_000
HARD_FILE_MAX_BYTES = 5 * 1024 * 1024
DEFAULT_UPLOAD_CONTENT_TYPE = "application/octet-stream"
MAX_UPLOAD_BYTES = 20 * 1024 * 1024
SLACK_MAX_SECTION_TEXT_CHARS = 3000


def parse_message_url(url: str) -> tuple[str, str] | None:
    match = SLACK_MESSAGE_URL_PATTERN.search(url)
    if match is None:
        return None
    channel = match.group(1)
    ts = f"{match.group(2)}.{match.group(3)}"
    return channel, ts


def find_message_urls(text: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for match in SLACK_MESSAGE_URL_PATTERN.finditer(text):
        url = match.group(0)
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


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
        raise SlackAPIError(
            HTTPStatus.NOT_FOUND, {"error": f"no message found at timestamp {ts}"}
        )
    return {"data": {"message": messages[0]}}


def post_message(
    token: str,
    *,
    channel: str,
    text: str,
    thread_ts: str = "",
    unfurl_links: bool | None = None,
    unfurl_media: bool | None = None,
    blocks: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
    client_msg_id: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {"channel": channel, "text": text}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    if unfurl_links is not None:
        payload["unfurl_links"] = unfurl_links
    if unfurl_media is not None:
        payload["unfurl_media"] = unfurl_media
    if blocks:
        payload["blocks"] = blocks
    if metadata:
        payload["metadata"] = metadata
    if client_msg_id:
        payload["client_msg_id"] = client_msg_id
    return slack_post("chat.postMessage", payload, token)


def update_message(token: str, *, channel: str, ts: str, text: str) -> dict[str, Any]:
    return slack_post(
        "chat.update", {"channel": channel, "ts": ts, "text": text}, token
    )


def delete_message(token: str, *, channel: str, ts: str) -> dict[str, Any]:
    return slack_post("chat.delete", {"channel": channel, "ts": ts}, token)


def add_reaction(
    token: str, *, channel: str, timestamp: str, name: str
) -> dict[str, Any]:
    return slack_post(
        "reactions.add",
        {"channel": channel, "timestamp": timestamp, "name": name},
        token,
    )


def remove_reaction(
    token: str, *, channel: str, timestamp: str, name: str
) -> dict[str, Any]:
    return slack_post(
        "reactions.remove",
        {"channel": channel, "timestamp": timestamp, "name": name},
        token,
    )


def set_assistant_thread_status(
    token: str,
    *,
    channel_id: str,
    thread_ts: str,
    status: str,
    loading_messages: list[str] | None = None,
    icon_emoji: str = "",
    icon_url: str = "",
    username: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "channel_id": channel_id,
        "thread_ts": thread_ts,
        "status": status,
    }
    if loading_messages:
        payload["loading_messages"] = loading_messages[:10]
    if icon_emoji:
        payload["icon_emoji"] = icon_emoji
    if icon_url:
        payload["icon_url"] = icon_url
    if username:
        payload["username"] = username
    return slack_post("assistant.threads.setStatus", payload, token)


def set_assistant_thread_title(
    token: str, *, channel_id: str, thread_ts: str, title: str
) -> dict[str, Any]:
    return slack_post(
        "assistant.threads.setTitle",
        {"channel_id": channel_id, "thread_ts": thread_ts, "title": title},
        token,
    )


def set_assistant_thread_suggested_prompts(
    token: str,
    *,
    channel_id: str,
    thread_ts: str,
    prompts: list[dict[str, str]],
    title: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "channel_id": channel_id,
        "thread_ts": thread_ts,
        "prompts": prompts[:4],
    }
    if title:
        payload["title"] = title
    return slack_post("assistant.threads.setSuggestedPrompts", payload, token)


def start_stream(
    token: str,
    *,
    channel: str,
    thread_ts: str,
    markdown_text: str = "",
    chunks: list[dict[str, Any]] | None = None,
    recipient_user_id: str = "",
    recipient_team_id: str = "",
    task_display_mode: str = "",
    icon_emoji: str = "",
    icon_url: str = "",
    username: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {"channel": channel, "thread_ts": thread_ts}
    if markdown_text:
        payload["markdown_text"] = markdown_text
    if chunks:
        payload["chunks"] = chunks
    if recipient_user_id:
        payload["recipient_user_id"] = recipient_user_id
    if recipient_team_id:
        payload["recipient_team_id"] = recipient_team_id
    if task_display_mode:
        payload["task_display_mode"] = task_display_mode
    if icon_emoji:
        payload["icon_emoji"] = icon_emoji
    if icon_url:
        payload["icon_url"] = icon_url
    if username:
        payload["username"] = username
    return slack_post("chat.startStream", payload, token)


def append_stream(
    token: str,
    *,
    channel: str,
    ts: str,
    markdown_text: str,
    chunks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"channel": channel, "ts": ts}
    if markdown_text:
        payload["markdown_text"] = markdown_text
    if chunks:
        payload["chunks"] = chunks
    return slack_post("chat.appendStream", payload, token)


def stop_stream(
    token: str,
    *,
    channel: str,
    ts: str,
    markdown_text: str = "",
    chunks: list[dict[str, Any]] | None = None,
    blocks: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"channel": channel, "ts": ts}
    if markdown_text:
        payload["markdown_text"] = markdown_text
    if chunks:
        payload["chunks"] = chunks
    if blocks:
        payload["blocks"] = blocks
    if metadata:
        payload["metadata"] = metadata
    return slack_post("chat.stopStream", payload, token)


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


def get_thread_context(
    token: str,
    *,
    channel: str,
    ts: str,
    cursor: str,
    limit: int,
    include_user_info: bool,
    include_bots: bool,
    include_files: bool,
    include_file_content: bool,
    include_image_data: bool,
    max_file_bytes: int,
) -> dict[str, Any]:
    limit = max(1, min(limit, 1000))
    query = {"channel": channel, "ts": ts, "limit": str(limit)}
    if cursor:
        query["cursor"] = cursor
    data = slack_get("conversations.replies", query, token)
    raw_messages = map_slice(data.get("messages"))
    messages = [
        _context_message(
            token,
            channel=channel,
            message=message,
            include_files=include_files,
            include_file_content=include_file_content,
            include_image_data=include_image_data,
            max_file_bytes=max_file_bytes,
        )
        for message in raw_messages
        if include_bots or not string_field(message, "bot_id")
    ]
    participants = _thread_participants_from_messages(
        token,
        messages=messages,
        include_user_info=include_user_info,
        include_bots=include_bots,
    )
    files = [
        file_data
        for message in messages
        for file_data in map_slice(message.get("files"))
    ]
    response_metadata = map_field(data, "response_metadata")
    return {
        "data": {
            "channel": channel,
            "thread_ts": ts,
            "event_ref": {
                "channel": channel,
                "message_ts": ts,
                "thread_ts": ts,
                "reply_thread_ts": ts,
            },
            "root_message": messages[0] if messages else {},
            "messages": messages,
            "messages_returned": len(messages),
            "has_more": bool(data.get("has_more") is True),
            "next_cursor": string_field(response_metadata, "next_cursor"),
            "participants": participants,
            "participant_count": len(participants),
            "files": files,
            "file_count": len(files),
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
    participants.sort(
        key=lambda participant: (
            str(participant["first_reply_ts"]),
            str(participant["user_id"]),
        )
    )

    if include_user_info:
        for participant in participants:
            try:
                user_data = slack_get(
                    "users.info", {"user": str(participant["user_id"])}, token
                )
            except SlackAPIError, SlackClientError:
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


def get_file(
    token: str,
    *,
    file_id: str,
    url_private: str,
    include_content: bool,
    max_bytes: int,
) -> dict[str, Any]:
    file_data: dict[str, Any] = {}
    if file_id:
        response = slack_get("files.info", {"file": file_id}, token)
        file_data = map_field(response, "file")
    if not url_private:
        url_private = string_field(file_data, "url_private_download") or string_field(
            file_data, "url_private"
        )
    normalized = _normalize_file(file_data, channel="", message_ts="")
    if url_private and not string_field(normalized, "url_private"):
        normalized["url_private"] = url_private

    result: dict[str, Any] = {"file": normalized}
    if include_content:
        if not url_private:
            raise SlackAPIError(
                HTTPStatus.BAD_REQUEST,
                {"error": "file does not include a private download URL"},
            )
        result["content"] = _download_file_content(
            token,
            url_private=url_private,
            mimetype=string_field(normalized, "mimetype"),
            max_bytes=max_bytes,
            include_image_data=True,
        )
    return {"data": result}


def upload_file(
    token: str,
    *,
    channel: str,
    filename: str,
    content: str = "",
    content_base64: str = "",
    thread_ts: str = "",
    title: str = "",
    initial_comment: str = "",
    content_type: str = "",
    alt_txt: str = "",
    snippet_type: str = "",
    blocks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    channel = channel.strip()
    filename = filename.strip()
    thread_ts = thread_ts.strip()
    title = title.strip()
    initial_comment = initial_comment.strip()
    content_type = content_type.strip() or DEFAULT_UPLOAD_CONTENT_TYPE
    alt_txt = alt_txt.strip()
    snippet_type = snippet_type.strip()
    body = _upload_payload(content=content, content_base64=content_base64)
    normalized_blocks = _upload_blocks(blocks)

    if not channel:
        raise ValueError("channel is required")
    if not filename:
        raise ValueError("filename is required")

    upload_request: dict[str, str] = {
        "filename": filename,
        "length": str(len(body)),
    }
    if alt_txt:
        upload_request["alt_txt"] = alt_txt
    if snippet_type:
        upload_request["snippet_type"] = snippet_type

    upload_response = slack_post_form(
        "files.getUploadURLExternal", upload_request, token
    )
    upload_url = string_field(upload_response, "upload_url")
    file_id = string_field(upload_response, "file_id")
    if not upload_url:
        raise SlackAPIError(
            HTTPStatus.BAD_GATEWAY,
            {"error": "Slack upload URL response missing upload_url"},
        )
    if not file_id:
        raise SlackAPIError(
            HTTPStatus.BAD_GATEWAY,
            {"error": "Slack upload URL response missing file_id"},
        )

    upload_bytes_to_slack_url(upload_url, body, content_type)

    complete_file: dict[str, str] = {"id": file_id}
    if title:
        complete_file["title"] = title
    complete_request: dict[str, str] = {
        "files": json.dumps([complete_file], separators=(",", ":")),
        "channel_id": channel,
    }
    if thread_ts:
        complete_request["thread_ts"] = thread_ts
    initial_comment, normalized_blocks = _upload_complete_message_fields(
        initial_comment, normalized_blocks
    )
    if initial_comment:
        complete_request["initial_comment"] = initial_comment
    if normalized_blocks:
        complete_request["blocks"] = json.dumps(
            normalized_blocks, separators=(",", ":")
        )

    complete_response = slack_post_form(
        "files.completeUploadExternal", complete_request, token
    )
    result = dict(complete_response)
    result.setdefault("file_id", file_id)
    result.setdefault("channel", channel)
    if thread_ts:
        result.setdefault("thread_ts", thread_ts)
    return result


def _context_message(
    token: str,
    *,
    channel: str,
    message: dict[str, Any],
    include_files: bool,
    include_file_content: bool,
    include_image_data: bool,
    max_file_bytes: int,
) -> dict[str, Any]:
    ts = string_field(message, "ts")
    text = string_field(message, "text")
    normalized: dict[str, Any] = {
        "type": string_field(message, "type"),
        "subtype": string_field(message, "subtype"),
        "ts": ts,
        "thread_ts": string_field(message, "thread_ts"),
        "user": string_field(message, "user"),
        "username": string_field(message, "username"),
        "bot_id": string_field(message, "bot_id"),
        "app_id": string_field(message, "app_id"),
        "text": text,
        "mentions": USER_MENTION_PATTERN.findall(text),
        "reply_count": _int_field(message, "reply_count"),
    }
    if include_files:
        normalized["files"] = [
            _context_file(
                token,
                channel=channel,
                message_ts=ts,
                file_data=file_data,
                include_content=include_file_content,
                include_image_data=include_image_data,
                max_file_bytes=max_file_bytes,
            )
            for file_data in map_slice(message.get("files"))
        ]
    return normalized


def _context_file(
    token: str,
    *,
    channel: str,
    message_ts: str,
    file_data: dict[str, Any],
    include_content: bool,
    include_image_data: bool,
    max_file_bytes: int,
) -> dict[str, Any]:
    normalized = _normalize_file(file_data, channel=channel, message_ts=message_ts)
    if include_content:
        url_private = string_field(file_data, "url_private_download") or string_field(
            file_data, "url_private"
        )
        if url_private:
            normalized["content"] = _download_file_content(
                token,
                url_private=url_private,
                mimetype=string_field(file_data, "mimetype"),
                max_bytes=max_file_bytes,
                include_image_data=include_image_data,
            )
    return normalized


def _upload_payload(*, content: str, content_base64: str) -> bytes:
    has_content = bool(content)
    normalized_base64 = content_base64.strip()
    has_content_base64 = bool(normalized_base64)
    if has_content and has_content_base64:
        raise ValueError("content and content_base64 are mutually exclusive")
    if not has_content and not has_content_base64:
        raise ValueError("content or content_base64 is required")
    if has_content_base64:
        if _base64_decoded_length(normalized_base64) > MAX_UPLOAD_BYTES:
            raise ValueError(f"file content exceeds {MAX_UPLOAD_BYTES} bytes")
        try:
            body = base64.b64decode(normalized_base64, validate=True)
        except binascii.Error as err:
            raise ValueError("content_base64 must be valid base64") from err
    else:
        body = content.encode("utf-8")
    if len(body) > MAX_UPLOAD_BYTES:
        raise ValueError(f"file content exceeds {MAX_UPLOAD_BYTES} bytes")
    if not body:
        raise ValueError("file content must not be empty")
    return body


def _base64_decoded_length(content_base64: str) -> int:
    padding = len(content_base64) - len(content_base64.rstrip("="))
    return (len(content_base64) // 4) * 3 - min(padding, 2)


def _upload_blocks(blocks: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if blocks is None:
        return []
    if not isinstance(blocks, list) or not all(
        isinstance(block, dict) for block in blocks
    ):
        raise ValueError("blocks must be an array of Slack block objects")
    return blocks


def _upload_complete_message_fields(
    initial_comment: str, blocks: list[dict[str, Any]]
) -> tuple[str, list[dict[str, Any]]]:
    if not initial_comment or not blocks:
        return initial_comment, blocks
    comment_blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": chunk}}
        for index in range(0, len(initial_comment), SLACK_MAX_SECTION_TEXT_CHARS)
        if (
            chunk := initial_comment[
                index : index + SLACK_MAX_SECTION_TEXT_CHARS
            ]
        )
    ]
    return "", [*comment_blocks, *blocks]


def _normalize_file(
    file_data: dict[str, Any], *, channel: str, message_ts: str
) -> dict[str, Any]:
    return {
        "id": string_field(file_data, "id"),
        "created": _int_field(file_data, "created"),
        "timestamp": _int_field(file_data, "timestamp"),
        "name": string_field(file_data, "name"),
        "title": string_field(file_data, "title"),
        "mimetype": string_field(file_data, "mimetype"),
        "filetype": string_field(file_data, "filetype"),
        "pretty_type": string_field(file_data, "pretty_type"),
        "user": string_field(file_data, "user"),
        "size": _int_field(file_data, "size"),
        "url_private": string_field(file_data, "url_private"),
        "url_private_download": string_field(file_data, "url_private_download"),
        "channel": channel,
        "message_ts": message_ts,
    }


def _download_file_content(
    token: str,
    *,
    url_private: str,
    mimetype: str,
    max_bytes: int,
    include_image_data: bool,
) -> dict[str, Any]:
    max_bytes = _bounded_file_bytes(max_bytes)
    body, truncated = get_bytes(url_private, token, max_bytes)
    base: dict[str, Any] = {
        "mime_type": mimetype,
        "bytes_read": len(body),
        "truncated": truncated,
    }
    if _is_text_mimetype(mimetype):
        base["encoding"] = "utf-8"
        base["text"] = body.decode("utf-8", errors="replace")
        return base
    if mimetype.startswith("image/") and not include_image_data:
        base["encoding"] = "omitted"
        base["omitted_reason"] = "image data was not requested"
        return base
    data = base64.b64encode(body).decode("ascii")
    base["encoding"] = "base64"
    base["data"] = data
    if mimetype.startswith("image/"):
        base["kind"] = "image"
        base["data_uri"] = f"data:{mimetype};base64,{data}"
    return base


def _thread_participants_from_messages(
    token: str,
    *,
    messages: list[dict[str, Any]],
    include_user_info: bool,
    include_bots: bool,
) -> list[dict[str, Any]]:
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
    participants.sort(
        key=lambda participant: (
            str(participant["first_reply_ts"]),
            str(participant["user_id"]),
        )
    )
    if include_user_info:
        for participant in participants:
            try:
                user_data = slack_get(
                    "users.info", {"user": str(participant["user_id"])}, token
                )
            except SlackAPIError, SlackClientError:
                continue
            user = map_field(user_data, "user")
            profile = map_field(user, "profile")
            participant["display_name"] = string_field(profile, "display_name")
            participant["real_name"] = string_field(user, "real_name")
            is_bot = bool_field(user, "is_bot")
            if is_bot is not None:
                participant["is_bot"] = is_bot
    return participants


def _is_text_mimetype(mimetype: str) -> bool:
    if mimetype.startswith("text/"):
        return True
    return mimetype in {
        "application/json",
        "application/xml",
        "application/x-yaml",
        "application/yaml",
        "application/csv",
        "application/javascript",
        "application/x-ndjson",
    }


def _bounded_file_bytes(max_bytes: int) -> int:
    if max_bytes <= 0:
        return DEFAULT_FILE_MAX_BYTES
    return min(max_bytes, HARD_FILE_MAX_BYTES)


def _int_field(data: object, key: str) -> int:
    if not isinstance(data, dict):
        return 0
    value = cast(dict[str, Any], data).get(key)
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(float(value))
        except ValueError:
            return 0
    return 0
