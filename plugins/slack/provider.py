from __future__ import annotations

from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

from internals import find_user_mentions, get_message, get_thread_participants, parse_message_url

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse


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
        parsed = parse_message_url(input.url)
        if parsed is None:
            return _bad_request(f"invalid Slack message URL: {input.url}")
        channel, ts = parsed

    if not channel or not ts:
        return _bad_request("either url or both channel and ts are required")

    try:
        return get_message(req.token, channel, ts)
    except RuntimeError as err:
        return _server_error(str(err))


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
        return _bad_request("channel is required")

    try:
        return find_user_mentions(
            req.token, input.channel, input.user_id, input.limit, input.oldest, input.latest, input.include_bots
        )
    except RuntimeError as err:
        return _server_error(str(err))


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
        return _bad_request("channel is required")
    if not input.ts:
        return _bad_request("ts is required")

    try:
        return get_thread_participants(
            req.token, input.channel, input.ts, input.include_user_info, input.include_bots
        )
    except RuntimeError as err:
        return _server_error(str(err))


def _validate_token(req: gestalt.Request) -> ErrorResponse | None:
    if not req.token:
        return gestalt.Response(status=HTTPStatus.UNAUTHORIZED, body={"error": "token is required"})
    return None


def _bad_request(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _server_error(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message})
