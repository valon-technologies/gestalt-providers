from __future__ import annotations

from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

import internals.agent as _agent
from internals.agent import (
    SLACK_ADD_REACTION_OPERATION,
    SLACK_CONTEXT_OPERATION,
    SLACK_DELETE_STATUS_OPERATION,
    SLACK_EVENT_OPERATION,
    SLACK_FILE_GET_OPERATION,
    SLACK_REMOVE_REACTION_OPERATION,
    SLACK_REPLY_OPERATION,
    SLACK_STATUS_OPERATION,
    add_slack_event_reaction,
    configure_agent,
    delete_slack_event_status,
    handle_slack_event,
    post_connect_metadata,
    reply_to_slack_event,
    resolve_slack_http_subject,
    remove_slack_event_reaction,
    set_slack_event_status,
)
from internals.client import SlackAPIError, SlackClientError, is_slack_file_download_url
from internals.operations import (
    find_user_mentions,
    get_file,
    get_message,
    get_thread_context,
    get_thread_participants,
    parse_message_url,
)

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse
PostConnectMetadata: TypeAlias = dict[str, str]

plugin = gestalt.Plugin("slack")

SLACK_AUTH_TEST_URL = _agent.SLACK_AUTH_TEST_URL
SLACK_DEFAULT_CONNECTION = _agent.SLACK_DEFAULT_CONNECTION
SlackAgentEvent = _agent.SlackAgentEvent
SlackAgentRoute = _agent.SlackAgentRoute
SlackAgentRouteMatch = _agent.SlackAgentRouteMatch
SlackReplyRef = _agent.SlackReplyRef
_agent_session_ref = _agent._agent_session_ref
_agent_tool_source_native_search = _agent._agent_tool_source_native_search
_select_agent_route = _agent._select_agent_route
_sign_reply_ref = _agent._sign_reply_ref
_slack_agent_event_from_payload = _agent._slack_agent_event_from_payload
_verify_reply_ref = _agent._verify_reply_ref
external_identity_resource_id = _agent.external_identity_resource_id
slack_external_identity_id = _agent.slack_external_identity_id


@plugin.configure
def configure(name: str, config: dict[str, Any]) -> None:
    configure_agent(name, config)


class GetMessageInput(gestalt.Model):
    url: str = gestalt.field(
        description="Slack message URL", default="", required=False
    )
    channel: str = gestalt.field(description="Channel ID", default="", required=False)
    ts: str = gestalt.field(description="Message timestamp", default="", required=False)


class FindUserMentionsInput(gestalt.Model):
    channel: str = gestalt.field(description="Channel ID to scan")
    user_id: str = gestalt.field(
        description="Optional user ID to filter mentions to", default="", required=False
    )
    limit: int = gestalt.field(
        description="Number of messages to scan", default=100, required=False
    )
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


class GetThreadContextInput(gestalt.Model):
    channel: str = gestalt.field(description="Channel ID containing the thread")
    ts: str = gestalt.field(description="Parent or root message timestamp")
    cursor: str = gestalt.field(
        description="Slack pagination cursor from a previous response",
        default="",
        required=False,
    )
    limit: int = gestalt.field(
        description="Maximum number of messages to return", default=15, required=False
    )
    include_user_info: bool = gestalt.field(
        description="Fetch user profile details for participants",
        default=False,
        required=False,
    )
    include_bots: bool = gestalt.field(
        description="Include bot messages and bot participants",
        default=True,
        required=False,
    )
    include_files: bool = gestalt.field(
        description="Include Slack file metadata attached to messages",
        default=True,
        required=False,
    )
    include_file_content: bool = gestalt.field(
        description="Download bounded text file contents",
        default=False,
        required=False,
    )
    include_image_data: bool = gestalt.field(
        description="Return image bytes as base64 when file content is requested",
        default=False,
        required=False,
    )
    max_file_bytes: int = gestalt.field(
        description="Maximum bytes to read from each Slack file",
        default=200_000,
        required=False,
    )


class GetFileInput(gestalt.Model):
    file_id: str = gestalt.field(
        description="Slack file ID to inspect", default="", required=False
    )
    url_private: str = gestalt.field(
        description="Slack private file URL to download", default="", required=False
    )
    include_content: bool = gestalt.field(
        description="Download bounded file content", default=True, required=False
    )
    max_bytes: int = gestalt.field(
        description="Maximum bytes to read from the Slack file",
        default=200_000,
        required=False,
    )


class SlackEventReplyInput(gestalt.Model):
    reply_ref: str = gestalt.field(description="Opaque Slack event reply reference")
    text: str = gestalt.field(description="Slack message text to send")


class SlackEventStatusInput(gestalt.Model):
    reply_ref: str = gestalt.field(description="Opaque Slack event reply reference")
    text: str = gestalt.field(description="Slack status message text")
    status_ts: str = gestalt.field(
        description="Existing status message timestamp to update",
        default="",
        required=False,
    )
    unfurl_links: bool = gestalt.field(
        description="Whether Slack should unfurl links for newly created statuses",
        default=False,
        required=False,
    )
    unfurl_media: bool = gestalt.field(
        description="Whether Slack should unfurl media for newly created statuses",
        default=False,
        required=False,
    )


class SlackEventDeleteStatusInput(gestalt.Model):
    reply_ref: str = gestalt.field(description="Opaque Slack event reply reference")
    status_ts: str = gestalt.field(description="Status message timestamp to delete")


class SlackEventReactionInput(gestalt.Model):
    reply_ref: str = gestalt.field(description="Opaque Slack event reply reference")
    name: str = gestalt.field(description="Emoji reaction name without colons")
    target_ts: str = gestalt.field(
        description="Message timestamp to react to; defaults to the source Slack event",
        default="",
        required=False,
    )


@gestalt.post_connect
def post_connect(token: gestalt.ConnectedToken) -> PostConnectMetadata:
    return post_connect_metadata(token)


@plugin.http_subject
def resolve_http_subject(
    request: gestalt.HTTPSubjectRequest, context: gestalt.Request
) -> gestalt.Subject | None:
    return resolve_slack_http_subject(request, context)


@gestalt.operation(
    id=SLACK_EVENT_OPERATION,
    method="POST",
    description="Handle Slack Events API callbacks and delegate supported user events to a Gestalt agent",
    visible=False,
)
def slack_events_handle(input: dict[str, Any], req: gestalt.Request) -> OperationResult:
    return handle_slack_event(input, req)


@gestalt.operation(
    id=SLACK_REPLY_OPERATION,
    method="POST",
    description="Reply to the Slack event that started an agent turn",
    visible=False,
)
def slack_events_reply(
    input: SlackEventReplyInput, req: gestalt.Request
) -> OperationResult:
    return reply_to_slack_event(input.reply_ref, input.text, req)


@gestalt.operation(
    id=SLACK_STATUS_OPERATION,
    method="POST",
    description="Create or update a Slack status message in the event thread",
    visible=False,
)
def slack_events_set_status(
    input: SlackEventStatusInput, req: gestalt.Request
) -> OperationResult:
    return set_slack_event_status(
        input.reply_ref,
        input.text,
        input.status_ts,
        input.unfurl_links,
        input.unfurl_media,
        req,
    )


@gestalt.operation(
    id=SLACK_DELETE_STATUS_OPERATION,
    method="POST",
    description="Delete a Slack status message created for the event thread",
    visible=False,
)
def slack_events_delete_status(
    input: SlackEventDeleteStatusInput, req: gestalt.Request
) -> OperationResult:
    return delete_slack_event_status(input.reply_ref, input.status_ts, req)


@gestalt.operation(
    id=SLACK_ADD_REACTION_OPERATION,
    method="POST",
    description="Add an emoji reaction to the Slack event message",
    visible=False,
)
def slack_events_add_reaction(
    input: SlackEventReactionInput, req: gestalt.Request
) -> OperationResult:
    return add_slack_event_reaction(input.reply_ref, input.name, input.target_ts, req)


@gestalt.operation(
    id=SLACK_REMOVE_REACTION_OPERATION,
    method="POST",
    description="Remove an emoji reaction from the Slack event message",
    visible=False,
)
def slack_events_remove_reaction(
    input: SlackEventReactionInput, req: gestalt.Request
) -> OperationResult:
    return remove_slack_event_reaction(input.reply_ref, input.name, input.target_ts, req)


@gestalt.operation(
    id="conversations.getMessage",
    method="POST",
    description="Fetch a single message by Slack URL or channel and timestamp",
)
def conversations_get_message(
    input: GetMessageInput, req: gestalt.Request
) -> OperationResult:
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
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="conversations.findUserMentions",
    method="POST",
    description="Find Slack user mentions in channel messages",
)
def conversations_find_user_mentions(
    input: FindUserMentionsInput, req: gestalt.Request
) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.channel:
        return _bad_request("channel is required")

    try:
        return find_user_mentions(
            req.token,
            input.channel,
            input.user_id,
            input.limit,
            input.oldest,
            input.latest,
            input.include_bots,
        )
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="conversations.getThreadParticipants",
    method="POST",
    description="Get unique participants in a Slack thread",
)
def conversations_get_thread_participants(
    input: GetThreadParticipantsInput, req: gestalt.Request
) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.channel:
        return _bad_request("channel is required")
    if not input.ts:
        return _bad_request("ts is required")

    try:
        return get_thread_participants(
            req.token,
            input.channel,
            input.ts,
            input.include_user_info,
            input.include_bots,
        )
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _server_error(str(err))


@gestalt.operation(
    id=SLACK_CONTEXT_OPERATION,
    method="POST",
    description="Build rich Slack thread context with messages, participants, and files",
)
def conversations_get_thread_context(
    input: GetThreadContextInput, req: gestalt.Request
) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.channel:
        return _bad_request("channel is required")
    if not input.ts:
        return _bad_request("ts is required")

    try:
        return get_thread_context(
            req.token,
            channel=input.channel,
            ts=input.ts,
            cursor=input.cursor,
            limit=input.limit,
            include_user_info=input.include_user_info,
            include_bots=input.include_bots,
            include_files=input.include_files,
            include_file_content=input.include_file_content,
            include_image_data=input.include_image_data,
            max_file_bytes=input.max_file_bytes,
        )
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _server_error(str(err))


@gestalt.operation(
    id=SLACK_FILE_GET_OPERATION,
    method="POST",
    description="Fetch Slack file metadata and bounded file or image content",
)
def files_get(input: GetFileInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.file_id and not input.url_private:
        return _bad_request("file_id or url_private is required")
    if input.url_private and not is_slack_file_download_url(input.url_private):
        return _bad_request("url_private must be a Slack HTTPS file URL")

    try:
        return get_file(
            req.token,
            file_id=input.file_id,
            url_private=input.url_private,
            include_content=input.include_content,
            max_bytes=input.max_bytes,
        )
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _server_error(str(err))


def _validate_token(req: gestalt.Request) -> ErrorResponse | None:
    if not req.token:
        return gestalt.Response(
            status=HTTPStatus.UNAUTHORIZED, body={"error": "token is required"}
        )
    return None


def _bad_request(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _server_error(message: str) -> ErrorResponse:
    return gestalt.Response(
        status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message}
    )
