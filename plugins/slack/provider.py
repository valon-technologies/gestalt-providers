from __future__ import annotations

from collections.abc import Mapping
from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

import internals.agent as _agent
from internals.agent import (
    SLACK_ADD_REACTION_OPERATION,
    SLACK_ASSISTANT_CLEAR_STATUS_OPERATION,
    SLACK_ASSISTANT_PROMPTS_OPERATION,
    SLACK_ASSISTANT_STATUS_OPERATION,
    SLACK_ASSISTANT_TITLE_OPERATION,
    SLACK_CONTEXT_OPERATION,
    SLACK_DELETE_STATUS_OPERATION,
    SLACK_EVENT_OPERATION,
    SLACK_FILE_GET_OPERATION,
    SLACK_INTERACTION_HANDLE_OPERATION,
    SLACK_INTERACTION_REQUEST_OPERATION,
    SLACK_REMOVE_REACTION_OPERATION,
    SLACK_REPLY_OPERATION,
    SLACK_SESSION_STARTED_REPLY_OPERATION,
    SLACK_STATUS_OPERATION,
    SLACK_STREAM_APPEND_OPERATION,
    SLACK_STREAM_START_OPERATION,
    SLACK_STREAM_STOP_OPERATION,
    add_slack_event_reaction,
    append_slack_event_stream,
    clear_slack_event_assistant_status,
    configure_agent,
    delete_slack_event_status,
    handle_slack_event,
    handle_slack_interaction,
    request_slack_interaction,
    reply_slack_event_session_started,
    reply_to_slack_event,
    resolve_slack_http_subject,
    remove_slack_event_reaction,
    set_slack_event_assistant_status,
    set_slack_event_suggested_prompts,
    set_slack_event_status,
    set_slack_event_thread_title,
    start_slack_event_stream,
    stop_slack_event_stream,
)
from internals.client import SlackAPIError, SlackClientError, is_slack_file_download_url
from internals.models import (
    SlackAgentEvent as SlackAgentEvent,
    SlackAgentRoute as SlackAgentRoute,
    SlackAgentRouteMatch as SlackAgentRouteMatch,
    SlackEventPublishConfig as SlackEventPublishConfig,
    SlackEventPublishRoute as SlackEventPublishRoute,
    SlackEventPublishRouteMatch as SlackEventPublishRouteMatch,
    SlackEventsConfig as SlackEventsConfig,
    SlackReplyRef as SlackReplyRef,
)
from internals.operations import (
    find_user_mentions,
    get_file,
    get_message,
    get_thread_context,
    get_thread_participants,
    parse_message_url,
    post_message,
)

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse

plugin = gestalt.Plugin("slack")
SLACK_BOT_SERVICE_ACCOUNT_SUBJECT_ID = "service_account:slack-bot"
SLACK_POST_MESSAGE_FOOTER_APP_NAME = "Gestalt"
SLACK_MAX_BLOCKS = 50
SLACK_MAX_SECTION_TEXT_CHARS = 3000
SLACK_MAX_SYNTHESIZED_TEXT_BLOCKS = SLACK_MAX_BLOCKS - 1

_agent_session_ref = _agent._agent_session_ref
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


class ChatPostMessageInput(gestalt.Model):
    channel: str = gestalt.field(description="Channel ID", default="", required=True)
    text: str = gestalt.field(
        description="Message text and Slack fallback text", default="", required=True
    )
    thread_ts: str = gestalt.field(
        description="Thread timestamp to reply to", default="", required=False
    )
    unfurl_links: bool | None = gestalt.field(
        description="Whether Slack should unfurl links", default=None, required=False
    )
    unfurl_media: bool | None = gestalt.field(
        description="Whether Slack should unfurl media", default=None, required=False
    )
    blocks: list[dict[str, Any]] | None = gestalt.field(
        description="Optional Slack Block Kit blocks. Gestalt appends an attribution context footer.",
        default_factory=list,
        required=False,
    )
    metadata: dict[str, Any] | None = gestalt.field(
        description="Optional Slack message metadata. Omit for default Gestalt metadata; pass {} for no metadata.",
        default=None,
        required=False,
    )


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
    url: str = gestalt.field(
        description="Slack message URL", default="", required=False
    )
    channel: str = gestalt.field(
        description="Channel ID containing the thread", default="", required=False
    )
    ts: str = gestalt.field(
        description="Parent or root message timestamp", default="", required=False
    )
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
    reply_ref: str = gestalt.field(
        description="Opaque Slack event reply reference from the current Slack signal"
    )
    text: str = gestalt.field(
        description="Required complete Slack message body to post in the event thread"
    )


class SlackEventSessionStartedInput(gestalt.Model):
    reply_ref: str = gestalt.field(
        description="Opaque Slack event reply reference from the current Slack signal"
    )
    session_id: str = gestalt.field(
        description="Gestalt agent session ID to include in the session link"
    )


class SlackEventReplyRefInput(gestalt.Model):
    reply_ref: str = gestalt.field(description="Opaque Slack event reply reference")


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


class SlackEventAssistantStatusInput(gestalt.Model):
    reply_ref: str = gestalt.field(description="Opaque Slack event reply reference")
    status: str = gestalt.field(
        description="Native Slack assistant status text; pass an empty string to clear",
        default="",
        required=False,
    )
    loading_messages: list[str] = gestalt.field(
        description="Optional rotating native loading messages; Slack accepts up to 10",
        default_factory=list,
        required=False,
    )
    icon_emoji: str = gestalt.field(
        description="Optional emoji to display for the assistant status",
        default="",
        required=False,
    )
    icon_url: str = gestalt.field(
        description="Optional icon URL to display for the assistant status",
        default="",
        required=False,
    )
    username: str = gestalt.field(
        description="Optional bot username to display for the assistant status",
        default="",
        required=False,
    )


class SlackEventThreadTitleInput(gestalt.Model):
    reply_ref: str = gestalt.field(description="Opaque Slack event reply reference")
    title: str = gestalt.field(description="Native Slack assistant thread title")


class SlackEventSuggestedPromptsInput(gestalt.Model):
    reply_ref: str = gestalt.field(description="Opaque Slack event reply reference")
    prompts: list[dict[str, Any]] = gestalt.field(
        description="Up to four Slack suggested prompts, each with title and message",
    )
    title: str = gestalt.field(
        description="Optional title for the suggested prompt list",
        default="",
        required=False,
    )


class SlackEventStreamStartInput(gestalt.Model):
    reply_ref: str = gestalt.field(description="Opaque Slack event reply reference")
    markdown_text: str = gestalt.field(
        description="Initial markdown text for the streamed Slack reply",
        default="",
        required=False,
    )
    chunks: list[dict[str, Any]] = gestalt.field(
        description="Optional Slack stream chunks for text, task updates, plans, or blocks",
        default_factory=list,
        required=False,
    )
    recipient_user_id: str = gestalt.field(
        description="Optional Slack user receiving a streamed channel reply",
        default="",
        required=False,
    )
    recipient_team_id: str = gestalt.field(
        description="Optional Slack team for the streamed reply recipient",
        default="",
        required=False,
    )
    task_display_mode: str = gestalt.field(
        description="Optional Slack task display mode, timeline or plan",
        default="",
        required=False,
    )
    icon_emoji: str = gestalt.field(
        description="Optional emoji to display for the streamed message",
        default="",
        required=False,
    )
    icon_url: str = gestalt.field(
        description="Optional icon URL to display for the streamed message",
        default="",
        required=False,
    )
    username: str = gestalt.field(
        description="Optional bot username to display for the streamed message",
        default="",
        required=False,
    )


class SlackEventStreamAppendInput(gestalt.Model):
    reply_ref: str = gestalt.field(description="Opaque Slack event reply reference")
    stream_ts: str = gestalt.field(description="Slack streaming message timestamp")
    markdown_text: str = gestalt.field(
        description="Markdown text to append to the Slack stream",
        default="",
        required=False,
    )
    chunks: list[dict[str, Any]] = gestalt.field(
        description="Optional Slack stream chunks to append",
        default_factory=list,
        required=False,
    )


class SlackEventStreamStopInput(gestalt.Model):
    reply_ref: str = gestalt.field(description="Opaque Slack event reply reference")
    stream_ts: str = gestalt.field(description="Slack streaming message timestamp")
    markdown_text: str = gestalt.field(
        description="Optional final markdown text for the Slack stream",
        default="",
        required=False,
    )
    chunks: list[dict[str, Any]] = gestalt.field(
        description="Optional final Slack stream chunks",
        default_factory=list,
        required=False,
    )
    blocks: list[dict[str, Any]] = gestalt.field(
        description="Optional Slack blocks rendered below the finalized stream",
        default_factory=list,
        required=False,
    )
    metadata: dict[str, Any] = gestalt.field(
        description="Optional Slack message metadata for the finalized stream",
        default_factory=dict,
        required=False,
    )


class SlackInteractionRequestInput(gestalt.Model):
    reply_ref: str = gestalt.field(description="Opaque Slack event reply reference")
    text: str = gestalt.field(description="Slack message text shown above the actions")
    actions: list[dict[str, Any]] = gestalt.field(
        description="Button actions with id/action_id, label/text, optional value, and optional primary/danger style",
    )
    expires_in_seconds: int = gestalt.field(
        description="Seconds before embedded Slack interaction refs expire",
        default=86_400,
        required=False,
    )


@plugin.http_subject
def resolve_http_subject(
    request: gestalt.HTTPSubjectRequest, context: gestalt.Request
) -> gestalt.Subject | None:
    return resolve_slack_http_subject(request, context)


@gestalt.operation(
    id=SLACK_EVENT_OPERATION,
    method="POST",
    description="Handle Slack Events API callbacks for workflow event publishing and supported agent events",
    visible=False,
)
def slack_events_handle(input: dict[str, Any], req: gestalt.Request) -> OperationResult:
    return handle_slack_event(input, req)


@gestalt.operation(
    id=SLACK_INTERACTION_HANDLE_OPERATION,
    method="POST",
    description="Handle Slack interaction callbacks and signal the matching workflow lane",
    visible=False,
)
def slack_interactions_handle(
    input: dict[str, Any], req: gestalt.Request
) -> OperationResult:
    return handle_slack_interaction(input, req)


@gestalt.operation(
    id=SLACK_INTERACTION_REQUEST_OPERATION,
    method="POST",
    description="Post a Slack message with signed button actions that signal the workflow lane",
    visible=False,
)
def slack_interactions_request(
    input: SlackInteractionRequestInput, req: gestalt.Request
) -> OperationResult:
    return request_slack_interaction(
        input.reply_ref,
        input.text,
        input.actions,
        input.expires_in_seconds,
        req,
    )


@gestalt.operation(
    id=SLACK_REPLY_OPERATION,
    method="POST",
    description="Reply to the Slack event that started an agent turn; requires reply_ref and text",
    visible=False,
)
def slack_events_reply(
    input: SlackEventReplyInput, req: gestalt.Request
) -> OperationResult:
    return reply_to_slack_event(input.reply_ref, input.text, req)


@gestalt.operation(
    id=SLACK_SESSION_STARTED_REPLY_OPERATION,
    method="POST",
    description="Reply to the Slack event thread with a link to the started Gestalt agent session",
    visible=False,
)
def slack_events_reply_session_started(
    input: SlackEventSessionStartedInput, req: gestalt.Request
) -> OperationResult:
    return reply_slack_event_session_started(input.reply_ref, input.session_id, req)


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
    return remove_slack_event_reaction(
        input.reply_ref, input.name, input.target_ts, req
    )


@gestalt.operation(
    id=SLACK_ASSISTANT_STATUS_OPERATION,
    method="POST",
    description="Set Slack's native assistant loading/status indicator for the event thread",
    visible=False,
)
def slack_events_set_assistant_status(
    input: SlackEventAssistantStatusInput, req: gestalt.Request
) -> OperationResult:
    return set_slack_event_assistant_status(
        input.reply_ref,
        input.status,
        input.loading_messages,
        input.icon_emoji,
        input.icon_url,
        input.username,
        req,
    )


@gestalt.operation(
    id=SLACK_ASSISTANT_CLEAR_STATUS_OPERATION,
    method="POST",
    description="Clear Slack's native assistant loading/status indicator for the event thread",
    visible=False,
)
def slack_events_clear_assistant_status(
    input: SlackEventReplyRefInput, req: gestalt.Request
) -> OperationResult:
    return clear_slack_event_assistant_status(input.reply_ref, req)


@gestalt.operation(
    id=SLACK_ASSISTANT_TITLE_OPERATION,
    method="POST",
    description="Set Slack's native assistant thread title for the event thread",
    visible=False,
)
def slack_events_set_thread_title(
    input: SlackEventThreadTitleInput, req: gestalt.Request
) -> OperationResult:
    return set_slack_event_thread_title(input.reply_ref, input.title, req)


@gestalt.operation(
    id=SLACK_ASSISTANT_PROMPTS_OPERATION,
    method="POST",
    description="Set Slack's native assistant suggested prompts for the event thread",
    visible=False,
)
def slack_events_set_suggested_prompts(
    input: SlackEventSuggestedPromptsInput, req: gestalt.Request
) -> OperationResult:
    return set_slack_event_suggested_prompts(
        input.reply_ref, input.prompts, input.title, req
    )


@gestalt.operation(
    id=SLACK_STREAM_START_OPERATION,
    method="POST",
    description="Start a native Slack streaming reply in the event thread",
    visible=False,
)
def slack_events_start_stream(
    input: SlackEventStreamStartInput, req: gestalt.Request
) -> OperationResult:
    return start_slack_event_stream(
        input.reply_ref,
        input.markdown_text,
        input.chunks,
        input.recipient_user_id,
        input.recipient_team_id,
        input.task_display_mode,
        input.icon_emoji,
        input.icon_url,
        input.username,
        req,
    )


@gestalt.operation(
    id=SLACK_STREAM_APPEND_OPERATION,
    method="POST",
    description="Append markdown text or chunks to a native Slack streaming reply",
    visible=False,
)
def slack_events_append_stream(
    input: SlackEventStreamAppendInput, req: gestalt.Request
) -> OperationResult:
    return append_slack_event_stream(
        input.reply_ref, input.stream_ts, input.markdown_text, input.chunks, req
    )


@gestalt.operation(
    id=SLACK_STREAM_STOP_OPERATION,
    method="POST",
    description="Finalize a native Slack streaming reply in the event thread",
    visible=False,
)
def slack_events_stop_stream(
    input: SlackEventStreamStopInput, req: gestalt.Request
) -> OperationResult:
    return stop_slack_event_stream(
        input.reply_ref,
        input.stream_ts,
        input.markdown_text,
        input.chunks,
        input.blocks,
        input.metadata,
        req,
    )


@gestalt.operation(
    id="chat.postMessage",
    method="POST",
    description="Send a Slack message with a visible Gestalt attribution footer",
)
def chat_post_message(
    input: ChatPostMessageInput, req: gestalt.Request
) -> OperationResult:
    token_or_error = _chat_post_message_token(req)
    if isinstance(token_or_error, gestalt.Response):
        return token_or_error
    if not input.channel:
        return _bad_request("channel is required")
    if input.text is None:
        return _bad_request("text is required")

    blocks_or_error = _chat_post_message_blocks(input.text, input.blocks, req)
    if isinstance(blocks_or_error, gestalt.Response):
        return blocks_or_error
    metadata = _chat_post_message_metadata(input.metadata)

    try:
        return post_message(
            token_or_error,
            channel=input.channel,
            text=input.text,
            thread_ts=input.thread_ts,
            unfurl_links=input.unfurl_links,
            unfurl_media=input.unfurl_media,
            blocks=blocks_or_error,
            metadata=metadata,
        )
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _server_error(str(err))


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
    description="Build rich Slack thread context by Slack URL or channel/timestamp, with messages, participants, and files",
)
def conversations_get_thread_context(
    input: GetThreadContextInput, req: gestalt.Request
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

    if not channel:
        return _bad_request("channel is required")
    if not ts:
        return _bad_request("ts is required")

    try:
        return get_thread_context(
            req.token,
            channel=channel,
            ts=ts,
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


def _chat_post_message_token(req: gestalt.Request) -> str | ErrorResponse:
    subject_id = str(getattr(req.subject, "id", "") or "").strip()
    if subject_id == SLACK_BOT_SERVICE_ACCOUNT_SUBJECT_ID:
        bot_token = _agent._agent_config.bot.token
        if not bot_token:
            return gestalt.Response(
                status=HTTPStatus.PRECONDITION_FAILED,
                body={"error": "Slack bot token is not configured"},
            )
        return bot_token

    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    return req.token


def _chat_post_message_gestalt_label() -> str:
    bot_user_id = _agent._agent_config.bot.user_id.strip()
    if bot_user_id:
        return f"<@{bot_user_id}>"
    return SLACK_POST_MESSAGE_FOOTER_APP_NAME


def _external_identity_field(identity: Any, field: str) -> str:
    if isinstance(identity, Mapping):
        value = identity.get(field)
    else:
        value = getattr(identity, field, None)
    return str(value or "").strip()


def _slack_user_id_from_external_identity(identity: Any) -> str:
    if identity is None:
        return ""
    if (
        _external_identity_field(identity, "type")
        != _agent.SLACK_EXTERNAL_IDENTITY_TYPE
    ):
        return ""
    identity_id = _external_identity_field(identity, "id")
    parts = identity_id.split(":")
    if len(parts) != 4 or parts[0] != "team" or parts[2] != "user":
        return ""
    return parts[3].strip()


def _chat_post_message_footer_text(req: gestalt.Request) -> str:
    gestalt_label = _chat_post_message_gestalt_label()
    subject_id = str(getattr(req.subject, "id", "") or "").strip()
    if subject_id == SLACK_BOT_SERVICE_ACCOUNT_SUBJECT_ID:
        user_id = _slack_user_id_from_external_identity(
            getattr(req, "agent_external_identity", None)
        )
        if user_id:
            return f"Sent by <@{user_id}> with {gestalt_label}"
    return f"Sent with {gestalt_label}"


def _chat_post_message_footer_block(req: gestalt.Request) -> dict[str, Any]:
    return {
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": _chat_post_message_footer_text(req)}],
    }


def _chat_post_message_blocks(
    text: str, blocks: list[dict[str, Any]] | None, req: gestalt.Request
) -> list[dict[str, Any]] | ErrorResponse:
    if blocks:
        if not isinstance(blocks, list) or not all(
            isinstance(block, dict) for block in blocks
        ):
            return _bad_request("blocks must be an array of Slack block objects")
        if len(blocks) >= SLACK_MAX_BLOCKS:
            return _bad_request("blocks must leave room for the Gestalt footer")
        return [*blocks, _chat_post_message_footer_block(req)]
    if blocks is not None and not isinstance(blocks, list):
        return _bad_request("blocks must be an array of Slack block objects")

    chunks = [
        text[index : index + SLACK_MAX_SECTION_TEXT_CHARS]
        for index in range(0, len(text), SLACK_MAX_SECTION_TEXT_CHARS)
    ]
    if len(chunks) > SLACK_MAX_SYNTHESIZED_TEXT_BLOCKS:
        return _bad_request("text is too long to send with the Gestalt footer")
    synthesized = [
        {"type": "section", "text": {"type": "mrkdwn", "text": chunk}}
        for chunk in chunks
        if chunk
    ]
    return [*synthesized, _chat_post_message_footer_block(req)]


def _chat_post_message_metadata(
    metadata: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if metadata is None:
        return {
            "event_type": "gestalt_message",
            "event_payload": {"sent_with": "gestalt"},
        }
    return metadata


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
