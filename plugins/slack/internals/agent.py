from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from enum import StrEnum
from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt
from google.protobuf import json_format
from google.protobuf import struct_pb2 as _struct_pb2
from gestalt.gen.v1 import authorization_pb2 as _authorization_pb2
from gestalt.gen.v1 import workflow_pb2 as _workflow_pb2

from .client import SlackAPIError, SlackClientError
from .helpers import map_field, map_slice, string_field
from .operations import (
    add_reaction,
    append_stream,
    delete_message,
    post_message,
    remove_reaction,
    set_assistant_thread_status,
    set_assistant_thread_suggested_prompts,
    set_assistant_thread_title,
    start_stream,
    stop_stream,
    update_message,
)

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse
PostConnectMetadata: TypeAlias = dict[str, str]

authorization_pb2: Any = _authorization_pb2
workflow_pb2: Any = _workflow_pb2
struct_pb2: Any = _struct_pb2

SLACK_AUTH_TEST_URL = "https://slack.com/api/auth.test"
SLACK_DEFAULT_CONNECTION = "default"
SLACK_EVENT_OPERATION = "events.handle"
SLACK_REPLY_OPERATION = "events.reply"
SLACK_STATUS_OPERATION = "events.setStatus"
SLACK_DELETE_STATUS_OPERATION = "events.deleteStatus"
SLACK_ADD_REACTION_OPERATION = "events.addReaction"
SLACK_REMOVE_REACTION_OPERATION = "events.removeReaction"
SLACK_ASSISTANT_STATUS_OPERATION = "events.setAssistantStatus"
SLACK_ASSISTANT_CLEAR_STATUS_OPERATION = "events.clearAssistantStatus"
SLACK_ASSISTANT_TITLE_OPERATION = "events.setThreadTitle"
SLACK_ASSISTANT_PROMPTS_OPERATION = "events.setSuggestedPrompts"
SLACK_STREAM_START_OPERATION = "events.startStream"
SLACK_STREAM_APPEND_OPERATION = "events.appendStream"
SLACK_STREAM_STOP_OPERATION = "events.stopStream"
SLACK_CONTEXT_OPERATION = "conversations.getThreadContext"
SLACK_FILE_GET_OPERATION = "files.get"
SLACK_WORKFLOW_EVENT_TYPE = "com.valon.slack.event"
SLACK_WORKFLOW_EVENT_SOURCE = "slack"
SLACK_WORKFLOW_FILE_FIELDS = frozenset(
    (
        "id",
        "created",
        "timestamp",
        "name",
        "title",
        "mimetype",
        "filetype",
        "pretty_type",
        "user",
        "mode",
        "size",
        "is_external",
        "external_type",
    )
)
SLACK_EXTERNAL_IDENTITY_TYPE = "slack_identity"
SLACK_REPLY_REF_TTL_SECONDS = 60 * 60
EXTERNAL_IDENTITY_RESOURCE_TYPE = "external_identity"
EXTERNAL_IDENTITY_ASSUME_ACTION = "assume"
EXTERNAL_IDENTITY_TYPE_METADATA_KEY = "gestalt.external_identity.type"
EXTERNAL_IDENTITY_ID_METADATA_KEY = "gestalt.external_identity.id"


class SlackCallbackType(StrEnum):
    URL_VERIFICATION = "url_verification"
    EVENT_CALLBACK = "event_callback"


class SlackEventType(StrEnum):
    APP_MENTION = "app_mention"
    MESSAGE = "message"
    ASSISTANT_THREAD_STARTED = "assistant_thread_started"
    ASSISTANT_THREAD_CONTEXT_CHANGED = "assistant_thread_context_changed"


class SlackChannelType(StrEnum):
    IM = "im"
    MPIM = "mpim"


SUPPORTED_EVENT_TYPES = frozenset(event.value for event in SlackEventType)
DIRECT_MESSAGE_CHANNEL_TYPES = frozenset(
    channel.value for channel in (SlackChannelType.IM, SlackChannelType.MPIM)
)
ASSISTANT_THREAD_EVENT_TYPES = frozenset(
    {
        SlackEventType.ASSISTANT_THREAD_STARTED.value,
        SlackEventType.ASSISTANT_THREAD_CONTEXT_CHANGED.value,
    }
)


@dataclass(frozen=True, slots=True)
class SlackAgentRouteMatch:
    team_ids: tuple[str, ...] = ()
    channel_ids: tuple[str, ...] = ()
    channel_types: tuple[str, ...] = ()
    event_types: tuple[str, ...] = ()
    user_ids: tuple[str, ...] = ()

    def matches(self, event: SlackAgentEvent) -> bool:
        if self.team_ids and event.team_id not in self.team_ids:
            return False
        if self.channel_ids and event.channel_id not in self.channel_ids:
            return False
        if self.channel_types and event.channel_type.lower() not in self.channel_types:
            return False
        if self.event_types and event.event_type.lower() not in self.event_types:
            return False
        if self.user_ids and event.user_id not in self.user_ids:
            return False
        return True


@dataclass(frozen=True, slots=True)
class SlackAgentRoute:
    id: str = ""
    match: SlackAgentRouteMatch = field(default_factory=SlackAgentRouteMatch)


@dataclass(frozen=True, slots=True)
class SlackBotConfig:
    token: str = ""


@dataclass(frozen=True, slots=True)
class SlackAssistantConfig:
    enabled: bool = False
    status: str = "is thinking..."
    loading_messages: tuple[str, ...] = ()
    icon_emoji: str = ""
    icon_url: str = ""
    username: str = ""
    suggested_prompts_title: str = ""
    suggested_prompts: tuple[dict[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class SlackAgentConfig:
    plugin_name: str = "slack"
    bot: SlackBotConfig = field(default_factory=SlackBotConfig)
    assistant: SlackAssistantConfig = field(default_factory=SlackAssistantConfig)
    workflow_provider: str = ""
    routes: tuple[SlackAgentRoute, ...] = ()


@dataclass(frozen=True, slots=True)
class SlackAgentEvent:
    callback_type: str
    event_type: str
    event_id: str
    team_id: str
    user_id: str
    channel_id: str
    channel_type: str
    text: str
    message_ts: str
    thread_ts: str
    reply_thread_ts: str
    files: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class SlackReplyRef:
    team_id: str
    channel_id: str
    message_ts: str
    reply_thread_ts: str
    event_id: str
    subject_id: str
    expires_at: int
    user_id: str = ""
    channel_type: str = ""


_agent_config = SlackAgentConfig()


def configure_agent(name: str, config: dict[str, Any]) -> None:
    global _agent_config

    _agent_config = _agent_config_from_provider_config(name, config)


def post_connect_metadata(token: gestalt.ConnectedToken) -> PostConnectMetadata:
    if token.connection != SLACK_DEFAULT_CONNECTION:
        return {}
    if not token.access_token:
        raise RuntimeError("Slack post-connect requires an access token")

    identity = _auth_test(token.access_token)
    return {
        EXTERNAL_IDENTITY_TYPE_METADATA_KEY: SLACK_EXTERNAL_IDENTITY_TYPE,
        EXTERNAL_IDENTITY_ID_METADATA_KEY: slack_external_identity_id(
            identity["team_id"], identity["user_id"]
        ),
        "slack.team_id": identity["team_id"],
        "slack.user_id": identity["user_id"],
    }


def resolve_slack_http_subject(
    request: gestalt.HTTPSubjectRequest, context: gestalt.Request
) -> gestalt.Subject | None:
    payload = _json_payload_from_http_request(request)
    event, _ignored_reason = _slack_agent_event_from_payload(payload)
    if event is None:
        return None
    _route, ignored_reason = _select_agent_route(event)
    if ignored_reason:
        return None
    if not event.team_id or not event.user_id:
        raise gestalt.http_subject_error(
            HTTPStatus.BAD_REQUEST, "Slack event is missing team_id or user"
        )

    subject = _resolve_slack_subject(
        context.authorization(),
        team_id=event.team_id,
        user_id=event.user_id,
    )
    if subject is None:
        raise gestalt.http_subject_error(
            HTTPStatus.FORBIDDEN, "Slack user is not linked to a Gestalt subject"
        )
    return subject


def handle_slack_event(input: dict[str, Any], req: gestalt.Request) -> OperationResult:
    if _is_url_verification(input):
        return {"challenge": str(input.get("challenge") or "")}

    event, ignored_reason = _slack_agent_event_from_payload(input)
    if event is None:
        return {"ok": True, "ignored": ignored_reason}

    route, ignored_reason = _select_agent_route(event)
    if ignored_reason:
        return {"ok": True, "ignored": ignored_reason}

    if not req.subject.id or req.subject.id.startswith("system:"):
        return gestalt.Response(
            status=HTTPStatus.FORBIDDEN, body={"error": "Slack user is not linked"}
        )
    if not _agent_config.bot.token:
        return gestalt.Response(
            status=HTTPStatus.PRECONDITION_FAILED,
            body={"error": "Slack bot token is not configured"},
        )
    if event.event_type in ASSISTANT_THREAD_EVENT_TYPES:
        return _handle_assistant_thread_event(event)

    assistant_status_error = ""
    try:
        reply_ref = _sign_reply_ref(event, req.subject.id)
        if _agent_config.assistant.enabled:
            try:
                _set_initial_assistant_status(event)
            except SlackAPIError as err:
                assistant_status_error = str(err.body.get("error") or err.body)
            except SlackClientError as err:
                assistant_status_error = str(err)
        with req.workflow_manager() as workflow_manager:
            published_event = workflow_manager.publish_event(
                _build_slack_workflow_event_request(event, route, reply_ref)
            )
    except Exception as err:
        return _server_error(f"failed to publish Slack workflow event: {err}")

    response = {
        "ok": True,
        "workflow_event_id": str(getattr(published_event, "id", "") or event.event_id),
        "workflow_event_type": SLACK_WORKFLOW_EVENT_TYPE,
        "workflow_event_source": SLACK_WORKFLOW_EVENT_SOURCE,
    }
    if assistant_status_error:
        response["assistant_status_error"] = assistant_status_error
    return response


def _build_slack_workflow_event_request(
    event: SlackAgentEvent,
    route: SlackAgentRoute | None,
    reply_ref: str,
) -> Any:
    request = workflow_pb2.WorkflowManagerPublishEventRequest(
        provider_name=_agent_config.workflow_provider,
        event=workflow_pb2.WorkflowEvent(
            id=_slack_workflow_event_id(event),
            source=SLACK_WORKFLOW_EVENT_SOURCE,
            spec_version="1.0",
            type=SLACK_WORKFLOW_EVENT_TYPE,
            subject=_slack_workflow_event_subject(event),
            datacontenttype="application/json",
        ),
    )
    request.event.data.CopyFrom(_dict_to_struct(_slack_workflow_event_data(event, route)))
    request.private_input.CopyFrom(_dict_to_struct({"reply_ref": reply_ref}))
    return request


def _slack_workflow_event_id(event: SlackAgentEvent) -> str:
    if event.event_id:
        return event.event_id
    return ":".join(
        [
            "slack",
            event.team_id,
            event.channel_id,
            event.message_ts,
            event.user_id,
        ]
    )


def _slack_workflow_event_subject(event: SlackAgentEvent) -> str:
    if event.team_id and event.channel_id:
        return f"team:{event.team_id}:channel:{event.channel_id}"
    if event.team_id:
        return f"team:{event.team_id}"
    return ""


def _slack_workflow_event_data(
    event: SlackAgentEvent,
    route: SlackAgentRoute | None,
) -> dict[str, Any]:
    return {
        "callback_type": event.callback_type,
        "event_type": event.event_type,
        "event_id": event.event_id,
        "team_id": event.team_id,
        "user_id": event.user_id,
        "channel_id": event.channel_id,
        "channel_type": event.channel_type,
        "text": event.text,
        "message_ts": event.message_ts,
        "thread_ts": event.thread_ts,
        "reply_thread_ts": event.reply_thread_ts,
        "file_ids": _event_file_ids(event),
        "files": [_slack_workflow_file_data(file_data) for file_data in event.files],
        "agent_route_id": route.id if route is not None else "",
    }


def _slack_workflow_file_data(file_data: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in file_data.items()
        if key in SLACK_WORKFLOW_FILE_FIELDS and value not in (None, "")
    }


def reply_to_slack_event(
    reply_ref: str, text: str, req: gestalt.Request
) -> OperationResult:
    normalized_text = text.strip()
    if not normalized_text:
        return _bad_request("text is required")

    try:
        verified_ref = _event_reply_ref(reply_ref, req)
        result = post_message(
            _agent_config.bot.token,
            channel=verified_ref.channel_id,
            text=normalized_text,
            thread_ts=verified_ref.reply_thread_ts,
        )
    except ValueError as err:
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _event_client_error(err)

    return {
        "ok": True,
        "channel": str(result.get("channel") or verified_ref.channel_id),
        "ts": str(result.get("ts") or ""),
        "thread_ts": verified_ref.reply_thread_ts,
    }


def set_slack_event_status(
    reply_ref: str,
    text: str,
    status_ts: str,
    unfurl_links: bool,
    unfurl_media: bool,
    req: gestalt.Request,
) -> OperationResult:
    normalized_text = text.strip()
    if not normalized_text:
        return _bad_request("text is required")

    try:
        verified_ref = _event_reply_ref(reply_ref, req)
        if status_ts:
            result = update_message(
                _agent_config.bot.token,
                channel=verified_ref.channel_id,
                ts=status_ts,
                text=normalized_text,
            )
            result_status_ts = str(result.get("ts") or status_ts)
            created = False
        else:
            result = post_message(
                _agent_config.bot.token,
                channel=verified_ref.channel_id,
                text=normalized_text,
                thread_ts=verified_ref.reply_thread_ts,
                unfurl_links=unfurl_links,
                unfurl_media=unfurl_media,
            )
            result_status_ts = str(result.get("ts") or "")
            created = True
    except ValueError as err:
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _event_client_error(err)

    return {
        "ok": True,
        "created": created,
        "channel": str(result.get("channel") or verified_ref.channel_id),
        "status_ts": result_status_ts,
        "thread_ts": verified_ref.reply_thread_ts,
    }


def delete_slack_event_status(
    reply_ref: str, status_ts: str, req: gestalt.Request
) -> OperationResult:
    normalized_status_ts = status_ts.strip()
    if not normalized_status_ts:
        return _bad_request("status_ts is required")

    try:
        verified_ref = _event_reply_ref(reply_ref, req)
        result = delete_message(
            _agent_config.bot.token,
            channel=verified_ref.channel_id,
            ts=normalized_status_ts,
        )
    except ValueError as err:
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _event_client_error(err)

    return {
        "ok": True,
        "channel": str(result.get("channel") or verified_ref.channel_id),
        "deleted_ts": str(result.get("ts") or normalized_status_ts),
    }


def set_slack_event_assistant_status(
    reply_ref: str,
    status: str,
    loading_messages: list[str],
    icon_emoji: str,
    icon_url: str,
    username: str,
    req: gestalt.Request,
) -> OperationResult:
    try:
        verified_ref = _event_reply_ref(reply_ref, req)
        thread_ts = _assistant_thread_ts(verified_ref)
        if not thread_ts:
            return _bad_request("assistant thread timestamp is required")
        normalized_loading_messages = _normalized_string_list(
            loading_messages, max_items=10
        )
        result = set_assistant_thread_status(
            _agent_config.bot.token,
            channel_id=verified_ref.channel_id,
            thread_ts=thread_ts,
            status=status.strip(),
            loading_messages=normalized_loading_messages,
            icon_emoji=icon_emoji.strip(),
            icon_url=icon_url.strip(),
            username=username.strip(),
        )
    except ValueError as err:
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _event_client_error(err)

    return {
        "ok": True,
        "channel": str(result.get("channel") or verified_ref.channel_id),
        "thread_ts": thread_ts,
        "status": status.strip(),
    }


def clear_slack_event_assistant_status(
    reply_ref: str, req: gestalt.Request
) -> OperationResult:
    return set_slack_event_assistant_status(
        reply_ref,
        "",
        [],
        "",
        "",
        "",
        req,
    )


def set_slack_event_thread_title(
    reply_ref: str, title: str, req: gestalt.Request
) -> OperationResult:
    normalized_title = title.strip()
    if not normalized_title:
        return _bad_request("title is required")

    try:
        verified_ref = _event_reply_ref(reply_ref, req)
        thread_ts = _assistant_thread_ts(verified_ref)
        if not thread_ts:
            return _bad_request("assistant thread timestamp is required")
        result = set_assistant_thread_title(
            _agent_config.bot.token,
            channel_id=verified_ref.channel_id,
            thread_ts=thread_ts,
            title=normalized_title,
        )
    except ValueError as err:
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _event_client_error(err)

    return {
        "ok": True,
        "channel": str(result.get("channel") or verified_ref.channel_id),
        "thread_ts": thread_ts,
        "title": normalized_title,
    }


def set_slack_event_suggested_prompts(
    reply_ref: str, prompts: list[dict[str, Any]], title: str, req: gestalt.Request
) -> OperationResult:
    try:
        normalized_prompts = _normalized_suggested_prompts(prompts)
    except ValueError as err:
        return _bad_request(str(err))

    try:
        verified_ref = _event_reply_ref(reply_ref, req)
        thread_ts = _assistant_thread_ts(verified_ref)
        if not thread_ts:
            return _bad_request("assistant thread timestamp is required")
        result = set_assistant_thread_suggested_prompts(
            _agent_config.bot.token,
            channel_id=verified_ref.channel_id,
            thread_ts=thread_ts,
            prompts=normalized_prompts,
            title=title.strip(),
        )
    except ValueError as err:
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _event_client_error(err)

    return {
        "ok": True,
        "channel": str(result.get("channel") or verified_ref.channel_id),
        "thread_ts": thread_ts,
        "suggested_prompt_count": len(normalized_prompts),
    }


def start_slack_event_stream(
    reply_ref: str,
    markdown_text: str,
    chunks: list[dict[str, Any]],
    recipient_user_id: str,
    recipient_team_id: str,
    task_display_mode: str,
    icon_emoji: str,
    icon_url: str,
    username: str,
    req: gestalt.Request,
) -> OperationResult:
    try:
        verified_ref = _event_reply_ref(reply_ref, req)
        thread_ts = _assistant_thread_ts(verified_ref)
        if not thread_ts:
            return _bad_request("assistant thread timestamp is required")
        result = start_stream(
            _agent_config.bot.token,
            channel=verified_ref.channel_id,
            thread_ts=thread_ts,
            markdown_text=markdown_text.strip(),
            chunks=chunks,
            recipient_user_id=recipient_user_id.strip() or verified_ref.user_id,
            recipient_team_id=recipient_team_id.strip() or verified_ref.team_id,
            task_display_mode=task_display_mode.strip(),
            icon_emoji=icon_emoji.strip(),
            icon_url=icon_url.strip(),
            username=username.strip(),
        )
    except ValueError as err:
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _event_client_error(err)

    return {
        "ok": True,
        "channel": str(result.get("channel") or verified_ref.channel_id),
        "thread_ts": thread_ts,
        "stream_ts": str(result.get("ts") or ""),
    }


def append_slack_event_stream(
    reply_ref: str,
    stream_ts: str,
    markdown_text: str,
    chunks: list[dict[str, Any]],
    req: gestalt.Request,
) -> OperationResult:
    normalized_stream_ts = stream_ts.strip()
    if not normalized_stream_ts:
        return _bad_request("stream_ts is required")
    if not markdown_text.strip() and not chunks:
        return _bad_request("markdown_text or chunks are required")

    try:
        verified_ref = _event_reply_ref(reply_ref, req)
        result = append_stream(
            _agent_config.bot.token,
            channel=verified_ref.channel_id,
            ts=normalized_stream_ts,
            markdown_text=markdown_text.strip(),
            chunks=chunks,
        )
    except ValueError as err:
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _event_client_error(err)

    return {
        "ok": True,
        "channel": str(result.get("channel") or verified_ref.channel_id),
        "stream_ts": str(result.get("ts") or normalized_stream_ts),
    }


def stop_slack_event_stream(
    reply_ref: str,
    stream_ts: str,
    markdown_text: str,
    chunks: list[dict[str, Any]],
    blocks: list[dict[str, Any]],
    metadata: dict[str, Any],
    req: gestalt.Request,
) -> OperationResult:
    normalized_stream_ts = stream_ts.strip()
    if not normalized_stream_ts:
        return _bad_request("stream_ts is required")

    try:
        verified_ref = _event_reply_ref(reply_ref, req)
        result = stop_stream(
            _agent_config.bot.token,
            channel=verified_ref.channel_id,
            ts=normalized_stream_ts,
            markdown_text=markdown_text.strip(),
            chunks=chunks,
            blocks=blocks,
            metadata=metadata,
        )
    except ValueError as err:
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _event_client_error(err)

    return {
        "ok": True,
        "channel": str(result.get("channel") or verified_ref.channel_id),
        "stream_ts": str(result.get("ts") or normalized_stream_ts),
        "message": result.get("message")
        if isinstance(result.get("message"), dict)
        else {},
    }


def add_slack_event_reaction(
    reply_ref: str, name: str, target_ts: str, req: gestalt.Request
) -> OperationResult:
    return _slack_event_reaction(reply_ref, name, target_ts, req, remove=False)


def remove_slack_event_reaction(
    reply_ref: str, name: str, target_ts: str, req: gestalt.Request
) -> OperationResult:
    return _slack_event_reaction(reply_ref, name, target_ts, req, remove=True)


def _event_reply_ref(reply_ref: str, req: gestalt.Request) -> SlackReplyRef:
    if not req.subject.id or req.subject.id.startswith("system:"):
        raise ValueError("Slack user is not linked")
    if not _agent_config.bot.token:
        raise SlackClientError("Slack bot token is not configured")
    return _verify_reply_ref(reply_ref, req.subject.id)


def _event_client_error(err: SlackClientError) -> ErrorResponse:
    message = str(err)
    if message == "Slack bot token is not configured":
        return gestalt.Response(
            status=HTTPStatus.PRECONDITION_FAILED,
            body={"error": message},
        )
    return _server_error(message)


def _slack_event_reaction(
    reply_ref: str,
    name: str,
    target_ts: str,
    req: gestalt.Request,
    *,
    remove: bool,
) -> OperationResult:
    normalized_name = name.strip().strip(":")
    if not normalized_name:
        return _bad_request("name is required")

    try:
        verified_ref = _event_reply_ref(reply_ref, req)
        normalized_target_ts = target_ts.strip() or verified_ref.message_ts
        if not normalized_target_ts:
            return _bad_request("target_ts is required")
        if remove:
            result = remove_reaction(
                _agent_config.bot.token,
                channel=verified_ref.channel_id,
                timestamp=normalized_target_ts,
                name=normalized_name,
            )
        else:
            result = add_reaction(
                _agent_config.bot.token,
                channel=verified_ref.channel_id,
                timestamp=normalized_target_ts,
                name=normalized_name,
            )
    except ValueError as err:
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _event_client_error(err)

    return {
        "ok": True,
        "channel": str(result.get("channel") or verified_ref.channel_id),
        "target_ts": normalized_target_ts,
        "name": normalized_name,
        "removed": remove,
    }


def _assistant_thread_ts(ref: SlackReplyRef) -> str:
    return ref.reply_thread_ts or ref.message_ts


def _set_initial_assistant_status(event: SlackAgentEvent) -> None:
    assistant = _agent_config.assistant
    status = assistant.status.strip()
    thread_ts = event.reply_thread_ts or event.message_ts
    if not status or not thread_ts:
        return
    set_assistant_thread_status(
        _agent_config.bot.token,
        channel_id=event.channel_id,
        thread_ts=thread_ts,
        status=status,
        loading_messages=list(assistant.loading_messages),
        icon_emoji=assistant.icon_emoji,
        icon_url=assistant.icon_url,
        username=assistant.username,
    )


def _handle_assistant_thread_event(event: SlackAgentEvent) -> OperationResult:
    if event.event_type == SlackEventType.ASSISTANT_THREAD_CONTEXT_CHANGED:
        return {"ok": True, "event_type": event.event_type}

    assistant = _agent_config.assistant
    if not assistant.suggested_prompts:
        return {
            "ok": True,
            "event_type": event.event_type,
            "suggested_prompts_set": False,
        }

    try:
        result = set_assistant_thread_suggested_prompts(
            _agent_config.bot.token,
            channel_id=event.channel_id,
            thread_ts=event.reply_thread_ts or event.message_ts,
            prompts=list(assistant.suggested_prompts),
            title=assistant.suggested_prompts_title,
        )
    except SlackAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        return _event_client_error(err)

    return {
        "ok": True,
        "event_type": event.event_type,
        "channel": str(result.get("channel") or event.channel_id),
        "thread_ts": event.reply_thread_ts or event.message_ts,
        "suggested_prompts_set": True,
        "suggested_prompt_count": len(assistant.suggested_prompts),
    }


def _normalized_string_list(values: list[str], *, max_items: int) -> list[str]:
    normalized: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        stripped = value.strip()
        if stripped:
            normalized.append(stripped)
        if len(normalized) >= max_items:
            break
    return normalized


def _normalized_suggested_prompts(
    prompts: list[dict[str, Any]],
) -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    for prompt in prompts:
        if not isinstance(prompt, dict):
            continue
        title = str(prompt.get("title") or "").strip()
        message = str(prompt.get("message") or "").strip()
        if title and message:
            normalized.append({"title": title, "message": message})
        if len(normalized) >= 4:
            break
    if not normalized:
        raise ValueError("at least one prompt with title and message is required")
    return normalized


def slack_external_identity_id(team_id: str, user_id: str) -> str:
    team_id = team_id.strip()
    user_id = user_id.strip()
    if not team_id or not user_id:
        raise RuntimeError("Slack auth.test did not return team_id and user_id")
    return f"team:{team_id}:user:{user_id}"


def external_identity_resource_id(identity_type: str, identity_id: str) -> str:
    identity_type = identity_type.strip()
    identity_id = identity_id.strip()
    if not identity_type or not identity_id:
        return ""
    raw = f"{identity_type}\x00{identity_id}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _json_payload_from_http_request(
    request: gestalt.HTTPSubjectRequest,
) -> dict[str, Any]:
    if isinstance(request.params, dict) and request.params:
        return dict(request.params)
    if not request.raw_body:
        return {}
    try:
        payload = json.loads(request.raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _slack_agent_event_from_payload(
    payload: dict[str, Any],
) -> tuple[SlackAgentEvent | None, str]:
    callback_type = str(payload.get("type") or "").strip()
    if callback_type == SlackCallbackType.URL_VERIFICATION:
        return None, "url_verification"
    if callback_type != SlackCallbackType.EVENT_CALLBACK:
        return None, "unsupported_callback_type"

    event = payload.get("event")
    if not isinstance(event, dict):
        return None, "missing_event"
    if _is_ignored_event(event):
        return None, "ignored_event"

    event_type = str(event.get("type") or "").strip()
    channel_type = str(event.get("channel_type") or "").strip()
    if event_type not in SUPPORTED_EVENT_TYPES:
        return None, "unsupported_event_type"

    team_id = _slack_team_id(payload, event)
    if event_type in ASSISTANT_THREAD_EVENT_TYPES:
        assistant_thread = map_field(event, "assistant_thread")
        assistant_context = map_field(assistant_thread, "context")
        user_id = string_field(assistant_thread, "user_id")
        channel_id = string_field(assistant_thread, "channel_id")
        thread_ts = string_field(assistant_thread, "thread_ts")
        if not team_id:
            team_id = string_field(assistant_context, "team_id")
        if not channel_type and channel_id.startswith("D"):
            channel_type = SlackChannelType.IM
        if not user_id:
            return None, "missing_user"
        if not channel_id:
            return None, "missing_channel"
        if not thread_ts:
            return None, "missing_thread"
        return (
            SlackAgentEvent(
                callback_type=callback_type,
                event_type=event_type,
                event_id=str(payload.get("event_id") or "").strip(),
                team_id=team_id,
                user_id=user_id,
                channel_id=channel_id,
                channel_type=str(channel_type),
                text="",
                message_ts=thread_ts,
                thread_ts=thread_ts,
                reply_thread_ts=thread_ts,
                files=(),
            ),
            "",
        )

    user_id = str(event.get("user") or "").strip()
    channel_id = str(event.get("channel") or "").strip()
    text = str(event.get("text") or "").strip()
    message_ts = str(event.get("ts") or event.get("event_ts") or "").strip()
    thread_ts = str(event.get("thread_ts") or "").strip()
    files = tuple(map_slice(event.get("files")))
    reply_thread_ts = thread_ts
    if event_type == SlackEventType.APP_MENTION and not reply_thread_ts:
        reply_thread_ts = message_ts

    if not user_id:
        return None, "missing_user"
    if not channel_id:
        return None, "missing_channel"
    if not text and not files:
        return None, "missing_text"

    return (
        SlackAgentEvent(
            callback_type=callback_type,
            event_type=event_type,
            event_id=str(payload.get("event_id") or "").strip(),
            team_id=team_id,
            user_id=user_id,
            channel_id=channel_id,
            channel_type=channel_type,
            text=text,
            message_ts=message_ts,
            thread_ts=thread_ts,
            reply_thread_ts=reply_thread_ts,
            files=files,
        ),
        "",
    )


def _is_url_verification(payload: dict[str, Any]) -> bool:
    return str(payload.get("type") or "").strip() == SlackCallbackType.URL_VERIFICATION


def _is_ignored_event(event: dict[str, Any]) -> bool:
    subtype = str(event.get("subtype") or "").strip()
    if subtype in {
        "bot_message",
        "message_changed",
        "message_deleted",
        "message_replied",
    }:
        return True
    if event.get("bot_id") or event.get("bot_profile"):
        return True
    return False


def _slack_team_id(payload: dict[str, Any], event: dict[str, Any]) -> str:
    direct = str(payload.get("team_id") or event.get("team") or "").strip()
    if direct:
        return direct
    authorizations = payload.get("authorizations")
    if isinstance(authorizations, list):
        for authorization in authorizations:
            if isinstance(authorization, dict):
                team_id = str(authorization.get("team_id") or "").strip()
                if team_id:
                    return team_id
    return ""


def _resolve_slack_subject(
    authorization: gestalt.AuthorizationClient,
    *,
    team_id: str,
    user_id: str,
) -> gestalt.Subject | None:
    identity_id = slack_external_identity_id(team_id, user_id)
    resource_id = external_identity_resource_id(
        SLACK_EXTERNAL_IDENTITY_TYPE, identity_id
    )
    response = authorization.search_subjects(
        authorization_pb2.SubjectSearchRequest(
            resource=authorization_pb2.Resource(
                type=EXTERNAL_IDENTITY_RESOURCE_TYPE, id=resource_id
            ),
            action=authorization_pb2.Action(name=EXTERNAL_IDENTITY_ASSUME_ACTION),
            subject_type="user",
            page_size=2,
        )
    )
    subjects = list(response.subjects)
    if len(subjects) > 1:
        raise gestalt.http_subject_error(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            "Slack external identity resolved to multiple Gestalt subjects",
        )
    if not subjects:
        return None

    subject = subjects[0]
    subject_id = str(subject.id or "").strip()
    if not subject_id:
        return None
    return gestalt.Subject(
        id=subject_id,
        kind=str(subject.type or "").strip(),
        display_name=_subject_display_name(subject),
        auth_source="authorization",
    )


def _subject_display_name(subject: Any) -> str:
    properties = getattr(subject, "properties", None)
    if properties is not None and getattr(properties, "fields", None):
        data = json_format.MessageToDict(properties)
        for key in ("displayName", "display_name", "email", "name"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return str(getattr(subject, "id", "") or "").strip()


def _sign_reply_ref(event: SlackAgentEvent, subject_id: str) -> str:
    payload = {
        "v": 1,
        "team_id": event.team_id,
        "channel_id": event.channel_id,
        "user_id": event.user_id,
        "channel_type": event.channel_type,
        "message_ts": event.message_ts,
        "reply_thread_ts": event.reply_thread_ts,
        "event_id": event.event_id,
        "subject_id": subject_id,
        "expires_at": int(time.time()) + SLACK_REPLY_REF_TTL_SECONDS,
    }
    encoded_payload = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    signature = hmac.new(
        _reply_ref_signing_key(), encoded_payload, hashlib.sha256
    ).digest()
    return f"{_base64url_encode(encoded_payload)}.{_base64url_encode(signature)}"


def _verify_reply_ref(reply_ref: str, subject_id: str) -> SlackReplyRef:
    payload_part, separator, signature_part = reply_ref.strip().partition(".")
    if not separator:
        raise ValueError("invalid reply_ref")

    try:
        encoded_payload = _base64url_decode(payload_part)
        signature = _base64url_decode(signature_part)
    except (binascii.Error, ValueError) as err:
        raise ValueError("invalid reply_ref") from err

    expected_signature = hmac.new(
        _reply_ref_signing_key(), encoded_payload, hashlib.sha256
    ).digest()
    if not hmac.compare_digest(signature, expected_signature):
        raise ValueError("invalid reply_ref")

    try:
        payload = json.loads(encoded_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ValueError("invalid reply_ref") from err
    if not isinstance(payload, dict):
        raise ValueError("invalid reply_ref")

    ref = _reply_ref_from_payload(payload)
    if ref.subject_id != subject_id:
        raise ValueError("reply_ref does not belong to this subject")
    if ref.expires_at < int(time.time()):
        raise ValueError("reply_ref expired")
    return ref


def _reply_ref_from_payload(payload: dict[str, Any]) -> SlackReplyRef:
    if payload.get("v") != 1:
        raise ValueError("invalid reply_ref")
    try:
        expires_at = int(payload.get("expires_at") or 0)
    except (TypeError, ValueError) as err:
        raise ValueError("invalid reply_ref") from err

    ref = SlackReplyRef(
        team_id=str(payload.get("team_id") or "").strip(),
        channel_id=str(payload.get("channel_id") or "").strip(),
        message_ts=str(payload.get("message_ts") or "").strip(),
        reply_thread_ts=str(payload.get("reply_thread_ts") or "").strip(),
        event_id=str(payload.get("event_id") or "").strip(),
        subject_id=str(payload.get("subject_id") or "").strip(),
        expires_at=expires_at,
        user_id=str(payload.get("user_id") or "").strip(),
        channel_type=str(payload.get("channel_type") or "").strip(),
    )
    if not ref.team_id or not ref.channel_id or not ref.subject_id:
        raise ValueError("invalid reply_ref")
    return ref


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _base64url_decode(value: str) -> bytes:
    if not value:
        raise ValueError("empty base64url value")
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _reply_ref_signing_key() -> bytes:
    token = _agent_config.bot.token
    if not token:
        raise RuntimeError("Slack bot token is not configured")
    return token.encode("utf-8")


def _select_agent_route(event: SlackAgentEvent) -> tuple[SlackAgentRoute | None, str]:
    if _agent_config.routes:
        for route in _agent_config.routes:
            if route.match.matches(event):
                return route, ""
        return None, "no_matching_agent_route"
    if _default_agent_route_matches(event):
        return None, ""
    return None, "unsupported_event_type"


def _default_agent_route_matches(event: SlackAgentEvent) -> bool:
    return (
        event.event_type in ASSISTANT_THREAD_EVENT_TYPES
        or (event.event_type == SlackEventType.APP_MENTION)
        or (
            event.event_type == SlackEventType.MESSAGE
            and event.channel_type in DIRECT_MESSAGE_CHANNEL_TYPES
        )
    )


def _agent_config_from_provider_config(
    plugin_name: str, config: dict[str, Any]
) -> SlackAgentConfig:
    agent = _config_dict(config, "agent")
    routes = _agent_routes_from_provider_config(config, agent)
    workflow = _config_dict(config, "workflow")
    bot = _config_dict(config, "bot")
    assistant = _assistant_config_from_provider_config(config, agent)

    return SlackAgentConfig(
        plugin_name=plugin_name.strip() or "slack",
        bot=SlackBotConfig(
            token=_config_string(
                bot, "token", "botToken", "bot_token", "accessToken", "access_token"
            )
            or _config_string(
                config,
                "botToken",
                "bot_token",
                "slackBotToken",
                "slack_bot_token",
            )
        ),
        assistant=assistant,
        workflow_provider=_config_string(
            workflow,
            "provider",
            "providerName",
            "provider_name",
            "workflowProvider",
            "workflow_provider",
        )
        or _config_string(config, "workflowProvider", "workflow_provider"),
        routes=routes,
    )


def _assistant_config_from_provider_config(
    config: dict[str, Any], agent: dict[str, Any]
) -> SlackAssistantConfig:
    assistant = _config_dict(agent, "assistant")
    if not assistant:
        assistant = _config_dict(
            config, "assistant", "slackAssistant", "assistantConfig"
        )
    title, prompts = _assistant_suggested_prompts_from_config(assistant)
    status = _config_string(
        assistant, "status", "initialStatus", "initial_status", "loadingStatus"
    )

    return SlackAssistantConfig(
        enabled=_config_bool(assistant, "enabled", default=False),
        status=status or "is thinking...",
        loading_messages=_config_string_tuple(
            assistant, "loadingMessages", "loading_messages"
        ),
        icon_emoji=_config_string(assistant, "iconEmoji", "icon_emoji"),
        icon_url=_config_string(assistant, "iconUrl", "icon_url"),
        username=_config_string(assistant, "username"),
        suggested_prompts_title=title,
        suggested_prompts=tuple(prompts),
    )


def _assistant_suggested_prompts_from_config(
    assistant: dict[str, Any],
) -> tuple[str, list[dict[str, str]]]:
    suggested_config = _config_dict(assistant, "suggestedPrompts", "suggested_prompts")
    title = _config_string(suggested_config, "title")
    raw_prompts = _config_list(suggested_config, "prompts")
    if not raw_prompts:
        raw_prompts = _config_list(assistant, "prompts")
    if not raw_prompts:
        for key in ("suggestedPrompts", "suggested_prompts"):
            value = assistant.get(key)
            if isinstance(value, list):
                raw_prompts = list(value)
                break
    return title, _normalized_suggested_prompts_or_empty(raw_prompts)


def _normalized_suggested_prompts_or_empty(
    prompts: list[Any],
) -> list[dict[str, str]]:
    try:
        return _normalized_suggested_prompts(
            [prompt for prompt in prompts if isinstance(prompt, dict)]
        )
    except ValueError:
        return []


def _agent_routes_from_provider_config(
    config: dict[str, Any], agent: dict[str, Any]
) -> tuple[SlackAgentRoute, ...]:
    raw_routes = _config_list(agent, "routes")
    if not raw_routes:
        raw_routes = _config_list(config, "agentRoutes", "agent_routes")
    routes: list[SlackAgentRoute] = []
    for index, raw_route in enumerate(raw_routes, start=1):
        if isinstance(raw_route, dict):
            routes.append(_agent_route_from_config(raw_route, index))
    return tuple(routes)


def _agent_route_from_config(config: dict[str, Any], index: int) -> SlackAgentRoute:
    return SlackAgentRoute(
        id=_config_string(config, "id", "name") or f"route_{index}",
        match=_agent_route_match_from_config(_config_dict(config, "match")),
    )


def _agent_route_match_from_config(config: dict[str, Any]) -> SlackAgentRouteMatch:
    return SlackAgentRouteMatch(
        team_ids=_config_string_tuple(
            config, "team", "teams", "teamId", "teamIds", "team_id", "team_ids"
        ),
        channel_ids=_config_string_tuple(
            config,
            "channel",
            "channels",
            "channelId",
            "channelIds",
            "channel_id",
            "channel_ids",
        ),
        channel_types=_lower_tuple(
            _config_string_tuple(
                config, "channelType", "channelTypes", "channel_type", "channel_types"
            )
        ),
        event_types=_lower_tuple(
            _config_string_tuple(
                config, "eventType", "eventTypes", "event_type", "event_types"
            )
        ),
        user_ids=_config_string_tuple(
            config, "user", "users", "userId", "userIds", "user_id", "user_ids"
        ),
    )


def _event_file_ids(event: SlackAgentEvent) -> list[str]:
    return [
        file_id
        for file_id in (
            str(file_data.get("id") or "").strip() for file_data in event.files
        )
        if file_id
    ]


def _dict_to_struct(data: dict[str, Any]) -> Any:
    struct = struct_pb2.Struct()
    struct.update(data)
    return struct


def _config_string(config: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = config.get(key)
        if isinstance(value, str):
            return value.strip()
    return ""


def _config_dict(config: dict[str, Any], *keys: str) -> dict[str, Any]:
    for key in keys:
        value = config.get(key)
        if isinstance(value, dict):
            return dict(value)
    return {}


def _config_list(config: dict[str, Any], *keys: str) -> list[Any]:
    for key in keys:
        value = config.get(key)
        if isinstance(value, list):
            return list(value)
    return []


def _config_bool(config: dict[str, Any], *keys: str, default: bool) -> bool:
    for key in keys:
        value = config.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on"}:
                return True
            if normalized in {"0", "false", "no", "off"}:
                return False
    return default


def _config_string_tuple(config: dict[str, Any], *keys: str) -> tuple[str, ...]:
    values: list[str] = []
    for key in keys:
        value = config.get(key)
        if isinstance(value, str):
            value = value.strip()
            if value:
                values.append(value)
            break
        if isinstance(value, list):
            for item in value:
                if not isinstance(item, str):
                    continue
                item_value = item.strip()
                if item_value:
                    values.append(item_value)
            break
    return tuple(dict.fromkeys(values))


def _lower_tuple(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(value.lower() for value in values)


def _auth_test(access_token: str) -> PostConnectMetadata:
    request = urllib.request.Request(
        SLACK_AUTH_TEST_URL,
        data=b"",
        headers={"Authorization": f"Bearer {access_token}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"slack auth.test HTTP error (status {err.code}): {body}"
        ) from err
    except urllib.error.URLError as err:
        raise RuntimeError(f"slack auth.test request failed: {err.reason}") from err
    except json.JSONDecodeError as err:
        raise RuntimeError("slack auth.test returned invalid JSON") from err

    if not isinstance(payload, dict):
        raise RuntimeError("slack auth.test returned invalid response")
    if not payload.get("ok"):
        error = payload.get("error")
        if not isinstance(error, str) or not error:
            error = "unknown_error"
        raise RuntimeError(f"slack auth.test failed: {error}")

    team_id = payload.get("team_id")
    user_id = payload.get("user_id")
    if not isinstance(team_id, str) or not isinstance(user_id, str):
        raise RuntimeError("Slack auth.test did not return team_id and user_id")
    return {"team_id": team_id, "user_id": user_id}


def _bad_request(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _server_error(message: str) -> ErrorResponse:
    return gestalt.Response(
        status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message}
    )
