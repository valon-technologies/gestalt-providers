from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from dataclasses import dataclass, field
from http import HTTPStatus
import time
from typing import Any, TypeAlias
import urllib.error
import urllib.request

import gestalt
from google.protobuf import json_format
from google.protobuf import struct_pb2 as _struct_pb2
from gestalt.gen.v1 import agent_pb2 as _agent_pb2
from gestalt.gen.v1 import authorization_pb2 as _authorization_pb2

from internals import (
    find_user_mentions,
    get_message,
    get_thread_participants,
    parse_message_url,
    slack_base_url,
)

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse
PostConnectMetadata: TypeAlias = dict[str, str]

agent_pb2: Any = _agent_pb2
authorization_pb2: Any = _authorization_pb2
struct_pb2: Any = _struct_pb2

SLACK_AUTH_TEST_URL = "https://slack.com/api/auth.test"
SLACK_DEFAULT_CONNECTION = "default"
SLACK_EVENT_OPERATION = "events.handle"
SLACK_REPLY_OPERATION = "events.reply"
SLACK_EXTERNAL_IDENTITY_TYPE = "slack_identity"
SLACK_REPLY_REF_TTL_SECONDS = 60 * 60
EXTERNAL_IDENTITY_RESOURCE_TYPE = "external_identity"
EXTERNAL_IDENTITY_ASSUME_ACTION = "assume"
EXTERNAL_IDENTITY_TYPE_METADATA_KEY = "gestalt.external_identity.type"
EXTERNAL_IDENTITY_ID_METADATA_KEY = "gestalt.external_identity.id"
DEFAULT_AGENT_SYSTEM_PROMPT_TEMPLATE = """
You are a Slack bot running inside Gestalt.
Use the available Gestalt tools under the Slack user's authorization.
When you answer the Slack user, call {reply_tool} with the reply_ref exactly
as provided and the Slack message text. Do not use raw Slack message-posting
tools for the final reply.
After posting to Slack, return a concise final summary of what you did.
""".strip()

plugin = gestalt.Plugin("slack")


@dataclass(slots=True)
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


@dataclass(slots=True)
class SlackAgentRoute:
    id: str = ""
    match: SlackAgentRouteMatch = field(default_factory=SlackAgentRouteMatch)
    agent_provider: str = ""
    agent_model: str = ""
    agent_system_prompt: str = ""
    agent_provider_options: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SlackBotConfig:
    token: str = ""


@dataclass(slots=True)
class SlackAgentConfig:
    plugin_name: str = "slack"
    bot: SlackBotConfig = field(default_factory=SlackBotConfig)
    agent_provider: str = ""
    agent_model: str = ""
    agent_system_prompt: str = ""
    agent_provider_options: dict[str, Any] = field(default_factory=dict)
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


@dataclass(frozen=True, slots=True)
class SlackReplyRef:
    team_id: str
    channel_id: str
    message_ts: str
    reply_thread_ts: str
    event_id: str
    subject_id: str
    expires_at: int


_agent_config = SlackAgentConfig()


@plugin.configure
def configure(name: str, config: dict[str, Any]) -> None:
    global _agent_config

    _agent_config = _agent_config_from_provider_config(name, config)


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


class SlackEventReplyInput(gestalt.Model):
    reply_ref: str = gestalt.field(description="Opaque Slack event reply reference")
    text: str = gestalt.field(description="Slack message text to send")


@gestalt.post_connect
def post_connect(token: gestalt.ConnectedToken) -> PostConnectMetadata:
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


@plugin.http_subject
def resolve_http_subject(
    request: gestalt.HTTPSubjectRequest, context: gestalt.Request
) -> gestalt.Subject | None:
    payload = _json_payload_from_http_request(request)
    event, ignored_reason = _slack_agent_event_from_payload(payload)
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


@gestalt.operation(
    id=SLACK_EVENT_OPERATION,
    method="POST",
    description="Handle Slack Events API callbacks and delegate supported user events to a Gestalt agent",
    visible=False,
)
def slack_events_handle(input: dict[str, Any], req: gestalt.Request) -> OperationResult:
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

    try:
        reply_ref = _sign_reply_ref(event, req.subject.id)
        with req.agent_manager() as agent_manager:
            session_request = _build_agent_session_request(event, route)
            session = agent_manager.create_session(session_request)
            session_id = str(session.id or "").strip()
            if not session_id:
                return _server_error("agent manager did not return a session id")

            turn_request = _build_agent_turn_request(
                event, route, session_id, reply_ref
            )
            turn = agent_manager.create_turn(turn_request)
    except Exception as err:
        return _server_error(f"failed to start agent turn: {err}")

    return {
        "ok": True,
        "agent_session_id": session_id,
        "agent_turn_id": turn.id,
        "agent_provider": session.provider_name or _agent_provider(route),
        "status": _agent_execution_status_name(turn.status),
    }


@gestalt.operation(
    id=SLACK_REPLY_OPERATION,
    method="POST",
    description="Reply to the Slack event that started an agent turn",
    visible=False,
)
def slack_events_reply(
    input: SlackEventReplyInput, req: gestalt.Request
) -> OperationResult:
    if not req.subject.id or req.subject.id.startswith("system:"):
        return gestalt.Response(
            status=HTTPStatus.FORBIDDEN, body={"error": "Slack user is not linked"}
        )
    if not _agent_config.bot.token:
        return gestalt.Response(
            status=HTTPStatus.PRECONDITION_FAILED,
            body={"error": "Slack bot token is not configured"},
        )

    text = input.text.strip()
    if not text:
        return _bad_request("text is required")

    try:
        reply_ref = _verify_reply_ref(input.reply_ref, req.subject.id)
        result = _post_slack_message(
            _agent_config.bot.token,
            channel=reply_ref.channel_id,
            text=text,
            thread_ts=reply_ref.reply_thread_ts,
        )
    except ValueError as err:
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})
    except RuntimeError as err:
        return _server_error(str(err))

    return {
        "ok": True,
        "channel": str(result.get("channel") or reply_ref.channel_id),
        "ts": str(result.get("ts") or ""),
        "thread_ts": reply_ref.reply_thread_ts,
    }


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
    except RuntimeError as err:
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
    except RuntimeError as err:
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
    except RuntimeError as err:
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
    except UnicodeDecodeError, json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _slack_agent_event_from_payload(
    payload: dict[str, Any],
) -> tuple[SlackAgentEvent | None, str]:
    callback_type = str(payload.get("type") or "").strip()
    if callback_type == "url_verification":
        return None, "url_verification"
    if callback_type != "event_callback":
        return None, "unsupported_callback_type"

    event = payload.get("event")
    if not isinstance(event, dict):
        return None, "missing_event"
    if _is_ignored_event(event):
        return None, "ignored_event"

    event_type = str(event.get("type") or "").strip()
    channel_type = str(event.get("channel_type") or "").strip()
    if event_type not in {"app_mention", "message"}:
        return None, "unsupported_event_type"

    team_id = _slack_team_id(payload, event)
    user_id = str(event.get("user") or "").strip()
    channel_id = str(event.get("channel") or "").strip()
    text = str(event.get("text") or "").strip()
    message_ts = str(event.get("ts") or event.get("event_ts") or "").strip()
    thread_ts = str(event.get("thread_ts") or "").strip()
    reply_thread_ts = thread_ts
    if event_type == "app_mention" and not reply_thread_ts:
        reply_thread_ts = message_ts

    if not user_id:
        return None, "missing_user"
    if not channel_id:
        return None, "missing_channel"
    if not text:
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
        ),
        "",
    )


def _is_url_verification(payload: dict[str, Any]) -> bool:
    return str(payload.get("type") or "").strip() == "url_verification"


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


def _post_slack_message(
    token: str, *, channel: str, text: str, thread_ts: str
) -> dict[str, Any]:
    payload = {
        "channel": channel,
        "text": text,
    }
    if thread_ts:
        payload["thread_ts"] = thread_ts
    request = urllib.request.Request(
        f"{slack_base_url()}/chat.postMessage",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"slack chat.postMessage HTTP error (status {err.code}): {body}"
        ) from err
    except urllib.error.URLError as err:
        raise RuntimeError(
            f"slack chat.postMessage request failed: {err.reason}"
        ) from err
    except json.JSONDecodeError as err:
        raise RuntimeError("slack chat.postMessage returned invalid JSON") from err

    if not isinstance(response_payload, dict):
        raise RuntimeError("slack chat.postMessage returned invalid response")
    if not response_payload.get("ok"):
        error = response_payload.get("error")
        if not isinstance(error, str) or not error:
            error = "unknown_error"
        raise RuntimeError(f"slack chat.postMessage failed: {error}")
    return response_payload


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
    return event.event_type == "app_mention" or (
        event.event_type == "message" and event.channel_type in {"im", "mpim"}
    )


def _build_agent_session_request(
    event: SlackAgentEvent,
    route: SlackAgentRoute | None,
) -> Any:
    request = agent_pb2.AgentManagerCreateSessionRequest(
        provider_name=_agent_provider(route),
        model=_agent_model(route),
        client_ref=_agent_session_ref(event),
        idempotency_key=_agent_session_idempotency_key(event),
    )
    request.metadata.CopyFrom(_agent_session_metadata(event))
    return request


def _build_agent_turn_request(
    event: SlackAgentEvent,
    route: SlackAgentRoute | None,
    session_id: str,
    reply_ref: str,
) -> Any:
    request = agent_pb2.AgentManagerCreateTurnRequest(
        session_id=session_id,
        model=_agent_model(route),
        messages=[
            agent_pb2.AgentMessage(role="system", text=_agent_system_prompt(route)),
            agent_pb2.AgentMessage(
                role="user", text=_agent_user_prompt(event, reply_ref)
            ),
        ],
        tool_source=_agent_tool_source_native_search(),
        tool_refs=[
            _agent_tool_ref(
                plugin=_agent_config.plugin_name,
                operation=SLACK_REPLY_OPERATION,
            )
        ],
        idempotency_key=_agent_turn_idempotency_key(event),
    )
    request.metadata.CopyFrom(_agent_metadata(event, route))
    provider_options = _agent_provider_options(route)
    if provider_options:
        request.provider_options.CopyFrom(_dict_to_struct(provider_options))
    return request


def _agent_tool_source_native_search() -> int:
    native_value = getattr(agent_pb2, "AGENT_TOOL_SOURCE_MODE_NATIVE_SEARCH", None)
    if native_value is not None:
        return int(native_value)
    return int(agent_pb2.AGENT_TOOL_SOURCE_MODE_EXPLICIT)


def _agent_tool_ref(*, plugin: str, operation: str) -> Any:
    fields = agent_pb2.AgentToolRef.DESCRIPTOR.fields_by_name
    kwargs = {"operation": operation}
    if "plugin" in fields:
        kwargs["plugin"] = plugin
    else:
        kwargs["plugin_name"] = plugin
    return agent_pb2.AgentToolRef(**kwargs)


def _agent_session_metadata(event: SlackAgentEvent) -> Any:
    root_ts = event.thread_ts or event.message_ts
    return _dict_to_struct(
        {
            "slack": {
                "team_id": event.team_id,
                "channel_id": event.channel_id,
                "channel_type": event.channel_type,
                "root_message_ts": root_ts,
                "session_ref": _agent_session_ref(event),
            }
        }
    )


def _agent_metadata(
    event: SlackAgentEvent,
    route: SlackAgentRoute | None,
) -> Any:
    metadata = _dict_to_struct(
        {
            "slack": {
                "callback_type": event.callback_type,
                "event_type": event.event_type,
                "event_id": event.event_id,
                "team_id": event.team_id,
                "user_id": event.user_id,
                "channel_id": event.channel_id,
                "channel_type": event.channel_type,
                "message_ts": event.message_ts,
                "thread_ts": event.thread_ts,
                "reply_thread_ts": event.reply_thread_ts,
                "agent_route_id": route.id if route is not None else "",
            }
        }
    )
    return metadata


def _agent_provider(route: SlackAgentRoute | None) -> str:
    if route is not None and route.agent_provider:
        return route.agent_provider
    return _agent_config.agent_provider


def _agent_model(route: SlackAgentRoute | None) -> str:
    if route is not None and route.agent_model:
        return route.agent_model
    return _agent_config.agent_model


def _agent_provider_options(route: SlackAgentRoute | None) -> dict[str, Any]:
    options = dict(_agent_config.agent_provider_options)
    if route is not None and route.agent_provider_options:
        options.update(route.agent_provider_options)
    return options


def _agent_system_prompt(route: SlackAgentRoute | None) -> str:
    parts = [
        DEFAULT_AGENT_SYSTEM_PROMPT_TEMPLATE.format(
            reply_tool=f"{_agent_config.plugin_name}.{SLACK_REPLY_OPERATION}"
        )
    ]
    if _agent_config.agent_system_prompt:
        parts.append(_agent_config.agent_system_prompt.strip())
    if route is not None and route.agent_system_prompt:
        parts.append(route.agent_system_prompt.strip())
    return "\n\n".join(parts)


def _agent_config_from_provider_config(
    plugin_name: str, config: dict[str, Any]
) -> SlackAgentConfig:
    agent = _config_dict(config, "agent")
    provider = _config_string(agent, "provider", "agentProvider", "agent_provider")
    model = _config_string(agent, "model", "agentModel", "agent_model")
    system_prompt = _config_string(
        agent, "systemPrompt", "system_prompt", "agentSystemPrompt", "prompt"
    )
    provider_options = _config_dict(
        agent, "providerOptions", "provider_options", "agentProviderOptions"
    )
    routes = _agent_routes_from_provider_config(config, agent)
    bot = _config_dict(config, "bot")

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
        agent_provider=provider
        or _config_string(config, "agentProvider", "agent_provider"),
        agent_model=model or _config_string(config, "agentModel", "agent_model"),
        agent_system_prompt=system_prompt
        or _config_string(config, "agentSystemPrompt", "agent_system_prompt", "prompt"),
        agent_provider_options=provider_options
        or _config_dict(config, "agentProviderOptions", "agent_provider_options"),
        routes=routes,
    )


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
    agent = _config_dict(config, "agent")
    provider = _config_string(agent, "provider", "agentProvider", "agent_provider")
    model = _config_string(agent, "model", "agentModel", "agent_model")
    system_prompt = _config_string(
        agent, "systemPrompt", "system_prompt", "agentSystemPrompt", "prompt"
    )
    provider_options = _config_dict(
        agent, "providerOptions", "provider_options", "agentProviderOptions"
    )

    return SlackAgentRoute(
        id=_config_string(config, "id", "name") or f"route_{index}",
        match=_agent_route_match_from_config(_config_dict(config, "match")),
        agent_provider=provider
        or _config_string(config, "provider", "agentProvider", "agent_provider"),
        agent_model=model
        or _config_string(config, "model", "agentModel", "agent_model"),
        agent_system_prompt=system_prompt
        or _config_string(config, "systemPrompt", "agentSystemPrompt", "prompt"),
        agent_provider_options=provider_options
        or _config_dict(config, "providerOptions", "agentProviderOptions"),
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


def _agent_user_prompt(event: SlackAgentEvent, reply_ref: str) -> str:
    return "\n".join(
        [
            "Slack event:",
            f"team_id: {event.team_id}",
            f"channel_id: {event.channel_id}",
            f"user_id: {event.user_id}",
            f"message_ts: {event.message_ts}",
            f"thread_ts: {event.thread_ts}",
            f"reply_thread_ts: {event.reply_thread_ts}",
            f"reply_ref: {reply_ref}",
            "",
            "Message text:",
            event.text,
        ]
    )


def _agent_session_ref(event: SlackAgentEvent) -> str:
    if event.channel_type in {"im", "mpim"} and not event.thread_ts:
        return f"slack:{event.team_id}:{event.channel_id}"
    root_ts = event.thread_ts or event.message_ts
    return f"slack:{event.team_id}:{event.channel_id}:{root_ts}"


def _agent_session_idempotency_key(event: SlackAgentEvent) -> str:
    return f"slack:session:{_agent_session_ref(event)}"


def _agent_turn_idempotency_key(event: SlackAgentEvent) -> str:
    if event.event_id:
        return f"slack:event:{event.event_id}"
    return f"slack:event:{event.team_id}:{event.channel_id}:{event.message_ts}:{event.user_id}"


def _agent_execution_status_name(status: int) -> str:
    if not status:
        return ""
    try:
        return agent_pb2.AgentExecutionStatus.Name(status)
    except ValueError:
        return str(status)


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
