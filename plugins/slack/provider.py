from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from http import HTTPStatus
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
SLACK_EXTERNAL_IDENTITY_TYPE = "slack_identity"
EXTERNAL_IDENTITY_RESOURCE_TYPE = "external_identity"
EXTERNAL_IDENTITY_ASSUME_ACTION = "assume"
EXTERNAL_IDENTITY_TYPE_METADATA_KEY = "gestalt.external_identity.type"
EXTERNAL_IDENTITY_ID_METADATA_KEY = "gestalt.external_identity.id"
DEFAULT_AGENT_SYSTEM_PROMPT_TEMPLATE = """
You are a Slack bot running inside Gestalt.
Use the available Gestalt tools under the Slack user's authorization.
When you answer the Slack user, call {post_message_tool} with the provided
channel and reply_thread_ts. Omit thread_ts when reply_thread_ts is empty.
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
class SlackAgentConfig:
    plugin_name: str = "slack"
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

    run_request = _build_agent_run_request(event, route)
    try:
        with req.agent_manager() as agent_manager:
            managed = agent_manager.run(run_request)
    except Exception as err:
        return _server_error(f"failed to start agent run: {err}")

    run = managed.run
    return {
        "ok": True,
        "agent_run_id": run.id,
        "agent_provider": managed.provider_name,
        "status": agent_pb2.AgentRunStatus.Name(run.status) if run.status else "",
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


def _build_agent_run_request(
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
    request = agent_pb2.AgentManagerRunRequest(
        provider_name=_agent_provider(route),
        model=_agent_model(route),
        messages=[
            agent_pb2.AgentMessage(role="system", text=_agent_system_prompt(route)),
            agent_pb2.AgentMessage(role="user", text=_agent_user_prompt(event)),
        ],
        tool_source=agent_pb2.AGENT_TOOL_SOURCE_MODE_INHERIT_INVOKES,
        session_ref=_agent_session_ref(event),
        idempotency_key=_agent_idempotency_key(event),
    )
    request.metadata.CopyFrom(metadata)
    provider_options = _agent_provider_options(route)
    if provider_options:
        request.provider_options.CopyFrom(_dict_to_struct(provider_options))
    return request


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
            post_message_tool=f"{_agent_config.plugin_name}.chat.postMessage"
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

    return SlackAgentConfig(
        plugin_name=plugin_name.strip() or "slack",
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


def _agent_user_prompt(event: SlackAgentEvent) -> str:
    return "\n".join(
        [
            "Slack event:",
            f"team_id: {event.team_id}",
            f"channel_id: {event.channel_id}",
            f"user_id: {event.user_id}",
            f"message_ts: {event.message_ts}",
            f"thread_ts: {event.thread_ts}",
            f"reply_thread_ts: {event.reply_thread_ts}",
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


def _agent_idempotency_key(event: SlackAgentEvent) -> str:
    if event.event_id:
        return f"slack:event:{event.event_id}"
    return f"slack:event:{event.team_id}:{event.channel_id}:{event.message_ts}:{event.user_id}"


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
