from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
import uuid
from http import HTTPStatus
from typing import Any, Iterable, TypeAlias, cast

import gestalt

from .agent_links import agent_session_url
from .client import SlackAPIError, SlackClientError
from .config import agent_config_from_provider_config, normalize_suggested_prompts
from .helpers import map_field, map_slice, string_field
from .models import (
    ASSISTANT_THREAD_EVENT_TYPES,
    DIRECT_MESSAGE_CHANNEL_TYPES,
    SUPPORTED_EVENT_TYPES,
    SlackAgentConfig,
    SlackAgentEvent,
    SlackAgentRoute,
    SlackAgentStep,
    SlackAgentToolRef,
    SlackCallbackType,
    SlackChannelType,
    SlackEventPublishCallback,
    SlackEventPublishRoute,
    SlackEventType,
    SlackInteractionAction,
    SlackInteractionActionStyle,
    SlackInteractionRef,
    SlackReplyRef,
)
from .models import SlackAcknowledgementConfig as SlackAcknowledgementConfig  # noqa: F401
from .models import SlackAgentRouteMatch as SlackAgentRouteMatch  # noqa: F401
from .models import SlackAssistantConfig as SlackAssistantConfig  # noqa: F401
from .models import SlackBotConfig as SlackBotConfig  # noqa: F401
from .models import SlackThreadContextConfig as SlackThreadContextConfig  # noqa: F401
from .models import SlackWorkflowConfig as SlackWorkflowConfig  # noqa: F401
from .operations import (
    add_reaction,
    append_stream,
    delete_message,
    find_message_urls,
    get_thread_context,
    post_message,
    remove_reaction,
    set_assistant_thread_status,
    set_assistant_thread_suggested_prompts,
    set_assistant_thread_title,
    start_stream,
    stop_stream,
    update_message,
)

ErrorResponse: TypeAlias = gestalt.Response[dict[str, Any]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse

logger = logging.getLogger(__name__)

SLACK_EVENT_WORKFLOW_SIGNAL = "slack.event"
SLACK_INTERACTION_WORKFLOW_SIGNAL = "slack.interaction"
SLACK_EVENT_OPERATION = "events.handle"
SLACK_INTERACTION_HANDLE_OPERATION = "interactions.handle"
SLACK_INTERACTION_REQUEST_OPERATION = "interactions.request"
SLACK_REPLY_OPERATION = "events.reply"
SLACK_SESSION_STARTED_REPLY_OPERATION = "events.replySessionStarted"
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
SLACK_MESSAGE_OPERATION = "conversations.getMessage"
SLACK_CONTEXT_OPERATION = "conversations.getThreadContext"
SLACK_FILE_GET_OPERATION = "files.get"
MAX_PROMPT_MESSAGE_URLS = 5
SLACK_SIGNAL_CONTEXT_PROMPT = "\n".join(
    [
        "Current Slack request:",
        "${signalPayload.user_prompt}",
        (
            "Treat Background thread context inside this request as supporting "
            "context only, not as the current request."
        ),
    ]
)
SLACK_EXTERNAL_IDENTITY_TYPE = "slack_identity"
SLACK_REPLY_REF_TTL_SECONDS = 60 * 60
SLACK_INTERACTION_REF_TTL_SECONDS = 24 * 60 * 60
EXTERNAL_IDENTITY_RESOURCE_TYPE = "external_identity"
EXTERNAL_IDENTITY_ASSUME_ACTION = "assume"
DEFAULT_AGENT_SYSTEM_PROMPT_TEMPLATE = """
You are a Slack bot running inside Gestalt.
Use the available Gestalt tools under the Slack user's authorization.
Use {status_tool} only when you intentionally want a visible progress message
in the Slack thread; reuse the returned status_ts to update or delete the same
status message.
Use {context_tool} when you need the current Slack thread history, participants,
or attached files. Use {file_tool} to read Slack file contents or image bytes.
For Slack message permalinks, call the Slack message or thread-context tool with
the URL directly. If the current Slack request includes a "Slack message
permalink tools" section, use the listed operation and URL input exactly. Do not
enumerate channels or scan channel history just to resolve a Slack permalink.
Use the current signal's reply_ref only for Slack event helper tools that require
it, such as visible progress, reactions, or interactions.
When you answer the Slack user, return the complete Slack message body as your
final assistant answer. Gestalt will deliver that final answer to Slack.
Do not use raw Slack message-posting tools for the final reply.
""".strip()


def _log_context(**fields: Any) -> str:
    parts: list[str] = []
    for key, value in fields.items():
        text = str(value or "").strip()
        if text:
            parts.append(f"{key}={text}")
    return " ".join(parts)


def _slack_event_log_context(
    event: SlackAgentEvent,
    req: gestalt.Request,
    route: SlackAgentRoute | None,
) -> str:
    return _log_context(
        slack_event_type=event.event_type,
        slack_event_id=event.event_id,
        slack_team_id=event.team_id,
        slack_channel_id=event.channel_id,
        slack_channel_type=event.channel_type,
        slack_user_id=event.user_id,
        slack_bot_id=event.bot_id,
        slack_message_ts=event.message_ts,
        slack_thread_ts=event.thread_ts,
        slack_reply_thread_ts=event.reply_thread_ts,
        slack_route_id=route.id if route else "",
        subject_id=req.subject.id.strip(),
        workflow_key=_agent_session_ref(event),
    )


def _slack_interaction_log_context(
    payload: dict[str, Any],
    req: gestalt.Request,
    *,
    verified_ref: SlackInteractionRef | None = None,
    selected_action: dict[str, Any] | None = None,
    route: SlackAgentRoute | None = None,
) -> str:
    container = map_field(payload, "container")
    channel = map_field(payload, "channel")
    user = map_field(payload, "user")
    return _log_context(
        slack_team_id=verified_ref.team_id
        if verified_ref
        else _interaction_team_id(payload),
        slack_channel_id=(
            verified_ref.channel_id
            if verified_ref
            else string_field(channel, "id") or string_field(container, "channel_id")
        ),
        slack_channel_type=verified_ref.channel_type if verified_ref else "",
        slack_user_id=verified_ref.user_id
        if verified_ref
        else string_field(user, "id"),
        slack_message_ts=(
            verified_ref.message_ts
            if verified_ref
            else string_field(container, "message_ts")
        ),
        slack_reply_thread_ts=verified_ref.reply_thread_ts if verified_ref else "",
        slack_action_id=(
            verified_ref.action_id
            if verified_ref
            else string_field(selected_action or {}, "action_id")
        ),
        slack_route_id=route.id
        if route
        else (verified_ref.route_id if verified_ref else ""),
        subject_id=req.subject.id.strip(),
        workflow_key=verified_ref.workflow_key if verified_ref else "",
    )


def _workflow_signal_fields_log_context(fields: dict[str, Any]) -> str:
    return _log_context(
        workflow_provider=fields["workflow_provider"],
        workflow_run_id=fields["workflow_run_id"],
        workflow_key=fields["workflow_key"],
        workflow_signal_id=fields["workflow_signal_id"],
        workflow_started_run=fields["started_run"],
        workflow_status=fields["status"],
    )


def _workflow_log_context(req: gestalt.Request) -> str:
    workflow = req.workflow_run_context()
    signal = workflow.latest_signal
    return _log_context(
        workflow_provider=workflow.provider,
        workflow_run_id=workflow.run_id,
        workflow_definition_id=workflow.metadata.get("definition_id"),
        workflow_trigger_kind=workflow.trigger.kind,
        workflow_schedule_id=workflow.trigger.schedule_id,
        workflow_event_trigger_id=workflow.trigger.trigger_id,
        workflow_signal_id=signal.id if signal is not None else "",
        workflow_signal_name=signal.name if signal is not None else "",
        idempotency_key=req.idempotency_key,
    )


def _log_body(value: Any, *, max_bytes: int = 2048) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, sort_keys=True, separators=(",", ":"))
        except TypeError:
            text = str(value)
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    truncated = encoded[: max(0, max_bytes - 3)]
    while truncated and (truncated[-1] & 0xC0) == 0x80:
        truncated = truncated[:-1]
    return truncated.decode("utf-8", errors="ignore") + "..."


def _slack_delivery_log_context(
    req: gestalt.Request,
    operation: str,
    *,
    reply_ref: str,
    verified_ref: SlackReplyRef | None = None,
    text: str = "",
    session_id: str = "",
) -> str:
    return _log_context(
        operation=operation,
        subject_id=req.subject.id.strip(),
        credential_subject_id=req.credential.subject_id,
        agent_subject_id=req.agent_subject.id,
        slack_team_id=verified_ref.team_id if verified_ref else "",
        slack_channel_id=verified_ref.channel_id if verified_ref else "",
        slack_channel_type=verified_ref.channel_type if verified_ref else "",
        slack_user_id=verified_ref.user_id if verified_ref else "",
        slack_message_ts=verified_ref.message_ts if verified_ref else "",
        slack_reply_thread_ts=verified_ref.reply_thread_ts if verified_ref else "",
        slack_route_id=verified_ref.route_id if verified_ref else "",
        workflow_key=_reply_ref_workflow_key(verified_ref) if verified_ref else "",
        workflow_context=_workflow_log_context(req),
        reply_ref_sha256=_sha256_log_value(reply_ref),
        text_bytes=len(text.encode("utf-8")) if text else "",
        text_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest() if text else "",
        session_id=session_id,
    )


def _workflow_signal_response_fields(
    response: gestalt.WorkflowRunSignal,
    fallback_workflow_key: str = "",
    fallback_provider_name: str = "",
) -> dict[str, Any]:
    return {
        "workflow_provider": response.provider_name
        or fallback_provider_name
        or _agent_config.workflow.provider_name,
        "workflow_run_id": response.run.id if response.run is not None else "",
        "workflow_key": response.workflow_key
        or (response.run.workflow_key if response.run is not None else "")
        or fallback_workflow_key,
        "workflow_signal_id": response.signal.id if response.signal is not None else "",
        "started_run": response.started_run,
        "status": _workflow_run_status_name(
            response.run.status if response.run is not None else 0
        ),
    }


def _workflow_dispatched_ack_fallback() -> dict[str, Any]:
    return {
        "ok": True,
        "workflow_dispatched": True,
        "workflow_acknowledgement_failed": True,
    }


def _workflow_handoff_log_context(
    workflow_request: gestalt.WorkflowSignalOrStartRun | None,
    err: BaseException | None = None,
) -> str:
    idempotency_key = workflow_request.idempotency_key.strip() if workflow_request is not None else ""
    return _log_context(
        workflow_provider=workflow_request.provider_name if workflow_request is not None else "",
        idempotency_key_sha256=_sha256_log_value(idempotency_key),
        error_type=type(err).__name__ if err else "",
        error=_log_body(str(err), max_bytes=512) if err else "",
    )


def _sha256_log_value(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


_agent_config = SlackAgentConfig()


def configure_agent(name: str, config: dict[str, Any]) -> None:
    global _agent_config

    _agent_config = agent_config_from_provider_config(name, config)


def resolve_slack_http_subject(
    request: gestalt.HTTPSubjectRequest, context: gestalt.Request
) -> gestalt.Subject | None:
    payload = _json_payload_from_http_request(request)
    publish_event, _publish_ignored_reason = _slack_publish_callback_from_payload(
        payload
    )
    has_publish_route = bool(
        publish_event is not None and _matching_publish_routes(publish_event)
    )
    event, _ignored_reason = _slack_agent_event_from_payload(payload)
    if event is not None:
        route, ignored_reason = _select_agent_route(event)
        if ignored_reason:
            return None
        subject = _agent_route_run_as_subject(route)
        if subject is not None:
            return subject
        team_id = event.team_id
        user_id = event.user_id
    else:
        interaction = _slack_interaction_payload_from_input(payload)
        if interaction is None:
            return None
        subject = _interaction_route_run_as_subject(interaction)
        if subject is not None:
            return subject
        team_id = _interaction_team_id(interaction)
        user_id = _interaction_user_id(interaction)

    if not team_id or not user_id:
        if has_publish_route:
            return None
        raise gestalt.http_subject_error(
            HTTPStatus.BAD_REQUEST, "Slack request is missing team_id or user"
        )

    subject = _resolve_slack_subject(
        context.authorization(),
        team_id=team_id,
        user_id=user_id,
    )
    if subject is None:
        return None
    return subject


def handle_slack_event(input: dict[str, Any], req: gestalt.Request) -> OperationResult:
    if _is_url_verification(input):
        return {"challenge": str(input.get("challenge") or "")}

    event, ignored_reason = _slack_agent_event_from_payload(input)
    if event is None:
        publish_response = _publish_matching_workflow_events(input, req)
        if isinstance(publish_response, gestalt.Response):
            return publish_response
        if publish_response is not None:
            return publish_response
        return {"ok": True, "ignored": ignored_reason}

    route, ignored_reason = _select_agent_route(event)
    if ignored_reason:
        publish_response = _publish_matching_workflow_events(input, req)
        if isinstance(publish_response, gestalt.Response):
            return publish_response
        if publish_response is not None:
            return publish_response
        _log_ignored_slack_event(event, req, ignored_reason)
        return {"ok": True, "ignored": ignored_reason}
    if not _agent_dispatch_configured(route):
        publish_response = _publish_matching_workflow_events(input, req)
        if isinstance(publish_response, gestalt.Response):
            return publish_response
        if publish_response is not None:
            return publish_response

    log_context = _slack_event_log_context(event, req, route)
    subject_id = req.subject.id.strip()
    if not _slack_event_subject_allowed(event, route, subject_id):
        publish_response = _publish_matching_workflow_events(input, req)
        if isinstance(publish_response, gestalt.Response):
            return publish_response
        if publish_response is not None:
            return publish_response
        logger.warning("rejected Slack event without linked subject %s", log_context)
        if _should_notify_unlinked_slack_user_for_event(event):
            _notify_unlinked_slack_user_for_event(event, req)
        return {"ok": True, "unlinked": True}
    if not _agent_config.bot.token:
        logger.error("Slack event bot token is not configured %s", log_context)
        return gestalt.Response(
            status=HTTPStatus.PRECONDITION_FAILED,
            body={"error": "Slack bot token is not configured"},
        )
    if event.event_type in ASSISTANT_THREAD_EVENT_TYPES:
        return _handle_assistant_thread_event(event, route)

    acknowledgement_reaction_error = ""
    assistant_status_error = ""
    workflow_provider = _workflow_provider_name(route)
    workflow_request: gestalt.WorkflowSignalOrStartRun | None = None
    try:
        reply_ref = _sign_reply_ref(event, subject_id, route)
        if not workflow_provider:
            logger.error("Slack workflow provider is not configured %s", log_context)
            return gestalt.Response(
                status=HTTPStatus.PRECONDITION_FAILED,
                body={"error": "Slack workflow provider is not configured"},
            )
        workflow_request = _build_workflow_signal_or_start_request(
            event, route, reply_ref
        )
        logger.info(
            f"attempting Slack event workflow signal {log_context} "
            f"{_workflow_handoff_log_context(workflow_request)}"
        )
        with req.workflows() as workflows:
            workflow_response = workflows.signal_or_start_run(workflow_request)
    except Exception as err:
        logger.exception(
            f"failed to signal Slack event workflow {log_context} "
            f"{_workflow_handoff_log_context(workflow_request, err)}"
        )
        return _server_error(f"failed to signal workflow run: {err}")

    try:
        fields = _workflow_signal_response_fields(
            workflow_response,
            fallback_workflow_key=_agent_session_ref(event),
            fallback_provider_name=workflow_provider,
        )
        logger.info(
            "signaled Slack event workflow %s %s",
            log_context,
            _workflow_signal_fields_log_context(fields),
        )
        response = {"ok": True, **fields}
    except Exception:
        logger.exception("failed to ack Slack event workflow %s", log_context)
        response = _workflow_dispatched_ack_fallback()
    _publish_matching_workflow_events_after_agent_handoff(input, req, log_context)
    try:
        _add_acknowledgement_reaction(event, route)
    except SlackAPIError as err:
        error = str(err.body.get("error") or err.body)
        if error != "already_reacted":
            acknowledgement_reaction_error = error
            logger.warning(
                "failed to add Slack event acknowledgement reaction %s error=%s",
                log_context,
                error,
            )
    except SlackClientError as err:
        acknowledgement_reaction_error = str(err)
        logger.warning(
            "failed to add Slack event acknowledgement reaction %s error=%s",
            log_context,
            acknowledgement_reaction_error,
        )
    assistant = _assistant_config(route)
    if assistant.enabled:
        try:
            _set_initial_assistant_status(event, route)
        except SlackAPIError as err:
            assistant_status_error = str(err.body.get("error") or err.body)
            logger.warning(
                "failed to set initial Slack assistant status %s error=%s",
                log_context,
                assistant_status_error,
            )
        except SlackClientError as err:
            assistant_status_error = str(err)
            logger.warning(
                "failed to set initial Slack assistant status %s error=%s",
                log_context,
                assistant_status_error,
            )
    if acknowledgement_reaction_error:
        response["acknowledgement_reaction_error"] = acknowledgement_reaction_error
    if assistant_status_error:
        response["assistant_status_error"] = assistant_status_error
    return response


def _agent_dispatch_configured(route: SlackAgentRoute | None) -> bool:
    if route is not None:
        return True
    return bool(
        _agent_config.bot.token
        or _agent_config.workflow.provider_name
        or _agent_config.agent_provider
        or _agent_config.agent_model
        or _agent_config.agent_tool_set_refs
        or _agent_config.agent_tools
    )


def _log_ignored_slack_event(
    event: SlackAgentEvent, req: gestalt.Request, ignored_reason: str
) -> None:
    if not (_agent_config.routes or _agent_config.events.publish.routes):
        return
    log_context = _slack_event_log_context(event, req, None)
    logger.info(f"ignored Slack event {log_context} ignored_reason={ignored_reason}")


def _publish_matching_workflow_events_after_agent_handoff(
    payload: dict[str, Any], req: gestalt.Request, log_context: str
) -> None:
    publish_response = _publish_matching_workflow_events(payload, req)
    if isinstance(publish_response, gestalt.Response):
        logger.warning(
            "ignored Slack workflow event publish failure after agent handoff %s status=%s body=%r",
            log_context,
            publish_response.status,
            publish_response.body,
        )


def request_slack_interaction(
    reply_ref: str,
    text: str,
    actions: list[dict[str, Any]],
    expires_in_seconds: int,
    req: gestalt.Request,
) -> OperationResult:
    normalized_text = text.strip()
    if not normalized_text:
        return _bad_request("text is required")
    try:
        normalized_actions = _normalized_interaction_actions(actions)
    except ValueError as err:
        return _bad_request(str(err))
    if not normalized_actions:
        return _bad_request("actions are required")

    try:
        verified_ref = _event_reply_ref(reply_ref, req)
        expires_in = _interaction_ref_ttl_seconds(expires_in_seconds)
        blocks = _interaction_request_blocks(
            verified_ref,
            normalized_text,
            normalized_actions,
            expires_in,
        )
        result = post_message(
            _agent_config.bot.token,
            channel=verified_ref.channel_id,
            text=normalized_text,
            thread_ts=verified_ref.reply_thread_ts,
            blocks=blocks,
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
        "workflow_key": _reply_ref_workflow_key(verified_ref),
        "action_ids": [action.action_id for action in normalized_actions],
    }


def handle_slack_interaction(
    input: dict[str, Any], req: gestalt.Request
) -> OperationResult:
    payload = _slack_interaction_payload_from_input(input)
    if payload is None:
        return _bad_request("payload is required")
    log_context = _slack_interaction_log_context(payload, req)
    if not req.subject.id or req.subject.id.startswith("system:"):
        logger.warning(
            "rejected Slack interaction without linked subject %s", log_context
        )
        _notify_unlinked_slack_user_for_interaction(payload, req)
        return {"ok": True, "unlinked": True}

    verified_ref: SlackInteractionRef | None = None
    selected_action: dict[str, Any] | None = None
    route: SlackAgentRoute | None = None
    try:
        interaction_ref, selected_action = _interaction_ref_from_payload(payload)
        verified_ref = _verify_interaction_ref(interaction_ref, req.subject.id)
        _validate_interaction_payload_matches_ref(payload, verified_ref)
        route = _agent_route_from_signed_id(verified_ref.route_id)
        log_context = _slack_interaction_log_context(
            payload,
            req,
            verified_ref=verified_ref,
            selected_action=selected_action,
            route=route,
        )
    except ValueError as err:
        logger.warning("rejected Slack interaction %s error=%s", log_context, err)
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})

    workflow_provider = _workflow_provider_name(route)
    if not workflow_provider:
        logger.error(
            "Slack interaction workflow provider is not configured %s", log_context
        )
        return gestalt.Response(
            status=HTTPStatus.PRECONDITION_FAILED,
            body={"error": "Slack workflow provider is not configured"},
        )
    workflow_request: gestalt.WorkflowSignalOrStartRun | None = None
    try:
        workflow_request = _build_workflow_interaction_signal_or_start_request(
            payload, selected_action, verified_ref, route
        )
        logger.info(
            f"attempting Slack interaction workflow signal {log_context} "
            f"{_workflow_handoff_log_context(workflow_request)}"
        )
        with req.workflows() as workflows:
            workflow_response = workflows.signal_or_start_run(workflow_request)
    except Exception as err:
        logger.exception(
            f"failed to signal Slack interaction workflow {log_context} "
            f"{_workflow_handoff_log_context(workflow_request, err)}"
        )
        return _server_error(f"failed to signal workflow run: {err}")

    try:
        fields = _workflow_signal_response_fields(
            workflow_response,
            fallback_workflow_key=verified_ref.workflow_key,
            fallback_provider_name=workflow_provider,
        )
        logger.info(
            "signaled Slack interaction workflow %s %s",
            log_context,
            _workflow_signal_fields_log_context(fields),
        )
        response = {"ok": True, **fields}
    except Exception:
        logger.exception("failed to ack Slack interaction workflow %s", log_context)
        response = _workflow_dispatched_ack_fallback()
    response["action_id"] = verified_ref.action_id
    return response


def reply_to_slack_event(
    reply_ref: str, text: str, req: gestalt.Request
) -> OperationResult:
    normalized_text = text.strip()
    log_context = _slack_delivery_log_context(
        req, SLACK_REPLY_OPERATION, reply_ref=reply_ref, text=normalized_text
    )
    if not normalized_text:
        logger.warning(
            "rejected Slack event reply delivery %s error=text is required", log_context
        )
        return _bad_request("text is required")

    verified_ref: SlackReplyRef | None = None
    try:
        verified_ref = _event_reply_ref(reply_ref, req)
        log_context = _slack_delivery_log_context(
            req,
            SLACK_REPLY_OPERATION,
            reply_ref=reply_ref,
            verified_ref=verified_ref,
            text=normalized_text,
        )
        logger.info("attempting Slack event reply delivery %s", log_context)
        result = post_message(
            _agent_config.bot.token,
            channel=verified_ref.channel_id,
            text=normalized_text,
            thread_ts=verified_ref.reply_thread_ts,
            client_msg_id=_slack_client_msg_id(req.idempotency_key),
        )
    except ValueError as err:
        logger.warning(
            "failed Slack event reply delivery %s error=%s", log_context, err
        )
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})
    except SlackAPIError as err:
        logger.warning(
            "failed Slack event reply delivery %s status=%s slack_response_body=%s",
            log_context,
            err.status,
            _log_body(err.body),
        )
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        logger.warning(
            "failed Slack event reply delivery %s error=%s", log_context, err
        )
        return _event_client_error(err)

    logger.info(
        "delivered Slack event reply %s slack_channel_id=%s slack_ts=%s",
        log_context,
        str(result.get("channel") or verified_ref.channel_id),
        str(result.get("ts") or ""),
    )
    return {
        "ok": True,
        "channel": str(result.get("channel") or verified_ref.channel_id),
        "ts": str(result.get("ts") or ""),
        "thread_ts": verified_ref.reply_thread_ts,
    }


def reply_slack_event_session_started(
    reply_ref: str, session_id: str, req: gestalt.Request
) -> OperationResult:
    normalized_session_id = session_id.strip()
    log_context = _slack_delivery_log_context(
        req,
        SLACK_SESSION_STARTED_REPLY_OPERATION,
        reply_ref=reply_ref,
        session_id=normalized_session_id,
    )
    if not normalized_session_id:
        logger.warning(
            "rejected Slack session-started delivery %s error=session_id is required",
            log_context,
        )
        return _bad_request("session_id is required")

    try:
        verified_ref = _event_reply_ref(reply_ref, req)
        log_context = _slack_delivery_log_context(
            req,
            SLACK_SESSION_STARTED_REPLY_OPERATION,
            reply_ref=reply_ref,
            verified_ref=verified_ref,
            session_id=normalized_session_id,
        )
    except ValueError as err:
        logger.warning(
            "failed Slack session-started delivery %s error=%s", log_context, err
        )
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})

    if _reply_ref_is_thread_reply(verified_ref):
        logger.info(
            "skipped Slack session-started delivery %s reason=thread_reply",
            log_context,
        )
        return {
            "ok": True,
            "skipped": True,
            "reason": "thread_reply",
            "thread_ts": verified_ref.reply_thread_ts,
        }

    base_url = req.host.public_base_url.strip()
    if not base_url:
        logger.warning(
            "failed Slack session-started delivery %s error=host.public_base_url is required",
            log_context,
        )
        return gestalt.Response(
            status=HTTPStatus.PRECONDITION_FAILED,
            body={"error": "host.public_base_url is required"},
        )
    session_url = agent_session_url(base_url, normalized_session_id)
    text = f"Started a Gestalt session: <{session_url}|open session>"
    log_context = _slack_delivery_log_context(
        req,
        SLACK_SESSION_STARTED_REPLY_OPERATION,
        reply_ref=reply_ref,
        verified_ref=verified_ref,
        text=text,
        session_id=normalized_session_id,
    )

    try:
        logger.info("attempting Slack session-started delivery %s", log_context)
        result = post_message(
            _agent_config.bot.token,
            channel=verified_ref.channel_id,
            text=text,
            thread_ts=verified_ref.reply_thread_ts,
            unfurl_links=False,
            unfurl_media=False,
            client_msg_id=_slack_client_msg_id(req.idempotency_key),
        )
    except ValueError as err:
        logger.warning(
            "failed Slack session-started delivery %s error=%s", log_context, err
        )
        return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": str(err)})
    except SlackAPIError as err:
        logger.warning(
            "failed Slack session-started delivery %s status=%s slack_response_body=%s",
            log_context,
            err.status,
            _log_body(err.body),
        )
        return gestalt.Response(status=err.status, body=err.body)
    except SlackClientError as err:
        logger.warning(
            "failed Slack session-started delivery %s error=%s", log_context, err
        )
        return _event_client_error(err)

    logger.info(
        "delivered Slack session-started delivery %s slack_channel_id=%s slack_ts=%s",
        log_context,
        str(result.get("channel") or verified_ref.channel_id),
        str(result.get("ts") or ""),
    )
    return {
        "ok": True,
        "channel": str(result.get("channel") or verified_ref.channel_id),
        "ts": str(result.get("ts") or ""),
        "thread_ts": verified_ref.reply_thread_ts,
        "session_url": session_url,
    }


def _reply_ref_is_thread_reply(ref: SlackReplyRef) -> bool:
    return bool(ref.reply_thread_ts and ref.reply_thread_ts != ref.message_ts)


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
        normalized_prompts = normalize_suggested_prompts(prompts)
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
            prompts=[prompt.as_slack_payload() for prompt in normalized_prompts],
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


def _set_initial_assistant_status(
    event: SlackAgentEvent, route: SlackAgentRoute | None
) -> None:
    assistant = _assistant_config(route)
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


def _add_acknowledgement_reaction(
    event: SlackAgentEvent, route: SlackAgentRoute | None
) -> None:
    acknowledgement = _acknowledgement_config(route)
    if not acknowledgement.enabled:
        return
    reaction = acknowledgement.reaction.strip().strip(":")
    if not reaction or not event.message_ts:
        return
    add_reaction(
        _agent_config.bot.token,
        channel=event.channel_id,
        timestamp=event.message_ts,
        name=reaction,
    )


def _handle_assistant_thread_event(
    event: SlackAgentEvent, route: SlackAgentRoute | None
) -> OperationResult:
    if event.event_type == SlackEventType.ASSISTANT_THREAD_CONTEXT_CHANGED:
        return {"ok": True, "event_type": event.event_type}

    assistant = _assistant_config(route)
    if _assistant_thread_prompts_disabled(route):
        return {
            "ok": True,
            "event_type": event.event_type,
            "suggested_prompts_set": False,
        }
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
            prompts=[
                prompt.as_slack_payload() for prompt in assistant.suggested_prompts
            ],
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


def _notify_unlinked_slack_user_for_event(
    event: SlackAgentEvent, req: gestalt.Request
) -> None:
    thread_ts = event.reply_thread_ts or event.message_ts
    _notify_unlinked_slack_user(
        req,
        channel_id=event.channel_id,
        thread_ts=thread_ts,
        log_context=_slack_event_log_context(event, req, None),
    )


def _notify_unlinked_slack_user_for_interaction(
    payload: dict[str, Any], req: gestalt.Request
) -> None:
    container = map_field(payload, "container")
    thread_ts = (
        string_field(container, "thread_ts")
        or string_field(container, "message_ts")
        or string_field(payload, "message_ts")
    )
    _notify_unlinked_slack_user(
        req,
        channel_id=_interaction_channel_id(payload),
        thread_ts=thread_ts,
        log_context=_slack_interaction_log_context(payload, req),
    )


def _notify_unlinked_slack_user(
    req: gestalt.Request, *, channel_id: str, thread_ts: str, log_context: str
) -> None:
    token = _agent_config.bot.token
    if not token:
        logger.warning(
            "cannot notify unlinked Slack user without bot token %s", log_context
        )
        return
    if not channel_id:
        logger.warning(
            "cannot notify unlinked Slack user without channel id %s", log_context
        )
        return
    try:
        post_message(
            token,
            channel=channel_id,
            thread_ts=thread_ts,
            text=_unlinked_slack_user_message(req),
            unfurl_links=False,
            unfurl_media=False,
        )
    except SlackAPIError as err:
        logger.warning(
            "failed to notify unlinked Slack user %s status=%s error=%s",
            log_context,
            err.status,
            err.body,
        )
    except SlackClientError as err:
        logger.warning(
            "failed to notify unlinked Slack user %s error=%s",
            log_context,
            err,
        )


def _unlinked_slack_user_message(req: gestalt.Request) -> str:
    base_url = req.host.public_base_url.strip()
    if base_url:
        return (
            "Your Slack account is not yet connected at "
            f"{base_url.rstrip('/')}, please connect it first before trying again."
        )
    return (
        "Your Slack account is not yet connected to Gestalt, please connect it "
        "first before trying again."
    )


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
        payload = dict(request.params)
        interaction = _slack_interaction_payload_from_input(payload)
        return {"payload": interaction} if interaction is not None else payload
    if not request.raw_body:
        return {}
    raw_body = request.raw_body.decode("utf-8", errors="replace")
    form_payload = _slack_interaction_payload_from_input(
        {key: values[-1] for key, values in urllib.parse.parse_qs(raw_body).items()}
    )
    if form_payload is not None:
        return {"payload": form_payload}
    try:
        payload = json.loads(raw_body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _slack_interaction_payload_from_input(
    input: dict[str, Any],
) -> dict[str, Any] | None:
    raw_payload = input.get("payload") if isinstance(input, dict) else None
    if isinstance(raw_payload, dict):
        return raw_payload
    if isinstance(raw_payload, str) and raw_payload.strip():
        try:
            payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None
    if (
        isinstance(input, dict)
        and str(input.get("type") or "").strip() == "block_actions"
    ):
        return input
    return None


def _normalized_interaction_actions(
    actions: list[dict[str, Any]],
) -> list[SlackInteractionAction]:
    normalized: list[SlackInteractionAction] = []
    for index, action in enumerate(actions, start=1):
        if not isinstance(action, dict):
            continue
        action_id = str(
            action.get("id") or action.get("action_id") or f"action_{index}"
        ).strip()
        label = str(action.get("label") or action.get("text") or action_id).strip()
        value = str(action.get("value") or action_id).strip()
        style = str(action.get("style") or "").strip()
        if not action_id or not label:
            continue
        if style not in {"", "primary", "danger"}:
            raise ValueError("action style must be primary or danger")
        normalized.append(
            SlackInteractionAction(
                action_id=action_id[:255],
                label=label[:75],
                value=value[:2000],
                style=cast(SlackInteractionActionStyle, style),
            )
        )
    return normalized[:25]


def _interaction_request_blocks(
    ref: SlackReplyRef,
    text: str,
    actions: list[SlackInteractionAction],
    expires_in_seconds: int,
) -> list[dict[str, Any]]:
    elements: list[dict[str, Any]] = []
    for action in actions:
        button: dict[str, Any] = {
            "type": "button",
            "action_id": action.action_id,
            "text": {"type": "plain_text", "text": action.label},
            "value": _sign_interaction_ref(
                ref,
                action_id=action.action_id,
                action_value=action.value,
                expires_in_seconds=expires_in_seconds,
            ),
        }
        if action.style:
            button["style"] = action.style
        elements.append(button)
    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": text[:3000]}},
        {
            "type": "actions",
            "block_id": "gestalt_slack_interactions",
            "elements": elements,
        },
    ]


def _interaction_ref_ttl_seconds(value: int) -> int:
    if value <= 0:
        return SLACK_INTERACTION_REF_TTL_SECONDS
    return min(value, 7 * 24 * 60 * 60)


def _interaction_ref_from_payload(
    payload: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    payload_type = str(payload.get("type") or "").strip()
    if payload_type == "block_actions":
        actions = map_slice(payload.get("actions"))
        if not actions:
            raise ValueError("Slack interaction has no actions")
        for action in actions:
            value = string_field(action, "value")
            if value:
                return value, action
        raise ValueError("Slack interaction action is missing value")
    raise ValueError(f"unsupported Slack interaction type {payload_type!r}")


def _interaction_team_id(payload: dict[str, Any]) -> str:
    return string_field(map_field(payload, "team"), "id") or string_field(
        payload, "team_id"
    )


def _interaction_user_id(payload: dict[str, Any]) -> str:
    return string_field(map_field(payload, "user"), "id") or string_field(
        payload, "user_id"
    )


def _interaction_channel_id(payload: dict[str, Any]) -> str:
    return (
        string_field(map_field(payload, "channel"), "id")
        or string_field(map_field(payload, "container"), "channel_id")
        or string_field(payload, "channel_id")
    )


def _validate_interaction_payload_matches_ref(
    payload: dict[str, Any], ref: SlackInteractionRef
) -> None:
    team_id = _interaction_team_id(payload)
    channel_id = _interaction_channel_id(payload)
    user_id = _interaction_user_id(payload)
    if team_id and team_id != ref.team_id:
        raise ValueError("interaction_ref team does not match Slack payload")
    if channel_id and channel_id != ref.channel_id:
        raise ValueError("interaction_ref channel does not match Slack payload")
    if (
        user_id
        and ref.user_id
        and user_id != ref.user_id
        and _subject_kind_from_id(ref.subject_id) != "service_account"
    ):
        raise ValueError("interaction_ref user does not match Slack payload")


def _interaction_idempotency_key(
    payload: dict[str, Any], selected_action: dict[str, Any]
) -> str:
    view = map_field(payload, "view")
    parts = [
        _interaction_team_id(payload),
        _interaction_channel_id(payload),
        string_field(map_field(payload, "container"), "message_ts"),
        string_field(selected_action, "action_id"),
        string_field(selected_action, "action_ts"),
        string_field(view, "id"),
        string_field(view, "hash"),
        string_field(view, "callback_id"),
        str(payload.get("trigger_id") or "").strip(),
    ]
    body = json.dumps(parts, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "slack:interaction:" + hashlib.sha256(body).hexdigest()


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
    subtype = str(event.get("subtype") or "").strip()
    if event_type not in SUPPORTED_EVENT_TYPES:
        return None, "unsupported_event_type"

    team_id = _slack_team_id(payload, event)
    bot_user_ids = _slack_bot_user_ids(payload)
    bot_user_id = bot_user_ids[0] if bot_user_ids else ""
    if event_type in ASSISTANT_THREAD_EVENT_TYPES:
        assistant_thread = map_field(event, "assistant_thread")
        assistant_context = map_field(assistant_thread, "context")
        user_id = string_field(assistant_thread, "user_id")
        channel_id = string_field(assistant_thread, "channel_id")
        context_channel_id = string_field(assistant_context, "channel_id")
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
                subtype="",
                addressed_to_bot=True,
                assistant_context_present=True,
                bot_user_id=bot_user_id,
                context_channel_id=context_channel_id,
                files=(),
            ),
            "",
        )

    bot_id = _slack_agent_event_bot_id(event)
    is_bot_event = _slack_agent_event_is_bot(event, bot_id=bot_id)
    user_id = _slack_agent_event_user_id(event, bot_id=bot_id)
    channel_id = str(event.get("channel") or "").strip()
    text = _slack_agent_event_text(event)
    message_ts = str(event.get("ts") or event.get("event_ts") or "").strip()
    thread_ts = str(event.get("thread_ts") or "").strip()
    files = tuple(map_slice(event.get("files")))
    assistant_context_present = _slack_assistant_context_present(event)
    addressed_to_bot = _slack_message_addressed_to_bot(
        event=event,
        event_type=event_type,
        channel_type=channel_type,
        text=text,
        assistant_context_present=assistant_context_present,
        bot_user_ids=bot_user_ids,
    )
    reply_thread_ts = _slack_reply_thread_ts(
        event_type=event_type,
        channel_type=channel_type,
        message_ts=message_ts,
        thread_ts=thread_ts,
    )

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
            subtype=subtype,
            client_msg_id=str(event.get("client_msg_id") or "").strip(),
            addressed_to_bot=addressed_to_bot,
            assistant_context_present=assistant_context_present,
            bot_id=bot_id,
            bot_user_id=_slack_addressed_bot_user_id(
                event=event,
                text=text,
                assistant_context_present=assistant_context_present,
                bot_user_ids=bot_user_ids,
            ),
            files=files,
            is_bot_event=is_bot_event,
        ),
        "",
    )


def _publish_matching_workflow_events(
    payload: dict[str, Any], req: gestalt.Request
) -> dict[str, Any] | ErrorResponse | None:
    event, ignored_reason = _slack_publish_callback_from_payload(payload)
    if event is None:
        _ = ignored_reason
        return None

    routes = _matching_publish_routes(event)
    if not routes:
        return None

    log_context = _slack_publish_log_context(event, routes)
    try:
        workflow_event_ids: list[str] = []
        route_ids: list[str] = []
        with req.workflows() as workflows:
            for route in routes:
                workflow_request = _build_workflow_publish_event_request(
                    event, route, payload
                )
                workflows.publish_event(workflow_request)
                workflow_event_ids.append(str(workflow_request.event.id))
                route_ids.append(route.id)
    except Exception as err:
        logger.exception(f"failed to publish Slack workflow event {log_context}")
        return _server_error(f"failed to publish workflow event: {err}")

    logger.info(f"published Slack workflow event {log_context}")
    return {
        "ok": True,
        "published": True,
        "published_event_count": len(workflow_event_ids),
        "workflow_event_ids": workflow_event_ids,
        "route_ids": route_ids,
    }


def _slack_publish_callback_from_payload(
    payload: dict[str, Any],
) -> tuple[SlackEventPublishCallback | None, str]:
    callback_type = str(payload.get("type") or "").strip()
    if callback_type == SlackCallbackType.URL_VERIFICATION:
        return None, "url_verification"
    if callback_type != SlackCallbackType.EVENT_CALLBACK:
        return None, "unsupported_callback_type"

    event = payload.get("event")
    if not isinstance(event, dict):
        return None, "missing_event"

    event_type = string_field(event, "type")
    if not event_type:
        return None, "missing_event_type"

    nested_message = map_field(event, "message")
    item = map_field(event, "item")
    subtype = string_field(event, "subtype")
    bot_id = (
        string_field(event, "bot_id")
        or string_field(nested_message, "bot_id")
        or string_field(map_field(event, "bot_profile"), "id")
        or string_field(map_field(nested_message, "bot_profile"), "id")
    )
    user_id = (
        string_field(event, "user")
        or string_field(event, "user_id")
        or string_field(nested_message, "user")
        or string_field(nested_message, "user_id")
    )
    channel_id = (
        string_field(event, "channel")
        or string_field(event, "channel_id")
        or string_field(nested_message, "channel")
        or string_field(nested_message, "channel_id")
        or string_field(item, "channel")
    )
    message_ts = (
        string_field(event, "ts")
        or string_field(nested_message, "ts")
        or string_field(item, "ts")
    )
    event_ts = string_field(event, "event_ts")
    text = string_field(event, "text") or string_field(nested_message, "text")
    files = tuple(
        map_slice(event.get("files")) or map_slice(nested_message.get("files"))
    )
    return (
        SlackEventPublishCallback(
            callback_type=callback_type,
            event_type=event_type,
            event_id=string_field(payload, "event_id"),
            team_id=_slack_team_id(payload, event),
            enterprise_id=string_field(payload, "enterprise_id"),
            api_app_id=string_field(payload, "api_app_id"),
            event_context=string_field(payload, "event_context"),
            user_id=user_id,
            bot_id=bot_id,
            channel_id=channel_id,
            channel_type=string_field(event, "channel_type")
            or string_field(nested_message, "channel_type"),
            subtype=subtype,
            text=text,
            message_ts=message_ts,
            event_ts=event_ts,
            thread_ts=string_field(event, "thread_ts")
            or string_field(nested_message, "thread_ts"),
            files=files,
            is_bot_event=bool(
                subtype == "bot_message"
                or bot_id
                or event.get("bot_profile")
                or nested_message.get("bot_profile")
            ),
        ),
        "",
    )


def _matching_publish_routes(
    event: SlackEventPublishCallback,
) -> tuple[SlackEventPublishRoute, ...]:
    return tuple(
        route
        for route in _agent_config.events.publish.routes
        if route.match.matches(event)
    )


def _build_workflow_publish_event_request(
    event: SlackEventPublishCallback,
    route: SlackEventPublishRoute,
    raw_payload: dict[str, Any],
) -> gestalt.WorkflowPublishEvent:
    workflow_request = gestalt.WorkflowPublishEvent(
        event=gestalt.WorkflowEvent(
            id=_workflow_event_id(event, route),
            source=route.source or "slack",
            spec_version="1.0",
            type=route.workflow_event_type or "slack.event.received",
            subject=route.subject or f"route:{route.id}",
            datacontenttype="application/json",
            data=_slack_publish_event_data(event, route, raw_payload),
        )
    )
    workflow_provider = route.workflow_provider or _agent_config.workflow.provider_name
    if workflow_provider:
        workflow_request.provider_name = workflow_provider
    return workflow_request


def _slack_publish_event_data(
    event: SlackEventPublishCallback,
    route: SlackEventPublishRoute,
    raw_payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        "routeId": route.id,
        "slack": {
            "callback_type": event.callback_type,
            "event_type": event.event_type,
            "event_id": event.event_id,
            "team_id": event.team_id,
            "enterprise_id": event.enterprise_id,
            "api_app_id": event.api_app_id,
            "event_context": event.event_context,
            "user_id": event.user_id,
            "bot_id": event.bot_id,
            "channel_id": event.channel_id,
            "channel_type": event.channel_type,
            "subtype": event.subtype,
            "text": event.text,
            "message_ts": event.message_ts,
            "event_ts": event.event_ts,
            "thread_ts": event.thread_ts,
            "is_bot_event": event.is_bot_event,
            "file_ids": _publish_event_file_ids(event),
            "files": [dict(file_data) for file_data in event.files],
        },
        "raw": raw_payload,
    }


def _workflow_event_id(
    event: SlackEventPublishCallback, route: SlackEventPublishRoute
) -> str:
    if event.event_id:
        return f"slack:{event.event_id}"
    actor = event.user_id or event.bot_id
    parts = [
        "slack",
        "route",
        route.id,
        "team",
        event.team_id,
        "event",
        event.event_type,
        "subtype",
        event.subtype,
        "channel",
        event.channel_id,
        "ts",
        event.message_ts or event.event_ts,
        "thread",
        event.thread_ts,
        "actor",
        actor,
    ]
    return ":".join(_workflow_event_id_part(part) for part in parts)


def _workflow_event_id_part(value: str) -> str:
    normalized = str(value or "").strip().replace(":", "_")
    return normalized or "-"


def _publish_event_file_ids(event: SlackEventPublishCallback) -> list[str]:
    return [
        file_id
        for file_id in (
            str(file_data.get("id") or "").strip() for file_data in event.files
        )
        if file_id
    ]


def _slack_publish_log_context(
    event: SlackEventPublishCallback,
    routes: tuple[SlackEventPublishRoute, ...],
) -> str:
    return _log_context(
        slack_event_type=event.event_type,
        slack_event_subtype=event.subtype,
        slack_event_id=event.event_id,
        slack_team_id=event.team_id,
        slack_channel_id=event.channel_id,
        slack_channel_type=event.channel_type,
        slack_user_id=event.user_id,
        slack_bot_id=event.bot_id,
        slack_message_ts=event.message_ts,
        slack_thread_ts=event.thread_ts,
        slack_publish_route_ids=",".join(route.id for route in routes),
    )


def _is_url_verification(payload: dict[str, Any]) -> bool:
    return str(payload.get("type") or "").strip() == SlackCallbackType.URL_VERIFICATION


def _is_ignored_event(event: dict[str, Any]) -> bool:
    subtype = str(event.get("subtype") or "").strip()
    if subtype in {
        "message_changed",
        "message_deleted",
        "message_replied",
    }:
        return True
    return False


def _slack_agent_event_bot_id(event: dict[str, Any]) -> str:
    nested_message = map_field(event, "message")
    return (
        string_field(event, "bot_id")
        or string_field(nested_message, "bot_id")
        or string_field(map_field(event, "bot_profile"), "id")
        or string_field(map_field(nested_message, "bot_profile"), "id")
    )


def _slack_agent_event_is_bot(event: dict[str, Any], *, bot_id: str) -> bool:
    nested_message = map_field(event, "message")
    return bool(
        string_field(event, "subtype") == "bot_message"
        or bot_id
        or event.get("bot_profile")
        or nested_message.get("bot_profile")
    )


def _slack_agent_event_user_id(event: dict[str, Any], *, bot_id: str) -> str:
    nested_message = map_field(event, "message")
    bot_profile = map_field(event, "bot_profile") or map_field(
        nested_message, "bot_profile"
    )
    return (
        string_field(event, "user")
        or string_field(event, "user_id")
        or string_field(nested_message, "user")
        or string_field(nested_message, "user_id")
        or string_field(bot_profile, "user_id")
        or bot_id
    )


def _slack_agent_event_text(event: dict[str, Any]) -> str:
    text = string_field(event, "text")
    if text:
        return text
    parts: list[str] = []
    _collect_slack_text(event.get("blocks"), parts)
    _collect_slack_text(event.get("attachments"), parts)
    return "\n".join(dict.fromkeys(part for part in parts if part))


def _collect_slack_text(value: Any, parts: list[str]) -> None:
    if isinstance(value, str):
        text = value.strip()
        if text:
            parts.append(text)
        return
    if isinstance(value, list):
        for item in value:
            _collect_slack_text(item, parts)
        return
    if not isinstance(value, dict):
        return

    for key in ("fallback", "pretext", "title", "text", "value", "alt_text"):
        child = value.get(key)
        if child is not None:
            _collect_slack_text(child, parts)
    for key in ("blocks", "fields", "elements", "accessory"):
        child = value.get(key)
        if child is not None:
            _collect_slack_text(child, parts)


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


def _slack_bot_user_ids(payload: dict[str, Any]) -> tuple[str, ...]:
    user_ids: list[str] = []
    configured = _agent_config.bot.user_id.strip()
    if configured:
        user_ids.append(configured)
    authorizations = payload.get("authorizations")
    if isinstance(authorizations, list):
        for authorization in authorizations:
            if not isinstance(authorization, dict):
                continue
            if not _truthy_slack_value(authorization.get("is_bot")):
                continue
            user_id = str(authorization.get("user_id") or "").strip()
            if user_id:
                user_ids.append(user_id)
    return tuple(dict.fromkeys(user_ids))


def _truthy_slack_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return False


def _slack_assistant_context_present(event: dict[str, Any]) -> bool:
    assistant_thread = map_field(event, "assistant_thread")
    if not assistant_thread:
        return False
    return bool(
        string_field(assistant_thread, "action_token")
        or string_field(assistant_thread, "thread_ts")
        or map_field(assistant_thread, "context")
    )


def _slack_message_addressed_to_bot(
    *,
    event: dict[str, Any],
    event_type: str,
    channel_type: str,
    text: str,
    assistant_context_present: bool,
    bot_user_ids: tuple[str, ...],
) -> bool:
    if event_type in ASSISTANT_THREAD_EVENT_TYPES:
        return True
    if event_type == SlackEventType.APP_MENTION:
        return True
    if event_type != SlackEventType.MESSAGE:
        return False
    if channel_type in DIRECT_MESSAGE_CHANNEL_TYPES:
        return True
    if assistant_context_present:
        return True
    return bool(_slack_addressed_bot_user_id(event, text, False, bot_user_ids))


def _slack_addressed_bot_user_id(
    event: dict[str, Any],
    text: str,
    assistant_context_present: bool,
    bot_user_ids: tuple[str, ...],
) -> str:
    for user_id in bot_user_ids:
        if f"<@{user_id}>" in text or _slack_blocks_mention_user(
            event.get("blocks"), user_id
        ):
            return user_id
    if assistant_context_present and bot_user_ids:
        return bot_user_ids[0]
    return ""


def _slack_blocks_mention_user(value: Any, user_id: str) -> bool:
    if isinstance(value, dict):
        if value.get("type") == "user" and str(value.get("user_id") or "") == user_id:
            return True
        return any(
            _slack_blocks_mention_user(child, user_id) for child in value.values()
        )
    if isinstance(value, list):
        return any(_slack_blocks_mention_user(child, user_id) for child in value)
    return False


def _slack_reply_thread_ts(
    *,
    event_type: str,
    channel_type: str,
    message_ts: str,
    thread_ts: str,
) -> str:
    if thread_ts:
        return thread_ts
    if event_type == SlackEventType.APP_MENTION:
        return message_ts
    if (
        event_type == SlackEventType.MESSAGE
        and channel_type not in DIRECT_MESSAGE_CHANNEL_TYPES
    ):
        return message_ts
    return ""


def _should_notify_unlinked_slack_user_for_event(event: SlackAgentEvent) -> bool:
    if event.event_type != SlackEventType.MESSAGE:
        return True
    return event.addressed_to_bot or event.assistant_context_present


def _resolve_slack_subject(
    authorization: gestalt.AuthorizationProtocol,
    *,
    team_id: str,
    user_id: str,
) -> gestalt.Subject | None:
    identity_id = slack_external_identity_id(team_id, user_id)
    resource_id = external_identity_resource_id(
        SLACK_EXTERNAL_IDENTITY_TYPE, identity_id
    )
    response = authorization.search_subjects(
        gestalt.SubjectSearchRequest(
            resource=gestalt.AuthorizationResource(
                type=EXTERNAL_IDENTITY_RESOURCE_TYPE,
                id=resource_id,
            ),
            action=gestalt.AuthorizationAction(name=EXTERNAL_IDENTITY_ASSUME_ACTION),
            page_size=10,
        )
    )
    subjects = _dedupe_resolved_subjects(response.subjects)
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
        kind=_resolved_subject_kind(subject),
        display_name=_subject_display_name(subject),
        auth_source="authorization",
    )


def _agent_route_run_as_subject(
    route: SlackAgentRoute | None,
) -> gestalt.Subject | None:
    if route is None or not route.run_as_subject_id:
        return None
    return gestalt.Subject(
        id=route.run_as_subject_id,
        kind=route.run_as_subject_kind
        or _subject_kind_from_id(route.run_as_subject_id),
        display_name=route.run_as_display_name or route.run_as_subject_id,
        auth_source="slack_agent_route_run_as",
    )


def _interaction_route_run_as_subject(
    interaction: dict[str, Any],
) -> gestalt.Subject | None:
    try:
        interaction_ref, _selected_action = _interaction_ref_from_payload(interaction)
        ref = _decode_interaction_ref(interaction_ref)
        route = _agent_route_from_signed_id(ref.route_id)
    except ValueError:
        return None
    if route is None or ref.subject_id != route.run_as_subject_id:
        return None
    return _agent_route_run_as_subject(route)


def _dedupe_resolved_subjects(
    subjects: Iterable[gestalt.AuthorizationSubject],
) -> list[gestalt.AuthorizationSubject]:
    unique: dict[tuple[str, str], gestalt.AuthorizationSubject] = {}
    for subject in subjects:
        subject_id = subject.id.strip()
        if not subject_id:
            continue
        key = (_resolved_subject_kind(subject), subject_id)
        existing = unique.get(key)
        if existing is None or (
            existing.type.strip() != "subject" and subject.type.strip() == "subject"
        ):
            unique[key] = subject
    return list(unique.values())


def _resolved_subject_kind(subject: gestalt.AuthorizationSubject) -> str:
    subject_type = subject.type.strip()
    subject_id = subject.id.strip()
    if subject_type == "subject" and ":" in subject_id:
        kind = _subject_kind_from_id(subject_id)
        if kind:
            return kind
    return subject_type


def _subject_kind_from_id(subject_id: str) -> str:
    kind, _separator, _value = subject_id.partition(":")
    return kind


def _subject_display_name(subject: gestalt.AuthorizationSubject) -> str:
    properties = subject.properties or {}
    if properties:
        for key in ("displayName", "display_name", "email", "name"):
            value = properties.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return subject.id.strip()


def _sign_reply_ref(
    event: SlackAgentEvent, subject_id: str, route: SlackAgentRoute | None = None
) -> str:
    payload = {
        "v": 1,
        "team_id": event.team_id,
        "channel_id": event.channel_id,
        "user_id": event.user_id,
        "channel_type": event.channel_type,
        "message_ts": event.message_ts,
        "reply_thread_ts": event.reply_thread_ts,
        "event_id": event.event_id,
        "client_msg_id": event.client_msg_id,
        "subject_id": subject_id,
        "route_id": route.id if route is not None else "",
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
        route_id=str(payload.get("route_id") or "").strip(),
        client_msg_id=str(payload.get("client_msg_id") or "").strip(),
    )
    if not ref.team_id or not ref.channel_id or not ref.subject_id:
        raise ValueError("invalid reply_ref")
    return ref


def _reply_ref_workflow_key(ref: SlackReplyRef) -> str:
    if ref.channel_type in DIRECT_MESSAGE_CHANNEL_TYPES and not ref.reply_thread_ts:
        return f"slack:{ref.team_id}:{ref.channel_id}"
    root_ts = ref.reply_thread_ts or ref.message_ts
    return f"slack:{ref.team_id}:{ref.channel_id}:{root_ts}"


def _sign_interaction_ref(
    ref: SlackReplyRef,
    *,
    action_id: str,
    action_value: str,
    expires_in_seconds: int,
) -> str:
    expires_at = int(time.time()) + expires_in_seconds
    payload = {
        "v": 1,
        "team_id": ref.team_id,
        "channel_id": ref.channel_id,
        "channel_type": ref.channel_type,
        "message_ts": ref.message_ts,
        "reply_thread_ts": ref.reply_thread_ts,
        "workflow_key": _reply_ref_workflow_key(ref),
        "reply_ref": _resign_reply_ref(ref, expires_at=expires_at),
        "subject_id": ref.subject_id,
        "user_id": ref.user_id,
        "route_id": ref.route_id,
        "action_id": action_id,
        "action_value": action_value,
        "expires_at": expires_at,
    }
    encoded_payload = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    signature = hmac.new(
        _reply_ref_signing_key(), encoded_payload, hashlib.sha256
    ).digest()
    return f"{_base64url_encode(encoded_payload)}.{_base64url_encode(signature)}"


def _resign_reply_ref(ref: SlackReplyRef, *, expires_at: int) -> str:
    payload = {
        "v": 1,
        "team_id": ref.team_id,
        "channel_id": ref.channel_id,
        "user_id": ref.user_id,
        "channel_type": ref.channel_type,
        "message_ts": ref.message_ts,
        "reply_thread_ts": ref.reply_thread_ts,
        "event_id": ref.event_id,
        "client_msg_id": ref.client_msg_id,
        "subject_id": ref.subject_id,
        "route_id": ref.route_id,
        "expires_at": expires_at,
    }
    encoded_payload = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    signature = hmac.new(
        _reply_ref_signing_key(), encoded_payload, hashlib.sha256
    ).digest()
    return f"{_base64url_encode(encoded_payload)}.{_base64url_encode(signature)}"


def _verify_interaction_ref(
    interaction_ref: str, subject_id: str
) -> SlackInteractionRef:
    ref = _decode_interaction_ref(interaction_ref)
    if ref.subject_id != subject_id:
        raise ValueError("interaction_ref does not belong to this subject")
    return ref


def _decode_interaction_ref(interaction_ref: str) -> SlackInteractionRef:
    payload_part, separator, signature_part = interaction_ref.strip().partition(".")
    if not separator:
        raise ValueError("invalid interaction_ref")
    try:
        encoded_payload = _base64url_decode(payload_part)
        signature = _base64url_decode(signature_part)
    except (binascii.Error, ValueError) as err:
        raise ValueError("invalid interaction_ref") from err

    expected_signature = hmac.new(
        _reply_ref_signing_key(), encoded_payload, hashlib.sha256
    ).digest()
    if not hmac.compare_digest(signature, expected_signature):
        raise ValueError("invalid interaction_ref")
    try:
        payload = json.loads(encoded_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ValueError("invalid interaction_ref") from err
    if not isinstance(payload, dict) or payload.get("v") != 1:
        raise ValueError("invalid interaction_ref")
    try:
        expires_at = int(payload.get("expires_at") or 0)
    except (TypeError, ValueError) as err:
        raise ValueError("invalid interaction_ref") from err
    ref = SlackInteractionRef(
        team_id=str(payload.get("team_id") or "").strip(),
        channel_id=str(payload.get("channel_id") or "").strip(),
        channel_type=str(payload.get("channel_type") or "").strip(),
        message_ts=str(payload.get("message_ts") or "").strip(),
        reply_thread_ts=str(payload.get("reply_thread_ts") or "").strip(),
        workflow_key=str(payload.get("workflow_key") or "").strip(),
        reply_ref=str(payload.get("reply_ref") or "").strip(),
        subject_id=str(payload.get("subject_id") or "").strip(),
        user_id=str(payload.get("user_id") or "").strip(),
        route_id=str(payload.get("route_id") or "").strip(),
        action_id=str(payload.get("action_id") or "").strip(),
        action_value=str(payload.get("action_value") or "").strip(),
        expires_at=expires_at,
    )
    if (
        not ref.team_id
        or not ref.channel_id
        or not ref.workflow_key
        or not ref.reply_ref
        or not ref.subject_id
        or not ref.action_id
    ):
        raise ValueError("invalid interaction_ref")
    if ref.expires_at < int(time.time()):
        raise ValueError("interaction_ref expired")
    _verify_reply_ref(ref.reply_ref, ref.subject_id)
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
        matched_route = False
        for route in _agent_config.routes:
            if route.match.matches(event):
                matched_route = True
                if _agent_event_can_start_agent(event, route):
                    return route, ""
        if matched_route:
            return None, "unsupported_event_type"
        return None, "no_matching_agent_route"
    if _agent_event_can_start_agent(event, None):
        return None, ""
    return None, "unsupported_event_type"


def _agent_route_by_id(route_id: str) -> SlackAgentRoute | None:
    route_id = route_id.strip()
    if not route_id:
        return None
    for route in _agent_config.routes:
        if route.id == route_id:
            return route
    return None


def _agent_route_from_signed_id(route_id: str) -> SlackAgentRoute | None:
    route_id = route_id.strip()
    if not route_id:
        return None
    route = _agent_route_by_id(route_id)
    if route is None:
        raise ValueError("Slack interaction route is no longer configured")
    return route


def _agent_event_can_start_agent(
    event: SlackAgentEvent, route: SlackAgentRoute | None
) -> bool:
    if event.event_type in ASSISTANT_THREAD_EVENT_TYPES:
        return True
    if event.event_type == SlackEventType.APP_MENTION:
        return True
    if event.event_type != SlackEventType.MESSAGE:
        return False
    if event.channel_type in DIRECT_MESSAGE_CHANNEL_TYPES:
        return True
    if event.addressed_to_bot or event.assistant_context_present:
        return True
    return bool(
        route is not None and route.match.explicitly_matches_slack_message_event(event)
    )


def _slack_event_subject_allowed(
    event: SlackAgentEvent,
    route: SlackAgentRoute | None,
    subject_id: str,
) -> bool:
    if not subject_id:
        return False
    if not subject_id.startswith("system:"):
        return True
    return bool(
        event.is_bot_event
        and route is not None
        and (route.match.bot_ids or route.match.include_bot_events)
    )


def _build_workflow_signal_or_start_request(
    event: SlackAgentEvent,
    route: SlackAgentRoute | None,
    reply_ref: str,
) -> gestalt.WorkflowSignalOrStartRun:
    workflow_key = _agent_session_ref(event)
    thread_context = _prefetch_thread_context(event, route)
    return gestalt.WorkflowSignalOrStartRun(
        provider_name=_workflow_provider_name(route),
        workflow_key=workflow_key,
        idempotency_key=_agent_turn_idempotency_key(event),
        target=_build_workflow_agent_target(event, route),
        signal=gestalt.WorkflowSignal(
            name=SLACK_EVENT_WORKFLOW_SIGNAL,
            idempotency_key=_agent_turn_idempotency_key(event),
            payload=_slack_workflow_signal_payload(event, reply_ref, thread_context),
            metadata=_agent_metadata(event, route),
        ),
    )


def _build_workflow_agent_target(
    event: SlackAgentEvent,
    route: SlackAgentRoute | None,
) -> gestalt.BoundWorkflowTarget:
    if route is not None and route.agent_steps:
        steps: list[gestalt.WorkflowStep] = []
        for index, config_step in enumerate(route.agent_steps):
            steps.extend(
                _workflow_steps_for_configured_step(
                    route, config_step, include_signal_context=index == 0
                )
            )
        return gestalt.BoundWorkflowTarget(steps=steps)

    agent_step_id = "run"
    steps = [
        _workflow_agent_turn_step(
            agent_step_id,
            route,
            prompt=_workflow_agent_prompt(),
            messages=[
                gestalt.WorkflowAgentMessage(
                    role="system",
                    text=gestalt.WorkflowText(template=_agent_system_prompt(route)),
                )
            ],
            tools=_agent_event_tool_refs(route),
            model_options=_agent_model_options(route) or None,
            metadata=_agent_session_metadata(event),
            timeout_seconds=_agent_timeout_seconds(route),
        ),
        _workflow_session_ready_app_step(agent_step_id),
        _workflow_reply_app_step(agent_step_id, "agent.text"),
    ]
    return gestalt.BoundWorkflowTarget(steps=steps)


def _workflow_steps_for_configured_step(
    route: SlackAgentRoute,
    step: SlackAgentStep,
    *,
    include_signal_context: bool = False,
) -> list[gestalt.WorkflowStep]:
    messages = [
        gestalt.WorkflowAgentMessage(
            role="system",
            text=gestalt.WorkflowText(template=_agent_system_prompt(route)),
        )
    ]
    messages.extend(
        gestalt.WorkflowAgentMessage(
            role=message.get("role", "user"),
            text=gestalt.WorkflowText(template=message.get("text", "")),
            metadata=message.get("metadata") or None,
        )
        for message in step.messages
    )
    prompt = step.prompt
    if include_signal_context:
        if prompt:
            prompt = "\n\n".join(
                [
                    SLACK_SIGNAL_CONTEXT_PROMPT,
                    f"Configured task:\n{prompt}",
                ]
            )
        else:
            messages.append(
                gestalt.WorkflowAgentMessage(
                    role="user",
                    text=gestalt.WorkflowText(template=SLACK_SIGNAL_CONTEXT_PROMPT),
                )
            )
    when = _workflow_step_when(step.when) if step.when else None
    steps = [
        _workflow_agent_turn_step(
            step.id,
            route,
            session_key=step.session_key,
            prompt=prompt,
            messages=messages,
            tools=_agent_step_tool_refs(route, step),
            response_schema=step.response_schema or None,
            model_options=step.model_options or None,
            metadata=step.metadata or None,
            timeout_seconds=step.timeout_seconds,
            when=when,
        )
    ]
    agent_output = step.slack_reply_agent_output.strip()
    if agent_output:
        steps.append(
            _workflow_reply_app_step(
                step.id,
                _workflow_agent_output_path(agent_output),
                step_id=f"{step.id}_reply",
            )
        )
    return steps

def _workflow_agent_output_path(agent_output: str) -> str:
    agent_output = agent_output.strip()
    if not agent_output:
        return "agent.text"
    if agent_output.startswith("agent."):
        return agent_output
    if agent_output == "text":
        return "agent.text"
    return f"agent.structuredOutput.{agent_output}"


def _workflow_step_when(when: dict[str, Any]) -> gestalt.WorkflowStepWhen:
    step_id = str(when.get("step_id") or "").strip()
    output_path = str(when.get("output_path") or "").strip()
    equals = when.get("equals")
    path = output_path.replace("structured_output.", "structuredOutput.", 1)
    if not path.startswith("agent."):
        path = f"agent.{path}"
    return gestalt.WorkflowStepWhen(
        value=gestalt.WorkflowValue(
            step_output=gestalt.WorkflowStepOutputSource(step_id=step_id, path=path),
        ),
        equals=equals,
    )


def _workflow_reply_app_step(
    agent_step_id: str,
    agent_output_path: str,
    *,
    step_id: str = "reply",
    when: gestalt.WorkflowStepWhen | None = None,
) -> gestalt.WorkflowStep:
    return gestalt.WorkflowStep(
        id=step_id,
        app=gestalt.WorkflowStepAppCall(
            name=_agent_config.app_name,
            operation=SLACK_REPLY_OPERATION,
            credential_mode="none",
            input=gestalt.WorkflowValue(
                object={
                    "text": gestalt.WorkflowValue(
                        step_output=gestalt.WorkflowStepOutputSource(
                            step_id=agent_step_id,
                            path=agent_output_path,
                        ),
                    ),
                    "reply_ref": gestalt.WorkflowValue(signal_payload="reply_ref"),
                }
            ),
        ),
        when=when,
    )


def _workflow_session_ready_app_step(
    agent_step_id: str,
    *,
    step_id: str = "session_ready",
) -> gestalt.WorkflowStep:
    return gestalt.WorkflowStep(
        id=step_id,
        app=gestalt.WorkflowStepAppCall(
            name=_agent_config.app_name,
            operation=SLACK_SESSION_STARTED_REPLY_OPERATION,
            credential_mode="none",
            input=gestalt.WorkflowValue(
                object={
                    "session_id": gestalt.WorkflowValue(
                        step_output=gestalt.WorkflowStepOutputSource(
                            step_id=agent_step_id,
                            path="agent.sessionId",
                        ),
                    ),
                    "reply_ref": gestalt.WorkflowValue(signal_payload="reply_ref"),
                }
            ),
        ),
    )


def _workflow_agent_turn_step(
    step_id: str,
    route: SlackAgentRoute | None,
    *,
    prompt: str,
    messages: list[gestalt.WorkflowAgentMessage],
    tools: list[gestalt.AgentToolRef],
    session_key: str = "",
    response_schema: gestalt.WorkflowJsonObject | None = None,
    model_options: gestalt.WorkflowJsonObject | None = None,
    metadata: gestalt.WorkflowJsonObject | None = None,
    timeout_seconds: int = 0,
    when: gestalt.WorkflowStepWhen | None = None,
) -> gestalt.WorkflowStep:
    return gestalt.WorkflowStep(
        id=step_id,
        agent=gestalt.WorkflowStepAgentTurn(
            provider=_agent_provider(route),
            model=_agent_model(route),
            session_key=session_key,
            prompt=gestalt.WorkflowText(template=prompt),
            messages=messages,
            tools=tools,
            response_schema=response_schema,
            model_options=model_options,
        ),
        timeout_seconds=timeout_seconds if timeout_seconds > 0 else 0,
        metadata=metadata or None,
        when=when,
    )


def _workflow_agent_prompt() -> str:
    return "\n".join(
        [
            "Handle the current Slack event or interaction.",
            SLACK_SIGNAL_CONTEXT_PROMPT,
            "Treat the Slack request above as the current user request.",
            (
                "Use reply_ref only when a Slack helper tool requires it; do not "
                "include reply_ref in the visible Slack reply."
            ),
            "Return the complete Slack reply as your final assistant answer.",
        ]
    )


def _prefetch_thread_context(
    event: SlackAgentEvent, route: SlackAgentRoute | None
) -> dict[str, Any] | None:
    thread_context_config = _thread_context_config(route)
    if not thread_context_config.enabled:
        return None
    if event.event_type not in {SlackEventType.APP_MENTION, SlackEventType.MESSAGE}:
        return None
    root_ts = event.thread_ts
    if not root_ts:
        return None

    try:
        result = get_thread_context(
            _agent_config.bot.token,
            channel=event.channel_id,
            ts=root_ts,
            cursor="",
            limit=thread_context_config.max_messages,
            include_user_info=thread_context_config.include_user_info,
            include_bots=thread_context_config.include_bots,
            include_files=thread_context_config.include_files,
            include_file_content=thread_context_config.include_file_content,
            include_image_data=thread_context_config.include_image_data,
            max_file_bytes=thread_context_config.max_file_bytes,
        )
    except SlackAPIError as err:
        return {
            "thread_context_error": {
                "source": "bot",
                "channel": event.channel_id,
                "thread_ts": root_ts,
                "type": "slack_api",
                "status": int(err.status),
                "error": str(err.body.get("error") or "slack API error"),
            }
        }
    except SlackClientError as err:
        return {
            "thread_context_error": {
                "source": "bot",
                "channel": event.channel_id,
                "thread_ts": root_ts,
                "type": "slack_client",
                "status": 0,
                "error": str(err),
            }
        }

    data = result.get("data") if isinstance(result, dict) else None
    if not isinstance(data, dict):
        return {
            "thread_context_error": {
                "source": "bot",
                "channel": event.channel_id,
                "thread_ts": root_ts,
                "type": "slack_client",
                "status": 0,
                "error": "Slack thread context response was missing data",
            }
        }
    context = dict(data)
    context["source"] = "bot"
    context["truncated"] = bool(context.get("has_more") or context.get("next_cursor"))
    return {"thread_context": context}


def _slack_workflow_signal_payload(
    event: SlackAgentEvent,
    reply_ref: str,
    thread_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    user_prompt = _agent_user_prompt(event, reply_ref, thread_context)
    slack_payload: dict[str, Any] = {
        "callback_type": event.callback_type,
        "event_type": event.event_type,
        "subtype": event.subtype,
        "event_id": event.event_id,
        "team_id": event.team_id,
        "user_id": event.user_id,
        "bot_id": event.bot_id,
        "channel_id": event.channel_id,
        "channel_type": event.channel_type,
        "message_ts": event.message_ts,
        "thread_ts": event.thread_ts,
        "reply_thread_ts": event.reply_thread_ts,
        "client_msg_id": event.client_msg_id,
        "addressed_to_bot": event.addressed_to_bot,
        "assistant_context_present": event.assistant_context_present,
        "bot_user_id": event.bot_user_id,
        "is_bot_event": event.is_bot_event,
        "text": event.text,
        "file_ids": _event_file_ids(event),
        "files": [dict(file_data) for file_data in event.files],
    }
    if thread_context is not None:
        prefetched = thread_context.get("thread_context")
        if isinstance(prefetched, dict):
            slack_payload["thread_context"] = prefetched
        error = thread_context.get("thread_context_error")
        if isinstance(error, dict):
            slack_payload["thread_context_error"] = error
    return {
        "agent_request": _slack_agent_request(event, user_prompt),
        "user_prompt": user_prompt,
        "reply_ref": reply_ref,
        "slack": slack_payload,
    }


def _slack_agent_request(event: SlackAgentEvent, user_prompt: str) -> dict[str, Any]:
    return {
        "kind": "slack.event",
        "user_prompt": user_prompt,
        "current_message": {
            "text": event.text,
            "user_id": event.user_id,
            "bot_id": event.bot_id,
            "is_bot_event": event.is_bot_event,
            "message_ts": event.message_ts,
            "file_ids": _event_file_ids(event),
        },
    }


def _build_workflow_interaction_signal_or_start_request(
    payload: dict[str, Any],
    selected_action: dict[str, Any],
    interaction_ref: SlackInteractionRef,
    route: SlackAgentRoute | None,
) -> gestalt.WorkflowSignalOrStartRun:
    event = _interaction_event(payload, interaction_ref)
    signal = gestalt.WorkflowSignal(
        name=SLACK_INTERACTION_WORKFLOW_SIGNAL,
        idempotency_key=_interaction_idempotency_key(payload, selected_action),
        payload=_slack_interaction_signal_payload(
            payload, selected_action, interaction_ref
        ),
        metadata=_agent_metadata(event, route),
    )
    return gestalt.WorkflowSignalOrStartRun(
        provider_name=_workflow_provider_name(route),
        workflow_key=interaction_ref.workflow_key,
        idempotency_key=signal.idempotency_key,
        target=_build_workflow_agent_target(event, route),
        signal=signal,
    )


def _slack_interaction_signal_payload(
    payload: dict[str, Any],
    selected_action: dict[str, Any],
    interaction_ref: SlackInteractionRef,
) -> dict[str, Any]:
    view = map_field(payload, "view")
    container = map_field(payload, "container")
    return {
        "user_prompt": _interaction_user_prompt(
            payload, selected_action, interaction_ref
        ),
        "reply_ref": interaction_ref.reply_ref,
        "slack": {
            "callback_type": str(payload.get("type") or "").strip(),
            "team_id": interaction_ref.team_id,
            "user_id": _interaction_user_id(payload),
            "channel_id": interaction_ref.channel_id,
            "channel_type": interaction_ref.channel_type,
            "message_ts": string_field(container, "message_ts")
            or interaction_ref.message_ts,
            "thread_ts": interaction_ref.reply_thread_ts,
            "reply_thread_ts": interaction_ref.reply_thread_ts,
            "action_id": interaction_ref.action_id,
            "action_value": interaction_ref.action_value,
            "action_ts": string_field(selected_action, "action_ts"),
            "trigger_id": str(payload.get("trigger_id") or "").strip(),
            "response_url": str(payload.get("response_url") or "").strip(),
            "view_id": string_field(view, "id"),
            "view_callback_id": string_field(view, "callback_id"),
            "workflow_key": interaction_ref.workflow_key,
        },
    }


def _interaction_event(
    payload: dict[str, Any], interaction_ref: SlackInteractionRef
) -> SlackAgentEvent:
    container = map_field(payload, "container")
    message_ts = string_field(container, "message_ts") or interaction_ref.message_ts
    return SlackAgentEvent(
        callback_type="interaction",
        event_type=str(payload.get("type") or "").strip() or "interaction",
        event_id=_interaction_idempotency_key(payload, {}),
        team_id=interaction_ref.team_id,
        user_id=_interaction_user_id(payload) or interaction_ref.user_id,
        channel_id=interaction_ref.channel_id,
        channel_type=interaction_ref.channel_type,
        text=interaction_ref.action_value,
        message_ts=message_ts,
        thread_ts=interaction_ref.reply_thread_ts,
        reply_thread_ts=interaction_ref.reply_thread_ts,
        files=(),
    )


def _interaction_user_prompt(
    payload: dict[str, Any],
    selected_action: dict[str, Any],
    interaction_ref: SlackInteractionRef,
) -> str:
    lines = [
        "Slack interaction:",
        f"team_id: {interaction_ref.team_id}",
        f"channel_id: {interaction_ref.channel_id}",
        f"user_id: {_interaction_user_id(payload)}",
        f"action_id: {interaction_ref.action_id}",
        f"action_value: {interaction_ref.action_value}",
        f"action_ts: {string_field(selected_action, 'action_ts')}",
        f"trigger_id: {str(payload.get('trigger_id') or '').strip()}",
        f"reply_ref: {interaction_ref.reply_ref}",
        "",
        "Thread context tool:",
        f"operation: {_agent_config.app_name}.{SLACK_CONTEXT_OPERATION}",
        f"channel: {interaction_ref.channel_id}",
        f"ts: {interaction_ref.reply_thread_ts or interaction_ref.message_ts}",
    ]
    return "\n".join(lines)


def _agent_tool_ref(
    *,
    system: str = "",
    app: str = "",
    operation: str = "",
    connection: str = "",
    instance: str = "",
    title: str = "",
    description: str = "",
    run_as_subject_id: str = "",
) -> gestalt.AgentToolRef:
    return gestalt.AgentToolRef(
        system=system,
        app=app,
        operation=operation,
        connection=connection,
        instance=instance,
        title=title,
        description=description,
        run_as=_agent_tool_ref_run_as_subject(run_as_subject_id),
    )


def _agent_tool_ref_run_as_subject(
    subject_id: str,
) -> gestalt.Subject | None:
    subject_id = subject_id.strip()
    if not subject_id:
        return None
    return gestalt.Subject(id=subject_id, kind=_subject_kind_from_id(subject_id))


def _agent_event_tool_refs(route: SlackAgentRoute | None) -> list[gestalt.AgentToolRef]:
    refs = [
        *_agent_tool_set_refs(_agent_config.agent_tool_set_refs),
        *_agent_config.agent_tools,
        *_agent_tool_set_refs(route.agent_tool_set_refs if route is not None else ()),
        *(route.agent_tools if route is not None else ()),
    ]
    refs.extend(_agent_default_tool_refs(route))
    return [
        _agent_tool_ref(
            system=ref.system,
            app=ref.app,
            operation=ref.operation,
            connection=ref.connection,
            instance=ref.instance,
            title=ref.title,
            description=ref.description,
            run_as_subject_id=ref.run_as_subject_id,
        )
        for ref in _dedupe_agent_tool_refs(refs)
    ]


def _agent_default_tool_refs(route: SlackAgentRoute | None) -> list[SlackAgentToolRef]:
    operations = [
        SLACK_CONTEXT_OPERATION,
        SLACK_MESSAGE_OPERATION,
        SLACK_FILE_GET_OPERATION,
        SLACK_STATUS_OPERATION,
        SLACK_DELETE_STATUS_OPERATION,
        SLACK_ADD_REACTION_OPERATION,
        SLACK_REMOVE_REACTION_OPERATION,
    ]
    if _assistant_config(route).enabled:
        operations.extend(
            [
                SLACK_ASSISTANT_STATUS_OPERATION,
                SLACK_ASSISTANT_CLEAR_STATUS_OPERATION,
                SLACK_ASSISTANT_TITLE_OPERATION,
                SLACK_ASSISTANT_PROMPTS_OPERATION,
            ]
        )
    return [
        SlackAgentToolRef(
            app=_agent_config.app_name,
            operation=operation,
        )
        for operation in operations
    ]


def _agent_step_tool_refs(
    route: SlackAgentRoute, step: SlackAgentStep
) -> list[gestalt.AgentToolRef]:
    refs = [
        *_agent_tool_set_refs(_agent_config.agent_tool_set_refs),
        *_agent_config.agent_tools,
        *_agent_tool_set_refs(route.agent_tool_set_refs),
        *route.agent_tools,
        *_agent_tool_set_refs(step.tool_set_refs),
        *step.tools,
        *_agent_default_tool_refs(route),
    ]
    return [
        _agent_tool_ref(
            system=ref.system,
            app=ref.app,
            operation=ref.operation,
            connection=ref.connection,
            instance=ref.instance,
            title=ref.title,
            description=ref.description,
            run_as_subject_id=ref.run_as_subject_id,
        )
        for ref in _dedupe_agent_tool_refs(refs)
    ]


def _agent_tool_set_refs(tool_set_refs: Iterable[str]) -> list[SlackAgentToolRef]:
    refs: list[SlackAgentToolRef] = []
    for tool_set_ref in tool_set_refs:
        refs.extend(_agent_config.agent_tool_sets.get(tool_set_ref, ()))
    return refs


def _dedupe_agent_tool_refs(
    refs: Iterable[SlackAgentToolRef],
) -> list[SlackAgentToolRef]:
    deduped: list[SlackAgentToolRef] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for ref in refs:
        key = (ref.system, ref.app, ref.operation, ref.connection, ref.instance)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(ref)
    return deduped


def _agent_session_metadata(event: SlackAgentEvent) -> dict[str, Any]:
    root_ts = event.thread_ts or event.message_ts
    return {
        "slack": {
            "team_id": event.team_id,
            "channel_id": event.channel_id,
            "channel_type": event.channel_type,
            "bot_id": event.bot_id,
            "is_bot_event": event.is_bot_event,
            "root_message_ts": root_ts,
            "session_ref": _agent_session_ref(event),
        }
    }


def _agent_metadata(
    event: SlackAgentEvent,
    route: SlackAgentRoute | None,
) -> dict[str, Any]:
    return {
        "slack": {
            "callback_type": event.callback_type,
            "event_type": event.event_type,
            "subtype": event.subtype,
            "event_id": event.event_id,
            "team_id": event.team_id,
            "user_id": event.user_id,
            "bot_id": event.bot_id,
            "channel_id": event.channel_id,
            "channel_type": event.channel_type,
            "message_ts": event.message_ts,
            "thread_ts": event.thread_ts,
            "reply_thread_ts": event.reply_thread_ts,
            "client_msg_id": event.client_msg_id,
            "addressed_to_bot": event.addressed_to_bot,
            "assistant_context_present": event.assistant_context_present,
            "bot_user_id": event.bot_user_id,
            "is_bot_event": event.is_bot_event,
            "file_ids": _event_file_ids(event),
            "agent_route_id": route.id if route is not None else "",
        }
    }


def _agent_provider(route: SlackAgentRoute | None) -> str:
    if route is not None and route.agent_provider:
        return route.agent_provider
    return _agent_config.agent_provider


def _workflow_provider_name(route: SlackAgentRoute | None) -> str:
    if route is not None and route.workflow is not None:
        return route.workflow.provider_name
    return _agent_config.workflow.provider_name


def _assistant_config(route: SlackAgentRoute | None) -> SlackAssistantConfig:
    if route is not None and route.assistant is not None:
        return route.assistant
    return _agent_config.assistant


def _assistant_thread_prompts_disabled(route: SlackAgentRoute | None) -> bool:
    if route is None or route.assistant is None:
        return False
    return route.assistant.enabled_configured and not route.assistant.enabled


def _acknowledgement_config(
    route: SlackAgentRoute | None,
) -> SlackAcknowledgementConfig:
    if route is not None and route.acknowledgement is not None:
        return route.acknowledgement
    return _agent_config.acknowledgement


def _thread_context_config(route: SlackAgentRoute | None) -> SlackThreadContextConfig:
    if route is not None and route.thread_context is not None:
        return route.thread_context
    return _agent_config.thread_context


def _agent_model(route: SlackAgentRoute | None) -> str:
    if route is not None and route.agent_model:
        return route.agent_model
    return _agent_config.agent_model


def _agent_model_options(route: SlackAgentRoute | None) -> dict[str, Any]:
    options = dict(_agent_config.agent_model_options)
    if route is not None and route.agent_model_options:
        options.update(route.agent_model_options)
    return options


def _agent_timeout_seconds(route: SlackAgentRoute | None) -> int:
    if route is not None and route.agent_timeout_seconds > 0:
        return route.agent_timeout_seconds
    return _agent_config.agent_timeout_seconds


def _agent_system_prompt(route: SlackAgentRoute | None) -> str:
    parts = [
        DEFAULT_AGENT_SYSTEM_PROMPT_TEMPLATE.format(
            status_tool=f"{_agent_config.app_name}.{SLACK_STATUS_OPERATION}",
            context_tool=f"{_agent_config.app_name}.{SLACK_CONTEXT_OPERATION}",
            file_tool=f"{_agent_config.app_name}.{SLACK_FILE_GET_OPERATION}",
        )
    ]
    if _agent_config.agent_system_prompt:
        parts.append(_agent_config.agent_system_prompt.strip())
    if route is not None and route.agent_system_prompt:
        parts.append(route.agent_system_prompt.strip())
    return "\n\n".join(parts)


def _agent_user_prompt(
    event: SlackAgentEvent,
    reply_ref: str,
    thread_context: dict[str, Any] | None = None,
) -> str:
    root_ts = event.thread_ts or event.message_ts
    lines = [
        "Slack event:",
        f"team_id: {event.team_id}",
        f"channel_id: {event.channel_id}",
        f"user_id: {event.user_id}",
        f"bot_id: {event.bot_id}",
        f"is_bot_event: {event.is_bot_event}",
        f"message_ts: {event.message_ts}",
        f"thread_ts: {event.thread_ts}",
        f"reply_thread_ts: {event.reply_thread_ts}",
        f"reply_ref: {reply_ref}",
        "",
        "Message text:",
        event.text,
    ]
    lines.extend(
        [
            "",
            "File content tool:",
            f"operation: {_agent_config.app_name}.{SLACK_FILE_GET_OPERATION}",
        ]
    )
    file_summaries = _event_file_summaries(event)
    if file_summaries:
        lines.extend(["files:", *file_summaries])
    permalink_summaries = _event_permalink_summaries(event)
    if permalink_summaries:
        lines.extend(["", "Slack message permalink tools:", *permalink_summaries])
    lines.extend(
        [
            "",
            "Thread context tool:",
            f"operation: {_agent_config.app_name}.{SLACK_CONTEXT_OPERATION}",
            f"channel: {event.channel_id}",
            f"ts: {root_ts}",
        ]
    )
    thread_context_lines = _thread_context_prompt_lines(thread_context)
    if thread_context_lines:
        lines.extend(["", *thread_context_lines])
    return "\n".join(lines)


def _thread_context_prompt_lines(thread_context: dict[str, Any] | None) -> list[str]:
    if thread_context is None:
        return []
    prefetched = thread_context.get("thread_context")
    if isinstance(prefetched, dict):
        return [
            "Background thread context:",
            json.dumps(prefetched, ensure_ascii=False, indent=2, sort_keys=True),
        ]
    error = thread_context.get("thread_context_error")
    if isinstance(error, dict):
        return [
            "Background thread context error:",
            json.dumps(error, ensure_ascii=False, indent=2, sort_keys=True),
        ]
    return []


def _event_permalink_summaries(event: SlackAgentEvent) -> list[str]:
    summaries: list[str] = []
    for url in find_message_urls(event.text)[:MAX_PROMPT_MESSAGE_URLS]:
        summaries.extend(
            [
                f"- url: {url}",
                f"  operation: {_agent_config.app_name}.{SLACK_CONTEXT_OPERATION}",
                f"  input: {json.dumps({'url': url}, separators=(',', ': '))}",
            ]
        )
    return summaries


def _event_file_summaries(event: SlackAgentEvent) -> list[str]:
    summaries: list[str] = []
    for file_data in event.files:
        file_id = str(file_data.get("id") or "").strip()
        name = str(file_data.get("name") or file_data.get("title") or "").strip()
        mimetype = str(file_data.get("mimetype") or "").strip()
        size = str(file_data.get("size") or "").strip()
        parts = [f"id={file_id}"] if file_id else []
        if name:
            parts.append(f"name={name}")
        if mimetype:
            parts.append(f"mimetype={mimetype}")
        if size:
            parts.append(f"size={size}")
        if parts:
            summaries.append("- " + " ".join(parts))
    return summaries


def _event_file_ids(event: SlackAgentEvent) -> list[str]:
    return [
        file_id
        for file_id in (
            str(file_data.get("id") or "").strip() for file_data in event.files
        )
        if file_id
    ]


def _agent_session_ref(event: SlackAgentEvent) -> str:
    if event.channel_type in DIRECT_MESSAGE_CHANNEL_TYPES and not event.thread_ts:
        return f"slack:{event.team_id}:{event.channel_id}"
    root_ts = event.thread_ts or event.message_ts
    return f"slack:{event.team_id}:{event.channel_id}:{root_ts}"


def _agent_turn_idempotency_key(event: SlackAgentEvent) -> str:
    actor_id = event.user_id or event.bot_id
    if event.event_type in (SlackEventType.APP_MENTION, SlackEventType.MESSAGE):
        return f"slack:event:{event.team_id}:{event.channel_id}:{event.message_ts}:{actor_id}"
    if event.event_id:
        return f"slack:event:{event.event_id}"
    return (
        f"slack:event:{event.team_id}:{event.channel_id}:{event.message_ts}:{actor_id}"
    )


def _slack_client_msg_id(idempotency_key: str) -> str:
    key = idempotency_key.strip()
    if not key:
        return ""
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return str(uuid.UUID(hex=digest[:32]))


def _workflow_run_status_name(status: int) -> str:
    if not status:
        return ""
    try:
        return gestalt.workflow_run_status_name(status)
    except ValueError:
        return str(status)


def _bad_request(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _server_error(message: str) -> ErrorResponse:
    return gestalt.Response(
        status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message}
    )
