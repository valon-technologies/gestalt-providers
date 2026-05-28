from __future__ import annotations

from collections.abc import Iterable
import re
from typing import Any, cast

from .models import (
    SlackAcknowledgementConfig,
    SlackAgentConfig,
    SlackAgentRoute,
    SlackAgentRouteMatch,
    SlackAssistantConfig,
    SlackBotConfig,
    SlackEventPublishConfig,
    SlackEventPublishRoute,
    SlackEventPublishRouteMatch,
    SlackEventsConfig,
    SlackSuggestedPrompt,
    SlackThreadContextConfig,
    SlackWorkflowConfig,
    SUPPORTED_AGENT_ROUTE_EVENT_TYPES,
    SUPPORTED_AGENT_ROUTE_THREAD_MATCHES,
)

LEGACY_AGENT_TARGET_KEYS = frozenset(
    {
        "agentModel",
        "agentOptions",
        "agentProvider",
        "agentSystemPrompt",
        "agentTools",
        "agent_model",
        "agent_model_options",
        "agent_provider",
        "agent_system_prompt",
        "agent_tools",
        "definitionId",
        "definition_id",
        "model",
        "modelOptions",
        "model_options",
        "prompt",
        "provider",
        "responseSchema",
        "response_schema",
        "sessionKey",
        "session_key",
        "slackReply",
        "slack_reply",
        "steps",
        "systemPrompt",
        "system_prompt",
        "timeoutSeconds",
        "timeout_seconds",
        "toolRefs",
        "toolSetRefs",
        "toolSets",
        "tool_refs",
        "tool_set_refs",
        "tool_sets",
        "tools",
        "workflow",
        "workflowDefinitionId",
        "workflowProvider",
        "workflowProviderName",
        "workflow_definition_id",
        "workflow_provider",
        "workflow_provider_name",
        "workflowId",
        "workflow_id",
    }
)
WORKFLOW_KEY_TEMPLATE_FIELDS = frozenset(
    {
        "team_id",
        "channel_id",
        "message_ts",
        "thread_ts",
        "reply_thread_ts",
        "event_id",
        "route_id",
    }
)
WORKFLOW_KEY_TEMPLATE_FIELD_RE = re.compile(r"\$\{([^}]+)\}")


def agent_config_from_provider_config(
    app_name: str, config: dict[str, Any]
) -> SlackAgentConfig:
    agent = _config_dict(config, "agent")
    _reject_legacy_agent_target_config(
        config,
        "config",
        {
            "agentModel",
            "agentModelOptions",
            "agentProvider",
            "agentSystemPrompt",
            "agentTools",
            "agent_model",
            "agent_model_options",
            "agent_provider",
            "agent_system_prompt",
            "agent_tools",
            "workflowProvider",
            "workflow_provider",
            "workflowProviderName",
            "workflow_provider_name",
            "prompt",
        },
    )
    _reject_legacy_agent_target_config(agent, "agent")
    events = _events_config_from_provider_config(config)
    bot = _config_dict(config, "bot")
    assistant = _assistant_config_from_provider_config(config, agent)
    acknowledgement = _acknowledgement_config_from_provider_config(config, agent)
    workflow = _workflow_config_from_provider_config(config)
    thread_context = _thread_context_config_from_provider_config(config, agent)
    routes = _agent_routes_from_provider_config(
        config,
        agent,
        assistant=assistant,
        acknowledgement=acknowledgement,
        thread_context=thread_context,
    )
    _validate_agent_route_ids(routes)

    return SlackAgentConfig(
        app_name=app_name.strip() or "slack",
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
            ),
            user_id=_config_string(bot, "userId", "user_id", "botUserId", "bot_user_id")
            or _config_string(config, "botUserId", "bot_user_id"),
        ),
        events=events,
        assistant=assistant,
        acknowledgement=acknowledgement,
        workflow=workflow,
        thread_context=thread_context,
        routes=routes,
    )


def normalize_suggested_prompts(
    prompts: Iterable[object], *, require_one: bool = True
) -> list[SlackSuggestedPrompt]:
    normalized: list[SlackSuggestedPrompt] = []
    for prompt in prompts:
        if not isinstance(prompt, dict):
            continue
        prompt_data = cast(dict[str, Any], prompt)
        title = str(prompt_data.get("title") or "").strip()
        message = str(prompt_data.get("message") or "").strip()
        if title and message:
            normalized.append(SlackSuggestedPrompt(title=title, message=message))
        if len(normalized) >= 4:
            break
    if require_one and not normalized:
        raise ValueError("at least one prompt with title and message is required")
    return normalized


def _reject_legacy_agent_target_config(
    config: dict[str, Any],
    path: str,
    keys: set[str] | frozenset[str] | None = None,
    *,
    allowed_keys: set[str] | frozenset[str] | None = None,
) -> None:
    if not config:
        return
    if allowed_keys is not None:
        unsupported = sorted(str(key) for key in config if str(key) not in allowed_keys)
    else:
        unsupported = sorted(
            str(key) for key in config if str(key) in (keys or LEGACY_AGENT_TARGET_KEYS)
        )
    if unsupported:
        joined = ", ".join(unsupported)
        raise ValueError(
            f"{path} contains unsupported Slack agent target field(s): {joined}; "
            "define workflow steps in a global workflow definition and set "
            "workflow.definitionId"
        )


def _assistant_config_from_provider_config(
    config: dict[str, Any], agent: dict[str, Any]
) -> SlackAssistantConfig:
    assistant = _config_dict(agent, "assistant")
    if not assistant:
        assistant = _config_dict(
            config, "assistant", "slackAssistant", "assistantConfig"
        )
    return _assistant_config_from_config(assistant)


def _assistant_config_from_config(assistant: dict[str, Any]) -> SlackAssistantConfig:
    title, prompts = _assistant_suggested_prompts_from_config(assistant)
    status = _config_string(
        assistant, "status", "initialStatus", "initial_status", "loadingStatus"
    )

    return SlackAssistantConfig(
        enabled=_config_bool(assistant, "enabled", default=False),
        enabled_configured=_config_has_bool(assistant, "enabled"),
        status=status or "thinking...",
        loading_messages=_config_string_tuple(
            assistant, "loadingMessages", "loading_messages"
        ),
        icon_emoji=_config_string(assistant, "iconEmoji", "icon_emoji"),
        icon_url=_config_string(assistant, "iconUrl", "icon_url"),
        username=_config_string(assistant, "username"),
        suggested_prompts_title=title,
        suggested_prompts=tuple(prompts),
    )


def _acknowledgement_config_from_provider_config(
    config: dict[str, Any], agent: dict[str, Any]
) -> SlackAcknowledgementConfig:
    acknowledgement = _config_dict(agent, "acknowledgement", "acknowledgment", "ack")
    if not acknowledgement:
        acknowledgement = _config_dict(
            config, "acknowledgement", "acknowledgment", "ack"
        )
    if not acknowledgement:
        return SlackAcknowledgementConfig(enabled=False)
    if not _config_bool(acknowledgement, "enabled", default=True):
        return SlackAcknowledgementConfig(enabled=False)
    reaction = _config_string(
        acknowledgement,
        "reaction",
        "reactionName",
        "reaction_name",
        "emoji",
        "emojiName",
        "emoji_name",
    )
    return SlackAcknowledgementConfig(
        enabled=True, reaction=reaction.strip().strip(":")
    )


def _assistant_suggested_prompts_from_config(
    assistant: dict[str, Any],
) -> tuple[str, list[SlackSuggestedPrompt]]:
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
    return title, normalize_suggested_prompts(raw_prompts, require_one=False)


def _workflow_config_from_provider_config(
    config: dict[str, Any],
) -> SlackWorkflowConfig:
    workflow = _config_dict(config, "workflow")
    _validate_workflow_config_keys(
        workflow,
        "workflow",
        allowed_keys={"provider", "definitionId", "target"},
    )
    if "target" in workflow:
        raise ValueError(
            "workflow.target is not supported; define the workflow globally and "
            "set workflow.definitionId"
        )
    return SlackWorkflowConfig(
        provider_name=_config_string(workflow, "provider"),
        definition_id=_config_string(workflow, "definitionId"),
    )


def _thread_context_config_from_provider_config(
    config: dict[str, Any], agent: dict[str, Any]
) -> SlackThreadContextConfig:
    thread_context = _config_dict(agent, "threadContext", "thread_context")
    if not thread_context:
        thread_context = _config_dict(config, "threadContext", "thread_context")
    return _thread_context_config_from_config(thread_context)


def _thread_context_config_from_config(
    thread_context: dict[str, Any],
) -> SlackThreadContextConfig:
    return SlackThreadContextConfig(
        enabled=_config_bool(thread_context, "enabled", default=True),
        max_messages=_clamp_int(
            _config_int(
                thread_context,
                "maxMessages",
                "max_messages",
                "messageLimit",
                "message_limit",
                default=200,
            ),
            minimum=1,
            maximum=1000,
        ),
        include_user_info=_config_bool(
            thread_context,
            "includeUserInfo",
            "include_user_info",
            default=False,
        ),
        include_bots=_config_bool(
            thread_context,
            "includeBots",
            "include_bots",
            default=True,
        ),
        include_files=_config_bool(
            thread_context,
            "includeFiles",
            "include_files",
            default=True,
        ),
        include_file_content=_config_bool(
            thread_context,
            "includeFileContent",
            "include_file_content",
            default=False,
        ),
        include_image_data=_config_bool(
            thread_context,
            "includeImageData",
            "include_image_data",
            default=False,
        ),
        max_file_bytes=_clamp_int(
            _config_int(
                thread_context,
                "maxFileBytes",
                "max_file_bytes",
                default=200_000,
            ),
            minimum=0,
            maximum=25_000_000,
        ),
    )


def _agent_routes_from_provider_config(
    config: dict[str, Any],
    agent: dict[str, Any],
    *,
    assistant: SlackAssistantConfig,
    acknowledgement: SlackAcknowledgementConfig,
    thread_context: SlackThreadContextConfig,
) -> tuple[SlackAgentRoute, ...]:
    raw_routes = _config_list(agent, "routes")
    if not raw_routes:
        raw_routes = _config_list(config, "agentRoutes", "agent_routes")
    routes: list[SlackAgentRoute] = []
    for index, raw_route in enumerate(raw_routes, start=1):
        if isinstance(raw_route, dict):
            routes.append(
                _agent_route_from_config(
                    raw_route,
                    index,
                    assistant=assistant,
                    acknowledgement=acknowledgement,
                    thread_context=thread_context,
                )
            )
    return tuple(routes)


def _agent_route_from_config(
    config: dict[str, Any],
    index: int,
    *,
    assistant: SlackAssistantConfig,
    acknowledgement: SlackAcknowledgementConfig,
    thread_context: SlackThreadContextConfig,
) -> SlackAgentRoute:
    agent = _config_dict(config, "agent")
    _reject_legacy_agent_target_config(
        config,
        f"agent.routes[{index}]",
        {
            "agentModel",
            "agentModelOptions",
            "agentProvider",
            "agentSystemPrompt",
            "model",
            "modelOptions",
            "model_options",
            "prompt",
            "provider",
            "responseSchema",
            "response_schema",
            "sessionKey",
            "session_key",
            "slackReply",
            "slack_reply",
            "steps",
            "systemPrompt",
            "system_prompt",
            "timeoutSeconds",
            "timeout_seconds",
            "toolRefs",
            "toolSetRefs",
            "tools",
            "tool_refs",
            "tool_set_refs",
            "workflowDefinitionId",
            "workflowId",
            "workflowProvider",
            "workflowProviderName",
            "workflow_definition_id",
            "workflow_id",
            "workflow_provider",
            "workflow_provider_name",
        },
    )
    _reject_legacy_agent_target_config(
        agent,
        f"agent.routes[{index}].agent",
        allowed_keys={
            "ack",
            "acknowledgement",
            "acknowledgment",
            "assistant",
            "threadContext",
            "thread_context",
        },
    )

    match = _agent_route_match_from_config(_config_dict(config, "match"))
    run_as_subject_id = _agent_route_run_as_subject_id(config, index)
    if run_as_subject_id and not _agent_route_run_as_match_guarded(match):
        raise ValueError(
            f"agent.routes[{index}].runAs requires match.botIds or an explicit "
            "top-level unaddressed channel message match"
        )
    workflow = _route_workflow_config_from_config(config, index)

    return SlackAgentRoute(
        id=_config_string(config, "id", "name") or f"route_{index}",
        match=match,
        run_as_subject_id=run_as_subject_id,
        run_as_subject_kind=_agent_route_run_as_subject_kind(config, index),
        run_as_display_name=_agent_route_run_as_display_name(config, index),
        workflow=workflow,
        assistant=_route_assistant_config_from_config(config, agent, assistant),
        acknowledgement=_route_acknowledgement_config_from_config(
            config, agent, acknowledgement
        ),
        thread_context=_route_thread_context_config_from_config(
            config, agent, thread_context
        ),
    )


def _agent_route_run_as_subject_id(config: dict[str, Any], index: int) -> str:
    subject = _agent_route_run_as_subject_config(config, index)
    if subject is None:
        return ""
    subject_id = _config_string(subject, "id", "subjectId", "subjectID", "subject_id")
    if not subject_id:
        raise ValueError(f"agent.routes[{index}].runAs.subject.id is required")
    kind = _agent_route_run_as_subject_kind(config, index, subject_id=subject_id)
    subject_value = subject_id.partition(":")[2].strip()
    if (
        kind != "service_account"
        or not subject_id.startswith("service_account:")
        or not subject_value
    ):
        raise ValueError(
            f"agent.routes[{index}].runAs.subject must identify a service_account subject"
        )
    return subject_id


def _agent_route_run_as_match_guarded(match: SlackAgentRouteMatch) -> bool:
    if match.bot_ids:
        return True
    event_types = frozenset(value.strip().lower() for value in match.event_types)
    return bool(
        match.channel_ids
        and match.thread == "root"
        and match.addressed_to_bot is False
        and event_types
        and event_types.issubset({"message.channels"})
    )


def _agent_route_run_as_subject_kind(
    config: dict[str, Any], index: int, *, subject_id: str = ""
) -> str:
    subject = _agent_route_run_as_subject_config(config, index)
    if subject is None:
        return ""
    kind = _config_string(subject, "kind", "type", "subjectKind", "subject_kind")
    subject_id = subject_id or _config_string(
        subject, "id", "subjectId", "subjectID", "subject_id"
    )
    if not kind and ":" in subject_id:
        kind, _separator, _value = subject_id.partition(":")
    kind = kind.strip()
    if kind and kind != "service_account":
        raise ValueError(
            f"agent.routes[{index}].runAs.subject must identify a service_account subject"
        )
    return kind


def _agent_route_run_as_display_name(config: dict[str, Any], index: int) -> str:
    subject = _agent_route_run_as_subject_config(config, index)
    if subject is None:
        return ""
    return _config_string(subject, "displayName", "display_name")


def _agent_route_run_as_subject_config(
    config: dict[str, Any], index: int
) -> dict[str, Any] | None:
    run_as: Any = None
    configured = False
    for key in ("runAs", "run_as"):
        if key in config:
            run_as = config.get(key)
            configured = True
            break
    if not configured:
        return None
    if not isinstance(run_as, dict):
        raise ValueError(f"agent.routes[{index}].runAs must be an object")
    subject = run_as.get("subject")
    if not isinstance(subject, dict):
        raise ValueError(f"agent.routes[{index}].runAs.subject is required")
    return dict(subject)


def _route_workflow_config_from_config(
    config: dict[str, Any], index: int
) -> SlackWorkflowConfig | None:
    workflow = _config_dict_or_none(config, "workflow")
    workflow_data = workflow or {}
    _validate_workflow_config_keys(
        workflow_data,
        f"agent.routes[{index}].workflow",
        allowed_keys={"provider", "definitionId", "keyTemplate", "target"},
    )
    provider = _config_string(workflow_data, "provider")
    if "target" in workflow_data:
        raise ValueError(
            f"agent.routes[{index}].workflow.target is not supported; "
            "define the workflow globally and set workflow.definitionId"
        )
    definition_id = _config_string(workflow_data, "definitionId")
    key_template = _workflow_key_template_from_config(
        workflow_data, f"agent.routes[{index}].workflow"
    )
    if workflow is None and not provider and not definition_id and not key_template:
        return None
    return SlackWorkflowConfig(
        provider_name=provider,
        key_template=key_template,
        definition_id=definition_id,
    )


def _validate_workflow_config_keys(
    config: dict[str, Any],
    path: str,
    *,
    allowed_keys: set[str],
) -> None:
    unknown = sorted(str(key) for key in config if str(key) not in allowed_keys)
    if unknown:
        raise ValueError(
            f"{path} has unsupported key(s): {', '.join(unknown)}; use "
            "workflow.provider and workflow.definitionId"
        )


def _workflow_key_template_from_config(config: dict[str, Any], path: str) -> str:
    key_template = _config_string(config, "keyTemplate")
    for field in WORKFLOW_KEY_TEMPLATE_FIELD_RE.findall(key_template):
        if field not in WORKFLOW_KEY_TEMPLATE_FIELDS:
            allowed = ", ".join(sorted(WORKFLOW_KEY_TEMPLATE_FIELDS))
            raise ValueError(
                f"{path}.keyTemplate references unsupported field {field!r}; "
                f"supported fields: {allowed}"
            )
    return key_template


def _route_assistant_config_from_config(
    config: dict[str, Any],
    agent: dict[str, Any],
    inherited: SlackAssistantConfig,
) -> SlackAssistantConfig | None:
    assistant = _config_dict_or_none(agent, "assistant")
    if assistant is None:
        assistant = _config_dict_or_none(
            config, "assistant", "slackAssistant", "assistantConfig"
        )
    if assistant is None:
        return None
    parsed = _assistant_config_from_config(assistant)
    return SlackAssistantConfig(
        enabled=parsed.enabled if parsed.enabled_configured else inherited.enabled,
        enabled_configured=parsed.enabled_configured,
        status=parsed.status
        if _config_has_any(
            assistant, "status", "initialStatus", "initial_status", "loadingStatus"
        )
        else inherited.status,
        loading_messages=parsed.loading_messages
        if _config_has_any(assistant, "loadingMessages", "loading_messages")
        else inherited.loading_messages,
        icon_emoji=parsed.icon_emoji or inherited.icon_emoji,
        icon_url=parsed.icon_url or inherited.icon_url,
        username=parsed.username or inherited.username,
        suggested_prompts_title=parsed.suggested_prompts_title
        or inherited.suggested_prompts_title,
        suggested_prompts=parsed.suggested_prompts
        if _config_has_any(
            assistant, "suggestedPrompts", "suggested_prompts", "prompts"
        )
        else inherited.suggested_prompts,
    )


def _route_acknowledgement_config_from_config(
    config: dict[str, Any],
    agent: dict[str, Any],
    inherited: SlackAcknowledgementConfig,
) -> SlackAcknowledgementConfig | None:
    acknowledgement = _config_dict_or_none(
        agent, "acknowledgement", "acknowledgment", "ack"
    )
    if acknowledgement is None:
        acknowledgement = _config_dict_or_none(
            config, "acknowledgement", "acknowledgment", "ack"
        )
    if acknowledgement is None:
        return None
    if not _config_bool(acknowledgement, "enabled", default=True):
        return SlackAcknowledgementConfig(enabled=False)
    reaction = _config_string(
        acknowledgement,
        "reaction",
        "reactionName",
        "reaction_name",
        "emoji",
        "emojiName",
        "emoji_name",
    )
    return SlackAcknowledgementConfig(
        enabled=True, reaction=reaction.strip().strip(":") or inherited.reaction
    )


def _route_thread_context_config_from_config(
    config: dict[str, Any],
    agent: dict[str, Any],
    inherited: SlackThreadContextConfig,
) -> SlackThreadContextConfig | None:
    thread_context = _config_dict_or_none(agent, "threadContext", "thread_context")
    if thread_context is None:
        thread_context = _config_dict_or_none(config, "threadContext", "thread_context")
    if thread_context is None:
        return None
    parsed = _thread_context_config_from_config(thread_context)
    return SlackThreadContextConfig(
        enabled=_config_bool(thread_context, "enabled", default=inherited.enabled),
        max_messages=parsed.max_messages
        if _config_has_any(
            thread_context,
            "maxMessages",
            "max_messages",
            "messageLimit",
            "message_limit",
        )
        else inherited.max_messages,
        include_user_info=parsed.include_user_info
        if _config_has_any(thread_context, "includeUserInfo", "include_user_info")
        else inherited.include_user_info,
        include_bots=parsed.include_bots
        if _config_has_any(thread_context, "includeBots", "include_bots")
        else inherited.include_bots,
        include_files=parsed.include_files
        if _config_has_any(thread_context, "includeFiles", "include_files")
        else inherited.include_files,
        include_file_content=parsed.include_file_content
        if _config_has_any(thread_context, "includeFileContent", "include_file_content")
        else inherited.include_file_content,
        include_image_data=parsed.include_image_data
        if _config_has_any(thread_context, "includeImageData", "include_image_data")
        else inherited.include_image_data,
        max_file_bytes=parsed.max_file_bytes
        if _config_has_any(thread_context, "maxFileBytes", "max_file_bytes")
        else inherited.max_file_bytes,
    )


def _validate_agent_route_ids(routes: tuple[SlackAgentRoute, ...]) -> None:
    seen: set[str] = set()
    for index, route in enumerate(routes, start=1):
        if route.id in seen:
            raise ValueError(
                f"agent.routes[{index}].id duplicates another agent route: {route.id}"
            )
        seen.add(route.id)


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
        event_types=_config_choice_tuple(
            config,
            SUPPORTED_AGENT_ROUTE_EVENT_TYPES,
            "eventType",
            "eventTypes",
            "event_type",
            "event_types",
        ),
        subtypes=_config_optional_lower_string_tuple(
            config, "subtype", "subtypes", "sub_type", "sub_types"
        ),
        user_ids=_config_string_tuple(
            config, "user", "users", "userId", "userIds", "user_id", "user_ids"
        ),
        bot_ids=_config_string_tuple(
            config, "bot", "bots", "botId", "botIds", "bot_id", "bot_ids"
        ),
        include_bot_events=_config_bool(
            config,
            "includeBotEvents",
            "include_bot_events",
            "includeBots",
            "include_bots",
            default=False,
        ),
        addressed_to_bot=_config_optional_bool(
            config,
            "addressedToBot",
            "addressed_to_bot",
            "botMentioned",
            "bot_mentioned",
        ),
        thread=_config_choice(
            config,
            SUPPORTED_AGENT_ROUTE_THREAD_MATCHES,
            "thread",
            default="any",
        ),
    )


def _events_config_from_provider_config(config: dict[str, Any]) -> SlackEventsConfig:
    events = _config_dict(config, "events")
    publish = _config_dict(events, "publish")
    raw_routes = _config_list(publish, "routes")
    routes: list[SlackEventPublishRoute] = []
    for index, raw_route in enumerate(raw_routes, start=1):
        if isinstance(raw_route, dict):
            routes.append(_event_publish_route_from_config(raw_route, index))
    return SlackEventsConfig(publish=SlackEventPublishConfig(routes=tuple(routes)))


def _event_publish_route_from_config(
    config: dict[str, Any], index: int
) -> SlackEventPublishRoute:
    _validate_workflow_config_keys(
        config,
        f"events.publish.routes[{index}]",
        allowed_keys={
            "id",
            "name",
            "workflow",
            "workflowEventType",
            "workflow_event_type",
            "eventType",
            "event_type",
            "type",
            "source",
            "workflowEventSource",
            "subject",
            "workflowEventSubject",
            "match",
        },
    )
    route_id = _config_string(config, "id", "name") or f"route_{index}"
    workflow = _config_dict(config, "workflow")
    _validate_workflow_config_keys(
        workflow,
        f"events.publish.routes[{index}].workflow",
        allowed_keys={"provider"},
    )
    workflow_provider = _config_string(workflow, "provider")
    workflow_event_type = _config_string(
        config,
        "workflowEventType",
        "workflow_event_type",
        "eventType",
        "event_type",
        "type",
    )
    source = _config_string(config, "source", "workflowEventSource")
    subject = _config_string(config, "subject", "workflowEventSubject")
    return SlackEventPublishRoute(
        id=route_id,
        match=_event_publish_route_match_from_config(_config_dict(config, "match")),
        workflow_provider=workflow_provider,
        workflow_event_type=workflow_event_type or "slack.event.received",
        source=source or "slack",
        subject=subject or f"route:{route_id}",
    )


def _event_publish_route_match_from_config(
    config: dict[str, Any],
) -> SlackEventPublishRouteMatch:
    return SlackEventPublishRouteMatch(
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
        subtypes=_config_optional_lower_string_tuple(
            config, "subtype", "subtypes", "sub_type", "sub_types"
        ),
        user_ids=_config_string_tuple(
            config, "user", "users", "userId", "userIds", "user_id", "user_ids"
        ),
        bot_ids=_config_string_tuple(
            config, "bot", "bots", "botId", "botIds", "bot_id", "bot_ids"
        ),
        include_bot_events=_config_bool(
            config,
            "includeBotEvents",
            "include_bot_events",
            "includeBots",
            "include_bots",
            default=False,
        ),
    )


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


def _config_dict_or_none(config: dict[str, Any], *keys: str) -> dict[str, Any] | None:
    for key in keys:
        value = config.get(key)
        if isinstance(value, dict):
            return dict(value)
    return None


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


def _config_optional_bool(config: dict[str, Any], *keys: str) -> bool | None:
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
    return None


def _config_has_bool(config: dict[str, Any], *keys: str) -> bool:
    for key in keys:
        value = config.get(key)
        if isinstance(value, bool):
            return True
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on", "0", "false", "no", "off"}:
                return True
    return False


def _config_has_any(config: dict[str, Any], *keys: str) -> bool:
    return any(key in config for key in keys)


def _config_int(config: dict[str, Any], *keys: str, default: int) -> int:
    for key in keys:
        value = config.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value.strip())
            except ValueError:
                continue
    return default


def _clamp_int(value: int, *, minimum: int, maximum: int) -> int:
    return max(minimum, min(value, maximum))


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


def _config_optional_lower_string_tuple(
    config: dict[str, Any], *keys: str
) -> tuple[str, ...] | None:
    for key in keys:
        if key in config:
            return _lower_tuple(_config_string_tuple(config, key))
    return None


def _config_choice_tuple(
    config: dict[str, Any],
    allowed_values: frozenset[str],
    *keys: str,
) -> tuple[str, ...]:
    for key in keys:
        if key not in config:
            continue
        value = config.get(key)
        raw_values: list[str]
        if isinstance(value, str):
            raw_values = [value]
        elif isinstance(value, list):
            raw_values = []
            for index, item in enumerate(value, start=1):
                if not isinstance(item, str):
                    raise ValueError(f"{key}[{index}] must be a string")
                raw_values.append(item)
        else:
            raise ValueError(f"{key} must be a string or list of strings")

        values: list[str] = []
        for index, raw_value in enumerate(raw_values, start=1):
            normalized = raw_value.strip()
            if not normalized:
                raise ValueError(f"{key}[{index}] must not be empty")
            if normalized not in allowed_values:
                allowed = ", ".join(sorted(allowed_values))
                raise ValueError(
                    f"{key}[{index}] must be one of: {allowed}; got {normalized!r}"
                )
            values.append(normalized)
        return tuple(dict.fromkeys(values))
    return ()


def _config_choice(
    config: dict[str, Any],
    allowed_values: frozenset[str],
    *keys: str,
    default: str = "",
) -> str:
    for key in keys:
        if key not in config:
            continue
        value = config.get(key)
        if not isinstance(value, str):
            raise ValueError(f"{key} must be a string")
        normalized = value.strip()
        if not normalized:
            raise ValueError(f"{key} must not be empty")
        if normalized not in allowed_values:
            allowed = ", ".join(sorted(allowed_values))
            raise ValueError(f"{key} must be one of: {allowed}; got {normalized!r}")
        return normalized
    return default


def _lower_tuple(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(value.lower() for value in values)
