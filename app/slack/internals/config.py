from __future__ import annotations

from collections.abc import Iterable
import re
from typing import Any, cast

from .models import (
    SlackAgentConfig,
    SlackAgentRoute,
    SlackAgentRouteMatch,
    SlackAssistantConfig,
    SlackBotConfig,
    SlackEventDeliveryConfig,
    SlackEventDeliveryRoute,
    SlackEventDeliveryRouteMatch,
    SlackEventsConfig,
    SlackSuggestedPrompt,
    SlackWorkflowConfig,
    SUPPORTED_AGENT_ROUTE_EVENT_TYPES,
    SUPPORTED_AGENT_ROUTE_THREAD_MATCHES,
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
    events = _events_config_from_provider_config(config)
    bot = _config_dict(config, "bot")
    assistant = _assistant_config_from_provider_config(config, agent)
    workflow = _workflow_config_from_provider_config(config)
    routes = _agent_routes_from_provider_config(
        config,
        agent,
        assistant=assistant,
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
        workflow=workflow,
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

    return SlackAssistantConfig(
        enabled=_config_bool(assistant, "enabled", default=False),
        enabled_configured=_config_has_bool(assistant, "enabled"),
        suggested_prompts_title=title,
        suggested_prompts=tuple(prompts),
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
        allowed_keys={"provider", "definitionId"},
    )
    return SlackWorkflowConfig(
        provider_name=_config_string(workflow, "provider"),
        definition_id=_config_string(workflow, "definitionId"),
    )


def _agent_routes_from_provider_config(
    config: dict[str, Any],
    agent: dict[str, Any],
    *,
    assistant: SlackAssistantConfig,
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
                )
            )
    return tuple(routes)


def _agent_route_from_config(
    config: dict[str, Any],
    index: int,
    *,
    assistant: SlackAssistantConfig,
) -> SlackAgentRoute:
    agent = _config_dict(config, "agent")
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
        workflow=workflow,
        assistant=_route_assistant_config_from_config(config, agent, assistant),
    )


def _agent_route_run_as_subject_id(config: dict[str, Any], index: int) -> str:
    subject = _agent_route_run_as_subject_config(config, index)
    if subject is None:
        return ""
    subject_id = _config_string(subject, "id", "subjectId", "subjectID", "subject_id")
    if not subject_id:
        raise ValueError(f"agent.routes[{index}].runAs.subject.id is required")
    subject_value = subject_id.partition(":")[2].strip()
    if not subject_id.startswith("service_account:") or not subject_value:
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
        allowed_keys={"provider", "definitionId", "keyTemplate"},
    )
    provider = _config_string(workflow_data, "provider")
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
        suggested_prompts_title=parsed.suggested_prompts_title
        or inherited.suggested_prompts_title,
        suggested_prompts=parsed.suggested_prompts
        if _config_has_any(
            assistant, "suggestedPrompts", "suggested_prompts", "prompts"
        )
        else inherited.suggested_prompts,
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
    deliver = _config_dict(events, "deliver")
    raw_routes = _config_list(deliver, "routes")
    routes: list[SlackEventDeliveryRoute] = []
    for index, raw_route in enumerate(raw_routes, start=1):
        if isinstance(raw_route, dict):
            routes.append(_event_deliver_route_from_config(raw_route, index))
    return SlackEventsConfig(deliver=SlackEventDeliveryConfig(routes=tuple(routes)))


def _event_deliver_route_from_config(
    config: dict[str, Any], index: int
) -> SlackEventDeliveryRoute:
    _validate_workflow_config_keys(
        config,
        f"events.deliver.routes[{index}]",
        allowed_keys={
            "id",
            "name",
            "workflow",
            "workflowEventType",
            "workflow_event_type",
            "eventType",
            "event_type",
            "type",
            "subject",
            "workflowEventSubject",
            "match",
        },
    )
    route_id = _config_string(config, "id", "name") or f"route_{index}"
    workflow = _config_dict(config, "workflow")
    _validate_workflow_config_keys(
        workflow,
        f"events.deliver.routes[{index}].workflow",
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
    subject = _config_string(config, "subject", "workflowEventSubject")
    return SlackEventDeliveryRoute(
        id=route_id,
        match=_event_deliver_route_match_from_config(_config_dict(config, "match")),
        workflow_provider=workflow_provider,
        workflow_event_type=workflow_event_type or "slack.event.received",
        subject=subject or f"route:{route_id}",
    )


def _event_deliver_route_match_from_config(
    config: dict[str, Any],
) -> SlackEventDeliveryRouteMatch:
    return SlackEventDeliveryRouteMatch(
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
