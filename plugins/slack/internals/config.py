from __future__ import annotations

from collections.abc import Iterable
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
    SlackWorkflowConfig,
)


def agent_config_from_provider_config(
    plugin_name: str, config: dict[str, Any]
) -> SlackAgentConfig:
    agent = _config_dict(config, "agent")
    provider = _config_string(agent, "provider", "agentProvider", "agent_provider")
    model = _config_string(agent, "model", "agentModel", "agent_model")
    system_prompt = _config_string(
        agent, "systemPrompt", "system_prompt", "agentSystemPrompt", "prompt"
    )
    model_options = _config_dict(
        agent, "modelOptions", "model_options", "agentModelOptions"
    )
    routes = _agent_routes_from_provider_config(config, agent)
    events = _events_config_from_provider_config(config)
    bot = _config_dict(config, "bot")
    assistant = _assistant_config_from_provider_config(config, agent)
    acknowledgement = _acknowledgement_config_from_provider_config(config, agent)
    workflow = _workflow_config_from_provider_config(config)

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
        events=events,
        assistant=assistant,
        acknowledgement=acknowledgement,
        workflow=workflow,
        agent_provider=provider
        or _config_string(config, "agentProvider", "agent_provider"),
        agent_model=model or _config_string(config, "agentModel", "agent_model"),
        agent_system_prompt=system_prompt
        or _config_string(config, "agentSystemPrompt", "agent_system_prompt", "prompt"),
        agent_model_options=model_options
        or _config_dict(config, "agentModelOptions", "agent_model_options"),
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
    title, prompts = _assistant_suggested_prompts_from_config(assistant)
    status = _config_string(
        assistant, "status", "initialStatus", "initial_status", "loadingStatus"
    )

    return SlackAssistantConfig(
        enabled=_config_bool(assistant, "enabled", default=False),
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
    if not acknowledgement or not _config_bool(
        acknowledgement, "enabled", default=True
    ):
        return SlackAcknowledgementConfig()
    reaction = _config_string(
        acknowledgement,
        "reaction",
        "reactionName",
        "reaction_name",
        "emoji",
        "emojiName",
        "emoji_name",
    )
    return SlackAcknowledgementConfig(reaction=reaction.strip().strip(":"))


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
    return SlackWorkflowConfig(
        provider_name=_config_string(
            workflow, "provider", "providerName", "provider_name"
        )
        or _config_string(config, "workflowProvider", "workflow_provider"),
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
    model_options = _config_dict(
        agent, "modelOptions", "model_options", "agentModelOptions"
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
        agent_model_options=model_options
        or _config_dict(config, "modelOptions", "agentModelOptions"),
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
    route_id = _config_string(config, "id", "name") or f"route_{index}"
    workflow = _config_dict(config, "workflow")
    workflow_provider = _config_string(
        workflow, "provider", "providerName", "provider_name"
    ) or _config_string(
        config,
        "workflowProvider",
        "workflow_provider",
        "workflowProviderName",
        "workflow_provider_name",
    )
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


def _config_optional_lower_string_tuple(
    config: dict[str, Any], *keys: str
) -> tuple[str, ...] | None:
    for key in keys:
        if key in config:
            return _lower_tuple(_config_string_tuple(config, key))
    return None


def _lower_tuple(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(value.lower() for value in values)
