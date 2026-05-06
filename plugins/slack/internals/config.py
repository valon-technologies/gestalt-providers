from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast

from .models import (
    SlackAcknowledgementConfig,
    SlackAgentConfig,
    SlackAgentRoute,
    SlackAgentRouteMatch,
    SlackAgentToolRef,
    SlackAssistantLedgerConfig,
    SlackAssistantConfig,
    SlackBotConfig,
    SlackEventPublishConfig,
    SlackEventPublishRoute,
    SlackEventPublishRouteMatch,
    SlackEventsConfig,
    SlackSuggestedPrompt,
    SlackThreadContextConfig,
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
    agent_tool_sets = _agent_tool_sets_from_config(agent)
    agent_tool_set_refs = _config_string_tuple(
        agent, "toolSetRefs", "tool_set_refs"
    )
    events = _events_config_from_provider_config(config)
    bot = _config_dict(config, "bot")
    assistant = _assistant_config_from_provider_config(config, agent)
    acknowledgement = _acknowledgement_config_from_provider_config(config, agent)
    workflow = _workflow_config_from_provider_config(config)
    thread_context = _thread_context_config_from_provider_config(config, agent)
    assistant_ledger = _assistant_ledger_config_from_provider_config(config)
    routes = _agent_routes_from_provider_config(
        config,
        agent,
        assistant=assistant,
        acknowledgement=acknowledgement,
        thread_context=thread_context,
    )
    _validate_agent_route_ids(routes)
    _validate_agent_tool_set_refs(agent_tool_sets, agent_tool_set_refs, routes)

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
            ),
            user_id=_config_string(bot, "userId", "user_id", "botUserId", "bot_user_id")
            or _config_string(config, "botUserId", "bot_user_id"),
        ),
        events=events,
        assistant=assistant,
        acknowledgement=acknowledgement,
        workflow=workflow,
        thread_context=thread_context,
        assistant_ledger=assistant_ledger,
        agent_provider=provider
        or _config_string(config, "agentProvider", "agent_provider"),
        agent_model=model or _config_string(config, "agentModel", "agent_model"),
        agent_system_prompt=system_prompt
        or _config_string(config, "agentSystemPrompt", "agent_system_prompt", "prompt"),
        agent_model_options=model_options
        or _config_dict(config, "agentModelOptions", "agent_model_options"),
        agent_tool_sets=agent_tool_sets,
        agent_tool_set_refs=agent_tool_set_refs,
        agent_tools=_agent_tool_refs_from_config(agent, "agent.tools")
        or _agent_tool_refs_from_config(
            config, "agentTools", "agentTools", "agent_tools"
        ),
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
    return SlackWorkflowConfig(
        provider_name=_config_string(
            workflow, "provider", "providerName", "provider_name"
        )
        or _config_string(config, "workflowProvider", "workflow_provider"),
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


def _assistant_ledger_config_from_provider_config(
    config: dict[str, Any],
) -> SlackAssistantLedgerConfig:
    ledger = _config_dict(
        config,
        "assistantLedger",
        "assistant_ledger",
        "ledger",
        "state",
        "resilience",
    )
    if not ledger:
        return SlackAssistantLedgerConfig()
    enabled = _config_bool(ledger, "enabled", default=True)
    return SlackAssistantLedgerConfig(
        enabled=enabled,
        indexeddb_provider=_config_string(
            ledger, "indexeddb", "indexedDB", "indexeddbProvider", "indexeddb_provider"
        ),
        store=_config_string(ledger, "store", "objectStore", "object_store")
        or "slack_assistant_requests",
        stale_after_seconds=_clamp_int(
            _config_int(
                ledger,
                "staleAfterSeconds",
                "stale_after_seconds",
                "stuckAfterSeconds",
                "stuck_after_seconds",
                default=300,
            ),
            minimum=30,
            maximum=86_400,
        ),
        max_recovery_attempts=_clamp_int(
            _config_int(
                ledger,
                "maxRecoveryAttempts",
                "max_recovery_attempts",
                default=2,
            ),
            minimum=0,
            maximum=10,
        ),
        fallback_message=_config_string(
            ledger,
            "fallbackMessage",
            "fallback_message",
            "failureMessage",
            "failure_message",
        )
        or SlackAssistantLedgerConfig().fallback_message,
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
        workflow=_route_workflow_config_from_config(config, agent),
        assistant=_route_assistant_config_from_config(config, agent, assistant),
        acknowledgement=_route_acknowledgement_config_from_config(
            config, agent, acknowledgement
        ),
        thread_context=_route_thread_context_config_from_config(
            config, agent, thread_context
        ),
        agent_provider=provider
        or _config_string(config, "provider", "agentProvider", "agent_provider"),
        agent_model=model
        or _config_string(config, "model", "agentModel", "agent_model"),
        agent_system_prompt=system_prompt
        or _config_string(config, "systemPrompt", "agentSystemPrompt", "prompt"),
        agent_model_options=model_options
        or _config_dict(config, "modelOptions", "agentModelOptions"),
        agent_tool_set_refs=_config_string_tuple(
            agent, "toolSetRefs", "tool_set_refs"
        )
        or _config_string_tuple(config, "toolSetRefs", "tool_set_refs"),
        agent_tools=_agent_tool_refs_from_config(
            agent, f"agent.routes[{index}].agent.tools"
        )
        or _agent_tool_refs_from_config(config, f"agent.routes[{index}].tools"),
    )


def _route_workflow_config_from_config(
    config: dict[str, Any], agent: dict[str, Any]
) -> SlackWorkflowConfig | None:
    workflow = _config_dict_or_none(agent, "workflow")
    if workflow is None:
        workflow = _config_dict_or_none(config, "workflow")
    workflow_data = workflow or {}
    provider = _config_string(workflow_data, "provider", "providerName", "provider_name")
    provider = provider or _config_string(
        agent,
        "workflowProvider",
        "workflow_provider",
        "workflowProviderName",
        "workflow_provider_name",
    )
    provider = provider or _config_string(
        config,
        "workflowProvider",
        "workflow_provider",
        "workflowProviderName",
        "workflow_provider_name",
    )
    if workflow is None and not provider:
        return None
    return SlackWorkflowConfig(provider_name=provider)


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
        if _config_has_any(assistant, "suggestedPrompts", "suggested_prompts", "prompts")
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
            thread_context, "maxMessages", "max_messages", "messageLimit", "message_limit"
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
        if _config_has_any(
            thread_context, "includeFileContent", "include_file_content"
        )
        else inherited.include_file_content,
        include_image_data=parsed.include_image_data
        if _config_has_any(thread_context, "includeImageData", "include_image_data")
        else inherited.include_image_data,
        max_file_bytes=parsed.max_file_bytes
        if _config_has_any(thread_context, "maxFileBytes", "max_file_bytes")
        else inherited.max_file_bytes,
    )


def _agent_tool_sets_from_config(
    agent: dict[str, Any]
) -> dict[str, tuple[SlackAgentToolRef, ...]]:
    raw_tool_sets = _config_mapping(agent, "toolSets", "tool_sets")
    if not raw_tool_sets:
        return {}
    tool_sets: dict[str, tuple[SlackAgentToolRef, ...]] = {}
    for raw_name, raw_tool_set in raw_tool_sets.items():
        name = str(raw_name or "").strip()
        if not name:
            raise ValueError("agent.toolSets contains an empty tool set name")
        if isinstance(raw_tool_set, list):
            tool_set_config = {"tools": list(raw_tool_set)}
        elif isinstance(raw_tool_set, dict):
            tool_set_config = dict(raw_tool_set)
        else:
            raise ValueError(f"agent.toolSets.{name} must be a list or object")
        tool_sets[name] = _agent_tool_refs_from_config(
            tool_set_config, f"agent.toolSets.{name}"
        )
    return tool_sets


def _validate_agent_tool_set_refs(
    tool_sets: dict[str, tuple[SlackAgentToolRef, ...]],
    refs: tuple[str, ...],
    routes: tuple[SlackAgentRoute, ...],
) -> None:
    _validate_tool_set_refs(tool_sets, refs, "agent.toolSetRefs")
    for index, route in enumerate(routes, start=1):
        _validate_tool_set_refs(
            tool_sets,
            route.agent_tool_set_refs,
            f"agent.routes[{index}].agent.toolSetRefs",
        )


def _validate_agent_route_ids(routes: tuple[SlackAgentRoute, ...]) -> None:
    seen: set[str] = set()
    for index, route in enumerate(routes, start=1):
        if route.id in seen:
            raise ValueError(
                f"agent.routes[{index}].id duplicates another agent route: {route.id}"
            )
        seen.add(route.id)


def _validate_tool_set_refs(
    tool_sets: dict[str, tuple[SlackAgentToolRef, ...]],
    refs: tuple[str, ...],
    path: str,
) -> None:
    for index, ref in enumerate(refs, start=1):
        if ref not in tool_sets:
            raise ValueError(f"{path}[{index}] references unknown tool set: {ref}")


def _agent_tool_refs_from_config(
    config: dict[str, Any], path: str, *keys: str
) -> tuple[SlackAgentToolRef, ...]:
    if not keys:
        keys = ("tools", "toolRefs", "tool_refs")
    raw_tools = _config_list(config, *keys)
    if not raw_tools:
        return ()
    refs: list[SlackAgentToolRef] = []
    for index, raw_tool in enumerate(raw_tools, start=1):
        ref_path = f"{path}[{index}]"
        if not isinstance(raw_tool, dict):
            raise ValueError(f"{ref_path} must be an object")
        tool = cast(dict[str, Any], raw_tool)
        unsupported_fields = [
            name
            for name in (
                "credentialMode",
                "credential_mode",
                "runAs",
                "run_as",
                "system",
                "input",
                "inputBindings",
                "input_bindings",
                "permissions",
            )
            if name in tool
        ]
        if unsupported_fields:
            joined = ", ".join(unsupported_fields)
            raise ValueError(f"{ref_path} contains unsupported field(s): {joined}")
        plugin = _config_string(tool, "plugin", "pluginName", "plugin_name")
        operation = _config_string(tool, "operation", "operationName", "operation_name")
        connection = _config_string(tool, "connection", "connectionName")
        instance = _config_string(tool, "instance", "instanceName")
        if not plugin or plugin == "*" or plugin.lower() == "system":
            raise ValueError(f"{ref_path}.plugin must be an exact plugin name")
        if not operation or operation == "*":
            raise ValueError(f"{ref_path}.operation must be an exact operation name")
        if connection == "*" or instance == "*":
            raise ValueError(f"{ref_path} connection and instance must be exact")
        refs.append(
            SlackAgentToolRef(
                plugin=plugin,
                operation=operation,
                connection=connection,
                instance=instance,
                title=_config_string(tool, "title"),
                description=_config_string(tool, "description"),
            )
        )
    return tuple(refs)


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


def _config_dict_or_none(config: dict[str, Any], *keys: str) -> dict[str, Any] | None:
    for key in keys:
        value = config.get(key)
        if isinstance(value, dict):
            return dict(value)
    return None


def _config_mapping(config: dict[str, Any], *keys: str) -> dict[str, Any]:
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


def _lower_tuple(values: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(value.lower() for value in values)
