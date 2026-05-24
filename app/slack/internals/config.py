from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast

from .models import (
    SlackAcknowledgementConfig,
    SlackAgentConfig,
    SlackAgentRoute,
    SlackAgentRouteMatch,
    SlackAgentStep,
    SlackAgentToolRef,
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

MAX_WORKFLOW_AGENT_TIMEOUT_SECONDS = 2_147_483_647


def agent_config_from_provider_config(
    app_name: str, config: dict[str, Any]
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
    timeout_seconds = _config_timeout_seconds(
        agent, "timeoutSeconds", "timeout_seconds"
    )
    agent_tool_sets = _agent_tool_sets_from_config(agent)
    agent_tool_set_refs = _config_string_tuple(agent, "toolSetRefs", "tool_set_refs")
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
    _validate_agent_steps(routes)
    _validate_agent_tool_set_refs(agent_tool_sets, agent_tool_set_refs, routes)

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
        agent_provider=provider
        or _config_string(config, "agentProvider", "agent_provider"),
        agent_model=model or _config_string(config, "agentModel", "agent_model"),
        agent_system_prompt=system_prompt
        or _config_string(config, "agentSystemPrompt", "agent_system_prompt", "prompt"),
        agent_model_options=model_options
        or _config_dict(config, "agentModelOptions", "agent_model_options"),
        agent_timeout_seconds=timeout_seconds,
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
    timeout_seconds = _config_timeout_seconds(
        agent, "timeoutSeconds", "timeout_seconds"
    )
    if timeout_seconds <= 0:
        timeout_seconds = _config_timeout_seconds(
            config, "timeoutSeconds", "timeout_seconds"
        )

    match = _agent_route_match_from_config(_config_dict(config, "match"))
    run_as_subject_id = _agent_route_run_as_subject_id(config, index)
    if run_as_subject_id and not _agent_route_run_as_match_guarded(match):
        raise ValueError(
            f"agent.routes[{index}].runAs requires match.botIds or an explicit "
            "top-level unaddressed channel message match"
        )

    return SlackAgentRoute(
        id=_config_string(config, "id", "name") or f"route_{index}",
        match=match,
        run_as_subject_id=run_as_subject_id,
        run_as_subject_kind=_agent_route_run_as_subject_kind(config, index),
        run_as_display_name=_agent_route_run_as_display_name(config, index),
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
        agent_timeout_seconds=timeout_seconds,
        agent_tool_set_refs=_config_string_tuple(agent, "toolSetRefs", "tool_set_refs")
        or _config_string_tuple(config, "toolSetRefs", "tool_set_refs"),
        agent_tools=_agent_tool_refs_from_config(
            agent, f"agent.routes[{index}].agent.tools"
        )
        or _agent_tool_refs_from_config(config, f"agent.routes[{index}].tools"),
        agent_steps=_agent_steps_from_config(agent, index),
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
    config: dict[str, Any], agent: dict[str, Any]
) -> SlackWorkflowConfig | None:
    workflow = _config_dict_or_none(agent, "workflow")
    if workflow is None:
        workflow = _config_dict_or_none(config, "workflow")
    workflow_data = workflow or {}
    provider = _config_string(
        workflow_data, "provider", "providerName", "provider_name"
    )
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


def _agent_steps_from_config(
    agent: dict[str, Any], route_index: int
) -> tuple[SlackAgentStep, ...]:
    raw_steps = _config_list(agent, "steps")
    if not raw_steps:
        return ()
    steps: list[SlackAgentStep] = []
    for step_index, raw_step in enumerate(raw_steps, start=1):
        path = f"agent.routes[{route_index}].agent.steps[{step_index}]"
        if not isinstance(raw_step, dict):
            raise ValueError(f"{path} must be an object")
        step = cast(dict[str, Any], raw_step)
        slack_reply = _config_dict_or_none(step, "slackReply", "slack_reply")
        steps.append(
            SlackAgentStep(
                id=_config_string(step, "id", "name"),
                prompt=_config_string(step, "prompt"),
                messages=tuple(_agent_step_messages_from_config(step, path)),
                tool_set_refs=_config_string_tuple(
                    step, "toolSetRefs", "tool_set_refs"
                ),
                tools=_agent_tool_refs_from_config(step, f"{path}.tools"),
                response_schema=_config_json_object(
                    step, path, "responseSchema", "response_schema"
                ),
                model_options=_config_json_object(
                    step, path, "modelOptions", "model_options"
                ),
                metadata=_config_json_object(step, path, "metadata"),
                timeout_seconds=_config_timeout_seconds(
                    step, "timeoutSeconds", "timeout_seconds"
                ),
                when=_agent_step_when_from_config(step, path),
                slack_reply_agent_output=_config_string(
                    slack_reply or {}, "agentOutput", "agent_output"
                ),
            )
        )
    return tuple(steps)


def _agent_step_messages_from_config(
    config: dict[str, Any], path: str
) -> list[dict[str, Any]]:
    raw_messages = _config_list(config, "messages")
    messages: list[dict[str, Any]] = []
    for index, raw_message in enumerate(raw_messages, start=1):
        message_path = f"{path}.messages[{index}]"
        if not isinstance(raw_message, dict):
            raise ValueError(f"{message_path} must be an object")
        message = cast(dict[str, Any], raw_message)
        role = _config_string(message, "role") or "user"
        text = _config_string(message, "text", "content")
        metadata = _config_json_object(message, message_path, "metadata")
        item: dict[str, Any] = {"role": role, "text": text}
        if metadata:
            item["metadata"] = metadata
        messages.append(item)
    return messages


def _agent_step_when_from_config(config: dict[str, Any], path: str) -> dict[str, Any]:
    if "when" not in config:
        return {}
    value = config.get("when")
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{path}.when must be an object")
    raw = cast(dict[str, Any], value)
    allowed = {"stepId", "step_id", "outputPath", "output_path", "equals"}
    unknown = sorted(str(key) for key in raw if str(key) not in allowed)
    if unknown:
        raise ValueError(f"{path}.when has unknown keys: {', '.join(unknown)}")
    step_id = _config_string(raw, "stepId", "step_id")
    output_path = _config_string(raw, "outputPath", "output_path")
    if not step_id:
        raise ValueError(f"{path}.when.stepId is required")
    if not output_path:
        raise ValueError(f"{path}.when.outputPath is required")
    if "equals" not in raw:
        raise ValueError(f"{path}.when.equals is required")
    equals = raw.get("equals")
    if not _config_scalar(equals):
        raise ValueError(f"{path}.when.equals must be a scalar JSON value")
    return {"step_id": step_id, "output_path": output_path, "equals": equals}


def _config_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, bool, int, float))


def _agent_tool_sets_from_config(
    agent: dict[str, Any],
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
        for step_index, step in enumerate(route.agent_steps, start=1):
            _validate_tool_set_refs(
                tool_sets,
                step.tool_set_refs,
                f"agent.routes[{index}].agent.steps[{step_index}].toolSetRefs",
            )


def _validate_agent_route_ids(routes: tuple[SlackAgentRoute, ...]) -> None:
    seen: set[str] = set()
    for index, route in enumerate(routes, start=1):
        if route.id in seen:
            raise ValueError(
                f"agent.routes[{index}].id duplicates another agent route: {route.id}"
            )
        seen.add(route.id)


def _validate_agent_steps(routes: tuple[SlackAgentRoute, ...]) -> None:
    for route_index, route in enumerate(routes, start=1):
        seen: set[str] = set()
        for step_index, step in enumerate(route.agent_steps, start=1):
            path = f"agent.routes[{route_index}].agent.steps[{step_index}]"
            if not step.id:
                raise ValueError(f"{path}.id is required")
            if step.id in seen:
                raise ValueError(f"{path}.id duplicates another step: {step.id}")
            if not step.prompt and not step.messages:
                raise ValueError(f"{path}.prompt or messages is required")
            if step.when:
                step_id = str(step.when.get("step_id") or "").strip()
                if step_id not in seen:
                    raise ValueError(
                        f"{path}.when.stepId {step_id!r} must reference an earlier step"
                    )
            seen.add(step.id)


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
        if "system" in tool and not isinstance(tool.get("system"), str):
            raise ValueError(f"{ref_path}.system must be an exact system name")
        system = _config_string(tool, "system")
        app = _config_string(tool, "app", "appName", "app_name")
        operation = _config_string(tool, "operation", "operationName", "operation_name")
        connection = _config_string(tool, "connection", "connectionName")
        instance = _config_string(tool, "instance", "instanceName")
        title = _config_string(tool, "title")
        description = _config_string(tool, "description")
        run_as_subject_id = _agent_tool_ref_run_as_subject_id_from_config(
            tool, ref_path
        )
        if system:
            if app:
                raise ValueError(f"{ref_path} must set exactly one of app or system")
            if system != "workflow":
                raise ValueError(f"{ref_path}.system must be workflow")
            if not operation or operation == "*":
                raise ValueError(
                    f"{ref_path}.operation must be an exact operation name"
                )
            if connection or instance or title or description or run_as_subject_id:
                raise ValueError(
                    f"{ref_path} system refs cannot include connection, instance, title, description, or runAs"
                )
            refs.append(SlackAgentToolRef(system=system, operation=operation))
            continue
        if not app or app == "*" or app.lower() == "system":
            raise ValueError(f"{ref_path}.app must be an exact app name")
        if not operation or operation == "*":
            raise ValueError(f"{ref_path}.operation must be an exact operation name")
        if connection == "*" or instance == "*":
            raise ValueError(f"{ref_path} connection and instance must be exact")
        refs.append(
            SlackAgentToolRef(
                app=app,
                operation=operation,
                connection=connection,
                instance=instance,
                title=title,
                description=description,
                run_as_subject_id=run_as_subject_id,
            )
        )
    return tuple(refs)


def _agent_tool_ref_run_as_subject_id_from_config(
    config: dict[str, Any], path: str
) -> str:
    run_as = _config_object(config, path, "runAs", "run_as")
    if run_as is None:
        return ""
    if "externalIdentity" in run_as or "external_identity" in run_as:
        raise ValueError(f"{path}.runAs.externalIdentity is not supported")
    subject = _config_object(run_as, f"{path}.runAs", "subject")
    if subject is None:
        raise ValueError(f"{path}.runAs.subject is required")
    subject_id = _config_string(subject, "id")
    if not subject_id:
        raise ValueError(f"{path}.runAs.subject.id is required")
    return subject_id


def _config_object(
    config: dict[str, Any], path: str, *keys: str
) -> dict[str, Any] | None:
    for key in keys:
        if key in config:
            value = config.get(key)
            if not isinstance(value, dict):
                raise ValueError(f"{path}.{key} must be an object")
            return dict(value)
    return None


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


def _config_json_object(
    config: dict[str, Any], path: str, *keys: str
) -> dict[str, Any]:
    for key in keys:
        if key not in config:
            continue
        value = config.get(key)
        if value is None:
            return {}
        if not isinstance(value, dict):
            raise ValueError(f"{path}.{key} must be an object")
        return dict(value)
    return {}


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


def _config_timeout_seconds(config: dict[str, Any], *keys: str) -> int:
    for key in keys:
        if key not in config:
            continue
        value = config.get(key)
        if value is None:
            return 0
        if isinstance(value, bool):
            raise ValueError(f"{key} must be a positive integer number of seconds")
        if isinstance(value, int):
            seconds = value
        elif isinstance(value, float) and value.is_integer():
            seconds = int(value)
        elif isinstance(value, str):
            normalized = value.strip()
            if not normalized:
                return 0
            if not normalized.isdecimal():
                raise ValueError(f"{key} must be a positive integer number of seconds")
            seconds = int(normalized)
        else:
            raise ValueError(f"{key} must be a positive integer number of seconds")
        if seconds <= 0:
            raise ValueError(f"{key} must be a positive integer number of seconds")
        if seconds > MAX_WORKFLOW_AGENT_TIMEOUT_SECONDS:
            raise ValueError(
                f"{key} must be at most {MAX_WORKFLOW_AGENT_TIMEOUT_SECONDS} seconds"
            )
        return seconds
    return 0


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
