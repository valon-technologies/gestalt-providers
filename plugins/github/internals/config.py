from __future__ import annotations

import os
import re
import urllib.parse
from dataclasses import dataclass, field
from typing import Any, cast

from google.protobuf import struct_pb2 as _struct_pb2

from .constants import (
    BOT_OPERATION_ORDER,
    DEFAULT_WEBHOOK_EVENTS,
    DEFAULT_POLICY_OPERATIONS_BY_MODE,
    GITHUB_DEFAULT_API_BASE_URL,
    GITHUB_DEFAULT_GRAPHQL_BASE_URL,
    GITHUB_DEFAULT_WEB_BASE_URL,
    WEBHOOK_POLICY_ACTION_MODES,
    WEBHOOK_POLICY_OBSERVE_MODE,
)


_POLICY_ID_RE = re.compile(r"^[A-Za-z0-9._-]+$")

struct_pb2: Any = _struct_pb2


@dataclass(frozen=True, slots=True)
class GitHubWebhookPolicyMatch:
    events: tuple[str, ...] = ()
    actions: tuple[str, ...] = ()
    statuses: tuple[str, ...] = ()
    conclusions: tuple[str, ...] = ()
    repositories: tuple[str, ...] = ()
    branches: tuple[str, ...] = ()
    check_names: tuple[str, ...] = ()
    workflow_names: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GitHubWorkflowPluginTarget:
    plugin_name: str
    operation: str
    connection: str = ""
    instance: str = ""
    input: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GitHubWebhookPolicy:
    id: str
    match: GitHubWebhookPolicyMatch = field(default_factory=GitHubWebhookPolicyMatch)
    workflow_provider: str = ""
    agent_provider: str = ""
    agent_model: str = ""
    agent_system_prompt: str = ""
    agent_model_options: dict[str, Any] | None = None
    workflow_target: GitHubWorkflowPluginTarget | None = None
    action_mode: str = WEBHOOK_POLICY_OBSERVE_MODE
    allowed_operations: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GitHubAppConfig:
    app_id: str = ""
    private_key: str = ""
    private_key_path: str = ""
    api_base_url: str = GITHUB_DEFAULT_API_BASE_URL
    graphql_base_url: str = GITHUB_DEFAULT_GRAPHQL_BASE_URL
    web_base_url: str = GITHUB_DEFAULT_WEB_BASE_URL
    webhook_events: tuple[str, ...] = DEFAULT_WEBHOOK_EVENTS
    webhook_events_configured: bool = False
    webhook_policies: tuple[GitHubWebhookPolicy, ...] = ()
    workflow_provider: str = ""
    ignore_bot_sender: bool = True
    agent_provider: str = ""
    agent_model: str = ""
    agent_system_prompt: str = ""
    agent_model_options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GitHubBotIdentity:
    name: str
    login: str
    user_id: str
    email: str


_github_config = GitHubAppConfig()
_github_bot_identity: GitHubBotIdentity | None = None


def configure_from_mapping(config: dict[str, Any]) -> GitHubAppConfig:
    global _github_bot_identity, _github_config

    _github_config = github_config_from_mapping(config)
    _github_bot_identity = None
    return _github_config


def get_github_config() -> GitHubAppConfig:
    return _github_config


def get_cached_bot_identity() -> GitHubBotIdentity | None:
    return _github_bot_identity


def set_cached_bot_identity(identity: GitHubBotIdentity) -> None:
    global _github_bot_identity

    _github_bot_identity = identity


def github_config_from_mapping(config: dict[str, Any]) -> GitHubAppConfig:
    app_id = (
        config_string(config, "appId", "app_id")
        or os.environ.get("GITHUB_APP_ID", "").strip()
    )
    private_key = config_string(
        config, "appPrivateKey", "privateKey", "app_private_key", "private_key"
    )
    private_key_env = config_string(
        config,
        "appPrivateKeyEnv",
        "privateKeyEnv",
        "app_private_key_env",
        "private_key_env",
    )
    if not private_key:
        env_name = private_key_env or "GITHUB_APP_PRIVATE_KEY"
        private_key = os.environ.get(env_name, "").strip()

    private_key_path = config_string(
        config, "appPrivateKeyPath", "privateKeyPath", "app_private_key_path"
    )
    if not private_key_path:
        private_key_path = os.environ.get("GITHUB_APP_PRIVATE_KEY_PATH", "").strip()

    webhook_events = config_string_list(config, "webhookEvents", "webhook_events")
    workflow_provider = workflow_config_string(config, "provider")
    webhook_policies = parse_webhook_policies(config)
    api_base_url = (
        config_string(config, "apiBaseUrl", "api_base_url")
        or GITHUB_DEFAULT_API_BASE_URL
    ).rstrip("/")
    graphql_base_url = derive_graphql_base_url(
        config_string(config, "graphqlBaseUrl", "graphql_base_url"),
        api_base_url,
    )
    web_base_url = (
        config_string(config, "webBaseUrl", "web_base_url")
        or GITHUB_DEFAULT_WEB_BASE_URL
    ).rstrip("/")

    return GitHubAppConfig(
        app_id=app_id,
        private_key=normalize_private_key(private_key),
        private_key_path=private_key_path,
        api_base_url=api_base_url,
        graphql_base_url=graphql_base_url,
        web_base_url=web_base_url,
        webhook_events=tuple(
            event.lower()
            for event in (
                webhook_events
                if webhook_events is not None
                else list(DEFAULT_WEBHOOK_EVENTS)
            )
        ),
        webhook_events_configured=webhook_events is not None,
        webhook_policies=webhook_policies,
        workflow_provider=workflow_provider,
        ignore_bot_sender=config_bool(
            config, "ignoreBotSender", "ignore_bot_sender", default=True
        ),
        agent_provider=agent_config_string(config, "provider"),
        agent_model=agent_config_string(config, "model"),
        agent_system_prompt=agent_config_string(
            config, "systemPrompt", "system_prompt", "prompt"
        ),
        agent_model_options=agent_config_dict(config, "modelOptions", "model_options"),
    )


def derive_graphql_base_url(explicit: str, api_base_url: str) -> str:
    value = explicit.strip().rstrip("/")
    if value:
        return value
    if api_base_url == GITHUB_DEFAULT_API_BASE_URL:
        return GITHUB_DEFAULT_GRAPHQL_BASE_URL
    if api_base_url.rstrip("/").endswith("/api/v3"):
        parsed = urllib.parse.urlparse(api_base_url)
        graphql_path = parsed.path.rstrip("/")[: -len("/api/v3")] + "/api/graphql"
        return urllib.parse.urlunparse(
            parsed._replace(path=graphql_path, params="", query="", fragment="")
        ).rstrip("/")
    return api_base_url.rstrip("/") + "/graphql"


def parse_webhook_policies(config: dict[str, Any]) -> tuple[GitHubWebhookPolicy, ...]:
    raw = config.get("webhookPolicies", config.get("webhook_policies"))
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ValueError("webhookPolicies must be a list")

    policies: list[GitHubWebhookPolicy] = []
    seen_ids: set[str] = set()
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"webhookPolicies[{index}] must be an object")
        policy_config = cast(dict[str, Any], item)
        policy_id = config_string(policy_config, "id")
        if not policy_id:
            raise ValueError(f"webhookPolicies[{index}].id is required")
        if not _POLICY_ID_RE.fullmatch(policy_id):
            raise ValueError(
                f"webhookPolicies[{index}].id must match {_POLICY_ID_RE.pattern}"
            )
        if policy_id in seen_ids:
            raise ValueError(f"duplicate webhook policy id {policy_id!r}")
        seen_ids.add(policy_id)

        match_config = config_dict(policy_config, "match")
        workflow_config = policy_config_object(policy_config, "workflow", index)
        agent_config = config_dict(policy_config, "agent")
        action_config = config_dict(policy_config, "action")
        action_mode = (
            config_string(action_config, "mode") or WEBHOOK_POLICY_OBSERVE_MODE
        )
        if action_mode not in WEBHOOK_POLICY_ACTION_MODES:
            raise ValueError(
                f"webhookPolicies[{index}].action.mode must be one of "
                + ", ".join(WEBHOOK_POLICY_ACTION_MODES)
            )

        allowed_operations = policy_allowed_operations(
            action_config, action_mode, index
        )
        model_options = (
            config_dict(agent_config, "modelOptions", "model_options")
            if "modelOptions" in agent_config or "model_options" in agent_config
            else None
        )
        policies.append(
            GitHubWebhookPolicy(
                id=policy_id,
                match=GitHubWebhookPolicyMatch(
                    events=lower_string_tuple(match_config, "events"),
                    actions=lower_string_tuple(match_config, "actions"),
                    statuses=lower_string_tuple(match_config, "statuses"),
                    conclusions=lower_string_tuple(match_config, "conclusions"),
                    repositories=string_tuple(match_config, "repositories"),
                    branches=string_tuple(match_config, "branches"),
                    check_names=string_tuple(match_config, "checkNames", "check_names"),
                    workflow_names=string_tuple(
                        match_config, "workflowNames", "workflow_names"
                    ),
                ),
                workflow_provider=config_string(workflow_config, "provider"),
                agent_provider=config_string(agent_config, "provider"),
                agent_model=config_string(agent_config, "model"),
                agent_system_prompt=config_string(
                    agent_config, "systemPrompt", "system_prompt", "prompt"
                ),
                agent_model_options=model_options,
                workflow_target=parse_policy_workflow_target(workflow_config, index),
                action_mode=action_mode,
                allowed_operations=allowed_operations,
            )
        )
    return tuple(policies)


def parse_policy_workflow_target(
    workflow_config: dict[str, Any], policy_index: int
) -> GitHubWorkflowPluginTarget | None:
    if "target" not in workflow_config:
        return None
    target_path = f"webhookPolicies[{policy_index}].workflow.target"
    target_config = required_config_object(workflow_config, "target", target_path)
    plugin_path = f"{target_path}.plugin"
    plugin_config = required_config_object(target_config, "plugin", plugin_path)

    input_value: dict[str, Any] = {}
    input_path = f"{plugin_path}.input"
    if "input" in plugin_config:
        input_config = plugin_config.get("input")
        if not isinstance(input_config, dict):
            raise ValueError(f"{input_path} must be an object")
        input_value = dict(input_config)
    validate_struct_compatible(input_value, input_path)

    return GitHubWorkflowPluginTarget(
        plugin_name=required_string(plugin_config, "plugin", f"{plugin_path}.plugin"),
        operation=required_string(
            plugin_config, "operation", f"{plugin_path}.operation"
        ),
        connection=optional_string(
            plugin_config, "connection", f"{plugin_path}.connection"
        ),
        instance=optional_string(plugin_config, "instance", f"{plugin_path}.instance"),
        input=input_value,
    )


def policy_config_object(
    config: dict[str, Any], key: str, policy_index: int
) -> dict[str, Any]:
    if key not in config:
        return {}
    value = config.get(key)
    if isinstance(value, dict):
        return dict(value)
    raise ValueError(f"webhookPolicies[{policy_index}].{key} must be an object")


def required_config_object(
    config: dict[str, Any], key: str, path: str
) -> dict[str, Any]:
    if key not in config:
        raise ValueError(f"{path} is required")
    value = config.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must be an object")
    return dict(value)


def required_string(config: dict[str, Any], key: str, path: str) -> str:
    value = config.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path} is required")
    return value.strip()


def optional_string(config: dict[str, Any], key: str, path: str) -> str:
    if key not in config:
        return ""
    value = config.get(key)
    if not isinstance(value, str):
        raise ValueError(f"{path} must be a string")
    return value.strip()


def validate_struct_compatible(input_value: dict[str, Any], path: str) -> None:
    try:
        struct_pb2.Struct().update(input_value)
    except Exception as err:
        raise ValueError(f"{path} must be JSON-compatible") from err


def policy_allowed_operations(
    action_config: dict[str, Any], action_mode: str, policy_index: int
) -> tuple[str, ...]:
    configured = config_string_list(
        action_config, "allowedOperations", "allowed_operations"
    )
    if configured is None:
        configured = list(DEFAULT_POLICY_OPERATIONS_BY_MODE[action_mode])

    unknown = sorted(
        {operation for operation in configured if operation not in BOT_OPERATION_ORDER}
    )
    if unknown:
        raise ValueError(
            f"webhookPolicies[{policy_index}].action.allowedOperations contains "
            f"unknown operation(s): {', '.join(unknown)}"
        )

    configured_set = set(configured)
    return tuple(
        operation for operation in BOT_OPERATION_ORDER if operation in configured_set
    )


def lower_string_tuple(config: dict[str, Any], *keys: str) -> tuple[str, ...]:
    return tuple(item.lower() for item in string_tuple(config, *keys))


def string_tuple(config: dict[str, Any], *keys: str) -> tuple[str, ...]:
    return tuple(config_string_list(config, *keys) or [])


def config_string(config: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = config.get(key)
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, int):
            return str(value)
    return ""


def config_dict(config: dict[str, Any], *keys: str) -> dict[str, Any]:
    for key in keys:
        value = config.get(key)
        if isinstance(value, dict):
            return dict(value)
    return {}


def workflow_config_string(config: dict[str, Any], *keys: str) -> str:
    workflow = root_workflow_config(config)
    return config_string(workflow, *keys)


def root_workflow_config(config: dict[str, Any]) -> dict[str, Any]:
    if "workflow" not in config:
        return {}
    workflow = config.get("workflow")
    if not isinstance(workflow, dict):
        raise ValueError("workflow must be an object")
    workflow_config = dict(workflow)
    if "target" in workflow_config:
        raise ValueError(
            "workflow.target is not supported; configure "
            "webhookPolicies[].workflow.target instead"
        )
    return workflow_config


def agent_config_string(config: dict[str, Any], *keys: str) -> str:
    agent = config_dict(config, "agent")
    return config_string(agent, *keys)


def agent_config_dict(config: dict[str, Any], *keys: str) -> dict[str, Any]:
    agent = config_dict(config, "agent")
    return config_dict(agent, *keys)


def config_string_list(config: dict[str, Any], *keys: str) -> list[str] | None:
    for key in keys:
        if key not in config:
            continue
        value = config.get(key)
        if isinstance(value, list):
            return [
                item.strip() for item in value if isinstance(item, str) and item.strip()
            ]
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
    return None


def config_bool(config: dict[str, Any], *keys: str, default: bool) -> bool:
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


def normalize_private_key(value: str) -> str:
    value = value.strip()
    if "\\n" in value and "\n" not in value:
        value = value.replace("\\n", "\n")
    return value
