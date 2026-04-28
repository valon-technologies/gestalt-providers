from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from .constants import (
    DEFAULT_WEBHOOK_EVENTS,
    GITHUB_DEFAULT_API_BASE_URL,
    GITHUB_DEFAULT_WEB_BASE_URL,
)

WEBHOOK_DISPATCH_DIRECT = "direct"
WEBHOOK_DISPATCH_WORKFLOW = "workflow"
WEBHOOK_DISPATCH_MODES = frozenset({WEBHOOK_DISPATCH_DIRECT, WEBHOOK_DISPATCH_WORKFLOW})


@dataclass(frozen=True, slots=True)
class GitHubAppConfig:
    app_id: str = ""
    private_key: str = ""
    private_key_path: str = ""
    api_base_url: str = GITHUB_DEFAULT_API_BASE_URL
    web_base_url: str = GITHUB_DEFAULT_WEB_BASE_URL
    webhook_events: tuple[str, ...] = DEFAULT_WEBHOOK_EVENTS
    webhook_dispatch: str = WEBHOOK_DISPATCH_DIRECT
    ignore_bot_sender: bool = True
    agent_provider: str = ""
    agent_model: str = ""
    agent_system_prompt: str = ""
    agent_provider_options: dict[str, Any] = field(default_factory=dict)


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
    webhook_dispatch = (
        webhook_config_string(config, "dispatch") or WEBHOOK_DISPATCH_DIRECT
    ).lower()
    if webhook_dispatch not in WEBHOOK_DISPATCH_MODES:
        raise ValueError(
            "webhook dispatch must be one of: "
            + ", ".join(sorted(WEBHOOK_DISPATCH_MODES))
        )
    return GitHubAppConfig(
        app_id=app_id,
        private_key=normalize_private_key(private_key),
        private_key_path=private_key_path,
        api_base_url=(
            config_string(config, "apiBaseUrl", "api_base_url")
            or GITHUB_DEFAULT_API_BASE_URL
        ).rstrip("/"),
        web_base_url=(
            config_string(config, "webBaseUrl", "web_base_url")
            or GITHUB_DEFAULT_WEB_BASE_URL
        ).rstrip("/"),
        webhook_events=tuple(
            event.lower()
            for event in (
                webhook_events
                if webhook_events is not None
                else list(DEFAULT_WEBHOOK_EVENTS)
            )
        ),
        webhook_dispatch=webhook_dispatch,
        ignore_bot_sender=config_bool(
            config, "ignoreBotSender", "ignore_bot_sender", default=True
        ),
        agent_provider=agent_config_string(config, "provider"),
        agent_model=agent_config_string(config, "model"),
        agent_system_prompt=agent_config_string(
            config, "systemPrompt", "system_prompt", "prompt"
        ),
        agent_provider_options=agent_config_dict(
            config, "providerOptions", "provider_options"
        ),
    )


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


def webhook_config_string(config: dict[str, Any], *keys: str) -> str:
    webhook = config_dict(config, "webhook")
    return config_string(webhook, *keys)


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
