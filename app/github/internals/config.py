from __future__ import annotations

import os
import math
import urllib.parse
from dataclasses import dataclass
from typing import Any

from .constants import (
    DEFAULT_WEBHOOK_EVENTS,
    GITHUB_DEFAULT_API_BASE_URL,
    GITHUB_DEFAULT_GRAPHQL_BASE_URL,
    GITHUB_DEFAULT_WEB_BASE_URL,
)
from .cache_store import close_cache


@dataclass(frozen=True, slots=True)
class GitHubAppConfig:
    provider_name: str = "github"
    app_id: str = ""
    private_key: str = ""
    private_key_path: str = ""
    api_base_url: str = GITHUB_DEFAULT_API_BASE_URL
    graphql_base_url: str = GITHUB_DEFAULT_GRAPHQL_BASE_URL
    web_base_url: str = GITHUB_DEFAULT_WEB_BASE_URL
    webhook_events: tuple[str, ...] = DEFAULT_WEBHOOK_EVENTS
    workflow_provider: str = ""
    ignore_bot_sender: bool = True
    cache_enabled: bool = False
    cache_ttl_seconds: float = 60.0


@dataclass(frozen=True, slots=True)
class GitHubBotIdentity:
    name: str
    login: str
    user_id: str
    email: str
    slug: str = ""


@dataclass(frozen=True, slots=True)
class GitHubUserIdentity:
    name: str
    login: str
    user_id: str
    email: str


_github_config = GitHubAppConfig()
_github_bot_identity: GitHubBotIdentity | None = None


def configure_from_mapping(
    config: dict[str, Any], provider_name: str = "github"
) -> GitHubAppConfig:
    global _github_bot_identity, _github_config

    close_cache()
    _github_config = github_config_from_mapping(config, provider_name=provider_name)
    _github_bot_identity = None
    return _github_config


def get_github_config() -> GitHubAppConfig:
    return _github_config


def get_cached_bot_identity() -> GitHubBotIdentity | None:
    return _github_bot_identity


def set_cached_bot_identity(identity: GitHubBotIdentity) -> None:
    global _github_bot_identity

    _github_bot_identity = identity


def github_config_from_mapping(
    config: dict[str, Any], *, provider_name: str = "github"
) -> GitHubAppConfig:
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
        provider_name=provider_name.strip() or "github",
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
        workflow_provider=workflow_config_string(config, "provider"),
        ignore_bot_sender=config_bool(
            config, "ignoreBotSender", "ignore_bot_sender", default=True
        ),
        cache_enabled=config_bool(
            config, "cacheEnabled", "cache_enabled", default=False
        ),
        cache_ttl_seconds=config_float(
            config,
            "cacheTtlSeconds",
            "cache_ttl_seconds",
            default=60.0,
            minimum=1.0,
            maximum=3600.0,
        ),
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


def config_string(config: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = config.get(key)
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, int):
            return str(value)
    return ""


def workflow_config_string(config: dict[str, Any], *keys: str) -> str:
    workflow = root_workflow_config(config)
    return config_string(workflow, *keys)


def root_workflow_config(config: dict[str, Any]) -> dict[str, Any]:
    if "workflow" not in config:
        return {}
    workflow = config.get("workflow")
    if not isinstance(workflow, dict):
        raise ValueError("workflow must be an object")
    return dict(workflow)


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


def config_float(
    config: dict[str, Any],
    *keys: str,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    for key in keys:
        if key not in config:
            continue
        value = config.get(key)
        if isinstance(value, bool):
            raise ValueError(f"{key} must be a number")
        try:
            parsed = float(value)
        except (TypeError, ValueError) as err:
            raise ValueError(f"{key} must be a number") from err
        if not math.isfinite(parsed) or not minimum <= parsed <= maximum:
            raise ValueError(
                f"{key} must be between {minimum:g} and {maximum:g}"
            )
        return parsed
    return default


def normalize_private_key(value: str) -> str:
    value = value.strip()
    if "\\n" in value and "\n" not in value:
        value = value.replace("\\n", "\n")
    return value
