from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, TypeAlias

GMAIL_PROFILE_URL = (
    "https://gmail.googleapis.com/gmail/v1/users/me/profile?fields=emailAddress"
)
READ_ONLY_PLATFORM_OPERATIONS = frozenset(
    {
        "messages.list",
        "messages.get",
        "messages.attachments.get",
        "threads.get",
        "labels.list",
        "getProfile",
    }
)

JsonObject: TypeAlias = dict[str, Any]


class PlatformConnectionError(RuntimeError):
    pass


class PlatformConnectionAuthError(PlatformConnectionError):
    pass


class PlatformConnectionPolicyError(PlatformConnectionError):
    pass


@dataclass(frozen=True, slots=True)
class PlatformConnectionConfig:
    enabled: bool = False
    email: str = ""
    operations: frozenset[str] = frozenset()


_PROFILE_CACHE: set[tuple[str, str]] = set()


def parse_platform_connection_config(
    config: dict[str, Any],
) -> PlatformConnectionConfig:
    raw = config.get("platformConnection")
    if raw is None:
        return PlatformConnectionConfig()
    if not isinstance(raw, dict):
        raise ValueError("platformConnection must be an object")

    enabled = bool(raw.get("enabled", False))
    if not enabled:
        return PlatformConnectionConfig()

    operations = frozenset(_required_string_list(raw, "operations"))
    unsupported = sorted(operations - READ_ONLY_PLATFORM_OPERATIONS)
    if unsupported:
        raise ValueError(
            "platformConnection.operations contains unsupported read-only "
            f"operation(s): {', '.join(unsupported)}"
        )
    return PlatformConnectionConfig(
        enabled=True,
        email=_required_email(raw, "email"),
        operations=operations,
    )


def platform_connection_token_for_operation(
    config: PlatformConnectionConfig, operation: str, access_token: str
) -> str:
    if not config.enabled:
        raise PlatformConnectionPolicyError(
            "platform Gmail connection is not configured"
        )
    if operation not in config.operations:
        raise PlatformConnectionPolicyError(
            f"platform Gmail connection is not enabled for {operation}"
        )

    token = access_token.strip()
    if not token:
        raise PlatformConnectionError("token is required")
    _verify_profile_email(config, token)
    return token


def clear_profile_cache() -> None:
    _PROFILE_CACHE.clear()


def _required_string(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"platformConnection.{key} is required")
    return value.strip()


def _required_email(raw: dict[str, Any], key: str) -> str:
    email = _required_string(raw, key).lower()
    if "@" not in email:
        raise ValueError(f"platformConnection.{key} must be an email address")
    return email


def _required_string_list(raw: dict[str, Any], key: str) -> tuple[str, ...]:
    values = raw.get(key)
    if not isinstance(values, list) or not values:
        raise ValueError(f"platformConnection.{key} must be a non-empty array")
    strings: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(
                f"platformConnection.{key} values must be non-empty strings"
            )
        text = value.strip()
        if text in strings:
            raise ValueError(f"platformConnection.{key} contains duplicate values")
        strings.append(text)
    return tuple(strings)


def _verify_profile_email(config: PlatformConnectionConfig, access_token: str) -> None:
    cache_key = (_token_fingerprint(access_token), config.email)
    if cache_key in _PROFILE_CACHE:
        return

    request = urllib.request.Request(
        GMAIL_PROFILE_URL,
        method="GET",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    payload = _request_json(request, "platform connection profile verification")
    profile_email = payload.get("emailAddress")
    if not isinstance(profile_email, str) or not profile_email.strip():
        raise PlatformConnectionError("Gmail profile response missing emailAddress")
    normalized = profile_email.strip().lower()
    if normalized != config.email:
        raise PlatformConnectionAuthError(
            f"platform connection token belongs to {normalized}, expected {config.email}"
        )
    _PROFILE_CACHE.add(cache_key)


def _token_fingerprint(access_token: str) -> str:
    return hashlib.sha256(access_token.encode("utf-8")).hexdigest()


def _request_json(request: urllib.request.Request, label: str) -> JsonObject:
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw_body = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            raise PlatformConnectionAuthError(
                f"{label} failed with status {exc.code}"
            ) from exc
        body = exc.read().decode("utf-8", errors="replace").strip()
        raise PlatformConnectionError(
            f"{label} failed with status {exc.code}: {body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise PlatformConnectionError(f"{label} failed: {exc.reason}") from exc

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise PlatformConnectionError(f"parsing {label} response: {exc}") from exc
    if not isinstance(payload, dict):
        raise PlatformConnectionError(f"parsing {label} response: expected object")
    return payload
