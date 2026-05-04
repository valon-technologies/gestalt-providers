from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, TypeAlias

METADATA_TOKEN_URL = (
    "http://metadata.google.internal/computeMetadata/v1/instance/"
    "service-accounts/default/token"
)
IAM_SIGN_JWT_BASE_URL = (
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts"
)
OAUTH_TOKEN_URL = "https://oauth2.googleapis.com/token"
JWT_AUDIENCE = "https://oauth2.googleapis.com/token"
JWT_LIFETIME_SECONDS = 3600
TOKEN_EXPIRY_SKEW_SECONDS = 300

JsonObject: TypeAlias = dict[str, Any]


class PlatformIdentityError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PlatformIdentityConfig:
    enabled: bool = False
    subject_email: str = ""
    service_account_email: str = ""
    scopes: tuple[str, ...] = ()
    operations: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class CachedToken:
    access_token: str
    expires_at: float


_TOKEN_CACHE: dict[tuple[str, str, tuple[str, ...], tuple[str, ...]], CachedToken] = {}


def parse_platform_identity_config(config: dict[str, Any]) -> PlatformIdentityConfig:
    raw = config.get("platformIdentity")
    if raw is None:
        return PlatformIdentityConfig()
    if not isinstance(raw, dict):
        raise ValueError("platformIdentity must be an object")

    enabled = bool(raw.get("enabled", False))
    if not enabled:
        return PlatformIdentityConfig()

    subject_email = _required_string(raw, "subjectEmail")
    service_account_email = _required_string(raw, "serviceAccountEmail")
    scopes = _required_string_list(raw, "scopes")
    operation_set = frozenset(_required_string_list(raw, "operations"))

    return PlatformIdentityConfig(
        enabled=True,
        subject_email=subject_email,
        service_account_email=service_account_email,
        scopes=scopes,
        operations=operation_set,
    )


def platform_token_for_operation(
    config: PlatformIdentityConfig, operation: str, *, now: float | None = None
) -> str:
    if not config.enabled:
        raise PlatformIdentityError("platform Gmail identity is not configured")
    if operation not in config.operations:
        raise PlatformIdentityError(
            f"platform Gmail identity is not enabled for {operation}"
        )

    issued_at = int(now if now is not None else time.time())
    cache_key = (
        config.service_account_email,
        config.subject_email,
        config.scopes,
        tuple(sorted(config.operations)),
    )
    cached = _TOKEN_CACHE.get(cache_key)
    if cached is not None and issued_at < cached.expires_at:
        return cached.access_token

    metadata_token = _metadata_access_token()
    signed_jwt = _sign_jwt(config, metadata_token, issued_at)
    access_token, expires_in = _exchange_jwt(signed_jwt)
    expires_at = issued_at + max(0, expires_in - TOKEN_EXPIRY_SKEW_SECONDS)
    _TOKEN_CACHE[cache_key] = CachedToken(
        access_token=access_token, expires_at=expires_at
    )
    return access_token


def clear_token_cache() -> None:
    _TOKEN_CACHE.clear()


def _required_string(raw: dict[str, Any], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"platformIdentity.{key} is required")
    return value.strip()


def _required_string_list(raw: dict[str, Any], key: str) -> tuple[str, ...]:
    values = raw.get(key)
    if not isinstance(values, list) or not values:
        raise ValueError(f"platformIdentity.{key} must be a non-empty array")
    strings: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"platformIdentity.{key} values must be non-empty strings")
        text = value.strip()
        if text in strings:
            raise ValueError(f"platformIdentity.{key} contains duplicate values")
        strings.append(text)
    return tuple(strings)


def _metadata_access_token() -> str:
    request = urllib.request.Request(
        METADATA_TOKEN_URL,
        method="GET",
        headers={"Metadata-Flavor": "Google"},
    )
    payload = _request_json(request)
    token = payload.get("access_token")
    if not isinstance(token, str) or not token:
        raise PlatformIdentityError("metadata server returned no access_token")
    return token


def _sign_jwt(
    config: PlatformIdentityConfig, metadata_access_token: str, issued_at: int
) -> str:
    claims = {
        "iss": config.service_account_email,
        "sub": config.subject_email,
        "scope": " ".join(config.scopes),
        "aud": JWT_AUDIENCE,
        "iat": issued_at,
        "exp": issued_at + JWT_LIFETIME_SECONDS,
    }
    body = json.dumps(
        {"payload": json.dumps(claims, separators=(",", ":"))},
        separators=(",", ":"),
    ).encode("utf-8")
    encoded_email = urllib.parse.quote(config.service_account_email, safe="")
    request = urllib.request.Request(
        f"{IAM_SIGN_JWT_BASE_URL}/{encoded_email}:signJwt",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {metadata_access_token}",
            "Content-Type": "application/json",
        },
    )
    payload = _request_json(request)
    signed_jwt = payload.get("signedJwt")
    if not isinstance(signed_jwt, str) or not signed_jwt:
        raise PlatformIdentityError("IAM signJwt returned no signedJwt")
    return signed_jwt


def _exchange_jwt(signed_jwt: str) -> tuple[str, int]:
    body = urllib.parse.urlencode(
        {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": signed_jwt,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        OAUTH_TOKEN_URL,
        data=body,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    payload = _request_json(request)
    access_token = payload.get("access_token")
    if not isinstance(access_token, str) or not access_token:
        raise PlatformIdentityError("OAuth token exchange returned no access_token")
    expires_in = payload.get("expires_in", JWT_LIFETIME_SECONDS)
    if not isinstance(expires_in, int):
        expires_in = JWT_LIFETIME_SECONDS
    return access_token, expires_in


def _request_json(request: urllib.request.Request) -> JsonObject:
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw_body = response.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace").strip()
        raise PlatformIdentityError(
            f"platform identity request failed with status {exc.code}: {body}"
        ) from exc
    except urllib.error.URLError as exc:
        raise PlatformIdentityError(
            f"platform identity request failed: {exc.reason}"
        ) from exc

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise PlatformIdentityError(
            f"parsing platform identity response: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise PlatformIdentityError(
            "parsing platform identity response: expected object"
        )
    return payload
