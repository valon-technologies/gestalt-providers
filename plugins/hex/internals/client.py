from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, Protocol, TypeAlias
from urllib.parse import quote, urlencode

JsonObject: TypeAlias = dict[str, Any]
JsonPayload: TypeAlias = Mapping[str, Any]
JsonQuery: TypeAlias = Mapping[str, Any]

HEX_API_BASE: Final = "https://app.hex.tech/api/v1"
HEX_API_VERSION: Final = "1.0.0"
USER_AGENT: Final = "gestalt-hex-plugin/0.0.1a1"
REQUEST_TIMEOUT_SECONDS: Final = 30


class HexAPIClient(Protocol):
    def get_json(
        self, path: str, token: str, query: JsonQuery | None = None
    ) -> JsonObject: ...

    def post_json(self, path: str, payload: JsonPayload, token: str) -> JsonObject: ...


@dataclass(frozen=True, slots=True)
class UrllibHexAPIClient:
    def get_json(
        self, path: str, token: str, query: JsonQuery | None = None
    ) -> JsonObject:
        return request_json("GET", path, token, query=query)

    def post_json(self, path: str, payload: JsonPayload, token: str) -> JsonObject:
        return request_json("POST", path, token, payload=payload)


DEFAULT_HEX_CLIENT: Final[HexAPIClient] = UrllibHexAPIClient()


class HexAPIError(RuntimeError):
    def __init__(self, status: int, body: JsonObject) -> None:
        self.status = status
        self.body = body
        super().__init__(_message_from_error_body(status, body))


class HexClientError(RuntimeError):
    pass


def hex_api_base() -> str:
    return os.environ.get("HEX_API_BASE", HEX_API_BASE).rstrip("/")


def encode_path_component(value: str) -> str:
    return quote(value, safe="")


def get_json(
    path: str, token: str, query: JsonQuery | None = None
) -> JsonObject:
    return DEFAULT_HEX_CLIENT.get_json(path, token, query)


def post_json(path: str, payload: JsonPayload, token: str) -> JsonObject:
    return DEFAULT_HEX_CLIENT.post_json(path, payload, token)


def request_json(
    method: str,
    path: str,
    token: str,
    *,
    payload: JsonPayload | None = None,
    query: JsonQuery | None = None,
) -> JsonObject:
    url = f"{hex_api_base()}/{path.lstrip('/')}"
    if query:
        encoded_query = urlencode(
            {key: value for key, value in query.items() if value is not None}
        )
        if encoded_query:
            url = f"{url}?{encoded_query}"

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        "api-version": HEX_API_VERSION,
        "User-Agent": USER_AGENT,
    }

    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(dict(payload)).encode("utf-8")

    request = urllib.request.Request(
        url=url,
        data=data,
        method=method,
        headers=headers,
    )
    return _request_json(request)


def _request_json(request: urllib.request.Request) -> JsonObject:
    try:
        with urllib.request.urlopen(
            request, timeout=REQUEST_TIMEOUT_SECONDS
        ) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        error_body = _decode_error_body(exc.read(), exc.code)
        raise HexAPIError(exc.code, error_body) from exc
    except urllib.error.URLError as exc:
        raise HexClientError(f"hex API request failed: {exc.reason}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise HexClientError(f"parsing hex API response: {exc}") from exc

    if not isinstance(payload, dict):
        raise HexClientError("parsing hex API response: expected object")

    return payload


def _decode_error_body(body: bytes, status: int) -> JsonObject:
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return {"error": f"hex API error (status {status})"}

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {"error": f"hex API error (status {status}): {text}"}

    if not isinstance(payload, dict):
        return {"error": f"hex API error (status {status}): {text}"}

    if "error" not in payload:
        message = _message_from_error_body(status, payload)
        if message:
            payload = {"error": message, **payload}

    return payload


def _message_from_error_body(status: int, body: JsonObject) -> str:
    error = body.get("error")
    if isinstance(error, str) and error:
        return error

    reason = body.get("reason")
    details = body.get("details")
    if isinstance(reason, str) and reason:
        if isinstance(details, str) and details:
            return f"{reason}: {details}"
        return reason

    return f"hex API error (status {status})"
