import json
import os
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote, urlencode

HEX_API_BASE = "https://app.hex.tech/api/v1"
HEX_API_VERSION = "1.0.0"
USER_AGENT = "gestalt-hex-plugin/0.0.1a1"


class HexAPIError(RuntimeError):
    def __init__(self, status: int, body: dict[str, Any]) -> None:
        self.status = status
        self.body = body
        super().__init__(_message_from_error_body(status, body))


def hex_api_base() -> str:
    return os.environ.get("HEX_API_BASE", HEX_API_BASE).rstrip("/")


def encode_path_component(value: str) -> str:
    return quote(value, safe="")


def get_json(path: str, token: str, query: dict[str, Any] | None = None) -> dict[str, Any]:
    return request_json("GET", path, token, query=query)


def post_json(path: str, payload: dict[str, Any], token: str) -> dict[str, Any]:
    return request_json("POST", path, token, payload=payload)


def request_json(
    method: str,
    path: str,
    token: str,
    *,
    payload: dict[str, Any] | None = None,
    query: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"{hex_api_base()}/{path.lstrip('/')}"
    if query:
        encoded_query = urlencode({key: value for key, value in query.items() if value is not None})
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
        data = json.dumps(payload).encode("utf-8")

    request = urllib.request.Request(
        url=url,
        data=data,
        method=method,
        headers=headers,
    )
    return _request_json(request)


def _request_json(request: urllib.request.Request) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        error_body = _decode_error_body(exc.read(), exc.code)
        raise HexAPIError(exc.code, error_body) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(str(exc.reason)) from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"parsing hex API response: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("parsing hex API response: expected object")

    return payload


def _decode_error_body(body: bytes, status: int) -> dict[str, Any]:
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


def _message_from_error_body(status: int, body: dict[str, Any]) -> str:
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
