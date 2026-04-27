from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from http import HTTPStatus
from typing import Any
from urllib.parse import urlencode

SLACK_BASE_URL = "https://slack.com/api"


class SlackAPIError(RuntimeError):
    def __init__(self, status: int, body: dict[str, str]) -> None:
        self.status = status
        self.body = body
        super().__init__(body["error"])


class SlackClientError(RuntimeError):
    pass


def slack_base_url() -> str:
    return os.environ.get("SLACK_BASE_URL", SLACK_BASE_URL).rstrip("/")


def slack_get(endpoint: str, query: dict[str, str], token: str) -> dict[str, Any]:
    url = f"{slack_base_url()}/{endpoint.lstrip('/')}"
    if query:
        url = f"{url}?{urlencode(query)}"
    return get_json(url, token)


def get_json(url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url=url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    return _request_json(request)


def _request_json(request: urllib.request.Request) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        raise SlackAPIError(exc.code, _decode_error_body(exc.read(), exc.code)) from exc
    except urllib.error.URLError as exc:
        raise SlackClientError(f"slack API request failed: {exc.reason}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SlackClientError(f"parsing slack API response: {exc}") from exc

    if not isinstance(payload, dict):
        raise SlackClientError("parsing slack API response: expected object")

    ok = payload.get("ok")
    if isinstance(ok, bool) and not ok:
        error = payload.get("error")
        if isinstance(error, str) and error:
            raise SlackAPIError(HTTPStatus.BAD_GATEWAY, {"error": error})
        raise SlackAPIError(HTTPStatus.BAD_GATEWAY, {"error": "slack API error"})

    return payload


def _decode_error_body(body: bytes, status: int) -> dict[str, str]:
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return {"error": f"slack API error (status {status})"}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {"error": f"slack API error (status {status}): {text}"}
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, str) and error:
            return {"error": error}
        message = payload.get("message")
        if isinstance(message, str) and message:
            return {"error": message}
    return {"error": f"slack API error (status {status}): {text}"}
