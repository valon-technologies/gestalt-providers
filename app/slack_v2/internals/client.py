from __future__ import annotations

import json
import urllib.error
import urllib.request
from http import HTTPStatus
from typing import Any
from urllib.parse import urlencode

SLACK_BASE_URL = "https://slack.com/api"


class SlackAPIError(RuntimeError):
    def __init__(self, status: int, error: str) -> None:
        self.status = status
        self.error = error
        super().__init__(error)


def slack_get(endpoint: str, query: dict[str, str], token: str) -> dict[str, Any]:
    url = f"{SLACK_BASE_URL}/{endpoint.lstrip('/')}"
    if query:
        url = f"{url}?{urlencode(query)}"
    request = urllib.request.Request(
        url=url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        raise SlackAPIError(
            exc.code, _decode_error_body(exc.read(), exc.code)
        ) from exc
    except urllib.error.URLError as exc:
        raise SlackAPIError(
            HTTPStatus.BAD_GATEWAY, f"slack API request failed: {exc.reason}"
        ) from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SlackAPIError(
            HTTPStatus.BAD_GATEWAY, f"parsing slack API response: {exc}"
        ) from exc

    if not isinstance(payload, dict):
        raise SlackAPIError(
            HTTPStatus.BAD_GATEWAY, "parsing slack API response: expected object"
        )

    ok = payload.get("ok")
    if isinstance(ok, bool) and not ok:
        error = payload.get("error")
        if isinstance(error, str) and error:
            status = (
                HTTPStatus.NOT_FOUND
                if error == "user_not_found"
                else HTTPStatus.BAD_GATEWAY
            )
            raise SlackAPIError(status, error)
        raise SlackAPIError(HTTPStatus.BAD_GATEWAY, "slack API error")

    return payload


def _decode_error_body(body: bytes, status: int) -> str:
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        return f"slack API error (status {status})"
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return f"slack API error (status {status}): {text}"
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, str) and error:
            return error
        message = payload.get("message")
        if isinstance(message, str) and message:
            return message
    return f"slack API error (status {status}): {text}"
