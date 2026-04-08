from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urlencode

SLACK_BASE_URL = "https://slack.com/api"


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
        message = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"slack API error (status {exc.code}): {message}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(str(exc.reason)) from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"parsing slack API response: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("parsing slack API response: expected object")

    ok = payload.get("ok")
    if isinstance(ok, bool) and not ok:
        error = payload.get("error")
        if isinstance(error, str) and error:
            raise RuntimeError(f"slack API error: {error}")
        raise RuntimeError("slack API error")

    return payload
