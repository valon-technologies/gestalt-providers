from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote, urlencode

GMAIL_BASE_URL = "https://gmail.googleapis.com/gmail/v1/users/me"


def gmail_base_url() -> str:
    return os.environ.get("GMAIL_BASE_URL", GMAIL_BASE_URL).rstrip("/")


def get_json(url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url=url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    return _request_json(request)


def post_json(url: str, payload: dict[str, Any], token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    return _request_json(request)


def put_json(url: str, payload: dict[str, Any], token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        method="PUT",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    return _request_json(request)


def _request_json(request: urllib.request.Request) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        message = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"gmail API error (status {exc.code}): {message}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(str(exc.reason)) from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"parsing gmail API response: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("parsing gmail API response: expected object")
    return payload


def metadata_message_url(message_id: str) -> str:
    query = urlencode(
        [
            ("format", "metadata"),
            ("metadataHeaders", "From"),
            ("metadataHeaders", "To"),
            ("metadataHeaders", "Cc"),
            ("metadataHeaders", "Subject"),
            ("metadataHeaders", "Message-ID"),
            ("metadataHeaders", "References"),
            ("metadataHeaders", "Delivered-To"),
        ],
        doseq=True,
    )
    return f"{gmail_base_url()}/messages/{quote(message_id, safe='')}?{query}"


def full_message_url(message_id: str) -> str:
    return f"{gmail_base_url()}/messages/{quote(message_id, safe='')}?format=full"


def draft_url(draft_id: str) -> str:
    return f"{gmail_base_url()}/drafts/{quote(draft_id, safe='')}"
