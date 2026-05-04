from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from http import HTTPStatus
from typing import Any
from urllib.parse import urlsplit
from urllib.parse import urlencode

SLACK_BASE_URL = "https://slack.com/api"
SLACK_FILE_DOWNLOAD_HOSTS = {"files.slack.com"}
SLACK_FILE_DOWNLOAD_HOST_SUFFIXES = (".slack-files.com",)
MAX_RATE_LIMIT_RETRIES = 2
MAX_RETRY_AFTER_SECONDS = 5.0


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


def slack_post(endpoint: str, payload: dict[str, Any], token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{slack_base_url()}/{endpoint.lstrip('/')}",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )
    return _request_json(request)


def get_json(url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url=url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    return _request_json(request)


def get_bytes(url: str, token: str, max_bytes: int) -> tuple[bytes, bool]:
    if not is_slack_file_download_url(url):
        raise SlackClientError("slack file download URL must be a Slack HTTPS file URL")
    request = urllib.request.Request(
        url=url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        opener = urllib.request.build_opener(_SlackFileRedirectHandler())
        with opener.open(request, timeout=30) as response:
            body = response.read(max_bytes + 1)
    except urllib.error.HTTPError as exc:
        raise SlackAPIError(exc.code, _decode_error_body(exc.read(), exc.code)) from exc
    except urllib.error.URLError as exc:
        raise SlackClientError(f"slack file download failed: {exc.reason}") from exc

    truncated = len(body) > max_bytes
    if truncated:
        body = body[:max_bytes]
    return body, truncated


def is_slack_file_download_url(url: str) -> bool:
    parsed = urlsplit(url)
    hostname = parsed.hostname or ""
    if parsed.scheme != "https" or not hostname:
        return False
    return hostname in SLACK_FILE_DOWNLOAD_HOSTS or hostname.endswith(
        SLACK_FILE_DOWNLOAD_HOST_SUFFIXES
    )


class _SlackFileRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(  # ty: ignore[invalid-method-override]
        self,
        req,
        _fp,
        code,
        _msg,
        _headers,
        newurl,
    ):
        if not is_slack_file_download_url(newurl):
            raise SlackClientError("slack file download redirected to a non-Slack URL")
        authorization = req.get_header("Authorization") or dict(req.header_items()).get(
            "Authorization", ""
        )
        return urllib.request.Request(
            url=newurl,
            method=req.get_method(),
            headers={"Authorization": authorization},
        )


def _request_json(request: urllib.request.Request) -> dict[str, Any]:
    body = b""
    for attempt in range(MAX_RATE_LIMIT_RETRIES + 1):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read()
            break
        except urllib.error.HTTPError as exc:
            retry_after = _retry_after_seconds(exc)
            if (
                exc.code == HTTPStatus.TOO_MANY_REQUESTS
                and retry_after is not None
                and attempt < MAX_RATE_LIMIT_RETRIES
            ):
                time.sleep(retry_after)
                continue
            raise SlackAPIError(
                exc.code, _decode_error_body(exc.read(), exc.code)
            ) from exc
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
            status = (
                HTTPStatus.TOO_MANY_REQUESTS
                if error in {"ratelimited", "rate_limited"}
                else HTTPStatus.BAD_GATEWAY
            )
            raise SlackAPIError(status, {"error": error})
        raise SlackAPIError(HTTPStatus.BAD_GATEWAY, {"error": "slack API error"})

    return payload


def _retry_after_seconds(error: urllib.error.HTTPError) -> float | None:
    raw = error.headers.get("Retry-After") if error.headers is not None else None
    if raw is None:
        return None
    try:
        seconds = float(raw)
    except ValueError:
        return None
    if seconds < 0:
        return None
    return min(seconds, MAX_RETRY_AFTER_SECONDS)


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
