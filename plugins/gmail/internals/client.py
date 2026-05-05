from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, TypeAlias
from urllib.parse import quote, urlencode

GMAIL_BASE_URL = "https://gmail.googleapis.com/gmail/v1/users/me"
GmailJsonObject: TypeAlias = dict[str, Any]
GmailJsonPayload: TypeAlias = Mapping[str, Any]


class GmailAPIError(RuntimeError):
    def __init__(
        self, status: int, message: str, raw_body: GmailJsonObject | None = None
    ) -> None:
        self.status = status
        self.body = {"error": message}
        self.raw_body = raw_body or self.body
        super().__init__(message)


class GmailClientError(RuntimeError):
    pass


class GmailAPIClient(Protocol):
    """Runtime Gmail API contract used by source-backed operations."""

    def base_url(self) -> str: ...

    def get_json(self, url: str, token: str) -> GmailJsonObject: ...

    def post_json(
        self, url: str, payload: GmailJsonPayload, token: str
    ) -> GmailJsonObject: ...

    def put_json(
        self, url: str, payload: GmailJsonPayload, token: str
    ) -> GmailJsonObject: ...

    def metadata_message_url(self, message_id: str) -> str: ...

    def full_message_url(self, message_id: str) -> str: ...

    def draft_url(self, draft_id: str) -> str: ...

    def messages_list_url(
        self,
        *,
        q: str = "",
        label_ids: Iterable[str] = (),
        max_results: int | None = None,
        page_token: str = "",
        include_spam_trash: bool = False,
        fields: str = "",
    ) -> str: ...

    def message_url(
        self,
        message_id: str,
        *,
        format: str = "",
        metadata_headers: Iterable[str] = (),
        fields: str = "",
    ) -> str: ...

    def message_attachment_url(
        self,
        message_id: str,
        attachment_id: str,
        *,
        fields: str = "",
    ) -> str: ...

    def thread_url(
        self,
        thread_id: str,
        *,
        format: str = "",
        metadata_headers: Iterable[str] = (),
        fields: str = "",
    ) -> str: ...

    def labels_url(self, *, fields: str = "") -> str: ...

    def profile_url(self, *, fields: str = "") -> str: ...


@dataclass(frozen=True, slots=True)
class GmailHTTPClient:
    """Concrete Gmail API client backed by this module's HTTP helpers."""

    def base_url(self) -> str:
        return gmail_base_url()

    def get_json(self, url: str, token: str) -> GmailJsonObject:
        return get_json(url, token)

    def post_json(
        self, url: str, payload: GmailJsonPayload, token: str
    ) -> GmailJsonObject:
        return post_json(url, payload, token)

    def put_json(
        self, url: str, payload: GmailJsonPayload, token: str
    ) -> GmailJsonObject:
        return put_json(url, payload, token)

    def metadata_message_url(self, message_id: str) -> str:
        return metadata_message_url(message_id)

    def full_message_url(self, message_id: str) -> str:
        return full_message_url(message_id)

    def draft_url(self, draft_id: str) -> str:
        return draft_url(draft_id)

    def messages_list_url(
        self,
        *,
        q: str = "",
        label_ids: Iterable[str] = (),
        max_results: int | None = None,
        page_token: str = "",
        include_spam_trash: bool = False,
        fields: str = "",
    ) -> str:
        return messages_list_url(
            q=q,
            label_ids=label_ids,
            max_results=max_results,
            page_token=page_token,
            include_spam_trash=include_spam_trash,
            fields=fields,
        )

    def message_url(
        self,
        message_id: str,
        *,
        format: str = "",
        metadata_headers: Iterable[str] = (),
        fields: str = "",
    ) -> str:
        return message_url(
            message_id,
            format=format,
            metadata_headers=metadata_headers,
            fields=fields,
        )

    def message_attachment_url(
        self,
        message_id: str,
        attachment_id: str,
        *,
        fields: str = "",
    ) -> str:
        return message_attachment_url(message_id, attachment_id, fields=fields)

    def thread_url(
        self,
        thread_id: str,
        *,
        format: str = "",
        metadata_headers: Iterable[str] = (),
        fields: str = "",
    ) -> str:
        return thread_url(
            thread_id,
            format=format,
            metadata_headers=metadata_headers,
            fields=fields,
        )

    def labels_url(self, *, fields: str = "") -> str:
        return labels_url(fields=fields)

    def profile_url(self, *, fields: str = "") -> str:
        return profile_url(fields=fields)


DEFAULT_GMAIL_CLIENT = GmailHTTPClient()


def gmail_base_url() -> str:
    return os.environ.get("GMAIL_BASE_URL", GMAIL_BASE_URL).rstrip("/")


def get_json(url: str, token: str) -> GmailJsonObject:
    request = urllib.request.Request(
        url=url,
        method="GET",
        headers={"Authorization": f"Bearer {token}"},
    )
    return _request_json(request)


def post_json(url: str, payload: GmailJsonPayload, token: str) -> GmailJsonObject:
    request = urllib.request.Request(
        url=url,
        data=json.dumps(dict(payload)).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    return _request_json(request)


def put_json(url: str, payload: GmailJsonPayload, token: str) -> GmailJsonObject:
    request = urllib.request.Request(
        url=url,
        data=json.dumps(dict(payload)).encode("utf-8"),
        method="PUT",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    return _request_json(request)


def _request_json(request: urllib.request.Request) -> GmailJsonObject:
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        message, raw_body = _decode_error_body(exc.read(), exc.code)
        raise GmailAPIError(exc.code, message, raw_body=raw_body) from exc
    except urllib.error.URLError as exc:
        raise GmailClientError(f"gmail API request failed: {exc.reason}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise GmailClientError(f"parsing gmail API response: {exc}") from exc

    if not isinstance(payload, dict):
        raise GmailClientError("parsing gmail API response: expected object")
    return payload


def _decode_error_body(body: bytes, status: int) -> tuple[str, GmailJsonObject]:
    text = body.decode("utf-8", errors="replace").strip()
    if not text:
        message = f"gmail API error (status {status})"
        return message, {"error": message}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        message = f"gmail API error (status {status}): {text}"
        return message, {"error": message}
    if isinstance(payload, dict):
        message = ""
        error = payload.get("error")
        if isinstance(error, dict):
            raw_message = error.get("message")
            if isinstance(raw_message, str) and raw_message:
                message = raw_message
        if not message:
            raw_message = payload.get("message")
            if isinstance(raw_message, str) and raw_message:
                message = raw_message
        if isinstance(message, str) and message:
            return message, payload
    message = f"gmail API error (status {status}): {text}"
    return message, {"error": message}


def _append_query(url: str, params: Iterable[tuple[str, object]]) -> str:
    pairs: list[tuple[str, str]] = []
    for name, value in params:
        if value is None:
            continue
        if isinstance(value, bool):
            pairs.append((name, "true" if value else "false"))
            continue
        text = str(value)
        if text == "":
            continue
        pairs.append((name, text))
    if not pairs:
        return url
    return f"{url}?{urlencode(pairs, doseq=True)}"


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


def messages_list_url(
    *,
    q: str = "",
    label_ids: Iterable[str] = (),
    max_results: int | None = None,
    page_token: str = "",
    include_spam_trash: bool = False,
    fields: str = "",
) -> str:
    params: list[tuple[str, object]] = [
        ("q", q),
        ("maxResults", max_results),
        ("pageToken", page_token),
        ("fields", fields),
    ]
    if include_spam_trash:
        params.append(("includeSpamTrash", include_spam_trash))
    params.extend(("labelIds", label_id) for label_id in label_ids)
    return _append_query(f"{gmail_base_url()}/messages", params)


def message_url(
    message_id: str,
    *,
    format: str = "",
    metadata_headers: Iterable[str] = (),
    fields: str = "",
) -> str:
    params: list[tuple[str, object]] = [("format", format), ("fields", fields)]
    params.extend(("metadataHeaders", header) for header in metadata_headers)
    return _append_query(
        f"{gmail_base_url()}/messages/{quote(message_id, safe='')}", params
    )


def message_attachment_url(
    message_id: str,
    attachment_id: str,
    *,
    fields: str = "",
) -> str:
    return _append_query(
        (
            f"{gmail_base_url()}/messages/{quote(message_id, safe='')}"
            f"/attachments/{quote(attachment_id, safe='')}"
        ),
        [("fields", fields)],
    )


def thread_url(
    thread_id: str,
    *,
    format: str = "",
    metadata_headers: Iterable[str] = (),
    fields: str = "",
) -> str:
    params: list[tuple[str, object]] = [("format", format), ("fields", fields)]
    params.extend(("metadataHeaders", header) for header in metadata_headers)
    return _append_query(
        f"{gmail_base_url()}/threads/{quote(thread_id, safe='')}", params
    )


def labels_url(*, fields: str = "") -> str:
    return _append_query(f"{gmail_base_url()}/labels", [("fields", fields)])


def profile_url(*, fields: str = "") -> str:
    return _append_query(f"{gmail_base_url()}/profile", [("fields", fields)])
