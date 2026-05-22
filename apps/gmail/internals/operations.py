from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .client import DEFAULT_GMAIL_CLIENT, GmailAPIClient, GmailJsonObject
from .mime import (
    MIMEParams,
    build_mime,
    ensure_forward_prefix,
    ensure_reply_prefix,
    extract_plain_text,
    filter_self_from_recipients,
    get_header,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class GmailReplyRequest:
    message_id: str
    body: str
    cc: str = ""
    reply_all: bool = False
    html_body: str = ""


@dataclass(frozen=True, slots=True, kw_only=True)
class GmailForwardRequest:
    message_id: str
    to: str
    additional_text: str = ""
    cc: str = ""


def send_message(
    token: str, params: MIMEParams, *, client: GmailAPIClient | None = None
) -> GmailJsonObject:
    gmail = gmail_client(client)
    raw = build_mime(params)
    message = gmail.post_json(f"{gmail.base_url()}/messages/send", {"raw": raw}, token)
    return {"data": {"message": message}}


def create_draft(
    token: str, params: MIMEParams, *, client: GmailAPIClient | None = None
) -> GmailJsonObject:
    gmail = gmail_client(client)
    raw = build_mime(params)
    draft = gmail.post_json(
        f"{gmail.base_url()}/drafts",
        {"message": {"raw": raw}},
        token,
    )
    return {"data": {"draft": draft}}


def update_draft(
    token: str,
    draft_id: str,
    params: MIMEParams,
    *,
    client: GmailAPIClient | None = None,
) -> GmailJsonObject:
    gmail = gmail_client(client)
    raw = build_mime(params)
    draft = gmail.put_json(
        gmail.draft_url(draft_id),
        {"id": draft_id, "message": {"raw": raw}},
        token,
    )
    return {"data": {"draft": draft}}


def send_draft(
    token: str, draft_id: str, *, client: GmailAPIClient | None = None
) -> GmailJsonObject:
    gmail = gmail_client(client)
    message = gmail.post_json(
        f"{gmail.base_url()}/drafts/send",
        {"id": draft_id},
        token,
    )
    return {"data": {"message": message}}


def reply_message(
    token: str,
    request: GmailReplyRequest,
    *,
    client: GmailAPIClient | None = None,
) -> GmailJsonObject:
    gmail = gmail_client(client)
    original = gmail.get_json(gmail.metadata_message_url(request.message_id), token)
    payload = object_field(original, "payload")
    headers = header_items(payload)

    original_from = get_header(headers, "From")
    original_to = get_header(headers, "To")
    original_cc = get_header(headers, "Cc")
    original_subject = get_header(headers, "Subject")
    original_message_id = get_header(headers, "Message-ID")
    original_references = get_header(headers, "References")

    references = original_message_id
    if original_references:
        references = f"{original_references} {original_message_id}"

    cc = request.cc
    if request.reply_all:
        self_email = get_header(headers, "Delivered-To")
        all_cc = [value for value in (original_to, original_cc, request.cc) if value]
        cc = filter_self_from_recipients(", ".join(all_cc), self_email)

    raw = build_mime(
        MIMEParams(
            to=original_from,
            subject=ensure_reply_prefix(original_subject),
            body=request.body,
            cc=cc,
            html_body=request.html_body,
            in_reply_to=original_message_id,
            references=references,
        )
    )
    message = gmail.post_json(
        f"{gmail.base_url()}/messages/send",
        {"raw": raw, "threadId": string_field(original, "threadId")},
        token,
    )
    return {"data": {"message": message}}


def forward_message(
    token: str,
    request: GmailForwardRequest,
    *,
    client: GmailAPIClient | None = None,
) -> GmailJsonObject:
    gmail = gmail_client(client)
    original = gmail.get_json(gmail.full_message_url(request.message_id), token)

    payload = object_field(original, "payload")
    headers = header_items(payload)

    original_subject = get_header(headers, "Subject")
    original_from = get_header(headers, "From")
    original_date = get_header(headers, "Date")

    body = object_field(payload, "body")
    original_text = extract_plain_text(
        parts_from_payload(payload),
        string_field(body, "data"),
        string_field(payload, "mimeType"),
    )

    forwarded_body = ""
    if request.additional_text:
        forwarded_body = f"{request.additional_text}\r\n\r\n"
    forwarded_body += (
        "---------- Forwarded message ----------\r\n"
        f"From: {original_from}\r\n"
        f"Date: {original_date}\r\n"
        f"Subject: {original_subject}\r\n\r\n"
        f"{original_text}"
    )

    raw = build_mime(
        MIMEParams(
            to=request.to,
            subject=ensure_forward_prefix(original_subject),
            body=forwarded_body,
            cc=request.cc,
        )
    )
    message = gmail.post_json(f"{gmail.base_url()}/messages/send", {"raw": raw}, token)
    return {"data": {"message": message}}


def gmail_client(client: GmailAPIClient | None) -> GmailAPIClient:
    return client if client is not None else DEFAULT_GMAIL_CLIENT


def object_field(data: Mapping[str, Any], field_name: str) -> GmailJsonObject:
    value = data.get(field_name)
    if isinstance(value, dict):
        return value
    return {}


def string_field(data: Mapping[str, Any], field_name: str) -> str:
    value = data.get(field_name)
    if isinstance(value, str):
        return value
    return ""


def header_items(data: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    return mapping_items(data.get("headers"))


def parts_from_payload(data: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    return mapping_items(data.get("parts"))


def mapping_items(value: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    items: list[Mapping[str, Any]] = []
    for item in value:
        if isinstance(item, Mapping):
            items.append(
                {key: nested for key, nested in item.items() if isinstance(key, str)}
            )
    return tuple(items)
