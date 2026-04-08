from typing import Any

from .client import full_message_url, get_json, gmail_base_url, metadata_message_url, post_json
from .mime import (
    MIMEParams,
    build_mime,
    ensure_forward_prefix,
    ensure_reply_prefix,
    extract_plain_text,
    filter_self_from_recipients,
    get_header,
)


def send_message(token: str, to: str, subject: str, body: str, cc: str, bcc: str, html_body: str) -> dict[str, Any]:
    raw = build_mime(
        MIMEParams(
            to=to,
            subject=subject,
            body=body,
            cc=cc,
            bcc=bcc,
            html_body=html_body,
        )
    )
    message = post_json(f"{gmail_base_url()}/messages/send", {"raw": raw}, token)
    return {"data": {"message": message}}


def create_draft(token: str, to: str, subject: str, body: str, cc: str, bcc: str, html_body: str) -> dict[str, Any]:
    raw = build_mime(
        MIMEParams(
            to=to,
            subject=subject,
            body=body,
            cc=cc,
            bcc=bcc,
            html_body=html_body,
        )
    )
    draft = post_json(
        f"{gmail_base_url()}/drafts",
        {"message": {"raw": raw}},
        token,
    )
    return {"data": {"draft": draft}}


def reply_message(token: str, message_id: str, body: str, cc: str, reply_all: bool, html_body: str) -> dict[str, Any]:
    original = get_json(metadata_message_url(message_id), token)

    payload = original.get("payload")
    headers = payload.get("headers", []) if isinstance(payload, dict) else []
    if not isinstance(headers, list):
        headers = []

    original_from = get_header(headers, "From")
    original_to = get_header(headers, "To")
    original_cc = get_header(headers, "Cc")
    original_subject = get_header(headers, "Subject")
    original_message_id = get_header(headers, "Message-ID")
    original_references = get_header(headers, "References")

    references = original_message_id
    if original_references:
        references = f"{original_references} {original_message_id}"

    self_email = get_header(headers, "Delivered-To")

    if reply_all:
        all_cc = [value for value in (original_to, original_cc, cc) if value]
        cc = filter_self_from_recipients(", ".join(all_cc), self_email)

    raw = build_mime(
        MIMEParams(
            to=original_from,
            subject=ensure_reply_prefix(original_subject),
            body=body,
            cc=cc,
            html_body=html_body,
            in_reply_to=original_message_id,
            references=references,
        )
    )
    message = post_json(
        f"{gmail_base_url()}/messages/send",
        {"raw": raw, "threadId": original.get("threadId", "")},
        token,
    )
    return {"data": {"message": message}}


def forward_message(token: str, message_id: str, to: str, additional_text: str, cc: str) -> dict[str, Any]:
    original = get_json(full_message_url(message_id), token)

    payload = original.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    headers = payload.get("headers", [])
    if not isinstance(headers, list):
        headers = []

    original_subject = get_header(headers, "Subject")
    original_from = get_header(headers, "From")
    original_date = get_header(headers, "Date")

    body = payload.get("body")
    body_data = body.get("data", "") if isinstance(body, dict) else ""
    if not isinstance(body_data, str):
        body_data = ""
    parts = payload.get("parts", [])
    if not isinstance(parts, list):
        parts = []
    mime_type = payload.get("mimeType", "")
    if not isinstance(mime_type, str):
        mime_type = ""

    original_text = extract_plain_text(parts, body_data, mime_type)

    forwarded_body = ""
    if additional_text:
        forwarded_body = f"{additional_text}\r\n\r\n"
    forwarded_body += (
        "---------- Forwarded message ----------\r\n"
        f"From: {original_from}\r\n"
        f"Date: {original_date}\r\n"
        f"Subject: {original_subject}\r\n\r\n"
        f"{original_text}"
    )

    raw = build_mime(
        MIMEParams(
            to=to,
            subject=ensure_forward_prefix(original_subject),
            body=forwarded_body,
            cc=cc,
        )
    )
    message = post_json(f"{gmail_base_url()}/messages/send", {"raw": raw}, token)
    return {"data": {"message": message}}
