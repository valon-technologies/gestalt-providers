from __future__ import annotations

from http import HTTPStatus
from typing import Any, TypeAlias
from urllib.parse import quote, urlencode

import gestalt

from internals import (
    MIMEParams,
    build_mime,
    ensure_forward_prefix,
    ensure_reply_prefix,
    extract_plain_text,
    filter_self_from_recipients,
    get_header,
    get_json,
    gmail_base_url,
    post_json,
)

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse


class SendMessageInput(gestalt.Model):
    to: str = gestalt.field(description="Recipient email address")
    subject: str = gestalt.field(description="Email subject")
    body: str = gestalt.field(description="Plain text body")
    cc: str = gestalt.field(description="CC recipients (comma-separated)", default="", required=False)
    bcc: str = gestalt.field(description="BCC recipients (comma-separated)", default="", required=False)
    html_body: str = gestalt.field(
        description="HTML body (sent as alternative to plain text)",
        default="",
        required=False,
    )


class CreateDraftInput(gestalt.Model):
    to: str = gestalt.field(description="Recipient email address")
    subject: str = gestalt.field(description="Email subject")
    body: str = gestalt.field(description="Plain text body")
    cc: str = gestalt.field(description="CC recipients (comma-separated)", default="", required=False)
    bcc: str = gestalt.field(description="BCC recipients (comma-separated)", default="", required=False)
    html_body: str = gestalt.field(description="HTML body", default="", required=False)


class ReplyMessageInput(gestalt.Model):
    message_id: str = gestalt.field(description="Original message ID")
    body: str = gestalt.field(description="Reply body")
    cc: str = gestalt.field(description="CC recipients (comma-separated)", default="", required=False)
    reply_all: bool = gestalt.field(description="Reply to all recipients", default=False, required=False)
    html_body: str = gestalt.field(description="HTML body", default="", required=False)


class ForwardMessageInput(gestalt.Model):
    message_id: str = gestalt.field(description="Message to forward")
    to: str = gestalt.field(description="Forward recipient")
    additional_text: str = gestalt.field(
        description="Text to prepend to forwarded content",
        default="",
        required=False,
    )
    cc: str = gestalt.field(description="CC recipients (comma-separated)", default="", required=False)


@gestalt.operation(
    id="messages.send",
    method="POST",
    description="Send an email message",
)
def messages_send(input: SendMessageInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.to or not input.subject or not input.body:
        return _server_error("to, subject, and body are required")

    raw = build_mime(
        MIMEParams(
            to=input.to,
            subject=input.subject,
            body=input.body,
            cc=input.cc,
            bcc=input.bcc,
            html_body=input.html_body,
        )
    )
    try:
        message = post_json(f"{gmail_base_url()}/messages/send", {"raw": raw}, req.token)
    except RuntimeError as err:
        return _server_error(str(err))
    return {"data": {"message": message}}


@gestalt.operation(
    id="messages.createDraft",
    method="POST",
    description="Create an email draft",
)
def messages_create_draft(input: CreateDraftInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.to or not input.subject or not input.body:
        return _server_error("to, subject, and body are required")

    raw = build_mime(
        MIMEParams(
            to=input.to,
            subject=input.subject,
            body=input.body,
            cc=input.cc,
            bcc=input.bcc,
            html_body=input.html_body,
        )
    )
    try:
        draft = post_json(
            f"{gmail_base_url()}/drafts",
            {"message": {"raw": raw}},
            req.token,
        )
    except RuntimeError as err:
        return _server_error(str(err))
    return {"data": {"draft": draft}}


@gestalt.operation(
    id="messages.reply",
    method="POST",
    description="Reply to an existing message",
)
def messages_reply(input: ReplyMessageInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.message_id or not input.body:
        return _server_error("message_id and body are required")

    try:
        original = get_json(_metadata_message_url(input.message_id), req.token)
    except RuntimeError as err:
        return _server_error(f"fetching original message: {err}")

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

    cc = input.cc
    if input.reply_all:
        all_cc = [value for value in (original_to, original_cc, cc) if value]
        cc = filter_self_from_recipients(", ".join(all_cc), self_email)

    raw = build_mime(
        MIMEParams(
            to=original_from,
            subject=ensure_reply_prefix(original_subject),
            body=input.body,
            cc=cc,
            html_body=input.html_body,
            in_reply_to=original_message_id,
            references=references,
        )
    )
    try:
        message = post_json(
            f"{gmail_base_url()}/messages/send",
            {"raw": raw, "threadId": original.get("threadId", "")},
            req.token,
        )
    except RuntimeError as err:
        return _server_error(str(err))
    return {"data": {"message": message}}


@gestalt.operation(
    id="messages.forward",
    method="POST",
    description="Forward a message to new recipients",
)
def messages_forward(input: ForwardMessageInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.message_id or not input.to:
        return _server_error("message_id and to are required")

    try:
        original = get_json(_full_message_url(input.message_id), req.token)
    except RuntimeError as err:
        return _server_error(f"fetching original message: {err}")

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
    if input.additional_text:
        forwarded_body = f"{input.additional_text}\r\n\r\n"
    forwarded_body += (
        "---------- Forwarded message ----------\r\n"
        f"From: {original_from}\r\n"
        f"Date: {original_date}\r\n"
        f"Subject: {original_subject}\r\n\r\n"
        f"{original_text}"
    )

    raw = build_mime(
        MIMEParams(
            to=input.to,
            subject=ensure_forward_prefix(original_subject),
            body=forwarded_body,
            cc=input.cc,
        )
    )
    try:
        message = post_json(f"{gmail_base_url()}/messages/send", {"raw": raw}, req.token)
    except RuntimeError as err:
        return _server_error(str(err))
    return {"data": {"message": message}}


def _validate_token(req: gestalt.Request) -> ErrorResponse | None:
    if not req.token.strip():
        return _server_error("token is required")
    return None


def _server_error(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message})


def _metadata_message_url(message_id: str) -> str:
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


def _full_message_url(message_id: str) -> str:
    return f"{gmail_base_url()}/messages/{quote(message_id, safe='')}?format=full"
