from __future__ import annotations

import base64
import binascii
import re
import secrets
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from email.message import EmailMessage
from email.policy import SMTP
from typing import Any


MAX_ATTACHMENT_BYTES = 20 * 1024 * 1024
MAX_ATTACHMENT_COUNT = 20


@dataclass(frozen=True, slots=True)
class MIMEAttachment:
    filename: str
    content_type: str
    content_base64: str


@dataclass(frozen=True, slots=True)
class MIMEParams:
    to: str
    subject: str
    body: str
    cc: str = ""
    bcc: str = ""
    html_body: str = ""
    in_reply_to: str = ""
    references: str = ""
    attachments: tuple[MIMEAttachment, ...] = ()


def sanitize_header(value: str) -> str:
    return value.replace("\r", "").replace("\n", "")


def build_mime(params: MIMEParams) -> str:
    if params.attachments:
        return build_mime_with_attachments(params)
    lines = [
        "MIME-Version: 1.0",
        f"To: {sanitize_header(params.to)}",
        f"Subject: {sanitize_header(params.subject)}",
    ]
    if params.cc:
        lines.append(f"Cc: {sanitize_header(params.cc)}")
    if params.bcc:
        lines.append(f"Bcc: {sanitize_header(params.bcc)}")
    if params.in_reply_to:
        lines.append(f"In-Reply-To: {sanitize_header(params.in_reply_to)}")
        lines.append(f"References: {sanitize_header(params.references)}")

    if params.html_body:
        boundary = random_boundary()
        lines.extend(
            [
                f"Content-Type: multipart/alternative; boundary={boundary}",
                "",
                f"--{boundary}",
                "Content-Type: text/plain; charset=UTF-8",
                "",
                params.body,
                f"--{boundary}",
                "Content-Type: text/html; charset=UTF-8",
                "",
                params.html_body,
                f"--{boundary}--",
                "",
            ]
        )
        raw_mime = "\r\n".join(lines)
    else:
        lines.extend(
            [
                "Content-Type: text/plain; charset=UTF-8",
                "",
                params.body,
            ]
        )
        raw_mime = "\r\n".join(lines)

    return encode_base64url(raw_mime.encode("utf-8"))


def build_mime_with_attachments(params: MIMEParams) -> str:
    if len(params.attachments) > MAX_ATTACHMENT_COUNT:
        raise ValueError(f"At most {MAX_ATTACHMENT_COUNT} attachments are allowed")
    message = EmailMessage(policy=SMTP)
    headers = {
        "To": params.to,
        "Subject": params.subject,
        "Cc": params.cc,
        "Bcc": params.bcc,
        "In-Reply-To": params.in_reply_to,
        "References": params.references,
    }
    for name, value in headers.items():
        if any(char in value for char in ("\r", "\n", "\x00")):
            raise ValueError(f"{name} must not contain newlines or NUL")
        if value:
            message[name] = value
    message.set_content(params.body)
    if params.html_body:
        message.add_alternative(params.html_body, subtype="html")
    total = 0
    for attachment in params.attachments:
        if (
            not attachment.filename
            or len(attachment.filename) > 255
            or any(ord(char) < 32 or ord(char) == 127 for char in attachment.filename)
            or "/" in attachment.filename
            or "\\" in attachment.filename
        ):
            raise ValueError("Attachment filename must be a bounded plain filename")
        if not re.fullmatch(
            r"[a-zA-Z0-9!#$&^_.+-]+/[a-zA-Z0-9!#$&^_.+-]+", attachment.content_type
        ):
            raise ValueError(
                "Attachment content_type must be a MIME type without parameters"
            )
        # Bound encoded input before allocating the decoded file body.
        if (
            len(attachment.content_base64)
            > ((MAX_ATTACHMENT_BYTES - total + 2) // 3) * 4
        ):
            raise ValueError("Combined attachments exceed the 20 MiB limit")
        try:
            content = base64.b64decode(attachment.content_base64, validate=True)
        except (binascii.Error, ValueError) as error:
            raise ValueError(
                "Attachment content_base64 must be valid base64"
            ) from error
        if base64.b64encode(content).decode("ascii") != attachment.content_base64:
            raise ValueError(
                "Attachment content_base64 must use canonical padded base64"
            )
        total += len(content)
        if total > MAX_ATTACHMENT_BYTES:
            raise ValueError("Combined attachments exceed the 20 MiB limit")
        maintype, subtype = attachment.content_type.split("/", 1)
        message.add_attachment(
            content, maintype=maintype, subtype=subtype, filename=attachment.filename
        )
    return encode_base64url(message.as_bytes())


def random_boundary() -> str:
    return "gestalt_" + secrets.token_hex(16)


def encode_base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def decode_base64url(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def get_header(headers: Sequence[Mapping[str, Any]], name: str) -> str:
    expected = name.lower()
    for header in headers:
        header_name = header.get("name")
        if isinstance(header_name, str) and header_name.lower() == expected:
            header_value = header.get("value", "")
            return header_value if isinstance(header_value, str) else str(header_value)
    return ""


def ensure_reply_prefix(subject: str) -> str:
    if subject.lower().startswith("re:"):
        return subject
    return f"Re: {subject}"


def extract_email(address: str) -> str:
    start = address.rfind("<")
    if start != -1:
        end = address.find(">", start)
        if end != -1:
            return address[start + 1 : end]
    return address.strip()


def filter_self_from_recipients(recipients: str, self_email: str) -> str:
    if not self_email:
        return recipients

    normalized_self = self_email.lower()
    filtered: list[str] = []
    for address in recipients.split(","):
        candidate = address.strip()
        if not candidate:
            continue
        if extract_email(candidate).lower() == normalized_self:
            continue
        filtered.append(candidate)
    return ", ".join(filtered)


def ensure_forward_prefix(subject: str) -> str:
    if subject.lower().startswith("fwd:"):
        return subject
    return f"Fwd: {subject}"


def extract_plain_text(
    parts: Sequence[Mapping[str, Any]], body_data: str, mime_type: str
) -> str:
    for part in parts:
        if part.get("mimeType") != "text/plain":
            continue
        body = part.get("body")
        if not isinstance(body, dict):
            continue
        data = body.get("data")
        if isinstance(data, str) and data:
            try:
                return decode_base64url(data).decode("utf-8")
            except Exception:
                return ""

    if body_data and "html" not in mime_type:
        try:
            return decode_base64url(body_data).decode("utf-8")
        except Exception:
            return ""
    return ""
