from __future__ import annotations

import base64
import secrets
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class MIMEParams:
    to: str
    subject: str
    body: str
    cc: str = ""
    bcc: str = ""
    html_body: str = ""
    in_reply_to: str = ""
    references: str = ""


def sanitize_header(value: str) -> str:
    return value.replace("\r", "").replace("\n", "")


def build_mime(params: MIMEParams) -> str:
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


def random_boundary() -> str:
    return "gestalt_" + secrets.token_hex(16)


def encode_base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def decode_base64url(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def get_header(headers: list[dict[str, Any]], name: str) -> str:
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


def extract_plain_text(parts: list[dict[str, Any]], body_data: str, mime_type: str) -> str:
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
