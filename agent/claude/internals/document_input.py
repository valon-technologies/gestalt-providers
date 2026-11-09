"""Bounded native PDF input. URLs are capabilities: never include them in errors/logs."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
from collections.abc import AsyncIterator
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlsplit

import httpx

MAX_PDF_BYTES = 20 * 1024 * 1024
MAX_NATIVE_INPUT_BYTES = 30 * 1024 * 1024
DOWNLOAD_TIMEOUT_SECONDS = 30


class DocumentInputError(ValueError):
    pass


def document_urls(messages: list[dict[str, Any]], prefixes: tuple[str, ...]) -> list[str]:
    urls = []
    for message in messages:
        for part in message.get("parts") or []:
            if not isinstance(part, dict) or "image_ref" not in part:
                continue
            ref = part["image_ref"]
            if not isinstance(ref, dict) or ref.get("mime_type") != "application/pdf":
                continue  # Preserve pre-existing non-PDF prompt serialization.
            url = ref.get("uri")
            if not isinstance(url, str) or len(url) > 8192:
                raise DocumentInputError("Invalid PDF reference")
            if re.search(r"[\x00-\x20\x7f\\]", url):
                raise DocumentInputError("Invalid PDF reference")
            try:
                parsed = urlsplit(url)
                port = parsed.port
            except ValueError, TypeError:
                raise DocumentInputError("Invalid PDF reference") from None
            if (
                parsed.scheme != "https"
                or not parsed.hostname
                or parsed.username
                or parsed.password
                or port not in (None, 443)
                or parsed.query
                or parsed.fragment
                or "@" in parsed.netloc
                or not any(url.startswith(prefix) for prefix in prefixes)
                or not re.fullmatch(r"[A-Za-z0-9._~-]+", parsed.path.rsplit("/", 1)[-1])
                or "%" in parsed.path
                or ".." in parsed.path
            ):
                raise DocumentInputError("PDF URL is outside configured trusted download endpoints")
            urls.append(url)
    if len(urls) > 1:
        raise DocumentInputError("At most one PDF may be attached per turn")
    return urls


@contextmanager
def _redact_download_logs(url: str):
    # HTTPX logs the full request URL at INFO, including its signed capability.
    # Scope a filter to this download; concurrent downloads' filters compose.
    normalized_url = str(httpx.URL(url))
    private_path = urlsplit(normalized_url).path

    class Redact(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            message = record.getMessage()
            if url in message or normalized_url in message or private_path in message:
                record.__dict__.update(
                    msg=message.replace(url, "[private PDF link]")
                    .replace(normalized_url, "[private PDF link]")
                    .replace(private_path, "[private PDF path]"),
                    args=(),
                )
            return True

    loggers = [
        logging.getLogger(name)
        for name in (
            "httpx",
            "httpcore.connection",
            "httpcore.http11",
            "httpcore.http2",
            "httpcore.proxy",
            "httpcore.socks",
        )
    ]
    redact = Redact()
    for logger in loggers:
        logger.addFilter(redact)
    try:
        yield
    finally:
        for logger in loggers:
            logger.removeFilter(redact)


async def _download_pdf(url: str) -> bytes:
    try:
        # A total async deadline also cancels a server that drips bytes forever.
        with _redact_download_logs(url):
            return await _download_pdf_bounded(url)
    except DocumentInputError:
        raise
    except asyncio.CancelledError:
        raise
    except Exception:
        raise DocumentInputError("PDF download failed, timed out, or its signed URL expired") from None


async def _download_pdf_bounded(url: str) -> bytes:
    try:
        async with asyncio.timeout(DOWNLOAD_TIMEOUT_SECONDS):
            async with httpx.AsyncClient(
                follow_redirects=False, trust_env=False, timeout=DOWNLOAD_TIMEOUT_SECONDS
            ) as client:
                async with client.stream(
                    "GET", url, headers={"Accept": "application/pdf", "Accept-Encoding": "identity"}
                ) as response:
                    if response.status_code != 200:
                        raise DocumentInputError("PDF download failed or its signed URL expired")
                    if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                        raise DocumentInputError("Compressed PDF HTTP downloads are not supported")
                    length = response.headers.get("Content-Length")
                    declared_length = int(length) if length is not None else None
                    if declared_length is not None and (declared_length < 0 or declared_length > MAX_PDF_BYTES):
                        raise DocumentInputError("PDF exceeds 20 MiB input limit or has invalid length")
                    chunks = []
                    size = 0
                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        size += len(chunk)
                        if size > MAX_PDF_BYTES:
                            raise DocumentInputError("PDF exceeds 20 MiB input limit")
                        chunks.append(chunk)
                    if declared_length is not None and size != declared_length:
                        raise DocumentInputError("PDF download ended before the complete file was received")
                    data = b"".join(chunks)
        if not data.startswith(b"%PDF-"):
            raise DocumentInputError("PDF download did not contain PDF bytes")
        return data
    except DocumentInputError:
        raise
    except asyncio.CancelledError:
        raise
    except Exception:
        # HTTP exceptions include signed URLs; intentionally replace their details.
        raise DocumentInputError("PDF download failed, timed out, or its signed URL expired") from None


async def native_document_prompt(prompt: str, urls: list[str]) -> AsyncIterator[dict[str, Any]]:
    content: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
    for url in urls:
        data = await _download_pdf(url)
        content.append(
            {
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": base64.b64encode(data).decode("ascii"),
                },
            }
        )
    message = {"type": "user", "message": {"role": "user", "content": content}, "parent_tool_use_id": None}
    if len(json.dumps(message).encode("utf-8")) > MAX_NATIVE_INPUT_BYTES:
        raise DocumentInputError("PDF and prompt exceed the 30 MiB native input limit")
    yield message
