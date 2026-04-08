from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

import gestalt

DEFAULT_BASE_URL = "https://app.moderntreasury.com/api"
DEFAULT_TIMEOUT_SECONDS = 30.0


@dataclass(slots=True)
class ModernTreasuryAPIError(RuntimeError):
    status: int
    message: str
    details: Any = None

    def __str__(self) -> str:
        return self.message


class ModernTreasuryClient:
    def __init__(self, *, org_id: str, api_key: str, base_url: str = DEFAULT_BASE_URL) -> None:
        self.org_id = org_id.strip()
        self.api_key = api_key.strip()
        self.base_url = (base_url or DEFAULT_BASE_URL).rstrip("/")

    @classmethod
    def from_request(cls, req: gestalt.Request) -> ModernTreasuryClient:
        return cls(
            org_id=req.connection_param("org_id"),
            api_key=req.token,
            base_url=req.connection_param("base_url") or DEFAULT_BASE_URL,
        )

    def request(
        self,
        *,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        body: dict[str, Any] | list[Any] | None = None,
        idempotency_key: str = "",
    ) -> Any:
        url = self._build_url(path=path, query=query)
        headers = {
            "Accept": "application/json",
            "Authorization": self._authorization_header(),
        }
        data: bytes | None = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body).encode("utf-8")
        if idempotency_key.strip():
            headers["Idempotency-Key"] = idempotency_key.strip()

        request = urllib.request.Request(url=url, method=method.upper(), headers=headers, data=data)
        try:
            with urllib.request.urlopen(request, timeout=DEFAULT_TIMEOUT_SECONDS) as response:
                return _decode_json_bytes(response.read())
        except urllib.error.HTTPError as err:
            raise _http_error(err) from err
        except urllib.error.URLError as err:
            raise ModernTreasuryAPIError(
                status=HTTPStatus.BAD_GATEWAY,
                message=f"failed to reach Modern Treasury: {err.reason}",
            ) from err

    def _authorization_header(self) -> str:
        token = base64.b64encode(f"{self.org_id}:{self.api_key}".encode("utf-8")).decode("ascii")
        return f"Basic {token}"

    def _build_url(self, *, path: str, query: dict[str, Any] | None) -> str:
        encoded_query = urllib.parse.urlencode(_clean_query(query or {}), doseq=True)
        url = f"{self.base_url}/{path.lstrip('/')}"
        if encoded_query:
            return f"{url}?{encoded_query}"
        return url


def _clean_query(query: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in query.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        cleaned[key] = value
    return cleaned


def _decode_json_bytes(data: bytes) -> Any:
    if not data:
        return {}
    return json.loads(data.decode("utf-8"))


def _http_error(err: urllib.error.HTTPError) -> ModernTreasuryAPIError:
    raw_body = err.read().decode("utf-8", errors="replace")
    details: Any = None
    message = raw_body or err.reason or "Modern Treasury request failed"

    if raw_body:
        try:
            details = json.loads(raw_body)
        except json.JSONDecodeError:
            details = raw_body
        else:
            message = _extract_error_message(details) or message

    return ModernTreasuryAPIError(status=err.code, message=message, details=details)


def _extract_error_message(details: Any) -> str:
    if isinstance(details, dict):
        if isinstance(details.get("message"), str) and details["message"].strip():
            return details["message"].strip()
        errors = details.get("errors")
        if isinstance(errors, dict):
            if isinstance(errors.get("message"), str) and errors["message"].strip():
                return errors["message"].strip()
            first = next(iter(errors.values()), None)
            if isinstance(first, list) and first:
                return str(first[0])
        if isinstance(errors, list) and errors:
            return str(errors[0])
    if isinstance(details, list) and details:
        return str(details[0])
    if isinstance(details, str):
        return details
    return ""
