from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Final

from . import config as _config
from . import models as _models

DEFAULT_BLOB_API_URL: Final = "https://vercel.com/api/blob"
DEFAULT_BLOB_API_VERSION: Final = "11"
MAXIMUM_PATHNAME_LENGTH: Final = 950

VercelBlobConfig = _config.VercelBlobConfig
VercelBlobConfigurationError = _config.VercelBlobConfigurationError
VercelBlobAccess = _models.VercelBlobAccess


class VercelBlobAPIError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        self.status = status
        self.message = message
        super().__init__(message)


class VercelBlobClientError(RuntimeError):
    pass


def put_blob(
    config: VercelBlobConfig, request: _models.VercelBlobPutRequest
) -> dict[str, Any]:
    token = config.require_token()
    _validate_path(request.pathname)
    _validate_access(request.access)
    payload = _payload(body=request.body, body_base64=request.body_base64)
    headers = {
        "x-vercel-blob-access": request.access,
        "x-add-random-suffix": "1" if request.add_random_suffix else "0",
        "x-allow-overwrite": "1" if request.overwrite else "0",
    }
    if request.content_type:
        headers["x-content-type"] = request.content_type
    if request.cache_control_max_age is not None:
        headers["x-cache-control-max-age"] = str(request.cache_control_max_age)
    result = _request_json(
        method="PUT",
        token=token,
        params={"pathname": request.pathname},
        headers=headers,
        body=payload,
    )
    return {"data": {"blob": _put_result(result)}}


def get_blob(
    config: VercelBlobConfig,
    request: _models.VercelBlobGetRequest,
) -> dict[str, Any]:
    token = config.require_token()
    _validate_access(request.access)
    target_url = request.url_or_path.strip()
    if not _is_url(target_url):
        store_id = _extract_store_id_from_token(token)
        if store_id:
            target_url = _construct_blob_url(store_id, target_url, request.access)
        else:
            head_request = _models.VercelBlobHeadRequest(
                url_or_path=request.url_or_path
            )
            target_url = head_blob(config, head_request)["data"]["blob"]["url"]
    download_url = _download_url(target_url)
    if not request.use_cache:
        target_url = _cache_bypass_url(target_url)

    headers: dict[str, str] = {}
    if request.access == VercelBlobAccess.PRIVATE:
        headers["authorization"] = f"Bearer {token}"
    if request.if_none_match:
        headers["if-none-match"] = request.if_none_match

    http_request = urllib.request.Request(target_url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(
            http_request, timeout=request.timeout_seconds or 30.0
        ) as response:
            content = response.read()
            status = getattr(response, "status", 200)
            return {
                "data": {
                    "blob": _download_result(
                        target_url, download_url, response.headers, content, status
                    )
                }
            }
    except urllib.error.HTTPError as err:
        if err.code == 304:
            return {
                "data": {
                    "blob": _download_result(
                        target_url,
                        download_url,
                        err.headers,
                        b"",
                        304,
                    )
                }
            }
        raise _blob_error_from_http_error(err) from err
    except urllib.error.URLError as err:
        raise VercelBlobClientError(
            f"Vercel Blob download failed: {err.reason}"
        ) from err


def head_blob(
    config: VercelBlobConfig, request: _models.VercelBlobHeadRequest
) -> dict[str, Any]:
    token = config.require_token()
    result = _request_json(
        method="GET", token=token, params={"url": request.url_or_path}
    )
    return {"data": {"blob": _head_result(result)}}


def list_blobs(
    config: VercelBlobConfig,
    request: _models.VercelBlobListRequest,
) -> dict[str, Any]:
    token = config.require_token()
    params: dict[str, Any] = {}
    if request.limit is not None:
        params["limit"] = request.limit
    if request.prefix:
        params["prefix"] = request.prefix
    if request.cursor:
        params["cursor"] = request.cursor
    if request.mode:
        params["mode"] = request.mode
    result = _request_json(method="GET", token=token, params=params)
    return {
        "data": {
            "blobs": [_list_item(blob) for blob in result.get("blobs", [])],
            "cursor": str(result.get("cursor", "") or ""),
            "has_more": bool(result.get("hasMore", False)),
            "folders": list(result.get("folders", []) or []),
        }
    }


def delete_blobs(
    config: VercelBlobConfig, request: _models.VercelBlobDeleteRequest
) -> dict[str, Any]:
    token = config.require_token()
    _request_json(
        method="POST",
        token=token,
        path="/delete",
        headers={"content-type": "application/json"},
        body=json.dumps({"urls": list(request.targets)}).encode("utf-8"),
        decode_json=False,
    )
    return {"data": {"deleted": len(request.targets)}}


def copy_blob(
    config: VercelBlobConfig,
    request: _models.VercelBlobCopyRequest,
) -> dict[str, Any]:
    token = config.require_token()
    _validate_path(request.destination_path)
    _validate_access(request.access)
    source_url = request.source_url_or_path
    if not _is_url(source_url):
        head_request = _models.VercelBlobHeadRequest(
            url_or_path=request.source_url_or_path
        )
        source_url = head_blob(config, head_request)["data"]["blob"]["url"]
    headers = {
        "x-vercel-blob-access": request.access,
        "x-add-random-suffix": "1" if request.add_random_suffix else "0",
        "x-allow-overwrite": "1" if request.overwrite else "0",
    }
    if request.content_type:
        headers["x-content-type"] = request.content_type
    if request.cache_control_max_age is not None:
        headers["x-cache-control-max-age"] = str(request.cache_control_max_age)
    result = _request_json(
        method="PUT",
        token=token,
        params={"pathname": request.destination_path, "fromUrl": source_url},
        headers=headers,
    )
    return {"data": {"blob": _put_result(result)}}


def _request_json(
    *,
    method: str,
    token: str,
    path: str = "",
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    decode_json: bool = True,
) -> dict[str, Any]:
    query = urllib.parse.urlencode(
        {k: v for k, v in (params or {}).items() if v is not None}
    )
    url = _api_url(path)
    if query:
        url = f"{url}?{query}"
    request_body = body.encode("utf-8") if isinstance(body, str) else body
    request_headers = {
        "authorization": f"Bearer {token}",
        "x-api-version": DEFAULT_BLOB_API_VERSION,
    }
    if headers:
        request_headers.update(headers)
    request = urllib.request.Request(
        url, headers=request_headers, data=request_body, method=method
    )
    try:
        with urllib.request.urlopen(request, timeout=30.0) as response:
            if not decode_json:
                return {}
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        raise _blob_error_from_http_error(err) from err
    except urllib.error.URLError as err:
        raise VercelBlobClientError(
            f"Vercel Blob request failed: {err.reason}"
        ) from err
    except json.JSONDecodeError as err:
        raise VercelBlobClientError(
            f"Vercel Blob returned invalid JSON: {err}"
        ) from err

    if not isinstance(payload, dict):
        raise VercelBlobClientError("Vercel Blob returned a non-object JSON response")
    return payload


def _blob_error_from_http_error(err: urllib.error.HTTPError) -> VercelBlobAPIError:
    body = err.read().decode("utf-8", errors="replace")
    code = ""
    message = body or f"Vercel Blob request failed with status {err.code}"
    try:
        payload = json.loads(body)
        error = payload.get("error", {})
        if isinstance(error, dict):
            code = str(error.get("code", "") or "")
            message = str(error.get("message", "") or message)
    except json.JSONDecodeError:
        pass

    lowered = code.lower()
    if lowered == "forbidden":
        return VercelBlobAPIError(
            403,
            "Vercel Blob: Access denied, please provide a valid token for this resource.",
        )
    if lowered == "not_found":
        return VercelBlobAPIError(404, "Vercel Blob: The requested blob does not exist")
    if lowered == "store_not_found":
        return VercelBlobAPIError(
            404, "Vercel Blob: The requested blob store does not exist"
        )
    if lowered == "content_type_not_allowed":
        return VercelBlobAPIError(400, f"Vercel Blob: {message}")
    if lowered == "file_too_large":
        return VercelBlobAPIError(413, f"Vercel Blob: {message}")
    if lowered == "rate_limited":
        return VercelBlobAPIError(429, f"Vercel Blob: {message}")
    if lowered in {"store_suspended", "service_unavailable"}:
        return VercelBlobAPIError(503, f"Vercel Blob: {message}")
    if err.code == 403:
        return VercelBlobAPIError(
            403,
            "Vercel Blob: Access denied, please provide a valid token for this resource.",
        )
    if err.code == 404:
        return VercelBlobAPIError(404, "Vercel Blob: The requested blob does not exist")
    return VercelBlobAPIError(err.code, f"Vercel Blob: {message}")


def _payload(*, body: str, body_base64: str) -> bytes:
    if body and body_base64:
        raise ValueError("provide either body or body_base64, not both")
    if body_base64:
        return base64.b64decode(body_base64, validate=True)
    if not body:
        raise ValueError("body or body_base64 is required")
    return body.encode("utf-8")


def _put_result(result: dict[str, Any]) -> dict[str, Any]:
    url = str(result.get("url", ""))
    return {
        "url": url,
        "download_url": str(result.get("downloadUrl", "") or _download_url(url)),
        "pathname": str(result.get("pathname", "")),
        "content_type": str(result.get("contentType", "")),
        "content_disposition": str(result.get("contentDisposition", "")),
    }


def _head_result(result: dict[str, Any]) -> dict[str, Any]:
    url = str(result.get("url", ""))
    return {
        "size": int(result.get("size", 0)),
        "uploaded_at": _iso(_parse_datetime(result.get("uploadedAt"))),
        "pathname": str(result.get("pathname", "")),
        "content_type": str(result.get("contentType", "")),
        "content_disposition": str(result.get("contentDisposition", "")),
        "url": url,
        "download_url": str(result.get("downloadUrl", "") or _download_url(url)),
        "cache_control": str(result.get("cacheControl", "")),
    }


def _download_result(
    target_url: str,
    download_url: str,
    headers: Any,
    content: bytes,
    status_code: int,
) -> dict[str, Any]:
    content_text = ""
    try:
        content_text = content.decode("utf-8")
    except UnicodeDecodeError:
        content_text = ""
    path = urllib.parse.urlparse(target_url).path.lstrip("/")
    size_header = headers.get("content-length")
    size = int(size_header) if size_header else len(content)
    return {
        "url": target_url,
        "download_url": download_url,
        "pathname": path,
        "content_type": headers.get("content-type", "application/octet-stream"),
        "size": size if status_code != 304 else None,
        "content_disposition": headers.get("content-disposition", ""),
        "cache_control": headers.get("cache-control", ""),
        "uploaded_at": _iso(_parse_last_modified(headers.get("last-modified"))),
        "etag": headers.get("etag", ""),
        "status_code": status_code,
        "content_base64": base64.b64encode(content).decode("ascii"),
        "content_text": content_text,
    }


def _list_item(result: dict[str, Any]) -> dict[str, Any]:
    url = str(result.get("url", ""))
    return {
        "url": url,
        "download_url": str(result.get("downloadUrl", "") or _download_url(url)),
        "pathname": str(result.get("pathname", "")),
        "size": int(result.get("size", 0)),
        "uploaded_at": _iso(_parse_datetime(result.get("uploadedAt"))),
    }


def _api_url(path: str = "") -> str:
    base_url = os.getenv("VERCEL_BLOB_API_URL", "").strip() or DEFAULT_BLOB_API_URL
    return f"{base_url}{path}"


def _extract_store_id_from_token(token: str) -> str:
    parts = token.split("_")
    return parts[3] if len(parts) > 3 else ""


def _construct_blob_url(store_id: str, pathname: str, access: str) -> str:
    clean_path = pathname.lstrip("/")
    return f"https://{store_id}.{access}.blob.vercel-storage.com/{clean_path}"


def _download_url(blob_url: str) -> str:
    parsed = urllib.parse.urlparse(blob_url)
    params = dict(urllib.parse.parse_qsl(parsed.query))
    params["download"] = "1"
    return urllib.parse.urlunparse(
        parsed._replace(query=urllib.parse.urlencode(params))
    )


def _cache_bypass_url(blob_url: str) -> str:
    parsed = urllib.parse.urlparse(blob_url)
    params = dict(urllib.parse.parse_qsl(parsed.query))
    params["cache"] = "0"
    return urllib.parse.urlunparse(
        parsed._replace(query=urllib.parse.urlencode(params))
    )


def _is_url(value: str) -> bool:
    return value.startswith("http://") or value.startswith("https://")


def _validate_access(access: VercelBlobAccess) -> None:
    if access not in set(VercelBlobAccess):
        raise ValueError("access must be either private or public")


def _validate_path(path: str) -> None:
    if not path:
        raise ValueError("pathname is required")
    if len(path) > MAXIMUM_PATHNAME_LENGTH:
        raise ValueError(
            f"pathname exceeds the maximum length of {MAXIMUM_PATHNAME_LENGTH}"
        )
    if "//" in path:
        raise ValueError('pathname cannot contain "//"')


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(tz=UTC)


def _parse_last_modified(value: str | None) -> datetime:
    if not value:
        return datetime.now(tz=UTC)
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return datetime.now(tz=UTC)


def _iso(value: datetime) -> str:
    return value.isoformat()
