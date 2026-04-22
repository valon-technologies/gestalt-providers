import binascii
from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

from internals import (
    VercelBlobAPIError,
    VercelBlobConfig,
    VercelBlobConfigurationError,
    copy_blob,
    delete_blobs,
    get_blob,
    head_blob,
    list_blobs,
    put_blob,
)

plugin = gestalt.Plugin("vercel")

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse

_blob_config = VercelBlobConfig()


class BlobPutInput(gestalt.Model):
    pathname: str = gestalt.field(description="Blob pathname inside the store")
    access: str = gestalt.field(description="Blob access mode: private or public")
    body: str = gestalt.field(description="UTF-8 text payload to upload", default="", required=False)
    body_base64: str = gestalt.field(description="Base64-encoded payload to upload", default="", required=False)
    content_type: str = gestalt.field(description="Optional content type", default="", required=False)
    add_random_suffix: bool = gestalt.field(
        description="Whether to append a random suffix to the pathname",
        default=False,
        required=False,
    )
    overwrite: bool = gestalt.field(
        description="Whether to overwrite an existing blob with the same pathname",
        default=False,
        required=False,
    )
    cache_control_max_age: int | None = gestalt.field(
        description="Optional cache-control max age in seconds",
        default=None,
        required=False,
    )


class BlobGetInput(gestalt.Model):
    url_or_path: str = gestalt.field(description="Blob URL or pathname")
    access: str = gestalt.field(description="Blob access mode: private or public")
    if_none_match: str = gestalt.field(description="Optional ETag precondition", default="", required=False)
    timeout_seconds: float | None = gestalt.field(description="Optional request timeout in seconds", default=None, required=False)
    use_cache: bool = gestalt.field(description="Whether the SDK should use cached content", default=True, required=False)


class BlobHeadInput(gestalt.Model):
    url_or_path: str = gestalt.field(description="Blob URL or pathname")


class BlobListInput(gestalt.Model):
    prefix: str = gestalt.field(description="Optional pathname prefix", default="", required=False)
    limit: int | None = gestalt.field(description="Optional page size", default=None, required=False)
    cursor: str = gestalt.field(description="Optional pagination cursor", default="", required=False)
    mode: str = gestalt.field(description="Optional list mode, such as expanded or folded", default="", required=False)


class BlobDeleteInput(gestalt.Model):
    targets: list[str] = gestalt.field(description="Blob URLs or pathnames to delete")


class BlobCopyInput(gestalt.Model):
    source_url_or_path: str = gestalt.field(description="Source blob URL or pathname")
    destination_path: str = gestalt.field(description="Destination pathname inside the store")
    access: str = gestalt.field(description="Blob access mode for the copied object: private or public")
    content_type: str = gestalt.field(description="Optional content type for the copied object", default="", required=False)
    add_random_suffix: bool = gestalt.field(
        description="Whether to append a random suffix to the destination pathname",
        default=False,
        required=False,
    )
    overwrite: bool = gestalt.field(
        description="Whether to overwrite an existing destination blob",
        default=False,
        required=False,
    )
    cache_control_max_age: int | None = gestalt.field(
        description="Optional cache-control max age in seconds",
        default=None,
        required=False,
    )


@plugin.configure
def configure(_name: str, config: dict[str, Any]) -> None:
    global _blob_config
    _blob_config = VercelBlobConfig.from_config(config)


@plugin.operation(id="blob.put", method="POST", description="Upload a payload to Vercel Blob storage")
def blob_put(input: BlobPutInput) -> OperationResult:
    access = _normalize_access(input.access)
    if isinstance(access, gestalt.Response):
        return access
    pathname = _require_trimmed_text(input.pathname, "pathname")
    if isinstance(pathname, gestalt.Response):
        return pathname
    try:
        return put_blob(
            _blob_config,
            pathname=pathname,
            body=input.body,
            body_base64=input.body_base64,
            access=access,
            content_type=input.content_type.strip(),
            add_random_suffix=input.add_random_suffix,
            overwrite=input.overwrite,
            cache_control_max_age=input.cache_control_max_age,
        )
    except (ValueError, binascii.Error) as err:
        return _bad_request(str(err))
    except Exception as err:
        return _blob_error(err)


@plugin.operation(id="blob.get", method="POST", description="Download a blob from Vercel Blob storage")
def blob_get(input: BlobGetInput) -> OperationResult:
    access = _normalize_access(input.access)
    if isinstance(access, gestalt.Response):
        return access
    url_or_path = _require_trimmed_text(input.url_or_path, "url_or_path")
    if isinstance(url_or_path, gestalt.Response):
        return url_or_path
    try:
        return get_blob(
            _blob_config,
            url_or_path=url_or_path,
            access=access,
            if_none_match=input.if_none_match.strip(),
            timeout_seconds=input.timeout_seconds,
            use_cache=input.use_cache,
        )
    except Exception as err:
        return _blob_error(err)


@plugin.operation(id="blob.head", method="POST", description="Fetch metadata for a Vercel Blob object")
def blob_head(input: BlobHeadInput) -> OperationResult:
    url_or_path = _require_trimmed_text(input.url_or_path, "url_or_path")
    if isinstance(url_or_path, gestalt.Response):
        return url_or_path
    try:
        return head_blob(_blob_config, url_or_path=url_or_path)
    except Exception as err:
        return _blob_error(err)


@plugin.operation(id="blob.list", method="POST", description="List Vercel Blob objects in the configured store")
def blob_list(input: BlobListInput) -> OperationResult:
    if input.limit is not None and input.limit <= 0:
        return _bad_request("limit must be positive when provided")
    try:
        return list_blobs(
            _blob_config,
            limit=input.limit,
            prefix=input.prefix.strip(),
            cursor=input.cursor.strip(),
            mode=input.mode.strip(),
        )
    except Exception as err:
        return _blob_error(err)


@plugin.operation(id="blob.delete", method="POST", description="Delete one or more Vercel Blob objects")
def blob_delete(input: BlobDeleteInput) -> OperationResult:
    targets = [target.strip() for target in input.targets if target.strip()]
    if not targets:
        return _bad_request("targets must contain at least one non-empty value")
    try:
        return delete_blobs(_blob_config, targets=targets)
    except Exception as err:
        return _blob_error(err)


@plugin.operation(id="blob.copy", method="POST", description="Copy a blob to a new pathname in the same Vercel Blob store")
def blob_copy(input: BlobCopyInput) -> OperationResult:
    access = _normalize_access(input.access)
    if isinstance(access, gestalt.Response):
        return access
    source_url_or_path = _require_trimmed_text(input.source_url_or_path, "source_url_or_path")
    if isinstance(source_url_or_path, gestalt.Response):
        return source_url_or_path
    destination_path = _require_trimmed_text(input.destination_path, "destination_path")
    if isinstance(destination_path, gestalt.Response):
        return destination_path
    try:
        return copy_blob(
            _blob_config,
            source_url_or_path=source_url_or_path,
            destination_path=destination_path,
            access=access,
            content_type=input.content_type.strip(),
            add_random_suffix=input.add_random_suffix,
            overwrite=input.overwrite,
            cache_control_max_age=input.cache_control_max_age,
        )
    except Exception as err:
        return _blob_error(err)


def _normalize_access(value: str) -> str | ErrorResponse:
    access = value.strip().lower()
    if access not in {"private", "public"}:
        return _bad_request("access must be either private or public")
    return access


def _require_trimmed_text(value: str, name: str) -> str | ErrorResponse:
    trimmed = value.strip()
    if not trimmed:
        return _bad_request(f"{name} is required")
    return trimmed


def _blob_error(err: Exception) -> ErrorResponse:
    if isinstance(err, VercelBlobConfigurationError):
        return _server_error(str(err))
    if isinstance(err, VercelBlobAPIError):
        return gestalt.Response(status=err.status, body={"error": err.message})
    return _server_error(str(err))


def _bad_request(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _server_error(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message})
