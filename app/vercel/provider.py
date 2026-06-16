from __future__ import annotations

import binascii
import json
from http import HTTPStatus
from typing import Any, TypeAlias
import urllib.error
import urllib.parse
import urllib.request

import gestalt

from internals.blob import (
    VercelBlobAPIError,
    VercelBlobClientError,
    copy_blob,
    delete_blobs,
    get_blob,
    head_blob,
    list_blobs,
    put_blob,
)
from internals.config import (
    VercelBlobConfig,
    VercelBlobConfigurationError,
    blob_config_from_mapping,
)
from internals.models import (
    VercelBlobAccess,
    VercelBlobCopyRequest,
    VercelBlobDeleteRequest,
    VercelBlobGetRequest,
    VercelBlobHeadRequest,
    VercelBlobListRequest,
    VercelBlobPutRequest,
)

app = gestalt.App("vercel")

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse

_blob_config = VercelBlobConfig()


class BlobPutInput(gestalt.Model):
    pathname: str = gestalt.field(description="Blob pathname inside the store")
    access: str = gestalt.field(description="Blob access mode: private or public")
    body: str = gestalt.field(
        description="UTF-8 text payload to upload", default="", required=False
    )
    body_base64: str = gestalt.field(
        description="Base64-encoded payload to upload", default="", required=False
    )
    content_type: str = gestalt.field(
        description="Optional content type", default="", required=False
    )
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
    if_none_match: str = gestalt.field(
        description="Optional ETag precondition", default="", required=False
    )
    timeout_seconds: float | None = gestalt.field(
        description="Optional request timeout in seconds", default=None, required=False
    )
    use_cache: bool = gestalt.field(
        description="Whether the SDK should use cached content",
        default=True,
        required=False,
    )


class BlobHeadInput(gestalt.Model):
    url_or_path: str = gestalt.field(description="Blob URL or pathname")


class BlobListInput(gestalt.Model):
    prefix: str = gestalt.field(
        description="Optional pathname prefix", default="", required=False
    )
    limit: int | None = gestalt.field(
        description="Optional page size", default=None, required=False
    )
    cursor: str = gestalt.field(
        description="Optional pagination cursor", default="", required=False
    )
    mode: str = gestalt.field(
        description="Optional list mode, such as expanded or folded",
        default="",
        required=False,
    )


class BlobDeleteInput(gestalt.Model):
    targets: list[str] = gestalt.field(description="Blob URLs or pathnames to delete")


class BlobCopyInput(gestalt.Model):
    source_url_or_path: str = gestalt.field(description="Source blob URL or pathname")
    destination_path: str = gestalt.field(
        description="Destination pathname inside the store"
    )
    access: str = gestalt.field(
        description="Blob access mode for the copied object: private or public"
    )
    content_type: str = gestalt.field(
        description="Optional content type for the copied object",
        default="",
        required=False,
    )
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


class TeamMembersInviteInput(gestalt.Model):
    teamId: str = gestalt.field(description="Vercel team ID")
    email: str = gestalt.field(description="Email address to invite")
    role: str = gestalt.field(
        description="Team role to grant to the invited user",
        default="MEMBER",
        required=False,
    )
    slug: str = gestalt.field(
        description="Optional Vercel team slug",
        default="",
        required=False,
    )


class CommentsListInput(gestalt.Model):
    url: str = gestalt.field(
        description="Optional Vercel comment link to parse for team slug and comment id",
        default="",
        required=False,
    )
    teamId: str = gestalt.field(
        description="Optional Vercel team ID",
        default="",
        required=False,
    )
    slug: str = gestalt.field(
        description="Optional Vercel team slug",
        default="",
        required=False,
    )
    projectId: str = gestalt.field(
        description="Optional Vercel project ID filter",
        default="",
        required=False,
    )
    deploymentId: str = gestalt.field(
        description="Optional Vercel deployment ID filter",
        default="",
        required=False,
    )
    threadId: str = gestalt.field(
        description="Optional Toolbar comment thread ID filter",
        default="",
        required=False,
    )
    commentId: str = gestalt.field(
        description="Optional Toolbar comment ID filter",
        default="",
        required=False,
    )
    cursor: str = gestalt.field(
        description="Optional pagination cursor",
        default="",
        required=False,
    )
    limit: int | None = gestalt.field(
        description="Optional page size",
        default=None,
        required=False,
    )
    resolved: bool | None = gestalt.field(
        description="Optional resolved-state filter",
        default=None,
        required=False,
    )
    extra_query: dict[str, Any] = gestalt.field(
        description="Additional query parameters to pass through to Vercel",
        default_factory=dict,
        required=False,
    )


@app.configure
def configure(_name: str, config: dict[str, Any]) -> None:
    global _blob_config
    _blob_config = blob_config_from_mapping(config)


@app.operation(
    id="blob.put", method="POST", description="Upload a payload to Vercel Blob storage"
)
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
            VercelBlobPutRequest(
                pathname=pathname,
                body=input.body,
                body_base64=input.body_base64,
                access=access,
                content_type=input.content_type.strip(),
                add_random_suffix=input.add_random_suffix,
                overwrite=input.overwrite,
                cache_control_max_age=input.cache_control_max_age,
            ),
        )
    except (ValueError, binascii.Error) as err:
        return _bad_request(str(err))
    except (
        VercelBlobConfigurationError,
        VercelBlobAPIError,
        VercelBlobClientError,
    ) as err:
        return _blob_error(err)


@app.operation(
    id="blob.get", method="POST", description="Download a blob from Vercel Blob storage"
)
def blob_get(input: BlobGetInput) -> OperationResult:
    access = _normalize_access(input.access)
    if isinstance(access, gestalt.Response):
        return access
    url_or_path = _require_trimmed_text(input.url_or_path, "url_or_path")
    if isinstance(url_or_path, gestalt.Response):
        return url_or_path
    if input.timeout_seconds is not None and input.timeout_seconds <= 0:
        return _bad_request("timeout_seconds must be positive when provided")
    try:
        return get_blob(
            _blob_config,
            VercelBlobGetRequest(
                url_or_path=url_or_path,
                access=access,
                if_none_match=input.if_none_match.strip(),
                timeout_seconds=input.timeout_seconds,
                use_cache=input.use_cache,
            ),
        )
    except ValueError as err:
        return _bad_request(str(err))
    except (
        VercelBlobConfigurationError,
        VercelBlobAPIError,
        VercelBlobClientError,
    ) as err:
        return _blob_error(err)


@app.operation(
    id="blob.head", method="POST", description="Fetch metadata for a Vercel Blob object"
)
def blob_head(input: BlobHeadInput) -> OperationResult:
    url_or_path = _require_trimmed_text(input.url_or_path, "url_or_path")
    if isinstance(url_or_path, gestalt.Response):
        return url_or_path
    try:
        return head_blob(_blob_config, VercelBlobHeadRequest(url_or_path=url_or_path))
    except (
        VercelBlobConfigurationError,
        VercelBlobAPIError,
        VercelBlobClientError,
    ) as err:
        return _blob_error(err)


@app.operation(
    id="blob.list",
    method="POST",
    description="List Vercel Blob objects in the configured store",
)
def blob_list(input: BlobListInput) -> OperationResult:
    if input.limit is not None and input.limit <= 0:
        return _bad_request("limit must be positive when provided")
    try:
        return list_blobs(
            _blob_config,
            VercelBlobListRequest(
                limit=input.limit,
                prefix=input.prefix.strip(),
                cursor=input.cursor.strip(),
                mode=input.mode.strip(),
            ),
        )
    except (
        VercelBlobConfigurationError,
        VercelBlobAPIError,
        VercelBlobClientError,
    ) as err:
        return _blob_error(err)


@app.operation(
    id="blob.delete",
    method="POST",
    description="Delete one or more Vercel Blob objects",
)
def blob_delete(input: BlobDeleteInput) -> OperationResult:
    targets = [target.strip() for target in input.targets if target.strip()]
    if not targets:
        return _bad_request("targets must contain at least one non-empty value")
    try:
        return delete_blobs(
            _blob_config, VercelBlobDeleteRequest(targets=tuple(targets))
        )
    except (
        VercelBlobConfigurationError,
        VercelBlobAPIError,
        VercelBlobClientError,
    ) as err:
        return _blob_error(err)


@app.operation(
    id="blob.copy",
    method="POST",
    description="Copy a blob to a new pathname in the same Vercel Blob store",
)
def blob_copy(input: BlobCopyInput) -> OperationResult:
    access = _normalize_access(input.access)
    if isinstance(access, gestalt.Response):
        return access
    source_url_or_path = _require_trimmed_text(
        input.source_url_or_path, "source_url_or_path"
    )
    if isinstance(source_url_or_path, gestalt.Response):
        return source_url_or_path
    destination_path = _require_trimmed_text(input.destination_path, "destination_path")
    if isinstance(destination_path, gestalt.Response):
        return destination_path
    try:
        return copy_blob(
            _blob_config,
            VercelBlobCopyRequest(
                source_url_or_path=source_url_or_path,
                destination_path=destination_path,
                access=access,
                content_type=input.content_type.strip(),
                add_random_suffix=input.add_random_suffix,
                overwrite=input.overwrite,
                cache_control_max_age=input.cache_control_max_age,
            ),
        )
    except ValueError as err:
        return _bad_request(str(err))
    except (
        VercelBlobConfigurationError,
        VercelBlobAPIError,
        VercelBlobClientError,
    ) as err:
        return _blob_error(err)


@app.operation(
    id="teamMembers.invite",
    method="POST",
    description="Invite a user to a Vercel team using an email and role payload",
)
def team_members_invite(
    input: TeamMembersInviteInput, req: gestalt.Request
) -> OperationResult:
    team_id = _require_trimmed_text(input.teamId, "teamId")
    if isinstance(team_id, gestalt.Response):
        return team_id
    email = _require_trimmed_text(input.email, "email")
    if isinstance(email, gestalt.Response):
        return email
    token = req.token.strip()
    if not token:
        return gestalt.Response(
            status=HTTPStatus.UNAUTHORIZED, body={"error": "token is required"}
        )

    params = {"slug": input.slug.strip()} if input.slug.strip() else {}
    query = f"?{urllib.parse.urlencode(params)}" if params else ""
    url = (
        f"https://api.vercel.com/v2/teams/{urllib.parse.quote(team_id)}/members{query}"
    )
    body = json.dumps(
        [{"email": email, "role": input.role.strip() or "MEMBER"}]
    ).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return {"data": json.loads(raw) if raw else {}}
    except urllib.error.HTTPError as err:
        raw = err.read().decode("utf-8", errors="replace")
        err.close()
        message = _error_message_from_json(raw) or err.reason
        return gestalt.Response(status=err.code, body={"error": message})
    except urllib.error.URLError as err:
        return _server_error(str(err.reason))
    except json.JSONDecodeError as err:
        return _server_error(f"Vercel returned invalid JSON: {err}")


@app.operation(
    id="comments.list",
    method="GET",
    description="List Vercel Toolbar comments from the dashboard comments endpoint",
)
def comments_list(input: CommentsListInput, req: gestalt.Request) -> OperationResult:
    token = req.token.strip()
    if not token:
        return gestalt.Response(
            status=HTTPStatus.UNAUTHORIZED, body={"error": "token is required"}
        )
    if input.limit is not None and input.limit <= 0:
        return _bad_request("limit must be positive when provided")

    query = _comments_query(input)
    request_url = "https://vercel.com/api/dash/toolbar/comments"
    if query:
        request_url = f"{request_url}?{urllib.parse.urlencode(query, doseq=True)}"

    request = urllib.request.Request(
        request_url,
        method="GET",
        headers={
            "Authorization": f"Bearer {token}",
            "Cookie": f"authorization={token}",
            "Accept": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return {"data": json.loads(raw) if raw else {}}
    except urllib.error.HTTPError as err:
        raw = err.read().decode("utf-8", errors="replace")
        err.close()
        message = _error_message_from_json(raw) or err.reason
        return gestalt.Response(status=err.code, body={"error": message})
    except urllib.error.URLError as err:
        return _server_error(str(err.reason))
    except json.JSONDecodeError as err:
        return _server_error(f"Vercel returned invalid JSON: {err}")


def _comments_query(input: CommentsListInput) -> dict[str, Any]:
    query: dict[str, Any] = {
        key: value
        for key, value in input.extra_query.items()
        if value is not None and str(value).strip()
    }

    parsed_link = _parse_comment_link(input.url)
    for key, value in parsed_link.items():
        query.setdefault(key, value)

    for key, value in (
        ("teamId", input.teamId),
        ("slug", input.slug),
        ("projectId", input.projectId),
        ("deploymentId", input.deploymentId),
        ("threadId", input.threadId),
        ("commentId", input.commentId),
        ("cursor", input.cursor),
    ):
        trimmed = value.strip()
        if trimmed:
            query[key] = trimmed

    if input.limit is not None:
        query["limit"] = input.limit
    if input.resolved is not None:
        query["resolved"] = "true" if input.resolved else "false"
    return query


def _parse_comment_link(url: str) -> dict[str, str]:
    trimmed = url.strip()
    if not trimmed:
        return {}
    parsed = urllib.parse.urlsplit(trimmed)
    if not parsed.netloc and parsed.path.startswith("/"):
        parsed = urllib.parse.urlsplit(f"https://vercel.com{trimmed}")
    if parsed.netloc and parsed.netloc != "vercel.com":
        return {}

    query = {
        key: value[-1]
        for key, value in urllib.parse.parse_qs(parsed.query).items()
        if value
    }
    parts = [urllib.parse.unquote(part) for part in parsed.path.split("/") if part]
    if len(parts) >= 4 and parts[1] == "museum" and parts[2] == "c":
        query.setdefault("slug", parts[0])
        query.setdefault("commentId", parts[3])
        query.setdefault("threadId", parts[3])
    return query


def _normalize_access(value: str) -> VercelBlobAccess | ErrorResponse:
    access = value.strip().lower()
    try:
        return VercelBlobAccess(access)
    except ValueError:
        return _bad_request("access must be either private or public")


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
    if isinstance(err, VercelBlobClientError):
        return _server_error(str(err))
    return _server_error(str(err))


def _error_message_from_json(raw: str) -> str:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return raw.strip()
    if isinstance(parsed, dict):
        error = parsed.get("error")
        if isinstance(error, dict):
            message = error.get("message") or error.get("code")
            return str(message) if message else ""
        if isinstance(error, str):
            return error
        message = parsed.get("message")
        return str(message) if message else ""
    return ""


def _bad_request(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _server_error(message: str) -> ErrorResponse:
    return gestalt.Response(
        status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message}
    )
