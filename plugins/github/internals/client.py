from __future__ import annotations

import base64
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Protocol, TypeAlias

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey

from .config import (
    GitHubAppConfig,
    GitHubBotIdentity,
    get_cached_bot_identity,
    get_github_config,
    set_cached_bot_identity,
)
from .constants import GITHUB_API_VERSION
from .errors import GitHubAPIError, GitHubConfigError
from .helpers import int_field, nested_str, str_field

JsonObject: TypeAlias = dict[str, Any]
JsonPayload: TypeAlias = Mapping[str, Any]
GitHubPermissions: TypeAlias = Mapping[str, str]


class GitHubAPIClient(Protocol):
    """Runtime GitHub API contract used by bot operations."""

    def installation_token(
        self,
        installation_id: int,
        *,
        repositories: Sequence[str] | None = None,
        permissions: GitHubPermissions | None = None,
    ) -> str: ...

    def github_json(
        self,
        method: str,
        path: str,
        token: str | None,
        payload: JsonPayload | None = None,
    ) -> JsonObject: ...

    def github_json_value(
        self,
        method: str,
        path: str,
        token: str | None,
        payload: JsonPayload | None = None,
    ) -> Any: ...

    def repository_default_branch(self, token: str, owner: str, repo: str) -> str: ...

    def get_branch_ref(
        self, token: str, owner: str, repo: str, branch: str
    ) -> JsonObject | None: ...

    def require_branch_ref(
        self, token: str, owner: str, repo: str, branch: str, field_name: str
    ) -> JsonObject: ...

    def object_sha(self, ref: Mapping[str, Any], name: str) -> str: ...

    def bot_identity_or_none(self) -> GitHubBotIdentity | None: ...

    def commit_url(self, owner: str, repo: str, sha: str) -> str: ...


@dataclass(frozen=True, slots=True)
class GitHubAppClient:
    """Concrete GitHub App API client backed by this module's HTTP helpers."""

    def installation_token(
        self,
        installation_id: int,
        *,
        repositories: Sequence[str] | None = None,
        permissions: GitHubPermissions | None = None,
    ) -> str:
        return installation_token(
            installation_id,
            repositories=repositories,
            permissions=permissions,
        )

    def github_json(
        self,
        method: str,
        path: str,
        token: str | None,
        payload: JsonPayload | None = None,
    ) -> JsonObject:
        return github_json(method, path, token, payload)

    def github_json_value(
        self,
        method: str,
        path: str,
        token: str | None,
        payload: JsonPayload | None = None,
    ) -> Any:
        return github_json_value(method, path, token, payload)

    def repository_default_branch(self, token: str, owner: str, repo: str) -> str:
        return repository_default_branch(token, owner, repo)

    def get_branch_ref(
        self, token: str, owner: str, repo: str, branch: str
    ) -> JsonObject | None:
        return get_branch_ref(token, owner, repo, branch)

    def require_branch_ref(
        self, token: str, owner: str, repo: str, branch: str, field_name: str
    ) -> JsonObject:
        return require_branch_ref(token, owner, repo, branch, field_name)

    def object_sha(self, ref: Mapping[str, Any], name: str) -> str:
        return object_sha(ref, name)

    def bot_identity_or_none(self) -> GitHubBotIdentity | None:
        return bot_identity_or_none()

    def commit_url(self, owner: str, repo: str, sha: str) -> str:
        return commit_url(owner, repo, sha)


DEFAULT_GITHUB_CLIENT = GitHubAppClient()


def installation_token(
    installation_id: int,
    *,
    repositories: Sequence[str] | None = None,
    permissions: GitHubPermissions | None = None,
) -> str:
    if installation_id <= 0:
        raise ValueError("installation_id is required")
    payload: JsonObject = {}
    if repositories:
        payload["repositories"] = list(repositories)
    if permissions:
        payload["permissions"] = dict(permissions)

    response = github_json(
        "POST",
        f"/app/installations/{installation_id}/access_tokens",
        create_app_jwt(),
        payload,
    )
    token = str_field(response, "token")
    if not token:
        raise GitHubAPIError(502, "GitHub access token response did not include token")
    return token


def create_app_jwt() -> str:
    config = require_app_config()
    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT"}
    payload = {
        "iat": now - 60,
        "exp": now + 9 * 60,
        "iss": config.app_id,
    }
    signing_input = b".".join(
        [
            base64url_json(header),
            base64url_json(payload),
        ]
    )
    try:
        private_key = serialization.load_pem_private_key(
            private_key_bytes(config), password=None
        )
    except ValueError as err:
        raise GitHubConfigError(
            "GitHub App private key is not a valid PEM key"
        ) from err
    if not isinstance(private_key, RSAPrivateKey):
        raise GitHubConfigError("GitHub App private key must be an RSA private key")
    signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    return f"{signing_input.decode('ascii')}.{base64url(signature)}"


def require_app_config() -> GitHubAppConfig:
    config = get_github_config()
    if not config.app_id:
        raise GitHubConfigError("GitHub App appId is required")
    if not config.private_key and not config.private_key_path:
        raise GitHubConfigError(
            "GitHub App private key is required via appPrivateKey, "
            "appPrivateKeyPath, GITHUB_APP_PRIVATE_KEY, or "
            "GITHUB_APP_PRIVATE_KEY_PATH"
        )
    return config


def private_key_bytes(config: GitHubAppConfig) -> bytes:
    if config.private_key:
        return config.private_key.encode("utf-8")
    try:
        with open(config.private_key_path, "rb") as handle:
            return handle.read()
    except OSError as err:
        raise GitHubConfigError(f"reading GitHub App private key: {err}") from err


def github_json(
    method: str,
    path: str,
    token: str | None,
    payload: JsonPayload | None = None,
) -> JsonObject:
    decoded = github_json_value(method, path, token, payload)
    if not isinstance(decoded, dict):
        raise GitHubAPIError(502, "GitHub API returned a non-object JSON response")
    return decoded


def github_json_value(
    method: str,
    path: str,
    token: str | None,
    payload: JsonPayload | None = None,
) -> Any:
    data = None
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
        "User-Agent": "gestalt-github-plugin",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if payload is not None:
        data = json.dumps(dict(payload)).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(
        api_url(path), data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        err.close()
        raise GitHubAPIError(err.code, github_error_message(body, err.code)) from err
    except urllib.error.URLError as err:
        raise GitHubAPIError(502, f"GitHub API request failed: {err.reason}") from err

    if not body:
        return {}
    try:
        decoded = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as err:
        raise GitHubAPIError(502, f"GitHub API returned invalid JSON: {err}") from err
    return decoded


def api_url(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    return get_github_config().api_base_url.rstrip("/") + path


def repo_path(owner: str, repo: str, *parts: str, safe_last: str = "") -> str:
    path_parts = [
        "repos",
        urllib.parse.quote(owner, safe=""),
        urllib.parse.quote(repo, safe=""),
    ]
    for index, part in enumerate(parts):
        safe = safe_last if index == len(parts) - 1 else ""
        path_parts.append(urllib.parse.quote(str(part), safe=safe))
    return "/" + "/".join(path_parts)


def repository_default_branch(token: str, owner: str, repo: str) -> str:
    data = github_json("GET", repo_path(owner, repo), token)
    branch = str_field(data, "default_branch")
    if not branch:
        raise GitHubAPIError(
            502, "GitHub repository response did not include default_branch"
        )
    return branch


def get_branch_ref(token: str, owner: str, repo: str, branch: str) -> JsonObject | None:
    try:
        return github_json(
            "GET",
            repo_path(owner, repo, "git", "ref", "heads", branch, safe_last="/"),
            token,
        )
    except GitHubAPIError as err:
        if err.status == HTTPStatus.NOT_FOUND:
            return None
        raise


def require_branch_ref(
    token: str, owner: str, repo: str, branch: str, field_name: str
) -> JsonObject:
    ref = get_branch_ref(token, owner, repo, branch)
    if ref is None:
        raise ValueError(f"{field_name} branch {branch!r} was not found")
    return ref


def object_sha(ref: Mapping[str, Any], name: str) -> str:
    sha = nested_str(ref, "object", "sha")
    if not sha:
        raise GitHubAPIError(502, f"GitHub {name} response did not include object.sha")
    return sha


def bot_identity_or_none() -> GitHubBotIdentity | None:
    try:
        return bot_identity()
    except (GitHubAPIError, GitHubConfigError):
        return None


def bot_identity() -> GitHubBotIdentity:
    cached = get_cached_bot_identity()
    if cached is not None:
        return cached

    app = github_json("GET", "/app", create_app_jwt())
    slug = str_field(app, "slug")
    if not slug:
        raise GitHubAPIError(502, "GitHub app response did not include slug")

    login = f"{slug}[bot]"
    name = str_field(app, "name") or login
    user_id = ""
    email = ""
    try:
        user = github_json("GET", f"/users/{urllib.parse.quote(login, safe='')}", None)
        user_id_int = int_field(user, "id")
        if user_id_int > 0:
            user_id = str(user_id_int)
            email = f"{user_id}+{login}@users.noreply.github.com"
        login = str_field(user, "login") or login
    except GitHubAPIError:
        pass

    identity = GitHubBotIdentity(
        name=name,
        login=login,
        user_id=user_id,
        email=email,
    )
    if email:
        set_cached_bot_identity(identity)
    return identity


def commit_url(owner: str, repo: str, sha: str) -> str:
    return f"{get_github_config().web_base_url}/{owner}/{repo}/commit/{sha}"


def base64url_json(value: dict[str, Any]) -> bytes:
    return base64url(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).encode("ascii")


def base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def github_error_message(body: str, status: int) -> str:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return body or f"GitHub API error (status {status})"
    if isinstance(payload, dict):
        message = payload.get("message")
        if isinstance(message, str) and message:
            return message
    return f"GitHub API error (status {status})"
