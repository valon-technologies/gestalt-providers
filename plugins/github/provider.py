from __future__ import annotations

import base64
import binascii
import datetime as dt
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from google.protobuf import struct_pb2 as _struct_pb2
from gestalt.gen.v1 import agent_pb2 as _agent_pb2

agent_pb2: Any = _agent_pb2
struct_pb2: Any = _struct_pb2

OperationResult: TypeAlias = dict[str, Any] | gestalt.Response[dict[str, str]]

GITHUB_API_VERSION = "2022-11-28"
GITHUB_DEFAULT_API_BASE_URL = "https://api.github.com"
GITHUB_DEFAULT_WEB_BASE_URL = "https://github.com"
GITHUB_EVENT_OPERATION = "events.handle"
BOT_COMMIT_FILES_OPERATION = "bot.commitFiles"
BOT_OPEN_PULL_REQUEST_OPERATION = "bot.openPullRequest"
BOT_CREATE_PULL_REQUEST_OPERATION = "bot.createPullRequest"
GITHUB_INSTALLATION_SUBJECT_PREFIX = "workload:github_app_installation:"
GITHUB_REPOSITORY_SUBJECT_SEPARATOR = ":repo:"
DEFAULT_WEBHOOK_EVENTS = (
    "check_run",
    "check_suite",
    "issue_comment",
    "issues",
    "pull_request",
    "pull_request_review",
    "pull_request_review_comment",
    "workflow_run",
)
DEFAULT_AGENT_SYSTEM_PROMPT = """
You are a GitHub App bot running inside Gestalt.
You are responding to a verified GitHub App webhook, not a user OAuth connection.
Use only the explicit GitHub bot tools provided to inspect or change GitHub state.
When you create commits or pull requests, use the installation_id and repository
details from the event unless the user instruction says otherwise.
Return a concise final summary of what you did.
""".strip()
MAX_AGENT_PAYLOAD_CHARS = 20000

plugin = gestalt.Plugin(
    "github",
    securitySchemes={
        "github_app": {
            "type": "hmac",
            "secret": {"env": "GITHUB_WEBHOOK_SECRET"},
            "signatureHeader": "X-Hub-Signature-256",
            "signaturePrefix": "sha256=",
            "payloadTemplate": "{raw_body}",
        }
    },
    http={
        "event": {
            "path": "/event",
            "method": "POST",
            "credentialMode": "none",
            "security": "github_app",
            "target": GITHUB_EVENT_OPERATION,
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {},
                },
            },
        },
    },
)


@dataclass(slots=True)
class GitHubAppConfig:
    app_id: str = ""
    private_key: str = ""
    private_key_path: str = ""
    api_base_url: str = GITHUB_DEFAULT_API_BASE_URL
    web_base_url: str = GITHUB_DEFAULT_WEB_BASE_URL
    bot_name: str = "GitHub Bot"
    bot_email: str = ""
    bot_login: str = ""
    bot_user_id: str = ""
    webhook_events: tuple[str, ...] = DEFAULT_WEBHOOK_EVENTS
    ignore_bot_sender: bool = True
    agent_provider: str = ""
    agent_model: str = ""
    agent_system_prompt: str = ""
    agent_provider_options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class GitHubFileChange:
    path: str
    content: str
    content_base64: str
    delete: bool
    executable: bool


@dataclass(frozen=True, slots=True)
class CommitResult:
    owner: str
    repo: str
    branch: str
    base_branch: str
    installation_id: int
    commit_sha: str
    commit_url: str
    tree_sha: str
    branch_created: bool
    files_changed: int


class FileChangeInput(gestalt.Model):
    path: str = gestalt.field(description="Repository-relative file path")
    content: str = gestalt.field(
        description="UTF-8 text content for the file", default="", required=False
    )
    content_base64: str = gestalt.field(
        description="Base64-encoded file content for binary files",
        default="",
        required=False,
    )
    delete: bool = gestalt.field(
        description="Delete this file instead of writing content",
        default=False,
        required=False,
    )
    executable: bool = gestalt.field(
        description="Write the file with executable mode",
        default=False,
        required=False,
    )


class CoAuthorInput(gestalt.Model):
    name: str = gestalt.field(description="Co-author display name")
    email: str = gestalt.field(description="Co-author email address")


class CommitFilesInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    message: str = gestalt.field(description="Commit message")
    files: list[FileChangeInput] = gestalt.field(
        description="Files to create, update, or delete. Each item accepts path, content, content_base64, delete, and executable."
    )
    branch: str = gestalt.field(
        description="Branch to create or update. Defaults to a generated branch.",
        default="",
        required=False,
    )
    base_branch: str = gestalt.field(
        description="Base branch for a newly-created branch. Defaults to the repository default branch.",
        default="",
        required=False,
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook workload subject.",
        default=0,
        required=False,
    )
    coauthors: list[CoAuthorInput] = gestalt.field(
        description="Co-authors to append as commit trailers. Each item accepts name and email.",
        default_factory=list,
        required=False,
    )
    include_bot_coauthor: bool = gestalt.field(
        description="Append the configured GitHub App bot as a co-author when botEmail is configured",
        default=True,
        required=False,
    )
    author_name: str = gestalt.field(
        description="Optional Git commit author name", default="", required=False
    )
    author_email: str = gestalt.field(
        description="Optional Git commit author email", default="", required=False
    )
    committer_name: str = gestalt.field(
        description="Optional Git commit committer name", default="", required=False
    )
    committer_email: str = gestalt.field(
        description="Optional Git commit committer email", default="", required=False
    )
    force: bool = gestalt.field(
        description="Force-update the branch ref", default=False, required=False
    )
    allow_base_update: bool = gestalt.field(
        description="Allow updating the base branch directly",
        default=False,
        required=False,
    )


class OpenPullRequestInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    title: str = gestalt.field(description="Pull request title")
    head: str = gestalt.field(description="Head branch name")
    base: str = gestalt.field(description="Base branch name")
    body: str = gestalt.field(
        description="Pull request body", default="", required=False
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook workload subject.",
        default=0,
        required=False,
    )
    head_owner: str = gestalt.field(
        description="Optional owner for cross-repository pull requests",
        default="",
        required=False,
    )
    draft: bool = gestalt.field(
        description="Create the pull request as a draft", default=False, required=False
    )
    maintainer_can_modify: bool = gestalt.field(
        description="Allow maintainers to modify the pull request branch",
        default=True,
        required=False,
    )


class CreatePullRequestInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    title: str = gestalt.field(description="Pull request title")
    message: str = gestalt.field(description="Commit message")
    files: list[FileChangeInput] = gestalt.field(
        description="Files to create, update, or delete. Each item accepts path, content, content_base64, delete, and executable."
    )
    body: str = gestalt.field(
        description="Pull request body", default="", required=False
    )
    branch: str = gestalt.field(
        description="Head branch to create or update. Defaults to a generated branch.",
        default="",
        required=False,
    )
    base: str = gestalt.field(
        description="Base branch. Defaults to the repository default branch.",
        default="",
        required=False,
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook workload subject.",
        default=0,
        required=False,
    )
    coauthors: list[CoAuthorInput] = gestalt.field(
        description="Co-authors to append as commit trailers. Each item accepts name and email.",
        default_factory=list,
        required=False,
    )
    include_bot_coauthor: bool = gestalt.field(
        description="Append the configured GitHub App bot as a co-author when botEmail is configured",
        default=True,
        required=False,
    )
    author_name: str = gestalt.field(
        description="Optional Git commit author name", default="", required=False
    )
    author_email: str = gestalt.field(
        description="Optional Git commit author email", default="", required=False
    )
    committer_name: str = gestalt.field(
        description="Optional Git commit committer name", default="", required=False
    )
    committer_email: str = gestalt.field(
        description="Optional Git commit committer email", default="", required=False
    )
    force: bool = gestalt.field(
        description="Force-update the head branch ref", default=False, required=False
    )
    draft: bool = gestalt.field(
        description="Create the pull request as a draft", default=False, required=False
    )
    maintainer_can_modify: bool = gestalt.field(
        description="Allow maintainers to modify the pull request branch",
        default=True,
        required=False,
    )


class GitHubConfigError(RuntimeError):
    pass


class GitHubAPIError(RuntimeError):
    def __init__(self, status: int, message: str) -> None:
        self.status = status
        self.message = message
        super().__init__(message)


class GitHubAuthorizationError(RuntimeError):
    pass


_github_config = GitHubAppConfig()


@plugin.configure
def configure(_name: str, config: dict[str, Any]) -> None:
    global _github_config

    app_id = _config_string(config, "appId", "app_id") or os.environ.get(
        "GITHUB_APP_ID", ""
    ).strip()
    private_key = _config_string(
        config, "appPrivateKey", "privateKey", "app_private_key", "private_key"
    )
    private_key_env = _config_string(
        config,
        "appPrivateKeyEnv",
        "privateKeyEnv",
        "app_private_key_env",
        "private_key_env",
    )
    if not private_key:
        env_name = private_key_env or "GITHUB_APP_PRIVATE_KEY"
        private_key = os.environ.get(env_name, "").strip()

    private_key_path = _config_string(
        config, "appPrivateKeyPath", "privateKeyPath", "app_private_key_path"
    )
    if not private_key_path:
        private_key_path = os.environ.get("GITHUB_APP_PRIVATE_KEY_PATH", "").strip()

    webhook_events = _config_string_list(config, "webhookEvents", "webhook_events")
    _github_config = GitHubAppConfig(
        app_id=app_id,
        private_key=_normalize_private_key(private_key),
        private_key_path=private_key_path,
        api_base_url=(
            _config_string(config, "apiBaseUrl", "api_base_url")
            or GITHUB_DEFAULT_API_BASE_URL
        ).rstrip("/"),
        web_base_url=(
            _config_string(config, "webBaseUrl", "web_base_url")
            or GITHUB_DEFAULT_WEB_BASE_URL
        ).rstrip("/"),
        bot_name=_config_string(config, "botName", "bot_name") or "GitHub Bot",
        bot_email=_config_string(config, "botEmail", "bot_email"),
        bot_login=_config_string(config, "botLogin", "bot_login"),
        bot_user_id=_config_string(config, "botUserId", "bot_user_id"),
        webhook_events=tuple(
            event.lower()
            for event in (
                webhook_events
                if webhook_events is not None
                else list(DEFAULT_WEBHOOK_EVENTS)
            )
        ),
        ignore_bot_sender=_config_bool(
            config, "ignoreBotSender", "ignore_bot_sender", default=True
        ),
        agent_provider=_config_string(config, "agentProvider", "agent_provider"),
        agent_model=_config_string(config, "agentModel", "agent_model"),
        agent_system_prompt=_config_string(
            config, "agentSystemPrompt", "agent_system_prompt"
        ),
        agent_provider_options=_config_dict(
            config, "agentProviderOptions", "agent_provider_options"
        ),
    )


@plugin.http_subject
def resolve_http_subject(request: gestalt.HTTPSubjectRequest) -> gestalt.Subject | None:
    installation_id = _installation_id_from_payload(request.params)
    if installation_id <= 0:
        return None

    repo = _repository_full_name(request.params)
    subject_id = f"{GITHUB_INSTALLATION_SUBJECT_PREFIX}{installation_id}"
    display_name = f"GitHub App installation {installation_id}"
    if repo:
        subject_id = f"{subject_id}{GITHUB_REPOSITORY_SUBJECT_SEPARATOR}{repo}"
        display_name = f"{display_name} ({repo})"
    return gestalt.Subject(
        id=subject_id,
        kind="workload",
        display_name=display_name,
        auth_source="github_app_webhook",
    )


@plugin.operation(
    id=GITHUB_EVENT_OPERATION,
    method="POST",
    description="Handle GitHub App webhook callbacks and delegate repository events to a Gestalt agent",
    visible=False,
)
def github_events_handle(input: dict[str, Any], req: gestalt.Request) -> OperationResult:
    ignored_reason = _webhook_ignored_reason(input)
    if ignored_reason:
        return {"ok": True, "ignored": ignored_reason}

    installation_id = _installation_id_from_payload(input)
    run_request = _build_agent_run_request(input, installation_id)
    try:
        with req.agent_manager() as agent_manager:
            managed = agent_manager.run(run_request)
    except Exception as err:
        return _server_error(f"failed to start agent run: {err}")

    run = managed.run
    return {
        "ok": True,
        "agent_run_id": run.id,
        "agent_provider": managed.provider_name,
        "status": agent_pb2.AgentRunStatus.Name(run.status) if run.status else "",
    }


@plugin.operation(
    id=BOT_COMMIT_FILES_OPERATION,
    method="POST",
    description="Create a Git commit on a branch using a GitHub App installation token",
)
def bot_commit_files(input: CommitFilesInput, req: gestalt.Request) -> OperationResult:
    try:
        commit = _commit_files_from_input(
            input, req=req, pull_request_permissions=False
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    return {"data": {"commit": _commit_result_dict(commit)}}


@plugin.operation(
    id=BOT_OPEN_PULL_REQUEST_OPERATION,
    method="POST",
    description="Open a pull request using a GitHub App installation token",
)
def bot_open_pull_request(
    input: OpenPullRequestInput, req: gestalt.Request
) -> OperationResult:
    try:
        owner = _require_slug(input.owner, "owner")
        repo = _require_slug(input.repo, "repo")
        title = _require_text(input.title, "title")
        head = _require_branch_name(input.head, "head")
        base = _require_branch_name(input.base, "base")
        head_owner = _optional_slug(input.head_owner, "head_owner")
        installation_id = _scoped_installation_id(
            req, owner=owner, repo=repo, explicit=input.installation_id
        )
        token = _installation_token(
            installation_id,
            repositories=[repo],
            permissions={"pull_requests": "write"},
        )
        pull = _create_pull_request(
            token,
            owner=owner,
            repo=repo,
            title=title,
            head=head,
            base=base,
            body=input.body,
            head_owner=head_owner,
            draft=input.draft,
            maintainer_can_modify=input.maintainer_can_modify,
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    return {"data": {"pull_request": _pull_request_summary(pull)}}


@plugin.operation(
    id=BOT_CREATE_PULL_REQUEST_OPERATION,
    method="POST",
    description="Commit file changes to a branch and open a pull request using a GitHub App installation token",
)
def bot_create_pull_request(
    input: CreatePullRequestInput, req: gestalt.Request
) -> OperationResult:
    try:
        commit_input = CommitFilesInput(
            owner=input.owner,
            repo=input.repo,
            message=input.message,
            files=input.files,
            branch=input.branch,
            base_branch=input.base,
            installation_id=input.installation_id,
            coauthors=input.coauthors,
            include_bot_coauthor=input.include_bot_coauthor,
            author_name=input.author_name,
            author_email=input.author_email,
            committer_name=input.committer_name,
            committer_email=input.committer_email,
            force=input.force,
            allow_base_update=False,
        )
        commit = _commit_files_from_input(
            commit_input, req=req, pull_request_permissions=True
        )
        token = _installation_token(
            commit.installation_id,
            repositories=[commit.repo],
            permissions={"pull_requests": "write"},
        )
        pull = _create_pull_request(
            token,
            owner=commit.owner,
            repo=commit.repo,
            title=_require_text(input.title, "title"),
            head=commit.branch,
            base=commit.base_branch,
            body=input.body,
            head_owner="",
            draft=input.draft,
            maintainer_can_modify=input.maintainer_can_modify,
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    return {
        "data": {
            "commit": _commit_result_dict(commit),
            "pull_request": _pull_request_summary(pull),
        }
    }


def _commit_files_from_input(
    input: CommitFilesInput, *, req: gestalt.Request, pull_request_permissions: bool
) -> CommitResult:
    owner = _require_slug(input.owner, "owner")
    repo = _require_slug(input.repo, "repo")
    message = _commit_message_with_coauthors(
        _require_text(input.message, "message"),
        coauthors=input.coauthors,
        include_bot=input.include_bot_coauthor,
    )
    files = _normalize_file_changes(input.files)
    if not files:
        raise ValueError("files must contain at least one change")

    base_branch = (
        _require_branch_name(input.base_branch, "base_branch")
        if input.base_branch.strip()
        else ""
    )
    branch = (
        _require_branch_name(input.branch, "branch")
        if input.branch.strip()
        else _generated_branch_name(input.message)
    )

    installation_id = _scoped_installation_id(
        req, owner=owner, repo=repo, explicit=input.installation_id
    )
    permissions = {"contents": "write"}
    if pull_request_permissions:
        permissions["pull_requests"] = "write"
    token = _installation_token(
        installation_id, repositories=[repo], permissions=permissions
    )

    if not base_branch:
        base_branch = _repository_default_branch(token, owner, repo)
    if branch == base_branch and not input.allow_base_update:
        raise ValueError(
            "branch must differ from base_branch unless allow_base_update is true"
        )

    branch_ref = _get_branch_ref(token, owner, repo, branch)
    branch_created = branch_ref is None
    parent_ref = branch_ref or _require_branch_ref(
        token, owner, repo, base_branch, "base_branch"
    )
    parent_sha = _object_sha(parent_ref, "parent ref")
    parent_commit = _github_json(
        "GET",
        _repo_path(owner, repo, "git", "commits", parent_sha),
        token,
    )
    base_tree_sha = _nested_str(parent_commit, "tree", "sha")
    if not base_tree_sha:
        raise GitHubAPIError(502, "GitHub commit response did not include tree.sha")

    tree_entries = [
        _tree_entry_for_file(token, owner=owner, repo=repo, change=change)
        for change in files
    ]
    tree = _github_json(
        "POST",
        _repo_path(owner, repo, "git", "trees"),
        token,
        {
            "base_tree": base_tree_sha,
            "tree": tree_entries,
        },
    )
    tree_sha = _str_field(tree, "sha")
    if not tree_sha:
        raise GitHubAPIError(502, "GitHub tree response did not include sha")

    commit_payload: dict[str, Any] = {
        "message": message,
        "tree": tree_sha,
        "parents": [parent_sha],
    }
    author = _git_identity(input.author_name, input.author_email)
    if author:
        commit_payload["author"] = author
    committer = _git_identity(input.committer_name, input.committer_email)
    if committer:
        commit_payload["committer"] = committer

    commit = _github_json(
        "POST",
        _repo_path(owner, repo, "git", "commits"),
        token,
        commit_payload,
    )
    commit_sha = _str_field(commit, "sha")
    if not commit_sha:
        raise GitHubAPIError(502, "GitHub commit response did not include sha")

    if branch_created:
        _github_json(
            "POST",
            _repo_path(owner, repo, "git", "refs"),
            token,
            {"ref": f"refs/heads/{branch}", "sha": commit_sha},
        )
    else:
        _github_json(
            "PATCH",
            _repo_path(owner, repo, "git", "refs", "heads", branch, safe_last="/"),
            token,
            {"sha": commit_sha, "force": bool(input.force)},
        )

    return CommitResult(
        owner=owner,
        repo=repo,
        branch=branch,
        base_branch=base_branch,
        installation_id=installation_id,
        commit_sha=commit_sha,
        commit_url=_commit_url(owner, repo, commit_sha),
        tree_sha=tree_sha,
        branch_created=branch_created,
        files_changed=len(files),
    )


def _create_pull_request(
    token: str,
    *,
    owner: str,
    repo: str,
    title: str,
    head: str,
    base: str,
    body: str,
    head_owner: str,
    draft: bool,
    maintainer_can_modify: bool,
) -> dict[str, Any]:
    normalized_head = head.strip()
    if head_owner.strip():
        normalized_head = f"{head_owner.strip()}:{normalized_head}"
    payload = {
        "title": title,
        "head": normalized_head,
        "base": base,
        "body": body,
        "draft": bool(draft),
        "maintainer_can_modify": bool(maintainer_can_modify),
    }
    return _github_json(
        "POST", _repo_path(owner, repo, "pulls"), token, payload
    )


def _tree_entry_for_file(
    token: str, *, owner: str, repo: str, change: GitHubFileChange
) -> dict[str, Any]:
    mode = "100755" if change.executable else "100644"
    if change.delete:
        return {
            "path": change.path,
            "mode": mode,
            "type": "blob",
            "sha": None,
        }
    if change.content and change.content_base64:
        raise ValueError(f"{change.path}: content and content_base64 are mutually exclusive")
    if change.content_base64:
        blob = _github_json(
            "POST",
            _repo_path(owner, repo, "git", "blobs"),
            token,
            {
                "content": change.content_base64,
                "encoding": "base64",
            },
        )
        blob_sha = _str_field(blob, "sha")
        if not blob_sha:
            raise GitHubAPIError(502, "GitHub blob response did not include sha")
        return {
            "path": change.path,
            "mode": mode,
            "type": "blob",
            "sha": blob_sha,
        }
    return {
        "path": change.path,
        "mode": mode,
        "type": "blob",
        "content": change.content,
    }


def _normalize_file_changes(files: list[FileChangeInput]) -> list[GitHubFileChange]:
    normalized: list[GitHubFileChange] = []
    seen: set[str] = set()
    for item in files:
        path = item.path.strip().lstrip("/")
        if not path:
            raise ValueError("file path is required")
        if path in {".", ".."} or "/../" in f"/{path}/":
            raise ValueError(f"{path}: path must not contain '..'")
        if path in seen:
            raise ValueError(f"{path}: duplicate file path")
        content_base64 = item.content_base64.strip()
        if item.delete and (item.content or content_base64):
            raise ValueError(f"{path}: delete cannot include content")
        if item.content and content_base64:
            raise ValueError(f"{path}: content and content_base64 are mutually exclusive")
        if content_base64:
            try:
                base64.b64decode(content_base64, validate=True)
            except (binascii.Error, ValueError) as err:
                raise ValueError(f"{path}: content_base64 must be valid base64") from err
        seen.add(path)
        normalized.append(
            GitHubFileChange(
                path=path,
                content=item.content,
                content_base64=content_base64,
                delete=bool(item.delete),
                executable=bool(item.executable),
            )
        )
    return normalized


def _scoped_installation_id(
    req: gestalt.Request, *, owner: str, repo: str, explicit: int
) -> int:
    subject_installation_id, subject_repo = _github_scope_from_subject(req.subject)
    if subject_installation_id <= 0:
        raise GitHubAuthorizationError(
            "GitHub bot operations require a GitHub App installation workload subject"
        )
    if explicit > 0 and explicit != subject_installation_id:
        raise GitHubAuthorizationError(
            "installation_id must match the caller's GitHub App installation subject"
        )
    requested_repo = f"{owner}/{repo}".lower()
    if not subject_repo:
        raise GitHubAuthorizationError(
            "GitHub bot operations require a repository-scoped webhook workload subject"
        )
    if subject_repo.lower() != requested_repo:
        raise GitHubAuthorizationError(
            "repository must match the caller's GitHub App webhook subject"
        )
    return explicit or subject_installation_id


def _github_scope_from_subject(subject: gestalt.Subject) -> tuple[int, str]:
    if subject.kind != "workload" or subject.auth_source != "github_app_webhook":
        return 0, ""
    if not subject.id.startswith(GITHUB_INSTALLATION_SUBJECT_PREFIX):
        return 0, ""
    value = subject.id.removeprefix(GITHUB_INSTALLATION_SUBJECT_PREFIX)
    installation_text, separator, repo = value.partition(
        GITHUB_REPOSITORY_SUBJECT_SEPARATOR
    )
    if installation_text.isdigit():
        return int(installation_text), repo if separator else ""
    return 0, ""


def _installation_token(
    installation_id: int,
    *,
    repositories: list[str] | None = None,
    permissions: dict[str, str] | None = None,
) -> str:
    if installation_id <= 0:
        raise ValueError("installation_id is required")
    payload: dict[str, Any] = {}
    if repositories:
        payload["repositories"] = repositories
    if permissions:
        payload["permissions"] = permissions

    response = _github_json(
        "POST",
        f"/app/installations/{installation_id}/access_tokens",
        _create_app_jwt(),
        payload,
    )
    token = _str_field(response, "token")
    if not token:
        raise GitHubAPIError(502, "GitHub access token response did not include token")
    return token


def _create_app_jwt() -> str:
    config = _require_app_config()
    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT"}
    payload = {
        "iat": now - 60,
        "exp": now + 9 * 60,
        "iss": config.app_id,
    }
    signing_input = b".".join(
        [
            _base64url_json(header),
            _base64url_json(payload),
        ]
    )
    private_key = serialization.load_pem_private_key(
        _private_key_bytes(config), password=None
    )
    if not isinstance(private_key, RSAPrivateKey):
        raise GitHubConfigError("GitHub App private key must be an RSA private key")
    signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    return f"{signing_input.decode('ascii')}.{_base64url(signature)}"


def _require_app_config() -> GitHubAppConfig:
    if not _github_config.app_id:
        raise GitHubConfigError("GitHub App appId is required")
    if not _github_config.private_key and not _github_config.private_key_path:
        raise GitHubConfigError(
            "GitHub App private key is required via appPrivateKey, appPrivateKeyPath, GITHUB_APP_PRIVATE_KEY, or GITHUB_APP_PRIVATE_KEY_PATH"
        )
    return _github_config


def _private_key_bytes(config: GitHubAppConfig) -> bytes:
    if config.private_key:
        return config.private_key.encode("utf-8")
    try:
        with open(config.private_key_path, "rb") as handle:
            return handle.read()
    except OSError as err:
        raise GitHubConfigError(f"reading GitHub App private key: {err}") from err


def _github_json(
    method: str,
    path: str,
    token: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = None
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
        "User-Agent": "gestalt-github-plugin",
    }
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(
        _api_url(path), data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        err.close()
        raise GitHubAPIError(err.code, _github_error_message(body, err.code)) from err
    except urllib.error.URLError as err:
        raise GitHubAPIError(502, f"GitHub API request failed: {err.reason}") from err

    if not body:
        return {}
    try:
        decoded = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as err:
        raise GitHubAPIError(502, f"GitHub API returned invalid JSON: {err}") from err
    if not isinstance(decoded, dict):
        raise GitHubAPIError(502, "GitHub API returned a non-object JSON response")
    return decoded


def _api_url(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    return _github_config.api_base_url.rstrip("/") + path


def _repo_path(owner: str, repo: str, *parts: str, safe_last: str = "") -> str:
    path_parts = [
        "repos",
        urllib.parse.quote(owner, safe=""),
        urllib.parse.quote(repo, safe=""),
    ]
    for index, part in enumerate(parts):
        safe = safe_last if index == len(parts) - 1 else ""
        path_parts.append(urllib.parse.quote(str(part), safe=safe))
    return "/" + "/".join(path_parts)


def _repository_default_branch(token: str, owner: str, repo: str) -> str:
    data = _github_json("GET", _repo_path(owner, repo), token)
    branch = _str_field(data, "default_branch")
    if not branch:
        raise GitHubAPIError(502, "GitHub repository response did not include default_branch")
    return branch


def _get_branch_ref(
    token: str, owner: str, repo: str, branch: str
) -> dict[str, Any] | None:
    try:
        return _github_json(
            "GET", _repo_path(owner, repo, "git", "ref", "heads", branch, safe_last="/"), token
        )
    except GitHubAPIError as err:
        if err.status == HTTPStatus.NOT_FOUND:
            return None
        raise


def _require_branch_ref(
    token: str, owner: str, repo: str, branch: str, field_name: str
) -> dict[str, Any]:
    ref = _get_branch_ref(token, owner, repo, branch)
    if ref is None:
        raise ValueError(f"{field_name} branch {branch!r} was not found")
    return ref


def _object_sha(ref: dict[str, Any], name: str) -> str:
    sha = _nested_str(ref, "object", "sha")
    if not sha:
        raise GitHubAPIError(502, f"GitHub {name} response did not include object.sha")
    return sha


def _commit_message_with_coauthors(
    message: str, *, coauthors: list[CoAuthorInput], include_bot: bool
) -> str:
    trailers: list[str] = []
    seen: set[tuple[str, str]] = set()
    for coauthor in coauthors:
        name = coauthor.name.strip()
        email = coauthor.email.strip()
        if not name or not email:
            raise ValueError("coauthor name and email are required")
        key = (name, email)
        if key not in seen:
            seen.add(key)
            trailers.append(f"Co-authored-by: {name} <{email}>")

    bot = _bot_coauthor()
    if include_bot and bot is not None and bot not in seen:
        seen.add(bot)
        trailers.append(f"Co-authored-by: {bot[0]} <{bot[1]}>")

    if not trailers:
        return message
    return message.rstrip() + "\n\n" + "\n".join(trailers)


def _bot_coauthor() -> tuple[str, str] | None:
    name = _github_config.bot_name.strip() or _github_config.bot_login.strip()
    email = _github_config.bot_email.strip()
    if not email and _github_config.bot_login and _github_config.bot_user_id:
        email = (
            f"{_github_config.bot_user_id.strip()}+"
            f"{_github_config.bot_login.strip()}@users.noreply.github.com"
        )
    if not name or not email:
        return None
    return name, email


def _git_identity(name: str, email: str) -> dict[str, str] | None:
    name = name.strip()
    email = email.strip()
    if not name and not email:
        return None
    if not name or not email:
        raise ValueError("git author/committer name and email must be provided together")
    return {"name": name, "email": email}


def _build_agent_run_request(payload: dict[str, Any], installation_id: int) -> Any:
    summary = _event_summary(payload, installation_id)
    metadata = _dict_to_struct({"github": summary})
    request = agent_pb2.AgentManagerRunRequest(
        provider_name=_github_config.agent_provider,
        model=_github_config.agent_model,
        messages=[
            agent_pb2.AgentMessage(role="system", text=_agent_system_prompt()),
            agent_pb2.AgentMessage(role="user", text=_agent_user_prompt(payload, summary)),
        ],
        tool_refs=[
            agent_pb2.AgentToolRef(
                plugin_name="github",
                operation=BOT_COMMIT_FILES_OPERATION,
            ),
            agent_pb2.AgentToolRef(
                plugin_name="github",
                operation=BOT_OPEN_PULL_REQUEST_OPERATION,
            ),
            agent_pb2.AgentToolRef(
                plugin_name="github",
                operation=BOT_CREATE_PULL_REQUEST_OPERATION,
            ),
        ],
        tool_source=agent_pb2.AGENT_TOOL_SOURCE_MODE_EXPLICIT,
        session_ref=_agent_session_ref(summary),
        idempotency_key=_agent_idempotency_key(payload, summary),
    )
    request.metadata.CopyFrom(metadata)
    if _github_config.agent_provider_options:
        request.provider_options.CopyFrom(_dict_to_struct(_github_config.agent_provider_options))
    return request


def _event_summary(payload: dict[str, Any], installation_id: int) -> dict[str, Any]:
    repository = _map_field(payload, "repository")
    sender = _map_field(payload, "sender")
    pull_request = _map_field(payload, "pull_request")
    issue = _map_field(payload, "issue")
    summary: dict[str, Any] = {
        "installation_id": installation_id,
        "event_type": _github_event_type(payload),
        "action": _str_field(payload, "action"),
        "repository": _repository_full_name(payload),
        "repository_owner": _nested_str(repository, "owner", "login"),
        "repository_name": _str_field(repository, "name"),
        "sender": _str_field(sender, "login"),
    }
    number = _int_field(pull_request, "number") or _int_field(issue, "number")
    if number > 0:
        summary["number"] = number
    if _str_field(pull_request, "head", "ref"):
        summary["head_ref"] = _nested_str(pull_request, "head", "ref")
    if _str_field(pull_request, "base", "ref"):
        summary["base_ref"] = _nested_str(pull_request, "base", "ref")
    return {key: value for key, value in summary.items() if value not in ("", 0)}


def _agent_system_prompt() -> str:
    if not _github_config.agent_system_prompt:
        return DEFAULT_AGENT_SYSTEM_PROMPT
    return (
        DEFAULT_AGENT_SYSTEM_PROMPT
        + "\n\n"
        + _github_config.agent_system_prompt.strip()
    )


def _agent_user_prompt(payload: dict[str, Any], summary: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, sort_keys=True, indent=2)
    if len(payload_json) > MAX_AGENT_PAYLOAD_CHARS:
        payload_json = payload_json[:MAX_AGENT_PAYLOAD_CHARS] + "\n...<truncated>"
    lines = [
        "GitHub App webhook:",
        f"installation_id: {summary.get('installation_id', '')}",
        f"event_type: {summary.get('event_type', '')}",
        f"repository: {summary.get('repository', '')}",
        f"action: {summary.get('action', '')}",
        f"sender: {summary.get('sender', '')}",
    ]
    if "number" in summary:
        lines.append(f"number: {summary['number']}")
    lines.extend(["", "Payload:", payload_json])
    return "\n".join(lines)


def _agent_session_ref(summary: dict[str, Any]) -> str:
    installation_id = summary.get("installation_id", "")
    repo = summary.get("repository", "")
    number = summary.get("number", "")
    if repo and number:
        return f"github:{installation_id}:{repo}:{number}"
    if repo:
        return f"github:{installation_id}:{repo}"
    return f"github:{installation_id}"


def _agent_idempotency_key(payload: dict[str, Any], summary: dict[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    repo = summary.get("repository", "")
    event_type = summary.get("event_type", "")
    action = summary.get("action", "")
    return f"github:event:{repo}:{event_type}:{action}:{digest}"


def _installation_id_from_payload(payload: dict[str, Any]) -> int:
    installation = _map_field(payload, "installation")
    return _int_field(installation, "id")


def _repository_full_name(payload: dict[str, Any]) -> str:
    repository = _map_field(payload, "repository")
    full_name = _str_field(repository, "full_name")
    if full_name:
        return full_name
    owner = _nested_str(repository, "owner", "login")
    name = _str_field(repository, "name")
    if owner and name:
        return f"{owner}/{name}"
    return ""


def _is_ping_event(payload: dict[str, Any]) -> bool:
    return bool(payload.get("zen")) and isinstance(payload.get("hook"), dict)


def _webhook_ignored_reason(payload: dict[str, Any]) -> str:
    if _is_ping_event(payload):
        return "ping"
    if _installation_id_from_payload(payload) <= 0:
        return "missing_installation"

    event_type = _github_event_type(payload)
    if not event_type:
        return "unknown_event_type"
    if event_type not in _github_config.webhook_events:
        return f"unsupported_event_type:{event_type}"
    if _github_config.ignore_bot_sender and _is_configured_bot_sender(payload):
        return "configured_bot_sender"
    return ""


def _github_event_type(payload: dict[str, Any]) -> str:
    if "check_run" in payload:
        return "check_run"
    if "check_suite" in payload:
        return "check_suite"
    if "workflow_run" in payload:
        return "workflow_run"
    if "pull_request" in payload and "review" in payload:
        return "pull_request_review"
    if "pull_request" in payload and "comment" in payload:
        return "pull_request_review_comment"
    if "pull_request" in payload:
        return "pull_request"
    if "issue" in payload and "comment" in payload:
        return "issue_comment"
    if "issue" in payload:
        return "issues"
    if "ref" in payload and ("commits" in payload or "head_commit" in payload):
        return "push"
    if "repository" in payload and _str_field(payload, "action"):
        return _str_field(payload, "action")
    return ""


def _is_configured_bot_sender(payload: dict[str, Any]) -> bool:
    sender_login = _nested_str(_map_field(payload, "sender"), "login").lower()
    return bool(sender_login and sender_login in _configured_bot_logins())


def _configured_bot_logins() -> set[str]:
    logins: set[str] = set()
    if _github_config.bot_login.strip():
        logins.add(_github_config.bot_login.strip().lower())
    email = _github_config.bot_email.strip()
    match = re.match(r"^\d+\+([^@]+)@users\.noreply\.github\.com$", email)
    if match:
        logins.add(match.group(1).lower())
    return logins


def _commit_result_dict(commit: CommitResult) -> dict[str, Any]:
    return {
        "owner": commit.owner,
        "repo": commit.repo,
        "branch": commit.branch,
        "base_branch": commit.base_branch,
        "installation_id": commit.installation_id,
        "sha": commit.commit_sha,
        "html_url": commit.commit_url,
        "tree_sha": commit.tree_sha,
        "branch_created": commit.branch_created,
        "files_changed": commit.files_changed,
    }


def _pull_request_summary(pull: dict[str, Any]) -> dict[str, Any]:
    return {
        "number": _int_field(pull, "number"),
        "title": _str_field(pull, "title"),
        "state": _str_field(pull, "state"),
        "html_url": _str_field(pull, "html_url"),
        "url": _str_field(pull, "url"),
        "head": _nested_str(pull, "head", "ref"),
        "base": _nested_str(pull, "base", "ref"),
    }


def _commit_url(owner: str, repo: str, sha: str) -> str:
    return f"{_github_config.web_base_url}/{owner}/{repo}/commit/{sha}"


def _generated_branch_name(message: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", message.strip().lower())
    slug = re.sub(r"\.+", ".", slug)
    slug = slug.replace("..", ".").strip("-/.")[:48] or "changes"
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d%H%M%S")
    return f"gestalt/{slug}-{timestamp}"


def _base64url_json(value: dict[str, Any]) -> bytes:
    return _base64url(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).encode("ascii")


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _github_error_message(body: str, status: int) -> str:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return body or f"GitHub API error (status {status})"
    if isinstance(payload, dict):
        message = payload.get("message")
        if isinstance(message, str) and message:
            return message
    return f"GitHub API error (status {status})"


def _require_text(value: str, name: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        raise ValueError(f"{name} is required")
    return trimmed


def _require_slug(value: str, name: str) -> str:
    trimmed = _require_text(value, name)
    if "/" in trimmed:
        raise ValueError(f"{name} must not contain '/'")
    return trimmed


def _optional_slug(value: str, name: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        return ""
    return _require_slug(trimmed, name)


def _require_branch_name(value: str, name: str) -> str:
    trimmed = _require_text(value, name)
    invalid_chars = set("~^:?*[\\")
    if (
        trimmed.startswith("/")
        or trimmed.endswith("/")
        or trimmed.startswith("refs/")
        or "//" in trimmed
        or ".." in trimmed
        or trimmed.endswith(".lock")
        or any(char in invalid_chars or ord(char) < 32 for char in trimmed)
    ):
        raise ValueError(f"{name} is not a valid branch name")
    return trimmed


def _str_field(data: dict[str, Any], *path: str) -> str:
    if len(path) > 1:
        return _nested_str(data, *path)
    value = data.get(path[0]) if path else ""
    if isinstance(value, str):
        return value.strip()
    return ""


def _nested_str(data: dict[str, Any], *path: str) -> str:
    value: Any = data
    for key in path:
        if not isinstance(value, dict):
            return ""
        value = value.get(key)
    if isinstance(value, str):
        return value.strip()
    return ""


def _int_field(data: dict[str, Any], field_name: str) -> int:
    value = data.get(field_name)
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return 0


def _map_field(data: dict[str, Any], field_name: str) -> dict[str, Any]:
    value = data.get(field_name)
    if isinstance(value, dict):
        return value
    return {}


def _dict_to_struct(data: dict[str, Any]) -> Any:
    struct = struct_pb2.Struct()
    struct.update(data)
    return struct


def _config_string(config: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = config.get(key)
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, int):
            return str(value)
    return ""


def _config_dict(config: dict[str, Any], *keys: str) -> dict[str, Any]:
    for key in keys:
        value = config.get(key)
        if isinstance(value, dict):
            return dict(value)
    return {}


def _config_string_list(config: dict[str, Any], *keys: str) -> list[str] | None:
    for key in keys:
        if key not in config:
            continue
        value = config.get(key)
        if isinstance(value, list):
            return [item.strip() for item in value if isinstance(item, str) and item.strip()]
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
    return None


def _config_bool(config: dict[str, Any], *keys: str, default: bool) -> bool:
    for key in keys:
        value = config.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"1", "true", "yes", "on"}:
                return True
            if normalized in {"0", "false", "no", "off"}:
                return False
    return default


def _normalize_private_key(value: str) -> str:
    value = value.strip()
    if "\\n" in value and "\n" not in value:
        value = value.replace("\\n", "\n")
    return value


def _bad_request(message: str) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _forbidden(message: str) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": message})


def _server_error(message: str) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(
        status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message}
    )


def _github_error(err: GitHubAPIError) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(status=err.status, body={"error": err.message})
