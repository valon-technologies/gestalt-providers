from __future__ import annotations

from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt
from gestalt.gen.v1 import workflow_pb2 as _workflow_pb2

from internals.agent import (
    agent_execution_status_name,
    build_agent_session_request,
    build_agent_turn_request,
)
from internals.config import configure_from_mapping, get_github_config
from internals.config import WEBHOOK_DISPATCH_DIRECT, WEBHOOK_DISPATCH_WORKFLOW
from internals.constants import (
    BOT_COMMIT_FILES_OPERATION,
    BOT_CREATE_PULL_REQUEST_OPERATION,
    BOT_OPEN_PULL_REQUEST_OPERATION,
    GITHUB_EVENT_OPERATION,
    GITHUB_WORKFLOW_RUN_AGENT_OPERATION,
)
from internals.errors import GitHubAPIError, GitHubAuthorizationError, GitHubConfigError
from internals.operations import (
    GitHubCoAuthor,
    GitHubCommitRequest,
    GitHubCreatePullRequestRequest,
    GitHubFileChange,
    GitHubOpenPullRequestRequest,
    commit_files,
    commit_result_dict,
    create_pull_request_with_files,
    open_pull_request,
    pull_request_summary,
)
from internals.webhook import (
    event_summary,
    installation_id_from_payload,
    webhook_ignored_reason,
    webhook_subject_from_payload,
)
from internals.workflow_dispatch import (
    build_workflow_event,
    workflow_payload_from_context,
)

plugin = gestalt.Plugin("github")

OperationResult: TypeAlias = dict[str, Any] | gestalt.Response[dict[str, str]]
workflow_pb2: Any = _workflow_pb2


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
        description=(
            "Files to create, update, or delete. Each item accepts path, content, "
            "content_base64, delete, and executable."
        )
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
        description="Append the GitHub App bot as a co-author when its no-reply identity can be derived",
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
        description=(
            "Files to create, update, or delete. Each item accepts path, content, "
            "content_base64, delete, and executable."
        )
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
        description="Append the GitHub App bot as a co-author when its no-reply identity can be derived",
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


@plugin.configure
def configure(_name: str, config: dict[str, Any]) -> None:
    configure_from_mapping(config)


@plugin.http_subject
def resolve_http_subject(request: gestalt.HTTPSubjectRequest) -> gestalt.Subject | None:
    subject = webhook_subject_from_payload(request.params)
    if subject is None:
        return None
    return gestalt.Subject(
        id=subject.id,
        kind=subject.kind,
        display_name=subject.display_name,
        auth_source=subject.auth_source,
    )


@plugin.operation(
    id=GITHUB_EVENT_OPERATION,
    method="POST",
    description="Handle GitHub App webhook callbacks and delegate repository events to a Gestalt agent",
    visible=False,
)
def github_events_handle(
    input: dict[str, Any], req: gestalt.Request
) -> OperationResult:
    ignored_reason = webhook_ignored_reason(input)
    if ignored_reason:
        return {"ok": True, "ignored": ignored_reason}

    config = get_github_config()
    if config.webhook_dispatch == WEBHOOK_DISPATCH_WORKFLOW:
        return _publish_webhook_workflow_event(input, req)
    if config.webhook_dispatch != WEBHOOK_DISPATCH_DIRECT:
        return _server_error(
            f"unsupported webhook dispatch mode {config.webhook_dispatch!r}"
        )

    return _start_agent_for_webhook(input, req)


@plugin.operation(
    id=GITHUB_WORKFLOW_RUN_AGENT_OPERATION,
    method="POST",
    description="Start the GitHub webhook agent from a Workflow event trigger",
    visible=False,
)
def github_events_run_agent_from_workflow_event(
    input: dict[str, Any], req: gestalt.Request
) -> OperationResult:
    del input
    try:
        payload = workflow_payload_from_context(req.workflow)
    except ValueError as err:
        return _bad_request(str(err))
    ignored_reason = webhook_ignored_reason(payload)
    if ignored_reason:
        return {"ok": True, "ignored": ignored_reason}
    return _start_agent_for_webhook(payload, req)


def _publish_webhook_workflow_event(
    input: dict[str, Any], req: gestalt.Request
) -> OperationResult:
    try:
        workflow_event = build_workflow_event(input)
        workflow_manager_factory = getattr(req, "workflow_manager")
        with workflow_manager_factory() as workflow_manager:
            published = workflow_manager.publish_event(
                workflow_pb2.WorkflowManagerPublishEventRequest(event=workflow_event)
            )
    except Exception as err:
        return _service_unavailable(f"failed to publish workflow event: {err}")

    return {
        "ok": True,
        "dispatch": WEBHOOK_DISPATCH_WORKFLOW,
        "workflow_event_id": published.id,
        "workflow_event_type": published.type,
        "workflow_event_subject": published.subject,
    }


def _start_agent_for_webhook(
    input: dict[str, Any], req: gestalt.Request
) -> OperationResult:
    installation_id = installation_id_from_payload(input)
    summary = event_summary(input, installation_id)
    try:
        with req.agent_manager() as agent_manager:
            session_request = build_agent_session_request(summary)
            session = agent_manager.create_session(session_request)
            session_id = str(session.id or "").strip()
            if not session_id:
                return _server_error("agent manager did not return a session id")

            turn_request = build_agent_turn_request(input, summary, session_id)
            turn = agent_manager.create_turn(turn_request)
    except Exception as err:
        return _server_error(f"failed to start agent turn: {err}")

    return {
        "ok": True,
        "agent_session_id": session_id,
        "agent_turn_id": turn.id,
        "agent_provider": session.provider_name or get_github_config().agent_provider,
        "status": agent_execution_status_name(turn.status),
    }


@plugin.operation(
    id=BOT_COMMIT_FILES_OPERATION,
    method="POST",
    description="Create a Git commit on a branch using a GitHub App installation token",
)
def bot_commit_files(input: CommitFilesInput, req: gestalt.Request) -> OperationResult:
    try:
        commit = commit_files(
            _commit_request_from_input(input),
            subject=req.subject,
            pull_request_permissions=False,
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    return {"data": {"commit": commit_result_dict(commit)}}


@plugin.operation(
    id=BOT_OPEN_PULL_REQUEST_OPERATION,
    method="POST",
    description="Open a pull request using a GitHub App installation token",
)
def bot_open_pull_request(
    input: OpenPullRequestInput, req: gestalt.Request
) -> OperationResult:
    try:
        pull = open_pull_request(
            GitHubOpenPullRequestRequest(
                owner=input.owner,
                repo=input.repo,
                title=input.title,
                head=input.head,
                base=input.base,
                body=input.body,
                installation_id=input.installation_id,
                head_owner=input.head_owner,
                draft=input.draft,
                maintainer_can_modify=input.maintainer_can_modify,
            ),
            subject=req.subject,
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    return {"data": {"pull_request": pull_request_summary(pull)}}


@plugin.operation(
    id=BOT_CREATE_PULL_REQUEST_OPERATION,
    method="POST",
    description="Commit file changes to a branch and open a pull request using a GitHub App installation token",
)
def bot_create_pull_request(
    input: CreatePullRequestInput, req: gestalt.Request
) -> OperationResult:
    try:
        result = create_pull_request_with_files(
            GitHubCreatePullRequestRequest(
                owner=input.owner,
                repo=input.repo,
                title=input.title,
                message=input.message,
                files=_file_changes_from_input(input.files),
                body=input.body,
                branch=input.branch,
                base=input.base,
                installation_id=input.installation_id,
                coauthors=_coauthors_from_input(input.coauthors),
                include_bot_coauthor=input.include_bot_coauthor,
                author_name=input.author_name,
                author_email=input.author_email,
                committer_name=input.committer_name,
                committer_email=input.committer_email,
                force=input.force,
                draft=input.draft,
                maintainer_can_modify=input.maintainer_can_modify,
            ),
            subject=req.subject,
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
            "commit": commit_result_dict(result.commit),
            "pull_request": pull_request_summary(result.pull_request),
        }
    }


def _commit_request_from_input(input: CommitFilesInput) -> GitHubCommitRequest:
    return GitHubCommitRequest(
        owner=input.owner,
        repo=input.repo,
        message=input.message,
        files=_file_changes_from_input(input.files),
        branch=input.branch,
        base_branch=input.base_branch,
        installation_id=input.installation_id,
        coauthors=_coauthors_from_input(input.coauthors),
        include_bot_coauthor=input.include_bot_coauthor,
        author_name=input.author_name,
        author_email=input.author_email,
        committer_name=input.committer_name,
        committer_email=input.committer_email,
        force=input.force,
        allow_base_update=input.allow_base_update,
    )


def _file_changes_from_input(files: list[FileChangeInput]) -> list[GitHubFileChange]:
    return [
        GitHubFileChange(
            path=file.path,
            content=file.content,
            content_base64=file.content_base64,
            delete=file.delete,
            executable=file.executable,
        )
        for file in files
    ]


def _coauthors_from_input(coauthors: list[CoAuthorInput]) -> list[GitHubCoAuthor]:
    return [
        GitHubCoAuthor(name=coauthor.name, email=coauthor.email)
        for coauthor in coauthors
    ]


def _bad_request(message: str) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _forbidden(message: str) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": message})


def _server_error(message: str) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(
        status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message}
    )


def _service_unavailable(message: str) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(
        status=HTTPStatus.SERVICE_UNAVAILABLE, body={"error": message}
    )


def _github_error(err: GitHubAPIError) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(status=err.status, body={"error": err.message})
