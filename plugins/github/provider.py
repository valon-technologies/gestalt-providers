from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

from internals.agent import build_workflow_signal_or_start_request
from internals.client import user_external_identity_metadata
from internals.config import configure_from_mapping, get_github_config
from internals.constants import (
    BOT_COMMIT_FILES_OPERATION,
    BOT_CREATE_ISSUE_COMMENT_OPERATION,
    BOT_CREATE_PULL_REQUEST_OPERATION,
    BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
    BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
    BOT_GET_CHECK_RUN_OPERATION,
    BOT_GET_PULL_REQUEST_OPERATION,
    BOT_GET_WORKFLOW_RUN_OPERATION,
    BOT_LIST_CHECK_RUN_ANNOTATIONS_OPERATION,
    BOT_LIST_PULL_REQUEST_FILES_OPERATION,
    BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION,
    BOT_LIST_WORKFLOW_RUN_JOBS_OPERATION,
    BOT_OPEN_PULL_REQUEST_OPERATION,
    BOT_RESOLVE_PULL_REQUEST_REVIEW_THREAD_OPERATION,
    GITHUB_EVENT_OPERATION,
    REVIEW_PULL_REQUEST_OPERATION,
)
from internals.errors import GitHubAPIError, GitHubAuthorizationError, GitHubConfigError
from internals.operations import (
    GitHubCoAuthor,
    GitHubCommitRequest,
    GitHubCheckRunRequest,
    GitHubCreateIssueCommentRequest,
    GitHubCreatePullRequestConversationCommentRequest,
    GitHubCreatePullRequestRequest,
    GitHubCreatePullRequestReviewRequest,
    GitHubFileChange,
    GitHubListCheckRunAnnotationsRequest,
    GitHubListPullRequestFilesRequest,
    GitHubListPullRequestReviewThreadsRequest,
    GitHubListWorkflowRunJobsRequest,
    GitHubOpenPullRequestRequest,
    GitHubPullRequestRequest,
    GitHubPullRequestReviewComment,
    GitHubResolvePullRequestReviewThreadRequest,
    GitHubWorkflowRunRequest,
    check_run_annotation_summary,
    check_run_summary,
    commit_files,
    commit_result_dict,
    create_issue_comment,
    create_pull_request_conversation_comment,
    create_pull_request_review,
    create_pull_request_with_files,
    get_check_run,
    get_pull_request,
    get_workflow_run,
    issue_comment_summary,
    list_check_run_annotations,
    list_pull_request_files,
    list_pull_request_review_threads,
    list_workflow_run_jobs,
    open_pull_request,
    pull_request_file_summary,
    pull_request_review_summary,
    pull_request_summary,
    resolve_pull_request_review_thread,
    workflow_run_job_summary,
    workflow_run_summary,
)
from internals.policy import select_webhook_policy, webhook_event_type_for_policy
from internals.review import review_pull_request
from internals.webhook import (
    event_summary,
    installation_id_from_payload,
    webhook_ignored_reason,
    webhook_subject_from_payload,
)

plugin = gestalt.Plugin("github")
logger = logging.getLogger(__name__)

OperationResult: TypeAlias = dict[str, Any] | gestalt.Response[dict[str, str]]
PostConnectMetadata: TypeAlias = dict[str, str]


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
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
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
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
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
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
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


class CreateIssueCommentInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    issue_number: int = gestalt.field(description="Issue number")
    body: str = gestalt.field(description="Comment body")
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class CreatePullRequestConversationCommentInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    pull_number: int = gestalt.field(description="Pull request number")
    body: str = gestalt.field(description="Comment body")
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class PullRequestReviewCommentInput(gestalt.Model):
    path: str = gestalt.field(description="Repository-relative file path")
    body: str = gestalt.field(description="Inline review comment body")
    line: int = gestalt.field(
        description="Line number in the pull request diff", default=0, required=False
    )
    side: str = gestalt.field(
        description="Diff side for line, either LEFT or RIGHT", default="", required=False
    )
    start_line: int = gestalt.field(
        description="First line in a multi-line review comment",
        default=0,
        required=False,
    )
    start_side: str = gestalt.field(
        description="Diff side for start_line, either LEFT or RIGHT",
        default="",
        required=False,
    )
    position: int = gestalt.field(
        description="Deprecated diff position alternative to line and side",
        default=0,
        required=False,
    )


class CreatePullRequestReviewInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    pull_number: int = gestalt.field(description="Pull request number")
    body: str = gestalt.field(description="Pull request review body")
    comments: list[PullRequestReviewCommentInput] = gestalt.field(
        description=(
            "Inline review comments. Each item accepts path, body, line, side, "
            "start_line, start_side, and position."
        )
    )
    commit_id: str = gestalt.field(
        description="Optional commit SHA to review. Defaults to GitHub's latest PR commit.",
        default="",
        required=False,
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class ReviewPullRequestInput(gestalt.Model):
    agentProvider: str = gestalt.field(
        description="Agent provider used to inspect the pull request diff",
        default="",
        required=False,
    )
    model: str = gestalt.field(
        description="Agent model used to inspect the pull request diff",
        default="",
        required=False,
    )
    systemPrompt: str = gestalt.field(
        description="System prompt for the pull request review agent",
        default="",
        required=False,
    )
    maxComments: int = gestalt.field(
        description="Maximum inline review comments to post",
        default=10,
        required=False,
    )
    maxFiles: int = gestalt.field(
        description="Maximum changed files to inspect",
        default=50,
        required=False,
    )
    maxPatchChars: int = gestalt.field(
        description="Maximum patch characters per changed file to send to the agent",
        default=80000,
        required=False,
    )
    changedLinesOnly: bool = gestalt.field(
        description="Only allow comments on added RIGHT-side diff lines",
        default=True,
        required=False,
    )
    dryRun: bool = gestalt.field(
        description="Return validated findings without posting a GitHub review",
        default=False,
        required=False,
    )
    autoResolveStaleFindings: bool = gestalt.field(
        description="Resolve prior provider-owned inline comments when the latest review no longer reports the same finding",
        default=True,
        required=False,
    )
    turnTimeoutMs: int = gestalt.field(
        description="Maximum time to wait for the review agent turn",
        default=180000,
        required=False,
    )
    pollIntervalMs: int = gestalt.field(
        description="Polling interval while waiting for the review agent turn",
        default=1000,
        required=False,
    )


class ListPullRequestReviewThreadsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    pull_number: int = gestalt.field(description="Pull request number")
    first: int = gestalt.field(
        description="Number of review threads to fetch, from 1 through 100",
        default=100,
        required=False,
    )
    after: str = gestalt.field(
        description="Optional GitHub GraphQL reviewThreads page cursor",
        default="",
        required=False,
    )
    comments_first: int = gestalt.field(
        description="Number of comments to fetch per thread, from 1 through 50",
        default=20,
        required=False,
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class ResolvePullRequestReviewThreadInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    pull_number: int = gestalt.field(description="Pull request number")
    thread_id: str = gestalt.field(description="GitHub GraphQL pull request review thread node ID")
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class GetPullRequestInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    pull_number: int = gestalt.field(description="Pull request number")
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class ListPullRequestFilesInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    pull_number: int = gestalt.field(description="Pull request number")
    per_page: int = gestalt.field(
        description="Results per page, from 1 through 100",
        default=0,
        required=False,
    )
    page: int = gestalt.field(
        description="Page number, starting at 1", default=0, required=False
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class GetCheckRunInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    check_run_id: int = gestalt.field(description="GitHub check run ID")
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class ListCheckRunAnnotationsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    check_run_id: int = gestalt.field(description="GitHub check run ID")
    per_page: int = gestalt.field(
        description="Results per page, from 1 through 100",
        default=0,
        required=False,
    )
    page: int = gestalt.field(
        description="Page number, starting at 1", default=0, required=False
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class GetWorkflowRunInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    run_id: int = gestalt.field(description="GitHub Actions workflow run ID")
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class ListWorkflowRunJobsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    run_id: int = gestalt.field(description="GitHub Actions workflow run ID")
    filter: str = gestalt.field(
        description="GitHub jobs filter, either latest or all",
        default="",
        required=False,
    )
    per_page: int = gestalt.field(
        description="Results per page, from 1 through 100",
        default=0,
        required=False,
    )
    page: int = gestalt.field(
        description="Page number, starting at 1", default=0, required=False
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


@plugin.configure
def configure(_name: str, config: dict[str, Any]) -> None:
    configure_from_mapping(config)


@gestalt.post_connect
def post_connect(token: gestalt.ConnectedToken) -> PostConnectMetadata:
    if token.connection != "default":
        return {}
    return user_external_identity_metadata(token.access_token)


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
    description="Handle GitHub App webhook callbacks and delegate repository events to configured Gestalt workflow targets",
    visible=False,
)
def github_events_handle(
    input: dict[str, Any], req: gestalt.Request
) -> OperationResult:
    config = get_github_config()
    explicit_policies = bool(config.webhook_policies)
    event_type = webhook_event_type_for_policy(input) if explicit_policies else ""
    ignored_reason = webhook_ignored_reason(
        input,
        event_type=event_type,
        enforce_event_allowlist=(
            not explicit_policies or config.webhook_events_configured
        ),
    )
    if ignored_reason:
        return {"ok": True, "ignored": ignored_reason}

    installation_id = installation_id_from_payload(input)
    summary = event_summary(input, installation_id, event_type=event_type)
    policy = None
    if explicit_policies:
        policy = select_webhook_policy(config, input, summary)
        if policy is None:
            return {"ok": True, "ignored": "policy_not_matched"}

    return _signal_or_start_webhook_workflow(input, req, summary=summary, policy=policy)


def _signal_or_start_webhook_workflow(
    input: dict[str, Any],
    req: gestalt.Request,
    *,
    summary: dict[str, Any],
    policy: Any,
) -> OperationResult:
    workflow_key = ""
    try:
        workflow_request = build_workflow_signal_or_start_request(
            input, summary, policy
        )
        workflow_key = str(getattr(workflow_request, "workflow_key", "")).strip()
        logger.info(
            "dispatching GitHub webhook workflow",
            extra={
                "github_event": summary.get("event_type", ""),
                "github_action": summary.get("action", ""),
                "github_delivery_id": summary.get("delivery_id", ""),
                "github_repository": summary.get("repository", ""),
                "github_webhook_policy": getattr(policy, "id", ""),
                "workflow_key": workflow_key,
                "workflow_provider": workflow_request.provider_name,
            },
        )
        workflow_manager_factory = getattr(req, "workflow_manager")
        with workflow_manager_factory() as workflow_manager:
            response = workflow_manager.signal_or_start_run(workflow_request)
    except Exception as err:
        logger.exception(
            "failed to dispatch GitHub webhook workflow",
            extra={
                "github_event": summary.get("event_type", ""),
                "github_action": summary.get("action", ""),
                "github_delivery_id": summary.get("delivery_id", ""),
                "github_repository": summary.get("repository", ""),
                "github_webhook_policy": getattr(policy, "id", ""),
                "workflow_key": workflow_key,
            },
        )
        return _service_unavailable(f"failed to dispatch workflow run: {err}")

    run = getattr(response, "run", None)
    signal = getattr(response, "signal", None)
    workflow_key = str(
        getattr(response, "workflow_key", "") or getattr(run, "workflow_key", "")
    ).strip()
    logger.info(
        "dispatched GitHub webhook workflow",
        extra={
            "github_event": summary.get("event_type", ""),
            "github_action": summary.get("action", ""),
            "github_delivery_id": summary.get("delivery_id", ""),
            "github_repository": summary.get("repository", ""),
            "github_webhook_policy": getattr(policy, "id", ""),
            "workflow_key": workflow_key,
            "workflow_provider": str(
                getattr(response, "provider_name", "")
                or get_github_config().workflow_provider
            ),
            "workflow_run_id": str(getattr(run, "id", "")),
            "workflow_signal_id": str(getattr(signal, "id", "")),
            "workflow_started_run": bool(getattr(response, "started_run", False)),
        },
    )

    return {
        "ok": True,
        "dispatch": "workflow",
        "workflow_provider": str(
            getattr(response, "provider_name", "")
            or get_github_config().workflow_provider
        ),
        "workflow_run_id": str(getattr(run, "id", "")),
        "workflow_key": workflow_key,
        "workflow_signal_id": str(getattr(signal, "id", "")),
        "workflow_started_run": bool(getattr(response, "started_run", False)),
    }


@plugin.operation(
    id=REVIEW_PULL_REQUEST_OPERATION,
    method="POST",
    description=(
        "Review the latest GitHub pull_request workflow signal and post validated "
        "inline comments"
    ),
    tags=["pr", "prs", "review"],
)
def github_review_pull_request(
    input: ReviewPullRequestInput, req: gestalt.Request
) -> OperationResult:
    try:
        return {"data": review_pull_request(input, req)}
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    except RuntimeError as err:
        return _service_unavailable(str(err))


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
    tags=["pr", "prs"],
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
    tags=["pr", "prs"],
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


@plugin.operation(
    id=BOT_CREATE_ISSUE_COMMENT_OPERATION,
    method="POST",
    description="Create an issue comment using a GitHub App installation token",
)
def bot_create_issue_comment(
    input: CreateIssueCommentInput, req: gestalt.Request
) -> OperationResult:
    try:
        comment = create_issue_comment(
            GitHubCreateIssueCommentRequest(
                owner=input.owner,
                repo=input.repo,
                issue_number=input.issue_number,
                body=input.body,
                installation_id=input.installation_id,
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
    return {"data": {"comment": issue_comment_summary(comment)}}


@plugin.operation(
    id=BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
    method="POST",
    description="Create a pull request review with inline comments using a GitHub App installation token",
    tags=["pr", "prs", "review"],
)
def bot_create_pull_request_review(
    input: CreatePullRequestReviewInput, req: gestalt.Request
) -> OperationResult:
    try:
        review = create_pull_request_review(
            GitHubCreatePullRequestReviewRequest(
                owner=input.owner,
                repo=input.repo,
                pull_number=input.pull_number,
                body=input.body,
                comments=_pull_request_review_comments_from_input(input.comments),
                commit_id=input.commit_id,
                installation_id=input.installation_id,
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
    return {"data": {"review": pull_request_review_summary(review)}}


@plugin.operation(
    id=BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION,
    method="GET",
    description="List pull request review threads and their first comments using a GitHub App installation token",
    tags=["pr", "prs", "review"],
)
def bot_list_pull_request_review_threads(
    input: ListPullRequestReviewThreadsInput, req: gestalt.Request
) -> OperationResult:
    try:
        threads = list_pull_request_review_threads(
            GitHubListPullRequestReviewThreadsRequest(
                owner=input.owner,
                repo=input.repo,
                pull_number=input.pull_number,
                first=input.first,
                after=input.after,
                comments_first=input.comments_first,
                installation_id=input.installation_id,
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
    return {"data": threads}


@plugin.operation(
    id=BOT_RESOLVE_PULL_REQUEST_REVIEW_THREAD_OPERATION,
    method="POST",
    description="Resolve a pull request review thread after verifying it belongs to the requested pull request",
    tags=["pr", "prs", "review"],
)
def bot_resolve_pull_request_review_thread(
    input: ResolvePullRequestReviewThreadInput, req: gestalt.Request
) -> OperationResult:
    try:
        thread = resolve_pull_request_review_thread(
            GitHubResolvePullRequestReviewThreadRequest(
                owner=input.owner,
                repo=input.repo,
                pull_number=input.pull_number,
                thread_id=input.thread_id,
                installation_id=input.installation_id,
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
    return {"data": {"thread": thread}}


@plugin.operation(
    id=BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
    method="POST",
    description="Create a pull request conversation comment using a GitHub App installation token",
)
def bot_create_pull_request_conversation_comment(
    input: CreatePullRequestConversationCommentInput, req: gestalt.Request
) -> OperationResult:
    try:
        comment = create_pull_request_conversation_comment(
            GitHubCreatePullRequestConversationCommentRequest(
                owner=input.owner,
                repo=input.repo,
                pull_number=input.pull_number,
                body=input.body,
                installation_id=input.installation_id,
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
    return {"data": {"comment": issue_comment_summary(comment)}}


@plugin.operation(
    id=BOT_GET_PULL_REQUEST_OPERATION,
    method="GET",
    description="Get pull request metadata using a GitHub App installation token",
    tags=["pr", "prs", "review"],
)
def bot_get_pull_request(
    input: GetPullRequestInput, req: gestalt.Request
) -> OperationResult:
    try:
        pull_request = get_pull_request(
            GitHubPullRequestRequest(
                owner=input.owner,
                repo=input.repo,
                pull_number=input.pull_number,
                installation_id=input.installation_id,
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
    return {"data": {"pull_request": pull_request_summary(pull_request)}}


@plugin.operation(
    id=BOT_LIST_PULL_REQUEST_FILES_OPERATION,
    method="GET",
    description="List pull request changed files and bounded patches using a GitHub App installation token",
    tags=["pr", "prs", "review", "diff"],
)
def bot_list_pull_request_files(
    input: ListPullRequestFilesInput, req: gestalt.Request
) -> OperationResult:
    try:
        files = list_pull_request_files(
            GitHubListPullRequestFilesRequest(
                owner=input.owner,
                repo=input.repo,
                pull_number=input.pull_number,
                per_page=input.per_page,
                page=input.page,
                installation_id=input.installation_id,
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
            "count": len(files),
            "files": [pull_request_file_summary(file) for file in files],
        }
    }


@plugin.operation(
    id=BOT_GET_CHECK_RUN_OPERATION,
    method="GET",
    description="Get a GitHub check run using a GitHub App installation token",
)
def bot_get_check_run(input: GetCheckRunInput, req: gestalt.Request) -> OperationResult:
    try:
        check_run = get_check_run(
            GitHubCheckRunRequest(
                owner=input.owner,
                repo=input.repo,
                check_run_id=input.check_run_id,
                installation_id=input.installation_id,
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
    return {"data": {"check_run": check_run_summary(check_run)}}


@plugin.operation(
    id=BOT_LIST_CHECK_RUN_ANNOTATIONS_OPERATION,
    method="GET",
    description="List annotations for a GitHub check run using a GitHub App installation token",
)
def bot_list_check_run_annotations(
    input: ListCheckRunAnnotationsInput, req: gestalt.Request
) -> OperationResult:
    try:
        annotations = list_check_run_annotations(
            GitHubListCheckRunAnnotationsRequest(
                owner=input.owner,
                repo=input.repo,
                check_run_id=input.check_run_id,
                per_page=input.per_page,
                page=input.page,
                installation_id=input.installation_id,
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
            "count": len(annotations),
            "annotations": [
                check_run_annotation_summary(annotation) for annotation in annotations
            ],
        }
    }


@plugin.operation(
    id=BOT_GET_WORKFLOW_RUN_OPERATION,
    method="GET",
    description="Get a GitHub Actions workflow run using a GitHub App installation token",
)
def bot_get_workflow_run(
    input: GetWorkflowRunInput, req: gestalt.Request
) -> OperationResult:
    try:
        workflow_run = get_workflow_run(
            GitHubWorkflowRunRequest(
                owner=input.owner,
                repo=input.repo,
                run_id=input.run_id,
                installation_id=input.installation_id,
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
    return {"data": {"workflow_run": workflow_run_summary(workflow_run)}}


@plugin.operation(
    id=BOT_LIST_WORKFLOW_RUN_JOBS_OPERATION,
    method="GET",
    description="List jobs for a GitHub Actions workflow run using a GitHub App installation token",
)
def bot_list_workflow_run_jobs(
    input: ListWorkflowRunJobsInput, req: gestalt.Request
) -> OperationResult:
    try:
        jobs = list_workflow_run_jobs(
            GitHubListWorkflowRunJobsRequest(
                owner=input.owner,
                repo=input.repo,
                run_id=input.run_id,
                filter=input.filter,
                per_page=input.per_page,
                page=input.page,
                installation_id=input.installation_id,
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
    raw_jobs = jobs.get("jobs")
    if not isinstance(raw_jobs, list):
        raw_jobs = []
    return {
        "data": {
            "total_count": jobs.get("total_count", len(raw_jobs)),
            "jobs": [
                workflow_run_job_summary(job)
                for job in raw_jobs
                if isinstance(job, dict)
            ],
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


def _file_changes_from_input(
    files: list[FileChangeInput],
) -> tuple[GitHubFileChange, ...]:
    return tuple(
        GitHubFileChange(
            path=file.path,
            content=file.content,
            content_base64=file.content_base64,
            delete=file.delete,
            executable=file.executable,
        )
        for file in files
    )


def _coauthors_from_input(
    coauthors: list[CoAuthorInput],
) -> tuple[GitHubCoAuthor, ...]:
    return tuple(
        GitHubCoAuthor(name=coauthor.name, email=coauthor.email)
        for coauthor in coauthors
    )


def _pull_request_review_comments_from_input(
    comments: list[PullRequestReviewCommentInput],
) -> tuple[GitHubPullRequestReviewComment, ...]:
    return tuple(
        GitHubPullRequestReviewComment(
            path=comment.path,
            body=comment.body,
            line=comment.line,
            side=comment.side,
            start_line=comment.start_line,
            start_side=comment.start_side,
            position=comment.position,
        )
        for comment in comments
    )


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
