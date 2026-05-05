from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

from internals.agent import build_workflow_signal_or_start_request
from internals.client import DEFAULT_GITHUB_CLIENT, user_external_identity_metadata
from internals.config import configure_from_mapping, get_github_config
from internals.constants import (
    BOT_ADD_LABELS_OPERATION,
    BOT_ADD_REACTION_OPERATION,
    BOT_CLOSE_PULL_REQUEST_OPERATION,
    BOT_COMMIT_FILES_OPERATION,
    BOT_CREATE_ISSUE_COMMENT_OPERATION,
    BOT_CREATE_PULL_REQUEST_OPERATION,
    BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
    BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
    BOT_GET_CHECK_RUN_OPERATION,
    BOT_GET_PULL_REQUEST_OPERATION,
    BOT_GET_WORKFLOW_RUN_OPERATION,
    BOT_LIST_CHECK_SUITE_CHECK_RUNS_OPERATION,
    BOT_LIST_CHECK_RUN_ANNOTATIONS_OPERATION,
    BOT_LIST_PULL_REQUEST_FILES_OPERATION,
    BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION,
    BOT_LIST_WORKFLOW_RUN_JOBS_OPERATION,
    BOT_OPEN_PULL_REQUEST_OPERATION,
    BOT_REMOVE_LABELS_OPERATION,
    BOT_REQUEST_REVIEWERS_OPERATION,
    BOT_RESOLVE_PULL_REQUEST_REVIEW_THREAD_OPERATION,
    BOT_RESOLVE_INSTALLATION_OPERATION,
    GITHUB_EVENT_OPERATION,
    GITHUB_EXTERNAL_IDENTITY_TYPE,
    REVIEW_PULL_REQUEST_OPERATION,
)
from internals.errors import GitHubAPIError, GitHubAuthorizationError, GitHubConfigError
from internals.identity import (
    GitHubPullRequestContext,
    caller_preference_identity,
    needs_pull_request_context_for_preferences,
    preference_identity_from_webhook,
)
from internals.operations import (
    GitHubAddLabelsRequest,
    GitHubAddReactionRequest,
    GitHubCoAuthor,
    GitHubCommitRequest,
    GitHubCheckRunRequest,
    GitHubCreateIssueCommentRequest,
    GitHubCreatePullRequestConversationCommentRequest,
    GitHubCreatePullRequestRequest,
    GitHubCreatePullRequestReviewRequest,
    GitHubFileChange,
    GitHubListCheckSuiteCheckRunsRequest,
    GitHubListCheckRunAnnotationsRequest,
    GitHubListPullRequestFilesRequest,
    GitHubListPullRequestReviewThreadsRequest,
    GitHubListWorkflowRunJobsRequest,
    GitHubOpenPullRequestRequest,
    GitHubPullRequestRequest,
    GitHubPullRequestReviewComment,
    GitHubRemoveLabelsRequest,
    GitHubRequestReviewersRequest,
    GitHubResolvePullRequestReviewThreadRequest,
    GitHubResolveInstallationRequest,
    GitHubWorkflowRunRequest,
    add_labels,
    add_reaction,
    check_run_annotation_summary,
    check_run_summary,
    close_pull_request,
    commit_files,
    commit_result_dict,
    create_issue_comment,
    create_pull_request_conversation_comment,
    create_pull_request_review,
    create_pull_request_with_files,
    get_check_run,
    get_pull_request,
    get_workflow_run,
    installation_subject_summary,
    issue_comment_summary,
    label_summary,
    list_check_suite_check_runs,
    list_check_run_annotations,
    list_pull_request_files,
    list_pull_request_review_threads,
    list_workflow_run_jobs,
    open_pull_request,
    pull_request_file_summary,
    pull_request_review_summary,
    pull_request_summary,
    reaction_summary,
    remove_labels,
    request_reviewers,
    resolve_pull_request_review_thread,
    resolve_installation_subject,
    workflow_run_job_summary,
    workflow_run_summary,
)
from internals.policy import select_webhook_policy, webhook_event_type_for_policy
from internals.preferences import (
    apply_action_preferences,
    delete_action_preference,
    get_action_preference,
    normalize_identity_kind,
    reset_action_preference_store,
    set_action_preference,
)
from internals.review import review_pull_request
from internals.webhook import (
    event_summary,
    installation_id_from_payload,
    webhook_ignored_reason,
    webhook_subject_from_payload,
)

plugin = gestalt.Plugin("github")
logger = logging.getLogger(__name__)

OperationResult: TypeAlias = dict[str, Any] | gestalt.Response[dict[str, Any]]
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


class ResolveInstallationInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")


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
        description="Optional Git commit author name. Delegated agent calls use the invoking user's linked GitHub identity instead when available.",
        default="",
        required=False,
    )
    author_email: str = gestalt.field(
        description="Optional Git commit author email. Delegated agent calls use the invoking user's linked GitHub identity instead when available.",
        default="",
        required=False,
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


class ClosePullRequestInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    pull_number: int = gestalt.field(description="Pull request number")
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
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
        description="Optional Git commit author name. Delegated agent calls use the invoking user's linked GitHub identity instead when available.",
        default="",
        required=False,
    )
    author_email: str = gestalt.field(
        description="Optional Git commit author email. Delegated agent calls use the invoking user's linked GitHub identity instead when available.",
        default="",
        required=False,
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
        description="Diff side for line, either LEFT or RIGHT",
        default="",
        required=False,
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
    thread_id: str = gestalt.field(
        description="GitHub GraphQL pull request review thread node ID"
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class AddReactionInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    subject_type: str = gestalt.field(
        description="Reaction target type: issue, pull_request, issue_comment, or pull_request_review_comment"
    )
    content: str = gestalt.field(
        description="Reaction content: +1, -1, laugh, confused, heart, hooray, rocket, or eyes"
    )
    issue_number: int = gestalt.field(
        description="Issue number when subject_type is issue",
        default=0,
        required=False,
    )
    pull_number: int = gestalt.field(
        description="Pull request number when subject_type is pull_request",
        default=0,
        required=False,
    )
    comment_id: int = gestalt.field(
        description="Comment ID when subject_type is issue_comment or pull_request_review_comment",
        default=0,
        required=False,
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class AddLabelsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    subject_type: str = gestalt.field(
        description="Label target type: issue or pull_request"
    )
    labels: list[str] = gestalt.field(description="Label names to add")
    issue_number: int = gestalt.field(
        description="Issue number when subject_type is issue",
        default=0,
        required=False,
    )
    pull_number: int = gestalt.field(
        description="Pull request number when subject_type is pull_request",
        default=0,
        required=False,
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class RemoveLabelsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    subject_type: str = gestalt.field(
        description="Label target type: issue or pull_request"
    )
    labels: list[str] = gestalt.field(description="Label names to remove")
    issue_number: int = gestalt.field(
        description="Issue number when subject_type is issue",
        default=0,
        required=False,
    )
    pull_number: int = gestalt.field(
        description="Pull request number when subject_type is pull_request",
        default=0,
        required=False,
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class RequestReviewersInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    pull_number: int = gestalt.field(description="Pull request number")
    reviewers: list[str] = gestalt.field(
        description="GitHub usernames to request as reviewers",
        default_factory=list,
        required=False,
    )
    team_reviewers: list[str] = gestalt.field(
        description="GitHub team slugs to request as reviewers",
        default_factory=list,
        required=False,
    )
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


class ListCheckSuiteCheckRunsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    check_suite_id: int = gestalt.field(description="GitHub check suite ID")
    check_name: str = gestalt.field(
        description="Optional check run name filter", default="", required=False
    )
    status: str = gestalt.field(
        description="Optional check run status filter",
        default="",
        required=False,
    )
    filter: str = gestalt.field(
        description="GitHub check runs filter, either latest or all",
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


class ActionPreferenceInput(gestalt.Model):
    repository: str = gestalt.field(description="Repository full name, owner/repo")
    policy_id: str = gestalt.field(description="GitHub webhook policy id")
    identity_kind: str = gestalt.field(
        description="Preference identity kind: external_subject_id or subject_id. Defaults to the caller's linked GitHub identity when present.",
        default="",
        required=False,
    )


class SetActionPreferenceInput(ActionPreferenceInput):
    allow_code_review_comments: bool | None = gestalt.field(
        description="Whether this caller allows inline code review comments for this policy. Null leaves the policy default in effect.",
        default=None,
        required=False,
    )
    allow_self_fix: bool | None = gestalt.field(
        description="Whether this caller allows self-fix commits or pull requests for this policy. Null leaves the policy default in effect.",
        default=None,
        required=False,
    )


@plugin.configure
def configure(_name: str, config: dict[str, Any]) -> None:
    reset_action_preference_store()
    configure_from_mapping(config, provider_name=_name)


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
        try:
            pull_request_context = _pull_request_webhook_context(
                input, req, summary, policy
            )
            stale_reason = _stale_ci_head_ignored_reason(
                summary, policy, pull_request_context
            )
        except ValueError as err:
            return _bad_request(str(err))
        except GitHubAuthorizationError as err:
            return _forbidden(str(err))
        except GitHubConfigError as err:
            return _server_error(str(err))
        except GitHubAPIError as err:
            return _github_error(err)
        if stale_reason:
            return {"ok": True, "ignored": stale_reason}
        policy = _policy_with_action_preferences(
            input, req, summary, policy, pull_request_context
        )
        if _review_pull_request_disabled_by_preference(policy):
            return {
                "ok": True,
                "ignored": "policy_preference_disabled:code_review_comments",
            }

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


def _stale_ci_head_ignored_reason(
    summary: dict[str, Any],
    policy: Any,
    pull_request_context: GitHubPullRequestContext | None,
) -> str:
    if not getattr(getattr(policy, "comments", None), "suppress_stale_head", False):
        return ""
    if str(summary.get("event_type", "")).strip() not in (
        "check_run",
        "check_suite",
        "workflow_run",
    ):
        return ""
    event_head_sha = str(summary.get("head_sha", "")).strip()
    if not event_head_sha:
        return ""
    if pull_request_context is None:
        return ""
    current_head_sha = pull_request_context.head_sha
    if current_head_sha and current_head_sha != event_head_sha:
        return f"stale_head:{event_head_sha[:12]}:{current_head_sha[:12]}"
    return ""


def _pull_request_webhook_context(
    input: dict[str, Any],
    req: gestalt.Request,
    summary: dict[str, Any],
    policy: Any,
) -> GitHubPullRequestContext | None:
    if not _needs_pull_request_context(input, summary, policy):
        return None
    owner = str(summary.get("repository_owner", "")).strip()
    repo = str(summary.get("repository_name", "")).strip()
    pull_number = _single_pull_request_number(summary.get("pull_request_numbers"))
    if not owner or not repo or pull_number <= 0:
        return None
    pull = get_pull_request(
        GitHubPullRequestRequest(
            owner=owner,
            repo=repo,
            pull_number=pull_number,
            installation_id=int(summary.get("installation_id", 0) or 0),
        ),
        subject=req.subject,
    )
    return GitHubPullRequestContext(pull_request=pull)


def _needs_pull_request_context(
    input: dict[str, Any], summary: dict[str, Any], policy: Any
) -> bool:
    if _needs_pull_request_context_for_stale(summary, policy):
        return True
    config = get_github_config()
    return needs_pull_request_context_for_preferences(
        policy,
        input,
        summary,
        preferences_enabled=config.action_preferences.enabled,
    )


def _needs_pull_request_context_for_stale(summary: dict[str, Any], policy: Any) -> bool:
    if not getattr(getattr(policy, "comments", None), "suppress_stale_head", False):
        return False
    if str(summary.get("event_type", "")).strip() not in (
        "check_run",
        "check_suite",
        "workflow_run",
    ):
        return False
    if not str(summary.get("head_sha", "")).strip():
        return False
    return _single_pull_request_number(summary.get("pull_request_numbers")) > 0


def _policy_with_action_preferences(
    input: dict[str, Any],
    req: gestalt.Request,
    summary: dict[str, Any],
    policy: Any,
    pull_request_context: GitHubPullRequestContext | None,
) -> Any:
    config = get_github_config()
    if not config.action_preferences.enabled:
        return policy
    authorization = None
    try:
        authorization = req.authorization()
    except Exception as err:
        logger.warning("GitHub action preference authorization unavailable: %s", err)
    identity = preference_identity_from_webhook(
        input,
        summary,
        policy,
        pull_request_context=pull_request_context,
        authorization=authorization,
    )
    return apply_action_preferences(policy, config.action_preferences, identity)


def _review_pull_request_disabled_by_preference(policy: Any) -> bool:
    preferences = getattr(policy, "action_preferences", None)
    if not preferences or preferences.get("source") == "config_default":
        return False
    target = getattr(policy, "workflow_target", None)
    return (
        target is not None
        and getattr(target, "plugin_name", "") == "github"
        and getattr(target, "operation", "") == REVIEW_PULL_REQUEST_OPERATION
        and not getattr(policy, "allow_code_review_comments", True)
    )


def _single_pull_request_number(value: Any) -> int:
    if not isinstance(value, list) or len(value) != 1:
        return 0
    number = value[0]
    if isinstance(number, bool) or not isinstance(number, (int, float)):
        return 0
    return int(number) if int(number) > 0 else 0


def _pull_request_head_sha(pull: dict[str, Any]) -> str:
    head = pull.get("head")
    if not isinstance(head, dict):
        return ""
    value = head.get("sha")
    return value.strip() if isinstance(value, str) else ""


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
    id="actionPreferences.get",
    method="GET",
    description="Get this caller's GitHub webhook action preferences for a policy",
)
def github_action_preferences_get(
    input: ActionPreferenceInput, req: gestalt.Request
) -> OperationResult:
    config = get_github_config().action_preferences
    if not config.enabled:
        return _failed_precondition("GitHub action preferences are not configured")
    try:
        identity = caller_preference_identity(req, input.identity_kind)
        identity_kind = normalize_identity_kind(input.identity_kind, identity)
        preference = get_action_preference(
            config,
            repository=input.repository,
            policy_id=input.policy_id,
            identity=identity,
            identity_kind=identity_kind,
        )
    except ValueError as err:
        return _bad_request(str(err))
    except RuntimeError as err:
        return _failed_precondition(str(err))
    except Exception as err:
        return _service_unavailable(str(err))
    return {
        "data": {
            "found": preference is not None,
            "preference": _preference_summary(preference) if preference else None,
        }
    }


@plugin.operation(
    id="actionPreferences.set",
    method="POST",
    description="Set this caller's GitHub webhook action preferences for a policy",
)
def github_action_preferences_set(
    input: SetActionPreferenceInput, req: gestalt.Request
) -> OperationResult:
    config = get_github_config().action_preferences
    if not config.enabled:
        return _failed_precondition("GitHub action preferences are not configured")
    try:
        identity = caller_preference_identity(req, input.identity_kind)
        identity_kind = normalize_identity_kind(input.identity_kind, identity)
        preference = set_action_preference(
            config,
            repository=input.repository,
            policy_id=input.policy_id,
            identity=identity,
            identity_kind=identity_kind,
            allow_code_review_comments=input.allow_code_review_comments,
            allow_self_fix=input.allow_self_fix,
            updated_by_subject_id=identity.subject_id,
        )
    except ValueError as err:
        return _bad_request(str(err))
    except RuntimeError as err:
        return _failed_precondition(str(err))
    except Exception as err:
        return _service_unavailable(str(err))
    return {"data": {"preference": _preference_summary(preference)}}


@plugin.operation(
    id="actionPreferences.delete",
    method="POST",
    description="Delete this caller's GitHub webhook action preferences for a policy",
)
def github_action_preferences_delete(
    input: ActionPreferenceInput, req: gestalt.Request
) -> OperationResult:
    config = get_github_config().action_preferences
    if not config.enabled:
        return _failed_precondition("GitHub action preferences are not configured")
    try:
        identity = caller_preference_identity(req, input.identity_kind)
        identity_kind = normalize_identity_kind(input.identity_kind, identity)
        deleted = delete_action_preference(
            config,
            repository=input.repository,
            policy_id=input.policy_id,
            identity=identity,
            identity_kind=identity_kind,
        )
    except ValueError as err:
        return _bad_request(str(err))
    except RuntimeError as err:
        return _failed_precondition(str(err))
    except Exception as err:
        return _service_unavailable(str(err))
    return {"data": {"deleted": deleted}}


@plugin.operation(
    id=BOT_RESOLVE_INSTALLATION_OPERATION,
    method="GET",
    description="Resolve the GitHub App installation service-account subject for a repository",
)
def bot_resolve_installation(input: ResolveInstallationInput) -> OperationResult:
    try:
        subject = resolve_installation_subject(
            GitHubResolveInstallationRequest(owner=input.owner, repo=input.repo)
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    return {"data": {"installation": installation_subject_summary(subject)}}


@plugin.operation(
    id=BOT_COMMIT_FILES_OPERATION,
    method="POST",
    description="Create a Git commit on a branch using a GitHub App installation token",
)
def bot_commit_files(input: CommitFilesInput, req: gestalt.Request) -> OperationResult:
    try:
        commit = commit_files(
            _commit_request_from_input(input, req),
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
    id=BOT_CLOSE_PULL_REQUEST_OPERATION,
    method="POST",
    description="Close a pull request using a GitHub App installation token",
    tags=["pr", "prs"],
)
def bot_close_pull_request(
    input: ClosePullRequestInput, req: gestalt.Request
) -> OperationResult:
    try:
        pull = close_pull_request(
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
        author_name, author_email = _commit_author_from_request(
            input.author_name, input.author_email, req
        )
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
                author_name=author_name,
                author_email=author_email,
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
    id=BOT_ADD_REACTION_OPERATION,
    method="POST",
    description="Add a reaction using a GitHub App installation token",
)
def bot_add_reaction(input: AddReactionInput, req: gestalt.Request) -> OperationResult:
    try:
        reaction = add_reaction(
            GitHubAddReactionRequest(
                owner=input.owner,
                repo=input.repo,
                subject_type=input.subject_type,
                content=input.content,
                issue_number=input.issue_number,
                pull_number=input.pull_number,
                comment_id=input.comment_id,
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
    return {"data": {"reaction": reaction_summary(reaction)}}


@plugin.operation(
    id=BOT_ADD_LABELS_OPERATION,
    method="POST",
    description="Add labels to an issue or pull request using a GitHub App installation token",
)
def bot_add_labels(input: AddLabelsInput, req: gestalt.Request) -> OperationResult:
    try:
        labels = add_labels(
            GitHubAddLabelsRequest(
                owner=input.owner,
                repo=input.repo,
                subject_type=input.subject_type,
                labels=tuple(input.labels),
                issue_number=input.issue_number,
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
    return {"data": {"labels": [label_summary(label) for label in labels]}}


@plugin.operation(
    id=BOT_REMOVE_LABELS_OPERATION,
    method="POST",
    description="Remove labels from an issue or pull request using a GitHub App installation token",
)
def bot_remove_labels(
    input: RemoveLabelsInput, req: gestalt.Request
) -> OperationResult:
    try:
        removed, labels = remove_labels(
            GitHubRemoveLabelsRequest(
                owner=input.owner,
                repo=input.repo,
                subject_type=input.subject_type,
                labels=tuple(input.labels),
                issue_number=input.issue_number,
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
    return {
        "data": {
            "removed": list(removed),
            "labels": [label_summary(label) for label in labels],
        }
    }


@plugin.operation(
    id=BOT_REQUEST_REVIEWERS_OPERATION,
    method="POST",
    description="Request individual or team reviewers on a pull request using a GitHub App installation token",
    tags=["pr", "prs", "review"],
)
def bot_request_reviewers(
    input: RequestReviewersInput, req: gestalt.Request
) -> OperationResult:
    try:
        pull_request = request_reviewers(
            GitHubRequestReviewersRequest(
                owner=input.owner,
                repo=input.repo,
                pull_number=input.pull_number,
                reviewers=tuple(input.reviewers),
                team_reviewers=tuple(input.team_reviewers),
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
    id=BOT_LIST_CHECK_SUITE_CHECK_RUNS_OPERATION,
    method="GET",
    description="List check runs in a GitHub check suite using a GitHub App installation token",
)
def bot_list_check_suite_check_runs(
    input: ListCheckSuiteCheckRunsInput, req: gestalt.Request
) -> OperationResult:
    try:
        check_runs = list_check_suite_check_runs(
            GitHubListCheckSuiteCheckRunsRequest(
                owner=input.owner,
                repo=input.repo,
                check_suite_id=input.check_suite_id,
                check_name=input.check_name,
                status=input.status,
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
    raw_check_runs = check_runs.get("check_runs")
    if not isinstance(raw_check_runs, list):
        raw_check_runs = []
    return {
        "data": {
            "total_count": check_runs.get("total_count", len(raw_check_runs)),
            "check_runs": [
                check_run_summary(check_run)
                for check_run in raw_check_runs
                if isinstance(check_run, dict)
            ],
        }
    }


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


def _commit_request_from_input(
    input: CommitFilesInput, req: gestalt.Request
) -> GitHubCommitRequest:
    author_name, author_email = _commit_author_from_request(
        input.author_name, input.author_email, req
    )
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
        author_name=author_name,
        author_email=author_email,
        committer_name=input.committer_name,
        committer_email=input.committer_email,
        force=input.force,
        allow_base_update=input.allow_base_update,
    )


def _commit_author_from_request(
    input_name: str, input_email: str, req: Any
) -> tuple[str, str]:
    external_identity = getattr(req, "agent_external_identity", None)
    if external_identity is not None:
        identity_type = str(getattr(external_identity, "type", "") or "").strip()
        identity_id = str(getattr(external_identity, "id", "") or "").strip()
        if identity_type == GITHUB_EXTERNAL_IDENTITY_TYPE:
            user_id = _github_user_id_from_external_identity(identity_id)
            user_identity = DEFAULT_GITHUB_CLIENT.user_identity_by_id(user_id)
            if user_identity is not None and user_identity.email:
                return user_identity.name, user_identity.email
    return input_name, input_email


def _github_user_id_from_external_identity(identity_id: str) -> str:
    prefix, _, user_id = str(identity_id or "").strip().partition(":")
    if prefix != "user":
        return ""
    return user_id.strip()


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


def _preference_summary(preference: dict[str, Any]) -> dict[str, Any]:
    return {
        key: preference[key]
        for key in (
            "id",
            "repository",
            "policy_id",
            "identity_kind",
            "external_identity_type",
            "external_subject_id",
            "subject_id",
            "allow_code_review_comments",
            "allow_self_fix",
            "updated_by_subject_id",
            "created_at",
            "updated_at",
        )
        if key in preference
    }


def _bad_request(message: str) -> gestalt.Response[dict[str, Any]]:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _forbidden(message: str) -> gestalt.Response[dict[str, Any]]:
    return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": message})


def _failed_precondition(message: str) -> gestalt.Response[dict[str, Any]]:
    return gestalt.Response(
        status=HTTPStatus.PRECONDITION_FAILED, body={"error": message}
    )


def _server_error(message: str) -> gestalt.Response[dict[str, Any]]:
    return gestalt.Response(
        status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message}
    )


def _service_unavailable(message: str) -> gestalt.Response[dict[str, Any]]:
    return gestalt.Response(
        status=HTTPStatus.SERVICE_UNAVAILABLE, body={"error": message}
    )


def _github_error(err: GitHubAPIError) -> gestalt.Response[dict[str, Any]]:
    body: dict[str, Any] = {"error": err.message}
    if err.details:
        body["details"] = err.details
    return gestalt.Response(status=err.status, body=body)
