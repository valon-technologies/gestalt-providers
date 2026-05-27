from __future__ import annotations

import hashlib
import logging
from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

from internals.agent import build_workflow_signal_or_start_request
from internals.client import (
    DEFAULT_GITHUB_CLIENT,
    repo_path,
    user_external_identity_metadata,
)
from internals.config import (
    GitHubActionPreferencesConfig,
    GitHubWebhookPolicy,
    SELF_FIX_BRANCH_COMMIT,
    SELF_FIX_DISABLED,
    SELF_FIX_MODES,
    configure_from_mapping,
    effective_policy_operations,
    get_github_config,
)
from internals.constants import (
    ACTION_PREFERENCES_LIST_TARGETS_OPERATION,
    BOT_ADD_LABELS_OPERATION,
    BOT_ADD_REACTION_OPERATION,
    BOT_CLOSE_PULL_REQUEST_OPERATION,
    BOT_COMMIT_FILES_OPERATION,
    BOT_CREATE_CHECK_RUN_OPERATION,
    BOT_CREATE_ISSUE_COMMENT_OPERATION,
    BOT_CREATE_PULL_REQUEST_OPERATION,
    BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
    BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
    BOT_GET_CHECK_RUN_OPERATION,
    BOT_GET_CONTENT_OPERATION,
    BOT_GET_PULL_REQUEST_OPERATION,
    BOT_GET_REPOSITORY_OPERATION,
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
    BOT_SEARCH_CODE_OPERATION,
    BOT_UPDATE_CHECK_RUN_OPERATION,
    GITHUB_EVENT_OPERATION,
    GITHUB_EXTERNAL_IDENTITY_TYPE,
    REVIEW_PULL_REQUEST_OPERATION,
    USER_CREATE_PULL_REQUEST_OPERATION,
)
from internals.errors import GitHubAPIError, GitHubAuthorizationError, GitHubConfigError
from internals.identity import (
    GitHubPullRequestContext,
    GitHubPreferenceIdentity,
    caller_preference_identity,
    needs_pull_request_context_for_preferences,
    preference_identity_from_webhook,
)
from internals.operations import (
    GitHubAddLabelsRequest,
    GitHubAddReactionRequest,
    GitHubCodeSearchRequest,
    GitHubCoAuthor,
    GitHubCommitRequest,
    GitHubCheckRunRequest,
    GitHubCheckRunOutput,
    GitHubCreateIssueCommentRequest,
    GitHubCreateCheckRunRequest,
    GitHubCreatePullRequestConversationCommentRequest,
    GitHubCreatePullRequestRequest,
    GitHubCreatePullRequestReviewRequest,
    GitHubUserCreatePullRequestRequest,
    GitHubFileContentRequest,
    GitHubFileChange,
    GitHubListCheckRunsForRefRequest,
    GitHubListCheckSuiteCheckRunsRequest,
    GitHubListCheckRunAnnotationsRequest,
    GitHubListPullRequestFilesRequest,
    GitHubListPullRequestReviewThreadsRequest,
    GitHubListWorkflowRunJobsRequest,
    GitHubOpenPullRequestRequest,
    GitHubPullRequestRequest,
    GitHubPullRequestReviewComment,
    GitHubRepositoryRequest,
    GitHubRemoveLabelsRequest,
    GitHubRequestReviewersRequest,
    GitHubResolvePullRequestReviewThreadRequest,
    GitHubUpdateCheckRunRequest,
    GitHubWorkflowRunRequest,
    add_labels,
    add_reaction,
    check_run_annotation_summary,
    check_run_summary,
    close_pull_request,
    code_search_summary,
    commit_files,
    commit_result_dict,
    create_check_run,
    create_issue_comment,
    create_pull_request_conversation_comment,
    create_pull_request_review,
    create_pull_request_with_user_files,
    create_pull_request_with_files,
    get_check_run,
    get_file_text_at_ref,
    get_pull_request,
    get_repository,
    get_workflow_run,
    issue_comment_summary,
    installation_resolution_dict,
    label_summary,
    list_check_runs_for_ref,
    list_check_suite_check_runs,
    list_check_run_annotations,
    list_pull_request_files,
    list_pull_request_review_threads,
    list_workflow_run_jobs,
    non_empty_external_identity,
    open_pull_request,
    pull_request_file_summary,
    pull_request_review_summary,
    pull_request_summary,
    reaction_summary,
    remove_labels,
    repository_summary,
    request_reviewers,
    resolve_pull_request_review_thread,
    search_code,
    update_check_run,
    resolve_repository_installation,
    workflow_run_job_summary,
    workflow_run_summary,
)
from internals.helpers import int_field, map_field, nested_str, str_field
from internals.manual_trigger import (
    app_mention_body_matches,
    manual_command_body_matches,
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
from internals.review import (
    DEFAULT_AGENT_PROVIDER,
    DEFAULT_MODEL,
    DEFAULT_SYSTEM_PROMPT,
    ReviewSettings,
    SUPPORTED_PULL_REQUEST_ACTIONS,
    review_pull_request,
    utc_timestamp,
)
from internals.webhook import (
    event_summary,
    installation_id_from_payload,
    webhook_ignored_reason,
    webhook_subject_from_payload,
)

app = gestalt.App("github")
logger = logging.getLogger(__name__)

_ACTION_PREFERENCE_CONTROL_LABELS = {
    "allow_code_review_comments": "Automatic PR code review",
    "self_fix_mode": "Self-fix from PR comments",
}
_ACTION_PREFERENCE_CONTROL_DESCRIPTIONS = {
    "allow_code_review_comments": (
        "Allow the GitHub bot to post provider-owned inline review comments on your PRs."
    ),
    "self_fix_mode": (
        "Choose whether the GitHub bot may suggest, commit, or open pull requests "
        "to fix issues it finds."
    ),
}
_SELF_FIX_OPERATIONS = {
    BOT_COMMIT_FILES_OPERATION,
    BOT_OPEN_PULL_REQUEST_OPERATION,
    BOT_CREATE_PULL_REQUEST_OPERATION,
}
_REVIEW_DISPATCH_FAILED_TITLE = "Review dispatch failed"

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


class RepositoryInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
        default=0,
        required=False,
    )


class SearchCodeInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    query: str = gestalt.field(description="GitHub code search query")
    path: str = gestalt.field(
        description="Optional repository-relative path prefix to search within",
        default="",
        required=False,
    )
    per_page: int = gestalt.field(
        description="Results per page, from 1 through 100",
        default=10,
        required=False,
    )
    page: int = gestalt.field(
        description="Page number, starting at 1", default=0, required=False
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
        default=0,
        required=False,
    )


class GetContentInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    path: str = gestalt.field(description="Repository-relative file path")
    ref: str = gestalt.field(
        description="Git ref, branch, or SHA. Defaults to the repository default branch.",
        default="",
        required=False,
    )
    max_bytes: int = gestalt.field(
        description="Maximum UTF-8 bytes to return",
        default=80000,
        required=False,
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
        default=0,
        required=False,
    )


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
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
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
    expected_head_sha: str = gestalt.field(
        description="Require the target branch to still point at this commit SHA before writing",
        default="",
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
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
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
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
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


class UserCreatePullRequestInput(gestalt.Model):
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
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
        default=0,
        required=False,
    )


class CreatePullRequestConversationCommentInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    pull_number: int = gestalt.field(description="Pull request number")
    body: str = gestalt.field(description="Comment body")
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
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
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
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
    checkRunName: str = gestalt.field(
        description="GitHub check run name to create for the review",
        default="Gestalt Review",
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
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
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
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
        default=0,
        required=False,
    )


class GetCheckRunInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    check_run_id: int = gestalt.field(description="GitHub check run ID")
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
        default=0,
        required=False,
    )


class CheckRunOutputInput(gestalt.Model):
    title: str = gestalt.field(
        description="Check run output title", default="", required=False
    )
    summary: str = gestalt.field(
        description="Check run output summary", default="", required=False
    )
    text: str = gestalt.field(
        description="Optional check run output details", default="", required=False
    )


class CreateCheckRunInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    name: str = gestalt.field(description="Check run name")
    head_sha: str = gestalt.field(
        description="Git commit SHA to attach the check run to"
    )
    status: str = gestalt.field(
        description="Check run status: queued, in_progress, or completed",
        default="in_progress",
        required=False,
    )
    conclusion: str = gestalt.field(
        description="Completed check conclusion",
        default="",
        required=False,
    )
    details_url: str = gestalt.field(
        description="Optional details URL", default="", required=False
    )
    external_id: str = gestalt.field(
        description="Optional caller-owned external ID", default="", required=False
    )
    output: CheckRunOutputInput | None = gestalt.field(
        description="Optional check run output",
        default=None,
        required=False,
    )
    installation_id: int = gestalt.field(
        description="GitHub App installation ID. If omitted, it is taken from the webhook service account subject.",
        default=0,
        required=False,
    )


class UpdateCheckRunInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    check_run_id: int = gestalt.field(description="GitHub check run ID")
    name: str = gestalt.field(
        description="Optional replacement check run name", default="", required=False
    )
    status: str = gestalt.field(
        description="Optional check run status", default="", required=False
    )
    conclusion: str = gestalt.field(
        description="Optional completed check conclusion", default="", required=False
    )
    details_url: str = gestalt.field(
        description="Optional details URL", default="", required=False
    )
    output: CheckRunOutputInput | None = gestalt.field(
        description="Optional check run output",
        default=None,
        required=False,
    )
    completed_at: str = gestalt.field(
        description="Optional ISO timestamp for completed check runs",
        default="",
        required=False,
    )
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
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
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
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
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
        description="GitHub App installation ID. If omitted, it is resolved from external_identity or the legacy webhook service account subject.",
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
        description="Deprecated boolean self-fix preference. False disables self-fix; true/null leave self_fix_mode in effect.",
        default=None,
        required=False,
    )
    self_fix_mode: str | None = gestalt.field(
        description="Self-fix mode: disabled, suggest, branch_commit for same-PR commits, pull_request for follow-up PRs, or null for policy default.",
        default=None,
        required=False,
    )


class ActionPreferenceTargetsInput(gestalt.Model):
    identity_kind: str = gestalt.field(
        description="Preference identity kind: external_subject_id or subject_id. Defaults to the caller's linked GitHub identity when present.",
        default="",
        required=False,
    )


@app.configure
def configure(_name: str, config: dict[str, Any]) -> None:
    reset_action_preference_store()
    configure_from_mapping(config, provider_name=_name)


@gestalt.post_connect
def post_connect(token: gestalt.ConnectedToken) -> PostConnectMetadata:
    if token.connection != "default":
        return {}
    return user_external_identity_metadata(token.access_token)


@app.http_subject
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


@app.operation(
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
    policy: GitHubWebhookPolicy | None = None
    pull_request_context = None
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
        if (
            pull_request_context is None
            and _needs_pull_request_context_for_branch_commit(input, summary, policy)
        ):
            try:
                pull_request_context = _fetch_pull_request_webhook_context(req, summary)
            except ValueError as err:
                return _bad_request(str(err))
            except GitHubAuthorizationError as err:
                return _forbidden(str(err))
            except GitHubConfigError as err:
                return _server_error(str(err))
            except GitHubAPIError as err:
                return _github_error(err)
        if _review_pull_request_disabled_by_preference(policy):
            return {
                "ok": True,
                "ignored": "policy_preference_disabled:code_review_comments",
            }

    return _signal_or_start_webhook_workflow(
        input,
        req,
        summary=summary,
        policy=policy,
        pull_request_context=pull_request_context,
    )


def _signal_or_start_webhook_workflow(
    input: dict[str, Any],
    req: gestalt.Request,
    *,
    summary: dict[str, Any],
    policy: GitHubWebhookPolicy | None,
    pull_request_context: GitHubPullRequestContext | None,
) -> OperationResult:
    workflow_key = ""
    workflow_request = build_workflow_signal_or_start_request(
        input, summary, policy, pull_request_context=pull_request_context
    )
    workflow_key = workflow_request.workflow_key.strip()
    review_check_run: dict[str, Any] | None = None
    if policy is not None and _policy_targets_review_pull_request(policy):
        try:
            review_check_run = _ensure_review_check_run_for_workflow(
                input,
                req,
                summary=summary,
                policy=policy,
                workflow_request=workflow_request,
            )
        except ValueError as err:
            return _bad_request(str(err))
        except GitHubAuthorizationError as err:
            return _forbidden(str(err))
        except GitHubConfigError as err:
            return _server_error(str(err))
        except GitHubAPIError as err:
            return _github_error(err)
        if review_check_run is not None:
            workflow_request = build_workflow_signal_or_start_request(
                input,
                summary,
                policy,
                extra_signal_data={
                    "review_check_run": check_run_summary(review_check_run)
                },
                pull_request_context=pull_request_context,
            )
            workflow_key = workflow_request.workflow_key.strip()
    try:
        logger.info(
            "dispatching GitHub webhook workflow",
            extra={
                "github_event": summary.get("event_type", ""),
                "github_action": summary.get("action", ""),
                "github_delivery_id": summary.get("delivery_id", ""),
                "github_repository": summary.get("repository", ""),
                "github_webhook_policy": policy.id if policy is not None else "",
                "workflow_key": workflow_key,
                "workflow_provider": workflow_request.provider_name,
            },
        )
        with req.workflows() as workflows:
            response = workflows.signal_or_start_run(workflow_request)
    except Exception as err:
        _complete_review_check_run_dispatch_failed(
            review_check_run, req, summary=summary
        )
        logger.exception(
            "failed to dispatch GitHub webhook workflow",
            extra={
                "github_event": summary.get("event_type", ""),
                "github_action": summary.get("action", ""),
                "github_delivery_id": summary.get("delivery_id", ""),
                "github_repository": summary.get("repository", ""),
                "github_webhook_policy": policy.id if policy is not None else "",
                "workflow_key": workflow_key,
            },
        )
        return _service_unavailable(f"failed to dispatch workflow run: {err}")

    workflow_key = str(
        response.workflow_key
        or (response.run.workflow_key if response.run is not None else "")
    ).strip()
    logger.info(
        "dispatched GitHub webhook workflow",
        extra={
            "github_event": summary.get("event_type", ""),
            "github_action": summary.get("action", ""),
            "github_delivery_id": summary.get("delivery_id", ""),
            "github_repository": summary.get("repository", ""),
            "github_webhook_policy": policy.id if policy is not None else "",
            "workflow_key": workflow_key,
            "workflow_provider": response.provider_name
            or get_github_config().workflow_provider,
            "workflow_run_id": response.run.id if response.run is not None else "",
            "workflow_signal_id": response.signal.id
            if response.signal is not None
            else "",
            "workflow_started_run": response.started_run,
        },
    )

    return {
        "ok": True,
        "dispatch": "workflow",
        "workflow_provider": response.provider_name
        or get_github_config().workflow_provider,
        "workflow_run_id": response.run.id if response.run is not None else "",
        "workflow_key": workflow_key,
        "workflow_signal_id": response.signal.id if response.signal is not None else "",
        "workflow_started_run": response.started_run,
    }


def _policy_targets_review_pull_request(policy: GitHubWebhookPolicy) -> bool:
    if policy.workflow_target is None:
        return False
    return (
        policy.workflow_target.app_name == "github"
        and policy.workflow_target.operation == REVIEW_PULL_REQUEST_OPERATION
    )


def _ensure_review_check_run_for_workflow(
    input: dict[str, Any],
    req: gestalt.Request,
    *,
    summary: dict[str, Any],
    policy: GitHubWebhookPolicy,
    workflow_request: gestalt.WorkflowSignalOrStartRun,
) -> dict[str, Any] | None:
    if _review_target_dry_run(policy):
        return None
    context = _review_check_run_context(input, req, summary=summary, policy=policy)
    if context is None:
        return None
    external_id = _review_check_run_external_id(
        workflow_request.idempotency_key.strip()
    )
    existing = _find_reusable_review_check_run(
        req,
        owner=context["owner"],
        repo=context["repo"],
        head_sha=context["head_sha"],
        check_run_name=context["check_run_name"],
        external_id=external_id,
        installation_id=context["installation_id"],
    )
    if existing is not None:
        if str_field(existing, "status") != "completed" or (
            _review_check_run_was_dispatch_failed(existing)
        ):
            return update_check_run(
                GitHubUpdateCheckRunRequest(
                    owner=context["owner"],
                    repo=context["repo"],
                    check_run_id=int_field(existing, "id"),
                    status="in_progress",
                    output=_review_check_run_running_output(),
                    installation_id=context["installation_id"],
                ),
                subject=req.subject,
                external_identity=_request_external_identity(req),
            )
        return existing
    return create_check_run(
        GitHubCreateCheckRunRequest(
            owner=context["owner"],
            repo=context["repo"],
            name=context["check_run_name"],
            head_sha=context["head_sha"],
            status="in_progress",
            external_id=external_id,
            output=_review_check_run_running_output(),
            installation_id=context["installation_id"],
        ),
        subject=req.subject,
        external_identity=_request_external_identity(req),
    )


def _review_target_dry_run(policy: GitHubWebhookPolicy) -> bool:
    return (
        policy.workflow_target is not None
        and policy.workflow_target.input.get("dryRun") is True
    )


def _review_check_run_context(
    input: dict[str, Any],
    req: gestalt.Request,
    *,
    summary: dict[str, Any],
    policy: GitHubWebhookPolicy,
) -> dict[str, Any] | None:
    event_type = str(summary.get("event_type", "")).strip()
    action = str(summary.get("action", "")).strip()
    owner = str(summary.get("repository_owner", "")).strip()
    repo = str(summary.get("repository_name", "")).strip()
    installation_id = int(summary.get("installation_id", 0) or 0)
    if not owner or not repo or installation_id <= 0:
        raise ValueError("review check run requires repository and installation")
    pull_number = int(summary.get("number", 0) or 0)
    head_sha = str(summary.get("head_sha", "")).strip()
    if event_type == "pull_request":
        if action not in SUPPORTED_PULL_REQUEST_ACTIONS:
            return None
        if pull_number <= 0 or not head_sha:
            raise ValueError(
                "review check run requires pull request number and head SHA"
            )
    elif event_type == "issue_comment":
        if action != "created":
            return None
        issue = map_field(input, "issue")
        if not map_field(issue, "pull_request"):
            return None
        comment_body = str_field(map_field(input, "comment"), "body")
        if policy.trigger.require_app_mention:
            if not app_mention_body_matches(comment_body):
                return None
        else:
            if not policy.trigger.manual_commands:
                return None
            if not manual_command_body_matches(
                comment_body,
                policy.trigger.manual_commands,
                match_mode=policy.trigger.manual_command_match,
            ):
                return None
        if pull_number <= 0:
            raise ValueError("review check run requires pull request number")
        pull = get_pull_request(
            GitHubPullRequestRequest(
                owner=owner,
                repo=repo,
                pull_number=pull_number,
                installation_id=installation_id,
            ),
            subject=req.subject,
            external_identity=_request_external_identity(req),
        )
        pull_summary = pull_request_summary(pull)
        head_sha = str(pull_summary.get("head_sha", "")).strip()
        if not head_sha:
            raise ValueError("review check run requires pull request head SHA")
    else:
        return None
    return {
        "owner": owner,
        "repo": repo,
        "pull_number": pull_number,
        "head_sha": head_sha,
        "installation_id": installation_id,
        "check_run_name": _review_check_run_name(policy),
    }


def _review_check_run_name(policy: GitHubWebhookPolicy) -> str:
    if policy.workflow_target is None:
        return "Gestalt Review"
    if (
        isinstance(policy.workflow_target.input.get("checkRunName"), str)
        and policy.workflow_target.input["checkRunName"].strip()
    ):
        return policy.workflow_target.input["checkRunName"].strip()
    return "Gestalt Review"


def _review_check_run_external_id(idempotency_key: str) -> str:
    return (
        "gestalt-review:" + hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
    )


def _find_reusable_review_check_run(
    req: gestalt.Request,
    *,
    owner: str,
    repo: str,
    head_sha: str,
    check_run_name: str,
    external_id: str,
    installation_id: int,
) -> dict[str, Any] | None:
    matching: list[dict[str, Any]] = []
    for page in range(1, 11):
        data = list_check_runs_for_ref(
            GitHubListCheckRunsForRefRequest(
                owner=owner,
                repo=repo,
                ref=head_sha,
                check_name=check_run_name,
                filter="all",
                per_page=100,
                page=page,
                installation_id=installation_id,
            ),
            subject=req.subject,
            external_identity=_request_external_identity(req),
        )
        raw_check_runs = data.get("check_runs")
        if not isinstance(raw_check_runs, list):
            raw_check_runs = []
        for check_run in raw_check_runs:
            if (
                isinstance(check_run, dict)
                and str_field(check_run, "external_id") == external_id
            ):
                matching.append(check_run)
        if len(raw_check_runs) < 100:
            break
    for check_run in matching:
        if str_field(check_run, "status") != "completed":
            return check_run
    return matching[0] if matching else None


def _review_check_run_was_dispatch_failed(check_run: dict[str, Any]) -> bool:
    output = map_field(check_run, "output")
    return str_field(output, "title") == _REVIEW_DISPATCH_FAILED_TITLE


def _review_check_run_running_output() -> GitHubCheckRunOutput:
    return GitHubCheckRunOutput(
        title="Review running",
        summary="Gestalt is reviewing this pull request.",
    )


def _complete_review_check_run_dispatch_failed(
    check_run: dict[str, Any] | None,
    req: gestalt.Request,
    *,
    summary: dict[str, Any],
) -> None:
    if check_run is None or str_field(check_run, "status") == "completed":
        return
    owner = str(summary.get("repository_owner", "")).strip()
    repo = str(summary.get("repository_name", "")).strip()
    installation_id = int(summary.get("installation_id", 0) or 0)
    check_run_id = int_field(check_run, "id")
    if not owner or not repo or installation_id <= 0 or check_run_id <= 0:
        return
    try:
        update_check_run(
            GitHubUpdateCheckRunRequest(
                owner=owner,
                repo=repo,
                check_run_id=check_run_id,
                conclusion="failure",
                completed_at=utc_timestamp(),
                output=GitHubCheckRunOutput(
                    title=_REVIEW_DISPATCH_FAILED_TITLE,
                    summary="Gestalt could not enqueue the pull request review workflow.",
                ),
                installation_id=installation_id,
            ),
            subject=req.subject,
            external_identity=_request_external_identity(req),
        )
    except Exception:
        logger.exception(
            "failed to mark GitHub review check run dispatch failure",
            extra={
                "github_event": summary.get("event_type", ""),
                "github_delivery_id": summary.get("delivery_id", ""),
                "github_repository": summary.get("repository", ""),
                "check_run_id": check_run_id,
            },
        )


def _stale_ci_head_ignored_reason(
    summary: dict[str, Any],
    policy: GitHubWebhookPolicy,
    pull_request_context: GitHubPullRequestContext | None,
) -> str:
    if not policy.comments.suppress_stale_head:
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
    policy: GitHubWebhookPolicy,
) -> GitHubPullRequestContext | None:
    if not _needs_pull_request_context(input, summary, policy):
        return None
    return _fetch_pull_request_webhook_context(req, summary)


def _fetch_pull_request_webhook_context(
    req: gestalt.Request, summary: dict[str, Any]
) -> GitHubPullRequestContext | None:
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
    input: dict[str, Any], summary: dict[str, Any], policy: GitHubWebhookPolicy
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


def _needs_pull_request_context_for_branch_commit(
    input: dict[str, Any], summary: dict[str, Any], policy: GitHubWebhookPolicy
) -> bool:
    if policy.self_fix_mode != SELF_FIX_BRANCH_COMMIT:
        return False
    if not policy.allow_self_fix:
        return False
    if BOT_COMMIT_FILES_OPERATION not in effective_policy_operations(policy):
        return False
    if _pull_request_payload_has_same_pr_commit_metadata(input):
        return False
    return _single_pull_request_number(summary.get("pull_request_numbers")) > 0


def _pull_request_payload_has_same_pr_commit_metadata(input: dict[str, Any]) -> bool:
    pull_request = map_field(input, "pull_request")
    if not pull_request:
        return False
    head_repo = map_field(map_field(pull_request, "head"), "repo")
    base_repo = map_field(map_field(pull_request, "base"), "repo")
    return bool(
        nested_str(pull_request, "head", "ref")
        and nested_str(pull_request, "base", "ref")
        and str_field(head_repo, "full_name")
        and str_field(base_repo, "full_name")
    )


def _needs_pull_request_context_for_stale(
    summary: dict[str, Any], policy: GitHubWebhookPolicy
) -> bool:
    if not policy.comments.suppress_stale_head:
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
    policy: GitHubWebhookPolicy,
    pull_request_context: GitHubPullRequestContext | None,
) -> GitHubWebhookPolicy:
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


def _review_pull_request_disabled_by_preference(policy: GitHubWebhookPolicy) -> bool:
    if (
        not policy.action_preferences
        or policy.action_preferences.get("source") == "config_default"
    ):
        return False
    if policy.workflow_target is None:
        return False
    return (
        policy.workflow_target.app_name == "github"
        and policy.workflow_target.operation == REVIEW_PULL_REQUEST_OPERATION
        and not policy.allow_code_review_comments
    )


def _single_pull_request_number(value: Any) -> int:
    if not isinstance(value, list) or len(value) != 1:
        return 0
    number = value[0]
    if isinstance(number, bool) or not isinstance(number, (int, float)):
        return 0
    return int(number) if int(number) > 0 else 0


@app.operation(
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
        config = get_github_config()
        settings = ReviewSettings(
            agent_provider=input.agentProvider.strip()
            or config.agent_provider
            or DEFAULT_AGENT_PROVIDER,
            model=input.model.strip() or config.agent_model or DEFAULT_MODEL,
            system_prompt=input.systemPrompt.strip() or DEFAULT_SYSTEM_PROMPT,
            max_comments=max(1, min(25, input.maxComments)),
            max_files=max(1, min(100, input.maxFiles)),
            max_patch_chars=max(4_000, min(200_000, input.maxPatchChars)),
            changed_lines_only=input.changedLinesOnly,
            dry_run=input.dryRun,
            auto_resolve_stale_findings=input.autoResolveStaleFindings,
            check_run_name=input.checkRunName.strip() or "Gestalt Review",
            turn_timeout_ms=max(10_000, min(600_000, input.turnTimeoutMs)),
            poll_interval_ms=max(250, min(10_000, input.pollIntervalMs)),
        )
        return {"data": review_pull_request(settings, req)}
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


@app.operation(
    id=ACTION_PREFERENCES_LIST_TARGETS_OPERATION,
    method="GET",
    description=(
        "List repositories and GitHub webhook action controls this caller can configure"
    ),
)
def github_action_preferences_list_targets(
    input: ActionPreferenceTargetsInput, req: gestalt.Request
) -> OperationResult:
    app_config = get_github_config()
    config = app_config.action_preferences
    if not config.enabled:
        return _failed_precondition("GitHub action preferences are not configured")
    token = req.token.strip()
    if not token:
        return _failed_precondition(
            "A connected GitHub OAuth token is required to list configurable repositories"
        )

    try:
        identity = caller_preference_identity(req, input.identity_kind)
        identity_kind = normalize_identity_kind(input.identity_kind, identity)
        repositories = _action_preference_target_repositories(
            app_config.webhook_policies,
            token=token,
            identity=identity,
            identity_kind=identity_kind,
            fallback_identity_kind=(
                "subject_id" if not input.identity_kind.strip() else ""
            ),
            preferences_config=config,
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _action_preferences_github_error(err)
    except RuntimeError as err:
        return _failed_precondition(str(err))
    except Exception as err:
        return _service_unavailable(str(err))

    return {
        "data": {
            "identity": _preference_identity_summary(identity, identity_kind),
            "repositories": repositories,
        }
    }


@app.operation(
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
    except GitHubAPIError as err:
        return _action_preferences_github_error(err)
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


@app.operation(
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
            self_fix_mode=input.self_fix_mode,
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAPIError as err:
        return _action_preferences_github_error(err)
    except RuntimeError as err:
        return _failed_precondition(str(err))
    except Exception as err:
        return _service_unavailable(str(err))
    return {"data": {"preference": _preference_summary(preference)}}


@app.operation(
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
    except GitHubAPIError as err:
        return _action_preferences_github_error(err)
    except RuntimeError as err:
        return _failed_precondition(str(err))
    except Exception as err:
        return _service_unavailable(str(err))
    return {"data": {"deleted": deleted}}


@app.operation(
    id=BOT_RESOLVE_INSTALLATION_OPERATION,
    method="GET",
    description="Resolve the GitHub App installation and runAs identities for a repository",
)
def bot_resolve_installation(
    input: ResolveInstallationInput, _req: gestalt.Request
) -> OperationResult:
    try:
        resolution = resolve_repository_installation(input.owner, input.repo)
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    return {"data": installation_resolution_dict(resolution)}


@app.operation(
    id=BOT_GET_REPOSITORY_OPERATION,
    method="GET",
    description="Get repository metadata using a GitHub App installation token",
    tags=["repo", "repository"],
)
def bot_get_repository(input: RepositoryInput, req: gestalt.Request) -> OperationResult:
    try:
        repo = get_repository(
            GitHubRepositoryRequest(
                owner=input.owner,
                repo=input.repo,
                installation_id=input.installation_id,
            ),
            subject=req.subject,
            external_identity=_request_external_identity(req),
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    return {"data": {"repository": repository_summary(repo)}}


@app.operation(
    id=BOT_SEARCH_CODE_OPERATION,
    method="GET",
    description="Search code in one repository using a GitHub App installation token",
    tags=["repo", "code", "search"],
)
def bot_search_code(input: SearchCodeInput, req: gestalt.Request) -> OperationResult:
    try:
        results = search_code(
            GitHubCodeSearchRequest(
                owner=input.owner,
                repo=input.repo,
                query=input.query,
                path=input.path,
                per_page=input.per_page,
                page=input.page,
                installation_id=input.installation_id,
            ),
            subject=req.subject,
            external_identity=_request_external_identity(req),
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    return {"data": code_search_summary(results)}


@app.operation(
    id=BOT_GET_CONTENT_OPERATION,
    method="GET",
    description="Get UTF-8 file content using a GitHub App installation token",
    tags=["repo", "code", "content"],
)
def bot_get_content(input: GetContentInput, req: gestalt.Request) -> OperationResult:
    try:
        content = get_file_text_at_ref(
            GitHubFileContentRequest(
                owner=input.owner,
                repo=input.repo,
                path=input.path,
                ref=input.ref,
                max_bytes=input.max_bytes,
                installation_id=input.installation_id,
            ),
            subject=req.subject,
            external_identity=_request_external_identity(req),
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    return {"data": {"path": input.path, "ref": input.ref, "content": content}}


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
    id=USER_CREATE_PULL_REQUEST_OPERATION,
    method="POST",
    description="Commit file changes and open a pull request using the caller's GitHub OAuth token",
    tags=["pr", "prs"],
)
def user_create_pull_request(
    input: UserCreatePullRequestInput, req: gestalt.Request
) -> OperationResult:
    token = req.token.strip()
    if not token:
        return gestalt.Response(
            status=HTTPStatus.UNAUTHORIZED, body={"error": "token is required"}
        )
    try:
        result = create_pull_request_with_user_files(
            GitHubUserCreatePullRequestRequest(
                owner=input.owner,
                repo=input.repo,
                title=input.title,
                message=input.message,
                files=_file_changes_from_input(input.files),
                body=input.body,
                branch=input.branch,
                base=input.base,
                coauthors=_coauthors_from_input(input.coauthors),
                include_bot_coauthor=input.include_bot_coauthor,
                force=input.force,
                draft=input.draft,
                maintainer_can_modify=input.maintainer_can_modify,
            ),
            access_token=token,
        )
    except ValueError as err:
        return _bad_request(str(err))
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
    id=BOT_CREATE_CHECK_RUN_OPERATION,
    method="POST",
    description="Create a GitHub check run using a GitHub App installation token",
)
def bot_create_check_run(
    input: CreateCheckRunInput, req: gestalt.Request
) -> OperationResult:
    try:
        check_run = create_check_run(
            GitHubCreateCheckRunRequest(
                owner=input.owner,
                repo=input.repo,
                name=input.name,
                head_sha=input.head_sha,
                status=input.status,
                conclusion=input.conclusion,
                details_url=input.details_url,
                external_id=input.external_id,
                output=_check_run_output_from_input(input.output),
                installation_id=input.installation_id,
            ),
            subject=req.subject,
            external_identity=_request_external_identity(req),
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


@app.operation(
    id=BOT_UPDATE_CHECK_RUN_OPERATION,
    method="POST",
    description="Update a GitHub check run using a GitHub App installation token",
)
def bot_update_check_run(
    input: UpdateCheckRunInput, req: gestalt.Request
) -> OperationResult:
    try:
        check_run = update_check_run(
            GitHubUpdateCheckRunRequest(
                owner=input.owner,
                repo=input.repo,
                check_run_id=input.check_run_id,
                name=input.name,
                status=input.status,
                conclusion=input.conclusion,
                details_url=input.details_url,
                output=_check_run_output_from_input(input.output),
                completed_at=input.completed_at,
                installation_id=input.installation_id,
            ),
            subject=req.subject,
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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


@app.operation(
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
            external_identity=_request_external_identity(req),
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
        expected_head_sha=input.expected_head_sha,
    )


def _commit_author_from_request(
    input_name: str, input_email: str, req: gestalt.Request
) -> tuple[str, str]:
    external_identity = req.agent_external_identity
    if external_identity.type.strip() == GITHUB_EXTERNAL_IDENTITY_TYPE:
        user_id = _github_user_id_from_external_identity(external_identity.id)
        user_identity = DEFAULT_GITHUB_CLIENT.user_identity_by_id(user_id)
        if user_identity is not None and user_identity.email:
            return user_identity.name, user_identity.email
    return input_name, input_email


def _github_user_id_from_external_identity(identity_id: str) -> str:
    prefix, _, user_id = str(identity_id or "").strip().partition(":")
    if prefix != "user":
        return ""
    return user_id.strip()


def _request_external_identity(req: gestalt.Request) -> gestalt.ExternalIdentity | None:
    # This is the delegated GitHub App installation identity authorized by the
    # host. Do not fall back to agent_external_identity; that field identifies
    # the original agent caller's GitHub user.
    return non_empty_external_identity(req.external_identity)


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


def _check_run_output_from_input(
    output: CheckRunOutputInput | None,
) -> GitHubCheckRunOutput | None:
    if output is None:
        return None
    return GitHubCheckRunOutput(
        title=output.title,
        summary=output.summary,
        text=output.text,
    )


def _action_preference_target_repositories(
    policies: tuple[GitHubWebhookPolicy, ...],
    *,
    token: str,
    identity: GitHubPreferenceIdentity,
    identity_kind: str,
    fallback_identity_kind: str,
    preferences_config: GitHubActionPreferencesConfig,
) -> list[dict[str, Any]]:
    installed_repositories = _installed_app_repositories()
    repositories: list[dict[str, Any]] = []
    for repository in installed_repositories:
        accessible = _accessible_repository_summary(repository, token)
        if accessible is None:
            continue
        controls = _action_preference_controls_for_repository(
            policies,
            accessible["repository"],
            identity=identity,
            identity_kind=identity_kind,
            fallback_identity_kind=fallback_identity_kind,
            preferences_config=preferences_config,
        )
        if controls:
            accessible["controls"] = controls
            repositories.append(accessible)
    repositories.sort(key=lambda item: str(item.get("repository", "")).lower())
    return repositories


def _installed_app_repositories() -> list[dict[str, Any]]:
    repositories: dict[str, dict[str, Any]] = {}
    page = 1
    while True:
        installations = DEFAULT_GITHUB_CLIENT.app_installations(per_page=100, page=page)
        for installation in installations:
            installation_id = _positive_int(installation.get("id"))
            if installation_id <= 0:
                continue
            try:
                installation_token = DEFAULT_GITHUB_CLIENT.installation_token(
                    installation_id
                )
            except GitHubAPIError as err:
                logger.warning(
                    "GitHub action preference target installation skipped: "
                    "installation_id=%s status=%s error=%s",
                    installation_id,
                    err.status,
                    err.message,
                )
                continue
            repo_page = 1
            while True:
                try:
                    page_repositories = DEFAULT_GITHUB_CLIENT.installation_repositories(
                        installation_token,
                        per_page=100,
                        page=repo_page,
                    )
                except GitHubAPIError as err:
                    logger.warning(
                        "GitHub action preference target repositories skipped: "
                        "installation_id=%s page=%s status=%s error=%s",
                        installation_id,
                        repo_page,
                        err.status,
                        err.message,
                    )
                    break
                for repository in page_repositories:
                    full_name = _repository_full_name(repository)
                    if full_name and full_name not in repositories:
                        repositories[full_name] = {
                            "repository": full_name,
                            "html_url": str_field(repository, "html_url"),
                            "installation_id": installation_id,
                        }
                if len(page_repositories) < 100:
                    break
                repo_page += 1
        if len(installations) < 100:
            break
        page += 1
    return list(repositories.values())


def _accessible_repository_summary(
    repository: dict[str, Any], token: str
) -> dict[str, Any] | None:
    full_name = str_field(repository, "repository")
    owner, separator, repo = full_name.partition("/")
    if not owner or not separator or not repo:
        return None
    try:
        response = DEFAULT_GITHUB_CLIENT.github_json(
            "GET", repo_path(owner, repo), token
        )
    except GitHubAPIError as err:
        if err.status in {HTTPStatus.FORBIDDEN, HTTPStatus.NOT_FOUND}:
            return None
        raise
    accessible_full_name = _repository_full_name(response) or full_name
    return {
        "repository": accessible_full_name,
        "html_url": str_field(response, "html_url")
        or str_field(repository, "html_url"),
        "installation_id": repository.get("installation_id"),
    }


def _action_preference_controls_for_repository(
    policies: tuple[GitHubWebhookPolicy, ...],
    repository: str,
    *,
    identity: GitHubPreferenceIdentity,
    identity_kind: str,
    fallback_identity_kind: str,
    preferences_config: GitHubActionPreferencesConfig,
) -> list[dict[str, Any]]:
    controls: list[dict[str, Any]] = []
    for policy in policies:
        if not _policy_matches_repository(policy, repository):
            continue
        fields = _policy_action_preference_fields(policy)
        if not fields:
            continue
        preference, preference_identity_kind = _action_preference_record_for_policy(
            preferences_config,
            repository=repository,
            policy_id=policy.id,
            identity=identity,
            identity_kind=identity_kind,
            fallback_identity_kind=fallback_identity_kind,
        )
        for field in fields:
            controls.append(
                _action_preference_control_summary(
                    policy,
                    field,
                    preference,
                    identity_kind=preference_identity_kind,
                    multiple_controls=len(fields) > 1,
                )
            )
    return controls


def _action_preference_record_for_policy(
    preferences_config: GitHubActionPreferencesConfig,
    *,
    repository: str,
    policy_id: str,
    identity: GitHubPreferenceIdentity,
    identity_kind: str,
    fallback_identity_kind: str,
) -> tuple[dict[str, Any] | None, str]:
    candidates = [identity_kind]
    if fallback_identity_kind and fallback_identity_kind not in candidates:
        candidates.append(fallback_identity_kind)
    for candidate in candidates:
        try:
            preference = get_action_preference(
                preferences_config,
                repository=repository,
                policy_id=policy_id,
                identity=identity,
                identity_kind=candidate,
            )
        except ValueError:
            continue
        if preference is not None:
            return preference, candidate
    return None, identity_kind


def _policy_matches_repository(policy: GitHubWebhookPolicy, repository: str) -> bool:
    repositories = tuple(item.strip() for item in policy.match.repositories if item)
    return not repositories or repository in repositories


def _policy_action_preference_fields(
    policy: GitHubWebhookPolicy,
) -> tuple[str, ...]:
    operations = set(effective_policy_operations(policy))
    fields: list[str] = []
    is_review_workflow = (
        policy.workflow_target is not None
        and policy.workflow_target.app_name == "github"
        and policy.workflow_target.operation == REVIEW_PULL_REQUEST_OPERATION
    )
    if BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION in operations or is_review_workflow:
        fields.append("allow_code_review_comments")
    if operations.intersection(_SELF_FIX_OPERATIONS) or (
        policy.allow_self_fix and policy.self_fix_mode != SELF_FIX_DISABLED
    ):
        fields.append("self_fix_mode")
    return tuple(fields)


def _action_preference_control_summary(
    policy: GitHubWebhookPolicy,
    field: str,
    preference: dict[str, Any] | None,
    *,
    identity_kind: str,
    multiple_controls: bool,
) -> dict[str, Any]:
    raw_stored = _raw_stored_preference_value(field, preference)
    stored = _stored_preference_value(field, raw_stored)
    default_label = _ACTION_PREFERENCE_CONTROL_LABELS[field]
    if policy.display_name and not multiple_controls:
        label = policy.display_name
    elif policy.display_name:
        label = f"{policy.display_name}: {default_label}"
    else:
        label = default_label
    return {
        "id": f"{policy.id}:{field}",
        "policy_id": policy.id,
        "identity_kind": identity_kind,
        "field": field,
        "label": label,
        "description": policy.description
        or _ACTION_PREFERENCE_CONTROL_DESCRIPTIONS[field],
        "type": "enum" if field == "self_fix_mode" else "boolean",
        "options": _action_preference_control_options(field, policy),
        "config_default": _config_default_preference_value(field, policy),
        "stored": stored,
        "effective": _effective_preference_value(field, policy, stored),
    }


def _raw_stored_preference_value(field: str, preference: dict[str, Any] | None) -> Any:
    if preference is None:
        return None
    if field == "self_fix_mode" and preference.get("allow_self_fix") is False:
        return SELF_FIX_DISABLED
    return preference.get(field)


def _stored_preference_value(field: str, value: Any) -> Any:
    if field == "self_fix_mode":
        return value if isinstance(value, str) and value in SELF_FIX_MODES else None
    return value if isinstance(value, bool) else None


def _config_default_preference_value(field: str, policy: GitHubWebhookPolicy) -> Any:
    if field == "self_fix_mode":
        return policy.self_fix_mode if policy.allow_self_fix else SELF_FIX_DISABLED
    return True


def _effective_preference_value(
    field: str, policy: GitHubWebhookPolicy, stored: Any
) -> Any:
    if field == "self_fix_mode":
        if not policy.allow_self_fix or policy.self_fix_mode == SELF_FIX_DISABLED:
            return SELF_FIX_DISABLED
        if isinstance(stored, str):
            max_rank = SELF_FIX_MODES.index(policy.self_fix_mode)
            stored_rank = SELF_FIX_MODES.index(stored)
            return SELF_FIX_MODES[min(max_rank, stored_rank)]
        return policy.self_fix_mode
    return True if stored is None else stored


def _action_preference_control_options(
    field: str, policy: GitHubWebhookPolicy
) -> list[dict[str, str]]:
    if field != "self_fix_mode":
        return []
    max_rank = SELF_FIX_MODES.index(policy.self_fix_mode)
    labels = {
        "disabled": "Disabled",
        "suggest": "Suggest only",
        "branch_commit": "Commit to same PR",
        "pull_request": "Open follow-up PR",
    }
    return [
        {"value": mode, "label": labels[mode]}
        for mode in SELF_FIX_MODES[: max_rank + 1]
    ]


def _preference_identity_summary(
    identity: GitHubPreferenceIdentity, identity_kind: str
) -> dict[str, Any]:
    return {
        "identity_kind": identity_kind,
        "external_identity_type": identity.external_identity_type,
        "external_subject_id": identity.external_subject_id,
        "subject_id": identity.subject_id,
    }


def _repository_full_name(repository: dict[str, Any]) -> str:
    full_name = str_field(repository, "full_name")
    if full_name:
        return full_name
    owner = repository.get("owner")
    owner_login = str_field(owner, "login") if isinstance(owner, dict) else ""
    name = str_field(repository, "name")
    return f"{owner_login}/{name}" if owner_login and name else ""


def _positive_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    result = int(value)
    return result if result > 0 else 0


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
            "self_fix_mode",
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


def _action_preferences_github_error(
    err: GitHubAPIError,
) -> gestalt.Response[dict[str, Any]]:
    if err.status == HTTPStatus.UNAUTHORIZED:
        return _failed_precondition(
            "GitHub rejected the connected OAuth token; reconnect GitHub"
        )
    return _github_error(err)
