from __future__ import annotations

import logging
from dataclasses import fields
from http import HTTPStatus
from typing import Any, Callable, TypeAlias, TypeVar

import gestalt
from gestalt.authorization import RelationshipTargetSubject
from gestalt.migrations import MigrationRunOptions

from internals.cache_ingest import ingest_webhook_event
from internals.cache_migrations import cache_migration_options
from internals.client import DEFAULT_GITHUB_CLIENT
from internals.config import (
    configure_from_mapping,
    get_github_config,
)
from internals.constants import (
    BOT_ADD_LABELS_OPERATION,
    BOT_ADD_REACTION_OPERATION,
    BOT_CLOSE_PULL_REQUEST_OPERATION,
    BOT_COMMIT_FILES_OPERATION,
    BOT_CREATE_CHECK_RUN_OPERATION,
    BOT_CREATE_ISSUE_OPERATION,
    BOT_CREATE_ISSUE_COMMENT_OPERATION,
    BOT_CREATE_PULL_REQUEST_OPERATION,
    BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
    BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
    BOT_GET_CHECK_RUN_OPERATION,
    BOT_GET_CONTENT_OPERATION,
    BOT_GET_ISSUE_OPERATION,
    BOT_LIST_COMMITS_OPERATION,
    BOT_LIST_ISSUES_OPERATION,
    BOT_COMPARE_REFS_OPERATION,
    BOT_GET_PULL_REQUEST_OPERATION,
    BOT_GET_REPOSITORY_OPERATION,
    BOT_GET_MERGE_QUEUE_OPERATION,
    BOT_GET_USER_OPERATION,
    BOT_GET_WORKFLOW_JOB_LOGS_OPERATION,
    BOT_GET_WORKFLOW_RUN_OPERATION,
    BOT_LIST_CHECK_RUN_ANNOTATIONS_OPERATION,
    BOT_LIST_CHECK_SUITE_CHECK_RUNS_OPERATION,
    BOT_LIST_COMMIT_CHECK_RUNS_OPERATION,
    BOT_LIST_ISSUE_COMMENTS_OPERATION,
    BOT_LIST_ORG_MEMBERS_OPERATION,
    BOT_LIST_PULL_REQUEST_FILES_OPERATION,
    BOT_LIST_PULL_REQUEST_COMMITS_OPERATION,
    BOT_LIST_PULL_REQUEST_REVIEWS_OPERATION,
    BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION,
    BOT_LIST_PULL_REQUESTS_FOR_COMMIT_OPERATION,
    BOT_LIST_PULL_REQUESTS_OPERATION,
    BOT_LIST_REPO_CONTRIBUTORS_OPERATION,
    BOT_LIST_WORKFLOW_RUN_JOBS_OPERATION,
    BOT_LIST_WORKFLOW_RUNS_OPERATION,
    BOT_SEARCH_PULL_REQUESTS_OPERATION,
    BOT_OPEN_PULL_REQUEST_OPERATION,
    BOT_REMOVE_LABELS_OPERATION,
    BOT_REQUEST_REVIEWERS_OPERATION,
    BOT_RESOLVE_PULL_REQUEST_REVIEW_THREAD_OPERATION,
    BOT_SEARCH_CODE_OPERATION,
    BOT_UPDATE_CHECK_RUN_OPERATION,
    BOT_UPDATE_ISSUE_OPERATION,
    GITHUB_EVENT_OPERATION,
    GITHUB_USER_LINKED_ACTION,
    GITHUB_USER_RESOURCE_TYPE,
    IDENTITY_LINK_SELF_OPERATION,
    MAINTENANCE_RECONCILE_CACHE_OPERATION,
)
from internals.errors import GitHubAPIError, GitHubAuthorizationError, GitHubConfigError
from internals.helpers import int_field, str_field
from internals.reconcile import reconcile_cache
from internals.operations import (
    GitHubAddLabelsRequest,
    GitHubAddReactionRequest,
    GitHubCodeSearchRequest,
    GitHubCoAuthor,
    GitHubCommitRequest,
    GitHubCheckRunRequest,
    GitHubCheckRunOutput,
    GitHubCreateIssueCommentRequest,
    GitHubCreateIssueRequest,
    GitHubCreateCheckRunRequest,
    GitHubCreatePullRequestConversationCommentRequest,
    GitHubCreatePullRequestRequest,
    GitHubCreatePullRequestReviewRequest,
    GitHubFileContentRequest,
    GitHubListCommitsRequest,
    GitHubCompareRefsRequest,
    GitHubFileChange,
    GitHubGetIssueRequest,
    GitHubListIssuesRequest,
    GitHubListCheckSuiteCheckRunsRequest,
    GitHubListCommitCheckRunsRequest,
    GitHubListCheckRunAnnotationsRequest,
    GitHubListPullRequestFilesRequest,
    GitHubListPullRequestCommitsRequest,
    GitHubListPullRequestReviewsRequest,
    GitHubListPullRequestReviewThreadsRequest,
    GitHubListWorkflowRunJobsRequest,
    GitHubListWorkflowRunsRequest,
    GitHubGetWorkflowJobLogsRequest,
    GitHubListIssueCommentsRequest,
    GitHubSearchPullRequestsRequest,
    GitHubGetMergeQueueRequest,
    GitHubListPullRequestsRequest,
    GitHubListPullRequestsForCommitRequest,
    GitHubListOrgMembersRequest,
    GitHubListRepoContributorsRequest,
    GitHubGetUserRequest,
    GitHubOpenPullRequestRequest,
    GitHubPullRequestRequest,
    GitHubPullRequestReviewComment,
    GitHubRepositoryRequest,
    GitHubRemoveLabelsRequest,
    GitHubRequestReviewersRequest,
    GitHubResolvePullRequestReviewThreadRequest,
    GitHubUpdateCheckRunRequest,
    GitHubUpdateIssueRequest,
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
    create_issue,
    create_issue_comment,
    create_pull_request_conversation_comment,
    create_pull_request_review,
    create_pull_request_with_files,
    get_check_run,
    get_file_text_at_ref,
    get_issue,
    list_commits,
    list_issues,
    compare_refs,
    commit_list_summary,
    compare_refs_summary,
    get_pull_request,
    get_repository,
    get_workflow_run,
    issue_comment_summary,
    issue_summary,
    label_summary,
    list_check_suite_check_runs,
    list_commit_check_runs,
    list_check_run_annotations,
    list_pull_request_files,
    list_pull_request_commits,
    list_pull_request_reviews,
    list_pull_request_review_threads,
    list_workflow_run_jobs,
    list_workflow_runs,
    get_workflow_job_logs,
    list_issue_comments,
    search_pull_requests,
    get_merge_queue,
    list_pull_requests,
    list_pull_requests_for_commit,
    list_org_members,
    list_repo_contributors,
    get_user,
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
    update_issue,
    workflow_run_job_summary,
    workflow_run_summary,
    workflow_runs_list_summary,
    user_summary,
    contributor_summary,
)
from internals.webhook import (
    event_summary,
    github_delivery_id,
    github_event_header,
    github_event_type,
    installation_id_from_payload,
    payload_digest,
    repository_full_name,
    webhook_ignored_reason,
    webhook_subject_from_payload,
)


class GitHubApp(gestalt.App):
    def migration_options(
        self, name: str, config: dict[str, Any]
    ) -> MigrationRunOptions | None:
        _ = name
        return cache_migration_options(config)


app = GitHubApp("github")
logger = logging.getLogger(__name__)

OperationResult: TypeAlias = dict[str, Any] | gestalt.Response[dict[str, Any]]


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


class RepositoryInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")


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


class ListCommitsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    sha: str = gestalt.field(
        description="SHA or branch to start listing commits from.",
        default="",
        required=False,
    )
    path: str = gestalt.field(
        description="Only commits containing this file path.",
        default="",
        required=False,
    )
    author: str = gestalt.field(
        description="GitHub username or email to filter by author.",
        default="",
        required=False,
    )
    since: str = gestalt.field(
        description="ISO 8601 timestamp; only commits after this date.",
        default="",
        required=False,
    )
    until: str = gestalt.field(
        description="ISO 8601 timestamp; only commits before this date.",
        default="",
        required=False,
    )
    per_page: int = gestalt.field(
        description="Results per page, from 1 through 100",
        default=30,
        required=False,
    )
    page: int = gestalt.field(
        description="Page number, starting at 1",
        default=0,
        required=False,
    )


class CompareRefsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    base: str = gestalt.field(description="Base ref, branch, or SHA")
    head: str = gestalt.field(description="Head ref, branch, or SHA")


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


class CreateIssueInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    title: str = gestalt.field(description="Issue title")
    body: str = gestalt.field(description="Issue body", default="", required=False)
    labels: list[str] = gestalt.field(
        description="Label names to apply", default_factory=list, required=False
    )
    assignees: list[str] = gestalt.field(
        description="GitHub usernames to assign", default_factory=list, required=False
    )


class UpdateIssueInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    issue_number: int = gestalt.field(description="Issue number")
    title: str = gestalt.field(description="Issue title", default="", required=False)
    body: str | None = gestalt.field(
        description="Issue body", default=None, required=False
    )
    state: str = gestalt.field(
        description="Issue state: open or closed", default="", required=False
    )
    labels: list[str] = gestalt.field(
        description="Label names to apply", default_factory=list, required=False
    )
    assignees: list[str] = gestalt.field(
        description="GitHub usernames to assign", default_factory=list, required=False
    )


class GetIssueInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    issue_number: int = gestalt.field(description="Issue number")


class ListIssuesInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    state: str = gestalt.field(
        description="Issue state filter: open, closed, or all",
        default="all",
        required=False,
    )
    per_page: int = gestalt.field(
        description="Results per page (1-100)", default=100, required=False
    )
    page: int = gestalt.field(description="Page number", default=1, required=False)


class CreatePullRequestConversationCommentInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    pull_number: int = gestalt.field(description="Pull request number")
    body: str = gestalt.field(description="Comment body")


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


class CreatePullRequestReviewInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    pull_number: int = gestalt.field(description="Pull request number")
    body: str = gestalt.field(description="Pull request review body")
    comments: list[PullRequestReviewCommentInput] = gestalt.field(
        description=(
            "Inline review comments. Each item accepts path, body, line, side, "
            "start_line, and start_side."
        )
    )
    commit_id: str = gestalt.field(
        description="Optional commit SHA to review. Defaults to GitHub's latest PR commit.",
        default="",
        required=False,
    )


class ListPullRequestReviewsInput(gestalt.Model):
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


class ResolvePullRequestReviewThreadInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    pull_number: int = gestalt.field(description="Pull request number")
    thread_id: str = gestalt.field(
        description="GitHub GraphQL pull request review thread node ID"
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


class GetPullRequestInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    pull_number: int = gestalt.field(description="Pull request number")


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


class ListPullRequestCommitsInput(gestalt.Model):
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


class GetCheckRunInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    check_run_id: int = gestalt.field(description="GitHub check run ID")


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


class ListCommitCheckRunsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    ref: str = gestalt.field(description="Commit SHA or branch name")
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


class GetWorkflowRunInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    run_id: int = gestalt.field(description="GitHub Actions workflow run ID")


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


class ListWorkflowRunsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    workflow_id: str = gestalt.field(
        description="Filter runs to a specific workflow by numeric ID or workflow filename",
        default="",
        required=False,
    )
    branch: str = gestalt.field(
        description="Filter workflow runs by branch name",
        default="",
        required=False,
    )
    event: str = gestalt.field(
        description="Filter workflow runs by event type",
        default="",
        required=False,
    )
    head_sha: str = gestalt.field(
        description="Filter workflow runs by head commit SHA",
        default="",
        required=False,
    )
    status: str = gestalt.field(
        description="Filter workflow runs by status",
        default="",
        required=False,
    )
    created: str = gestalt.field(
        description="Filter workflow runs created on or after this date (YYYY-MM-DD)",
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


class GetWorkflowJobLogsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    job_id: int = gestalt.field(description="GitHub Actions workflow job ID")


class ListIssueCommentsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    issue_number: int = gestalt.field(description="Issue or pull request number")
    since: str = gestalt.field(
        description="Only comments updated at or after this ISO 8601 timestamp",
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


class SearchPullRequestsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    query: str = gestalt.field(description="GitHub search query for pull requests")
    first: int = gestalt.field(
        description="Number of results to fetch, from 1 through 100",
        default=30,
        required=False,
    )
    after: str = gestalt.field(
        description="Optional GitHub GraphQL search page cursor",
        default="",
        required=False,
    )


class GetMergeQueueInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    branch: str = gestalt.field(description="Branch name for the merge queue")
    first: int = gestalt.field(
        description="Number of merge queue entries to fetch, from 1 through 100",
        default=100,
        required=False,
    )


class ListPullRequestsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    state: str = gestalt.field(
        description="Pull request state filter: open, closed, or all",
        default="open",
        required=False,
    )
    sort: str = gestalt.field(
        description="Sort field: created, updated, popularity, or long-running",
        default="",
        required=False,
    )
    direction: str = gestalt.field(
        description="Sort direction: asc or desc",
        default="",
        required=False,
    )
    base: str = gestalt.field(
        description="Filter by base branch name",
        default="",
        required=False,
    )
    head: str = gestalt.field(
        description="Filter by head branch name",
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


class ListPullRequestsForCommitInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    commit_sha: str = gestalt.field(description="Commit SHA")
    per_page: int = gestalt.field(
        description="Results per page, from 1 through 100",
        default=0,
        required=False,
    )
    page: int = gestalt.field(
        description="Page number, starting at 1", default=0, required=False
    )


class ListOrgMembersInput(gestalt.Model):
    owner: str = gestalt.field(
        description="Organization login (also used with repo for bot authorization)"
    )
    repo: str = gestalt.field(
        description="Repository name used to anchor bot authorization"
    )
    role: str = gestalt.field(
        description="Member role filter: all, admin, or member",
        default="",
        required=False,
    )
    filter: str = gestalt.field(
        description="Member filter: 2fa_disabled or 2fa_insecure",
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


class ListRepoContributorsInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    anon: str = gestalt.field(
        description="Include anonymous contributors: true or false",
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


class GetUserInput(gestalt.Model):
    owner: str = gestalt.field(
        description="Repository owner used to anchor bot authorization"
    )
    repo: str = gestalt.field(
        description="Repository name used to anchor bot authorization"
    )
    login: str = gestalt.field(description="GitHub user login")


class ReconcileCacheInput(gestalt.Model):
    owner: str = gestalt.field(description="Repository owner")
    repo: str = gestalt.field(description="Repository name")
    max_entries: int = gestalt.field(
        description="Maximum expired cache entries to replay, from 1 through 100",
        default=25,
        required=False,
    )


@app.configure
def configure(_name: str, config: dict[str, Any]) -> None:
    configure_from_mapping(config, provider_name=_name)


@app.http_subject
def resolve_http_subject(request: gestalt.HTTPSubjectRequest) -> gestalt.Subject | None:
    subject = webhook_subject_from_payload(request.params)
    if subject is None:
        return None
    return gestalt.Subject(id=subject.id)


@app.operation(
    id=GITHUB_EVENT_OPERATION,
    method="POST",
    description="Handle GitHub App webhook callbacks by delivering canonical workflow events",
    visible=False,
)
def github_events_handle(
    input: dict[str, Any], req: gestalt.Request
) -> OperationResult:
    event_type = _github_workflow_event_name(input)
    ignored_reason = webhook_ignored_reason(
        input,
        event_type=event_type,
        enforce_event_allowlist=True,
        check_bot_sender=False,
    )
    if ignored_reason:
        return {"ok": True, "ignored": ignored_reason}

    installation_id = installation_id_from_payload(input)
    summary = event_summary(input, installation_id, event_type=event_type)
    try:
        ingested = ingest_webhook_event(event_type, input, summary)
        if ingested:
            logger.info(
                "Projected GitHub webhook into cache",
                extra={
                    "github_cache_outcome": "invalidate",
                    "github_event": event_type,
                    "github_repository": summary.get("repository", ""),
                },
            )
    except Exception:
        logger.exception(
            "Failed to project GitHub webhook into cache",
            extra={
                "github_cache_outcome": "error",
                "github_event": event_type,
                "github_repository": summary.get("repository", ""),
            },
        )
    ignored_reason = webhook_ignored_reason(
        input,
        event_type=event_type,
        enforce_event_allowlist=True,
    )
    if ignored_reason:
        return {"ok": True, "ignored": ignored_reason}

    workflow_request = _build_workflow_deliver_event_request(input, summary)
    try:
        logger.info(
            "delivering GitHub workflow event",
            extra={
                "github_event": summary.get("event_type", ""),
                "github_action": summary.get("action", ""),
                "github_delivery_id": summary.get("delivery_id", ""),
                "github_repository": summary.get("repository", ""),
                "workflow_provider": workflow_request.provider,
            },
        )
        with req.workflows() as workflows:
            workflows.deliver_event(workflow_request)
    except Exception as err:
        logger.exception(
            "failed to deliver GitHub workflow event",
            extra={
                "github_event": summary.get("event_type", ""),
                "github_action": summary.get("action", ""),
                "github_delivery_id": summary.get("delivery_id", ""),
                "github_repository": summary.get("repository", ""),
            },
        )
        return _server_error(f"failed to deliver workflow event: {err}")

    logger.info(
        "delivered GitHub workflow event",
        extra={
            "github_event": summary.get("event_type", ""),
            "github_action": summary.get("action", ""),
            "github_delivery_id": summary.get("delivery_id", ""),
            "github_repository": summary.get("repository", ""),
            "workflow_event_id": workflow_request.event.id
            if workflow_request.event is not None
            else "",
            "workflow_provider": workflow_request.provider,
        },
    )

    return {
        "ok": True,
        "delivered": True,
        "workflow_event_id": workflow_request.event.id
        if workflow_request.event is not None
        else "",
        "workflow_provider": workflow_request.provider,
    }


@app.operation(
    id=MAINTENANCE_RECONCILE_CACHE_OPERATION,
    method="POST",
    description="Reconcile a bounded set of expired GitHub cache entries",
    visible=False,
)
def maintenance_reconcile_cache(
    input: ReconcileCacheInput, req: gestalt.Request
) -> OperationResult:
    result = _run_bot(
        lambda: reconcile_cache(
            input.owner,
            input.repo,
            input.max_entries,
            **_bot_call(req),
        ).to_dict()
    )
    if isinstance(result, dict):
        logger.info(
            "Reconciled GitHub cache",
            extra={
                "github_cache_outcome": "reconcile",
                "github_cache_repository": result.get("repository", ""),
                "github_cache_checked": result.get("checked", 0),
                "github_cache_drifted": result.get("drifted", 0),
                "github_cache_refreshed": result.get("refreshed", 0),
                "github_cache_deleted": result.get("deleted", 0),
                "github_cache_failed": result.get("failed", 0),
            },
        )
    return result


def _build_workflow_deliver_event_request(
    payload: dict[str, Any], summary: dict[str, Any]
) -> gestalt.WorkflowDeliverEvent:
    delivery_id = github_delivery_id(payload)
    event_id = (
        f"github:{delivery_id}" if delivery_id else f"github:{payload_digest(payload)}"
    )
    return gestalt.WorkflowDeliverEvent(
        provider=get_github_config().workflow_provider,
        event=gestalt.WorkflowEvent(
            id=event_id,
            source="github",
            spec_version="1.0",
            type=_github_workflow_event_type(summary),
            subject=_github_workflow_event_subject(payload, summary),
            datacontenttype="application/json",
            data=_github_workflow_event_data(payload, summary),
        ),
    )


def _github_workflow_event_type(summary: dict[str, Any]) -> str:
    event_type = str(summary.get("event_type", "")).strip().lower()
    action = str(summary.get("action", "")).strip().lower()
    if action:
        return f"github.{event_type}.{action}"
    return f"github.{event_type}"


def _github_workflow_event_name(payload: dict[str, Any]) -> str:
    return (github_event_header(payload) or github_event_type(payload)).strip().lower()


def _github_workflow_event_subject(
    payload: dict[str, Any], summary: dict[str, Any]
) -> str:
    repository = repository_full_name(payload)
    if repository:
        return f"repo:{repository}"
    installation_id = int(summary.get("installation_id", 0) or 0)
    return f"installation:{installation_id}"


def _github_workflow_event_data(
    payload: dict[str, Any], summary: dict[str, Any]
) -> dict[str, Any]:
    github = dict(summary)
    header = github_event_header(payload)
    if header:
        github["event_header"] = header
    return {"github": github, "raw": payload}


@app.operation(
    id=IDENTITY_LINK_SELF_OPERATION,
    method="POST",
    description="Link the current Gestalt user subject to the GitHub user proven by the current GitHub credential",
    tags=["identity"],
)
def github_identity_link_self(
    input: dict[str, Any], req: gestalt.Request
) -> OperationResult:
    del input

    token = req.token.strip()
    if not token:
        return _unauthorized("token is required")
    subject_id = req.subject.id.strip()
    if not subject_id:
        return _bad_request("subject id is required")

    try:
        profile = DEFAULT_GITHUB_CLIENT.github_json("GET", "/user", token)
        user_id = str(int_field(profile, "id") or "").strip()
        login = str_field(profile, "login")
        if not user_id or not login:
            return _server_error("GitHub /user response did not include id and login")
        name = str_field(profile, "name")
        email = str_field(profile, "email")
        req.authorization().add_relationship(
            gestalt.AddRelationshipRequest(
                relationship=gestalt.Relationship(
                    tuple=gestalt.RelationshipTuple(
                        target=gestalt.RelationshipTarget(
                            kind=RelationshipTargetSubject(
                                value=gestalt.AuthorizationSubject(
                                    type="subject", id=subject_id
                                )
                            )
                        ),
                        relation=GITHUB_USER_LINKED_ACTION,
                        resource=gestalt.AuthorizationResource(
                            type=GITHUB_USER_RESOURCE_TYPE,
                            id=user_id,
                            properties={
                                "login": login,
                                "name": name,
                            },
                        ),
                    ),
                    source_layer=gestalt.SourceLayerValues.RUNTIME,
                )
            )
        )
    except GitHubAPIError as err:
        return _github_error(err)
    except Exception as err:
        return _server_error(f"failed to link GitHub identity: {err}")

    return {
        "ok": True,
        "user": {
        "id": user_id,
        "login": login,
        "name": name,
        "email": email,
        },
        "resource": {"type": GITHUB_USER_RESOURCE_TYPE, "id": user_id},
    }


@app.operation(
    id=BOT_GET_REPOSITORY_OPERATION,
    method="GET",
    description="Get repository metadata using a GitHub App installation token",
    tags=["repo", "repository"],
)
def bot_get_repository(input: RepositoryInput, req: gestalt.Request) -> OperationResult:
    return _run_bot(lambda: {"repository": repository_summary((get_repository(
_to_request(GitHubRepositoryRequest, input), **_bot_call(req))))})
@app.operation(
    id=BOT_SEARCH_CODE_OPERATION,
    method="GET",
    description="Search code in one repository using a GitHub App installation token",
    tags=["repo", "code", "search"],
)
def bot_search_code(input: SearchCodeInput, req: gestalt.Request) -> OperationResult:
    return _run_bot(lambda: code_search_summary((search_code(
_to_request(GitHubCodeSearchRequest, input), **_bot_call(req)))))
@app.operation(
    id=BOT_GET_CONTENT_OPERATION,
    method="GET",
    description="Get UTF-8 file content using a GitHub App installation token",
    tags=["repo", "code", "content"],
)
def bot_get_content(input: GetContentInput, req: gestalt.Request) -> OperationResult:
    return _run_bot(lambda: {"path": input.path, "ref": input.ref, "content": (get_file_text_at_ref(
_to_request(GitHubFileContentRequest, input), **_bot_call(req)))})
@app.operation(
    id=BOT_LIST_COMMITS_OPERATION,
    method="GET",
    description="List commits on a repository using a GitHub App installation token",
    tags=["repo", "code", "history"],
)
def bot_list_commits(input: ListCommitsInput, req: gestalt.Request) -> OperationResult:
    try:
        results = list_commits(
            GitHubListCommitsRequest(
                owner=input.owner,
                repo=input.repo,
                sha=input.sha,
                path=input.path,
                author=input.author,
                since=input.since,
                until=input.until,
                per_page=input.per_page,
                page=input.page,
            ),
            subject=req.subject,
            authorization=_request_authorization(req),
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    if not isinstance(results, list):
        return _server_error("GitHub commits response was not a list")
    return commit_list_summary(results)
@app.operation(
    id=BOT_COMPARE_REFS_OPERATION,
    method="GET",
    description="Compare two refs using a GitHub App installation token",
    tags=["repo", "code", "history"],
)
def bot_compare_refs(input: CompareRefsInput, req: gestalt.Request) -> OperationResult:
    return _run_bot(lambda: compare_refs_summary((compare_refs(
_to_request(GitHubCompareRefsRequest, input), **_bot_call(req)))))
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
            authorization=_request_authorization(req),
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    return {"commit": commit_result_dict(commit)}
@app.operation(
    id=BOT_OPEN_PULL_REQUEST_OPERATION,
    method="POST",
    description="Open a pull request using a GitHub App installation token",
    tags=["pr", "prs"],
)
def bot_open_pull_request(
    input: OpenPullRequestInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: {"pull_request": pull_request_summary((open_pull_request(
_to_request(GitHubOpenPullRequestRequest, input), **_bot_call(req))))})
@app.operation(
    id=BOT_CLOSE_PULL_REQUEST_OPERATION,
    method="POST",
    description="Close a pull request using a GitHub App installation token",
    tags=["pr", "prs"],
)
def bot_close_pull_request(
    input: ClosePullRequestInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: {"pull_request": pull_request_summary((close_pull_request(
_to_request(GitHubPullRequestRequest, input), **_bot_call(req))))})
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
            authorization=_request_authorization(req),
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
        "commit": commit_result_dict(result.commit),
        "pull_request": pull_request_summary(result.pull_request),
    }


@app.operation(
    id=BOT_CREATE_ISSUE_COMMENT_OPERATION,
    method="POST",
    description="Create an issue comment using a GitHub App installation token",
)
def bot_create_issue_comment(
    input: CreateIssueCommentInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: {"comment": issue_comment_summary((create_issue_comment(
_to_request(GitHubCreateIssueCommentRequest, input), **_bot_call(req))))})
@app.operation(
    id=BOT_CREATE_ISSUE_OPERATION,
    method="POST",
    description="Create an issue using a GitHub App installation token",
)
def bot_create_issue(
    input: CreateIssueInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: {"issue": issue_summary((create_issue(
_to_request(GitHubCreateIssueRequest, input, labels=tuple(input.labels), assignees=tuple(input.assignees)), **_bot_call(req))))})
@app.operation(
    id=BOT_UPDATE_ISSUE_OPERATION,
    method="POST",
    description="Update an issue using a GitHub App installation token",
)
def bot_update_issue(
    input: UpdateIssueInput, req: gestalt.Request
) -> OperationResult:
    try:
        labels = tuple(input.labels) if input.labels else None
        assignees = tuple(input.assignees) if input.assignees else None
        issue = update_issue(
            GitHubUpdateIssueRequest(
                owner=input.owner,
                repo=input.repo,
                issue_number=input.issue_number,
                title=input.title,
                body=input.body,
                state=input.state,
                labels=labels,
                assignees=assignees,
            ),
            subject=req.subject,
            authorization=_request_authorization(req),
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    return {"issue": issue_summary(issue)}
@app.operation(
    id=BOT_GET_ISSUE_OPERATION,
    method="GET",
    description="Get an issue using a GitHub App installation token",
)
def bot_get_issue(input: GetIssueInput, req: gestalt.Request) -> OperationResult:
    return _run_bot(lambda: {"issue": issue_summary((get_issue(
_to_request(GitHubGetIssueRequest, input), **_bot_call(req))))})
@app.operation(
    id=BOT_LIST_ISSUES_OPERATION,
    method="GET",
    description="List repository issues using a GitHub App installation token",
)
def bot_list_issues(input: ListIssuesInput, req: gestalt.Request) -> OperationResult:
    return _run_bot(lambda: _counted_summaries(list_issues(
_to_request(GitHubListIssuesRequest, input), **_bot_call(req)), issue_summary, key="issues"))


@app.operation(
    id=BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
    method="POST",
    description="Create a pull request review with inline comments using a GitHub App installation token",
    tags=["pr", "prs", "review"],
)
def bot_create_pull_request_review(
    input: CreatePullRequestReviewInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: {"review": pull_request_review_summary((create_pull_request_review(
_to_request(GitHubCreatePullRequestReviewRequest, input, comments=_pull_request_review_comments_from_input(input.comments)), **_bot_call(req))))})
@app.operation(
    id=BOT_LIST_PULL_REQUEST_REVIEWS_OPERATION,
    method="GET",
    description="List pull request reviews using a GitHub App installation token",
    tags=["pr", "prs", "review"],
)
def bot_list_pull_request_reviews(
    input: ListPullRequestReviewsInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: _counted_summaries(list_pull_request_reviews(
_to_request(GitHubListPullRequestReviewsRequest, input), **_bot_call(req)), pull_request_review_summary, key="reviews"))


@app.operation(
    id=BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION,
    method="GET",
    description="List pull request review threads and their first comments using a GitHub App installation token",
    tags=["pr", "prs", "review"],
)
def bot_list_pull_request_review_threads(
    input: ListPullRequestReviewThreadsInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: (list_pull_request_review_threads(
_to_request(GitHubListPullRequestReviewThreadsRequest, input), **_bot_call(req))))
@app.operation(
    id=BOT_RESOLVE_PULL_REQUEST_REVIEW_THREAD_OPERATION,
    method="POST",
    description="Resolve a pull request review thread after verifying it belongs to the requested pull request",
    tags=["pr", "prs", "review"],
)
def bot_resolve_pull_request_review_thread(
    input: ResolvePullRequestReviewThreadInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: {"thread": (resolve_pull_request_review_thread(
_to_request(GitHubResolvePullRequestReviewThreadRequest, input), **_bot_call(req)))})
@app.operation(
    id=BOT_ADD_REACTION_OPERATION,
    method="POST",
    description="Add a reaction using a GitHub App installation token",
)
def bot_add_reaction(input: AddReactionInput, req: gestalt.Request) -> OperationResult:
    return _run_bot(lambda: {"reaction": reaction_summary((add_reaction(
_to_request(GitHubAddReactionRequest, input), **_bot_call(req))))})
@app.operation(
    id=BOT_ADD_LABELS_OPERATION,
    method="POST",
    description="Add labels to an issue or pull request using a GitHub App installation token",
)
def bot_add_labels(input: AddLabelsInput, req: gestalt.Request) -> OperationResult:
    return _run_bot(lambda: {"labels": [label_summary(label) for label in (add_labels(
_to_request(GitHubAddLabelsRequest, input, labels=tuple(input.labels)), **_bot_call(req)))]})
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
            ),
            subject=req.subject,
            authorization=_request_authorization(req),
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
        "removed": list(removed),
        "labels": [label_summary(label) for label in labels],
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
    return _run_bot(lambda: {"pull_request": pull_request_summary((request_reviewers(
_to_request(GitHubRequestReviewersRequest, input), **_bot_call(req))))})
@app.operation(
    id=BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
    method="POST",
    description="Create a pull request conversation comment using a GitHub App installation token",
)
def bot_create_pull_request_conversation_comment(
    input: CreatePullRequestConversationCommentInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: {"comment": issue_comment_summary((create_pull_request_conversation_comment(
_to_request(GitHubCreatePullRequestConversationCommentRequest, input), **_bot_call(req))))})
@app.operation(
    id=BOT_GET_PULL_REQUEST_OPERATION,
    method="GET",
    description="Get pull request metadata using a GitHub App installation token",
    tags=["pr", "prs", "review"],
)
def bot_get_pull_request(
    input: GetPullRequestInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: {"pull_request": pull_request_summary((get_pull_request(
_to_request(GitHubPullRequestRequest, input), **_bot_call(req))))})
@app.operation(
    id=BOT_LIST_PULL_REQUEST_FILES_OPERATION,
    method="GET",
    description="List pull request changed files and bounded patches using a GitHub App installation token",
    tags=["pr", "prs", "review", "diff"],
)
def bot_list_pull_request_files(
    input: ListPullRequestFilesInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: _counted_summaries(list_pull_request_files(
_to_request(GitHubListPullRequestFilesRequest, input), **_bot_call(req)), pull_request_file_summary, key="files"))


@app.operation(
    id=BOT_LIST_PULL_REQUEST_COMMITS_OPERATION,
    method="GET",
    description="List commits on a pull request using a GitHub App installation token",
    tags=["pr", "prs", "history"],
)
def bot_list_pull_request_commits(
    input: ListPullRequestCommitsInput, req: gestalt.Request
) -> OperationResult:
    try:
        results = list_pull_request_commits(
            _to_request(GitHubListPullRequestCommitsRequest, input),
            **_bot_call(req),
        )
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)
    if not isinstance(results, list):
        return _server_error("GitHub pull request commits response was not a list")
    return commit_list_summary(results)


@app.operation(
    id=BOT_GET_CHECK_RUN_OPERATION,
    method="GET",
    description="Get a GitHub check run using a GitHub App installation token",
)
def bot_get_check_run(input: GetCheckRunInput, req: gestalt.Request) -> OperationResult:
    return _run_bot(lambda: {"check_run": check_run_summary((get_check_run(
_to_request(GitHubCheckRunRequest, input), **_bot_call(req))))})
@app.operation(
    id=BOT_CREATE_CHECK_RUN_OPERATION,
    method="POST",
    description="Create a GitHub check run using a GitHub App installation token",
)
def bot_create_check_run(
    input: CreateCheckRunInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: {"check_run": check_run_summary((create_check_run(
_to_request(GitHubCreateCheckRunRequest, input, output=_check_run_output_from_input(input.output)), **_bot_call(req))))})
@app.operation(
    id=BOT_UPDATE_CHECK_RUN_OPERATION,
    method="POST",
    description="Update a GitHub check run using a GitHub App installation token",
)
def bot_update_check_run(
    input: UpdateCheckRunInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: {"check_run": check_run_summary((update_check_run(
_to_request(GitHubUpdateCheckRunRequest, input, output=_check_run_output_from_input(input.output)), **_bot_call(req))))})
@app.operation(
    id=BOT_LIST_CHECK_SUITE_CHECK_RUNS_OPERATION,
    method="GET",
    description="List check runs in a GitHub check suite using a GitHub App installation token",
)
def bot_list_check_suite_check_runs(
    input: ListCheckSuiteCheckRunsInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: _check_runs_list_result(list_check_suite_check_runs(
_to_request(GitHubListCheckSuiteCheckRunsRequest, input), **_bot_call(req))))


@app.operation(
    id=BOT_LIST_COMMIT_CHECK_RUNS_OPERATION,
    method="GET",
    description="List check runs for a commit ref using a GitHub App installation token",
)
def bot_list_commit_check_runs(
    input: ListCommitCheckRunsInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: _check_runs_list_result(list_commit_check_runs(
_to_request(GitHubListCommitCheckRunsRequest, input), **_bot_call(req))))


@app.operation(
    id=BOT_LIST_CHECK_RUN_ANNOTATIONS_OPERATION,
    method="GET",
    description="List annotations for a GitHub check run using a GitHub App installation token",
)
def bot_list_check_run_annotations(
    input: ListCheckRunAnnotationsInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(
        lambda: _counted_summaries(
            list_check_run_annotations(
                _to_request(GitHubListCheckRunAnnotationsRequest, input),
                **_bot_call(req),
            ),
            check_run_annotation_summary,
            key="annotations",
        )
    )


@app.operation(
    id=BOT_GET_WORKFLOW_RUN_OPERATION,
    method="GET",
    description="Get a GitHub Actions workflow run using a GitHub App installation token",
)
def bot_get_workflow_run(
    input: GetWorkflowRunInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: {"workflow_run": workflow_run_summary((get_workflow_run(
_to_request(GitHubWorkflowRunRequest, input), **_bot_call(req))))})
@app.operation(
    id=BOT_LIST_WORKFLOW_RUN_JOBS_OPERATION,
    method="GET",
    description="List jobs for a GitHub Actions workflow run using a GitHub App installation token",
)
def bot_list_workflow_run_jobs(
    input: ListWorkflowRunJobsInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: _workflow_jobs_list_result(list_workflow_run_jobs(
_to_request(GitHubListWorkflowRunJobsRequest, input), **_bot_call(req))))


@app.operation(
    id=BOT_LIST_WORKFLOW_RUNS_OPERATION,
    method="GET",
    description="List GitHub Actions workflow runs using a GitHub App installation token",
)
def bot_list_workflow_runs(
    input: ListWorkflowRunsInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: workflow_runs_list_summary((list_workflow_runs(
_to_request(GitHubListWorkflowRunsRequest, input), **_bot_call(req)))))
@app.operation(
    id=BOT_GET_WORKFLOW_JOB_LOGS_OPERATION,
    method="GET",
    description="Get plain-text logs for a GitHub Actions workflow job using a GitHub App installation token",
)
def bot_get_workflow_job_logs(
    input: GetWorkflowJobLogsInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: {"logs": (get_workflow_job_logs(
_to_request(GitHubGetWorkflowJobLogsRequest, input), **_bot_call(req)))})
@app.operation(
    id=BOT_LIST_ISSUE_COMMENTS_OPERATION,
    method="GET",
    description="List issue or pull request comments using a GitHub App installation token",
)
def bot_list_issue_comments(
    input: ListIssueCommentsInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: _counted_summaries(list_issue_comments(
_to_request(GitHubListIssueCommentsRequest, input), **_bot_call(req)), issue_comment_summary, key="comments"))


@app.operation(
    id=BOT_SEARCH_PULL_REQUESTS_OPERATION,
    method="GET",
    description="Search pull requests using GitHub GraphQL and a GitHub App installation token",
    tags=["pr", "prs", "search"],
)
def bot_search_pull_requests(
    input: SearchPullRequestsInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: (search_pull_requests(
_to_request(GitHubSearchPullRequestsRequest, input), **_bot_call(req))))
@app.operation(
    id=BOT_GET_MERGE_QUEUE_OPERATION,
    method="GET",
    description="Get merge queue entries for a branch using GitHub GraphQL and a GitHub App installation token",
    tags=["pr", "prs"],
)
def bot_get_merge_queue(
    input: GetMergeQueueInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: {"merge_queue": (get_merge_queue(
_to_request(GitHubGetMergeQueueRequest, input), **_bot_call(req)))})
@app.operation(
    id=BOT_LIST_PULL_REQUESTS_OPERATION,
    method="GET",
    description="List pull requests using a GitHub App installation token",
    tags=["pr", "prs"],
)
def bot_list_pull_requests(
    input: ListPullRequestsInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(
        lambda: _counted_summaries(
            list_pull_requests(
                _to_request(GitHubListPullRequestsRequest, input), **_bot_call(req)
            ),
            pull_request_summary,
            key="pull_requests",
        )
    )


@app.operation(
    id=BOT_LIST_PULL_REQUESTS_FOR_COMMIT_OPERATION,
    method="GET",
    description="List pull requests associated with a commit using a GitHub App installation token",
    tags=["pr", "prs"],
)
def bot_list_pull_requests_for_commit(
    input: ListPullRequestsForCommitInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(
        lambda: _counted_summaries(
            list_pull_requests_for_commit(
                _to_request(GitHubListPullRequestsForCommitRequest, input),
                **_bot_call(req),
            ),
            pull_request_summary,
            key="pull_requests",
        )
    )


@app.operation(
    id=BOT_LIST_ORG_MEMBERS_OPERATION,
    method="GET",
    description="List organization members using a GitHub App installation token",
)
def bot_list_org_members(
    input: ListOrgMembersInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(lambda: _counted_summaries(list_org_members(
_to_request(GitHubListOrgMembersRequest, input), **_bot_call(req)), user_summary, key="members"))


@app.operation(
    id=BOT_LIST_REPO_CONTRIBUTORS_OPERATION,
    method="GET",
    description="List repository contributors using a GitHub App installation token",
)
def bot_list_repo_contributors(
    input: ListRepoContributorsInput, req: gestalt.Request
) -> OperationResult:
    return _run_bot(
        lambda: _counted_summaries(
            list_repo_contributors(
                _to_request(GitHubListRepoContributorsRequest, input), **_bot_call(req)
            ),
            contributor_summary,
            key="contributors",
        )
    )


@app.operation(
    id=BOT_GET_USER_OPERATION,
    method="GET",
    description="Get a GitHub user by login using a GitHub App installation token",
)
def bot_get_user(input: GetUserInput, req: gestalt.Request) -> OperationResult:
    return _run_bot(lambda: {"user": user_summary((get_user(
_to_request(GitHubGetUserRequest, input), **_bot_call(req))))})
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
    user_id = _linked_github_user_id(req)
    if user_id:
        user_identity = DEFAULT_GITHUB_CLIENT.user_identity_by_id(user_id)
        if user_identity is not None and user_identity.email:
            return user_identity.name, user_identity.email
    return input_name, input_email


def _linked_github_user_id(req: gestalt.Request) -> str:
    subject_id = req.agent_subject.id.strip() or req.subject.id.strip()
    if not subject_id:
        return ""
    try:
        response = req.authorization().list_relationships(
            gestalt.ListRelationshipsRequest(
                filter=gestalt.RelationshipFilter(
                    target=gestalt.RelationshipTarget(
                        kind=RelationshipTargetSubject(
                            value=gestalt.AuthorizationSubject(
                                type="subject", id=subject_id
                            )
                        )
                    ),
                    relation=GITHUB_USER_LINKED_ACTION,
                    resource_type=GITHUB_USER_RESOURCE_TYPE,
                ),
                page_size=2,
            )
        )
    except Exception as err:
        logger.warning("GitHub linked author lookup failed: %s", err)
        return ""
    resources = [
        relationship.tuple.resource
        for relationship in response.relationships
        if relationship.tuple is not None
        and relationship.tuple.resource is not None
        and relationship.tuple.resource.id.strip()
    ]
    if len(resources) != 1:
        if len(resources) > 1:
            logger.warning("GitHub subject resolved multiple linked users")
        return ""
    _, _, user_id = resources[0].id.strip().rpartition("/")
    return user_id


def _request_authorization(req: gestalt.Request) -> gestalt.Authorization:
    try:
        return req.authorization()
    except Exception as err:
        raise GitHubAuthorizationError(
            "GitHub bot repository authorization is unavailable"
        ) from err


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



T = TypeVar("T")


def _to_request(request_cls: type[T], input: Any, **overrides: Any) -> T:
    field_values = {
        field.name: getattr(input, field.name) for field in fields(request_cls)
    }
    field_values.update(overrides)
    return request_cls(**field_values)


def _bot_call(req: gestalt.Request) -> dict[str, Any]:
    return {
        "subject": req.subject,
        "authorization": _request_authorization(req),
    }


def _run_bot(fn: Callable[[], T]) -> T | gestalt.Response[dict[str, Any]]:
    try:
        return fn()
    except ValueError as err:
        return _bad_request(str(err))
    except GitHubAuthorizationError as err:
        return _forbidden(str(err))
    except GitHubConfigError as err:
        return _server_error(str(err))
    except GitHubAPIError as err:
        return _github_error(err)


def _check_runs_list_result(check_runs: dict[str, Any]) -> dict[str, Any]:
    raw_check_runs = check_runs.get("check_runs")
    if not isinstance(raw_check_runs, list):
        raw_check_runs = []
    return {
        "total_count": check_runs.get("total_count", len(raw_check_runs)),
        "check_runs": [
            check_run_summary(check_run)
            for check_run in raw_check_runs
            if isinstance(check_run, dict)
        ],
    }


def _workflow_jobs_list_result(jobs: dict[str, Any]) -> dict[str, Any]:
    raw_jobs = jobs.get("jobs")
    if not isinstance(raw_jobs, list):
        raw_jobs = []
    return {
        "total_count": jobs.get("total_count", len(raw_jobs)),
        "jobs": [
            workflow_run_job_summary(job)
            for job in raw_jobs
            if isinstance(job, dict)
        ],
    }


def _counted_summaries(
    items: list[Any],
    summarize: Callable[[Any], dict[str, Any]],
    *,
    key: str,
) -> dict[str, Any]:
    return {
        "count": len(items),
        key: [summarize(item) for item in items],
    }


def _bad_request(message: str) -> gestalt.Response[dict[str, Any]]:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _unauthorized(message: str) -> gestalt.Response[dict[str, Any]]:
    return gestalt.Response(status=HTTPStatus.UNAUTHORIZED, body={"error": message})


def _forbidden(message: str) -> gestalt.Response[dict[str, Any]]:
    return gestalt.Response(status=HTTPStatus.FORBIDDEN, body={"error": message})


def _server_error(message: str) -> gestalt.Response[dict[str, Any]]:
    return gestalt.Response(
        status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message}
    )


def _github_error(err: GitHubAPIError) -> gestalt.Response[dict[str, Any]]:
    body: dict[str, Any] = {"error": err.message}
    if err.details:
        body["details"] = err.details
    return gestalt.Response(status=err.status, body=body)
