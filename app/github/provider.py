from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt
from gestalt.authorization import RelationshipTargetSubject

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
    BOT_COMPARE_REFS_OPERATION,
    BOT_GET_PULL_REQUEST_OPERATION,
    BOT_GET_REPOSITORY_OPERATION,
    BOT_GET_WORKFLOW_RUN_OPERATION,
    BOT_LIST_CHECK_RUN_ANNOTATIONS_OPERATION,
    BOT_LIST_CHECK_SUITE_CHECK_RUNS_OPERATION,
    BOT_LIST_PULL_REQUEST_FILES_OPERATION,
    BOT_LIST_PULL_REQUEST_REVIEWS_OPERATION,
    BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION,
    BOT_LIST_WORKFLOW_RUN_JOBS_OPERATION,
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
)
from internals.errors import GitHubAPIError, GitHubAuthorizationError, GitHubConfigError
from internals.helpers import int_field, str_field
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
    GitHubListCheckSuiteCheckRunsRequest,
    GitHubListCheckRunAnnotationsRequest,
    GitHubListPullRequestFilesRequest,
    GitHubListPullRequestReviewsRequest,
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
    list_check_run_annotations,
    list_pull_request_files,
    list_pull_request_reviews,
    list_pull_request_review_threads,
    list_workflow_run_jobs,
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

app = gestalt.App("github")
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
    )
    if ignored_reason:
        return {"ok": True, "ignored": ignored_reason}

    installation_id = installation_id_from_payload(input)
    summary = event_summary(input, installation_id, event_type=event_type)
    workflow_request = _build_workflow_deliver_event_request(input, summary)
    try:
        logger.info(
            "delivering GitHub workflow event",
            extra={
                "github_event": summary.get("event_type", ""),
                "github_action": summary.get("action", ""),
                "github_delivery_id": summary.get("delivery_id", ""),
                "github_repository": summary.get("repository", ""),
                "workflow_provider": workflow_request.provider_name,
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
            "workflow_provider": workflow_request.provider_name,
        },
    )

    return {
        "ok": True,
        "delivered": True,
        "workflow_event_id": workflow_request.event.id
        if workflow_request.event is not None
        else "",
        "workflow_provider": workflow_request.provider_name,
    }


def _build_workflow_deliver_event_request(
    payload: dict[str, Any], summary: dict[str, Any]
) -> gestalt.WorkflowDeliverEvent:
    delivery_id = github_delivery_id(payload)
    event_id = (
        f"github:{delivery_id}" if delivery_id else f"github:{payload_digest(payload)}"
    )
    return gestalt.WorkflowDeliverEvent(
        provider_name=get_github_config().workflow_provider,
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
    try:
        repo = get_repository(
            GitHubRepositoryRequest(
                owner=input.owner,
                repo=input.repo,
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
    return {"data": {"path": input.path, "ref": input.ref, "content": content}}


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
    return {"data": commit_list_summary(results)}


@app.operation(
    id=BOT_COMPARE_REFS_OPERATION,
    method="GET",
    description="Compare two refs using a GitHub App installation token",
    tags=["repo", "code", "history"],
)
def bot_compare_refs(input: CompareRefsInput, req: gestalt.Request) -> OperationResult:
    try:
        comparison = compare_refs(
            GitHubCompareRefsRequest(
                owner=input.owner,
                repo=input.repo,
                base=input.base,
                head=input.head,
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
    return {"data": compare_refs_summary(comparison)}



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
                head_owner=input.head_owner,
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
    return {"data": {"comment": issue_comment_summary(comment)}}


@app.operation(
    id=BOT_CREATE_ISSUE_OPERATION,
    method="POST",
    description="Create an issue using a GitHub App installation token",
)
def bot_create_issue(
    input: CreateIssueInput, req: gestalt.Request
) -> OperationResult:
    try:
        issue = create_issue(
            GitHubCreateIssueRequest(
                owner=input.owner,
                repo=input.repo,
                title=input.title,
                body=input.body,
                labels=tuple(input.labels),
                assignees=tuple(input.assignees),
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
    return {"data": {"issue": issue_summary(issue)}}


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
    return {"data": {"issue": issue_summary(issue)}}


@app.operation(
    id=BOT_GET_ISSUE_OPERATION,
    method="GET",
    description="Get an issue using a GitHub App installation token",
)
def bot_get_issue(input: GetIssueInput, req: gestalt.Request) -> OperationResult:
    try:
        issue = get_issue(
            GitHubGetIssueRequest(
                owner=input.owner,
                repo=input.repo,
                issue_number=input.issue_number,
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
    return {"data": {"issue": issue_summary(issue)}}


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
    return {"data": {"review": pull_request_review_summary(review)}}


@app.operation(
    id=BOT_LIST_PULL_REQUEST_REVIEWS_OPERATION,
    method="GET",
    description="List pull request reviews using a GitHub App installation token",
    tags=["pr", "prs", "review"],
)
def bot_list_pull_request_reviews(
    input: ListPullRequestReviewsInput, req: gestalt.Request
) -> OperationResult:
    try:
        reviews = list_pull_request_reviews(
            GitHubListPullRequestReviewsRequest(
                owner=input.owner,
                repo=input.repo,
                pull_number=input.pull_number,
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
    return {
        "data": {
            "count": len(reviews),
            "reviews": [pull_request_review_summary(review) for review in reviews],
        }
    }


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
