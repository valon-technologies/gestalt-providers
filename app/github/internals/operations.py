from __future__ import annotations

import base64
import binascii
import datetime as dt
import re
import urllib.parse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

import gestalt

from .cached_client import CachingGitHubClient
from .client import (
    DEFAULT_GITHUB_CLIENT,
    GitHubAPIClient,
    JsonObject,
    org_path,
    repo_path,
)
from .config import get_github_config
from .constants import (
    GITHUB_DEFAULT_WEB_BASE_URL,
    GITHUB_REPOSITORY_ACTION_BOT,
    GITHUB_REPOSITORY_RESOURCE_TYPE,
    MAX_GITHUB_PATCH_CHARS,
)
from .errors import GitHubAPIError, GitHubAuthorizationError
from .helpers import (
    int_field,
    map_field,
    nested_str,
    optional_slug,
    require_branch_name,
    require_slug,
    require_text,
    str_field,
)


@dataclass(frozen=True, slots=True)
class GitHubFileChange:
    path: str
    content: str = ""
    content_base64: str = ""
    delete: bool = False
    executable: bool = False


@dataclass(frozen=True, slots=True)
class GitHubCoAuthor:
    name: str
    email: str


@dataclass(frozen=True, slots=True)
class GitHubCommitRequest:
    owner: str
    repo: str
    message: str
    files: tuple[GitHubFileChange, ...]
    branch: str = ""
    base_branch: str = ""
    coauthors: tuple[GitHubCoAuthor, ...] = ()
    include_bot_coauthor: bool = True
    author_name: str = ""
    author_email: str = ""
    committer_name: str = ""
    committer_email: str = ""
    force: bool = False
    allow_base_update: bool = False
    expected_head_sha: str = ""


@dataclass(frozen=True, slots=True)
class GitHubFileContentRequest:
    owner: str
    repo: str
    path: str
    ref: str = ""
    max_bytes: int = 80_000


@dataclass(frozen=True, slots=True)
class GitHubListCommitsRequest:
    owner: str
    repo: str
    sha: str = ""
    path: str = ""
    author: str = ""
    since: str = ""
    until: str = ""
    per_page: int = 30
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubCompareRefsRequest:
    owner: str
    repo: str
    base: str
    head: str


@dataclass(frozen=True, slots=True)
class GitHubRepositoryRequest:
    owner: str
    repo: str


@dataclass(frozen=True, slots=True)
class GitHubCodeSearchRequest:
    owner: str
    repo: str
    query: str
    path: str = ""
    per_page: int = 10
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubOpenPullRequestRequest:
    owner: str
    repo: str
    title: str
    head: str
    base: str
    body: str = ""
    head_owner: str = ""
    draft: bool = False
    maintainer_can_modify: bool = True


@dataclass(frozen=True, slots=True)
class GitHubCreatePullRequestRequest:
    owner: str
    repo: str
    title: str
    message: str
    files: tuple[GitHubFileChange, ...]
    body: str = ""
    branch: str = ""
    base: str = ""
    coauthors: tuple[GitHubCoAuthor, ...] = ()
    include_bot_coauthor: bool = True
    author_name: str = ""
    author_email: str = ""
    committer_name: str = ""
    committer_email: str = ""
    force: bool = False
    draft: bool = False
    maintainer_can_modify: bool = True


@dataclass(frozen=True, slots=True)
class GitHubCreateIssueRequest:
    owner: str
    repo: str
    title: str
    body: str = ""
    labels: tuple[str, ...] = ()
    assignees: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GitHubUpdateIssueRequest:
    owner: str
    repo: str
    issue_number: int
    title: str = ""
    body: str | None = None
    state: str = ""
    labels: tuple[str, ...] | None = None
    assignees: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class GitHubGetIssueRequest:
    owner: str
    repo: str
    issue_number: int


@dataclass(frozen=True, slots=True)
class GitHubListIssuesRequest:
    owner: str
    repo: str
    state: str = "all"
    per_page: int = 100
    page: int = 1


@dataclass(frozen=True, slots=True)
class GitHubCreateIssueCommentRequest:
    owner: str
    repo: str
    issue_number: int
    body: str


@dataclass(frozen=True, slots=True)
class GitHubCreatePullRequestConversationCommentRequest:
    owner: str
    repo: str
    pull_number: int
    body: str


@dataclass(frozen=True, slots=True)
class GitHubPullRequestReviewComment:
    path: str
    body: str
    line: int = 0
    side: str = ""
    start_line: int = 0
    start_side: str = ""


@dataclass(frozen=True, slots=True)
class GitHubCreatePullRequestReviewRequest:
    owner: str
    repo: str
    pull_number: int
    body: str
    comments: tuple[GitHubPullRequestReviewComment, ...]
    commit_id: str = ""


@dataclass(frozen=True, slots=True)
class GitHubListPullRequestReviewsRequest:
    owner: str
    repo: str
    pull_number: int
    per_page: int = 0
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubListPullRequestReviewThreadsRequest:
    owner: str
    repo: str
    pull_number: int
    first: int = 100
    after: str = ""
    comments_first: int = 20


@dataclass(frozen=True, slots=True)
class GitHubResolvePullRequestReviewThreadRequest:
    owner: str
    repo: str
    pull_number: int
    thread_id: str


@dataclass(frozen=True, slots=True)
class GitHubAddReactionRequest:
    owner: str
    repo: str
    subject_type: str
    content: str
    issue_number: int = 0
    pull_number: int = 0
    comment_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubAddLabelsRequest:
    owner: str
    repo: str
    subject_type: str
    labels: tuple[str, ...]
    issue_number: int = 0
    pull_number: int = 0


@dataclass(frozen=True, slots=True)
class GitHubRemoveLabelsRequest:
    owner: str
    repo: str
    subject_type: str
    labels: tuple[str, ...]
    issue_number: int = 0
    pull_number: int = 0


@dataclass(frozen=True, slots=True)
class GitHubRequestReviewersRequest:
    owner: str
    repo: str
    pull_number: int
    reviewers: tuple[str, ...] = ()
    team_reviewers: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class GitHubPullRequestRequest:
    owner: str
    repo: str
    pull_number: int


@dataclass(frozen=True, slots=True)
class GitHubListPullRequestFilesRequest:
    owner: str
    repo: str
    pull_number: int
    per_page: int = 0
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubListPullRequestCommitsRequest:
    owner: str
    repo: str
    pull_number: int
    per_page: int = 0
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubCheckRunRequest:
    owner: str
    repo: str
    check_run_id: int


@dataclass(frozen=True, slots=True)
class GitHubCheckRunOutput:
    title: str = ""
    summary: str = ""
    text: str = ""


@dataclass(frozen=True, slots=True)
class GitHubCreateCheckRunRequest:
    owner: str
    repo: str
    name: str
    head_sha: str
    status: str = "in_progress"
    conclusion: str = ""
    details_url: str = ""
    external_id: str = ""
    output: GitHubCheckRunOutput | None = None


@dataclass(frozen=True, slots=True)
class GitHubUpdateCheckRunRequest:
    owner: str
    repo: str
    check_run_id: int
    name: str = ""
    status: str = ""
    conclusion: str = ""
    details_url: str = ""
    output: GitHubCheckRunOutput | None = None
    completed_at: str = ""


@dataclass(frozen=True, slots=True)
class GitHubListCheckSuiteCheckRunsRequest:
    owner: str
    repo: str
    check_suite_id: int
    check_name: str = ""
    status: str = ""
    filter: str = ""
    per_page: int = 0
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubListCommitCheckRunsRequest:
    owner: str
    repo: str
    ref: str
    check_name: str = ""
    status: str = ""
    filter: str = ""
    per_page: int = 0
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubListCheckRunAnnotationsRequest:
    owner: str
    repo: str
    check_run_id: int
    per_page: int = 0
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubWorkflowRunRequest:
    owner: str
    repo: str
    run_id: int


@dataclass(frozen=True, slots=True)
class GitHubListWorkflowRunJobsRequest:
    owner: str
    repo: str
    run_id: int
    filter: str = ""
    per_page: int = 0
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubListWorkflowRunsRequest:
    owner: str
    repo: str
    workflow_id: str = ""
    branch: str = ""
    event: str = ""
    head_sha: str = ""
    status: str = ""
    created: str = ""
    per_page: int = 0
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubGetWorkflowJobLogsRequest:
    owner: str
    repo: str
    job_id: int


@dataclass(frozen=True, slots=True)
class GitHubListIssueCommentsRequest:
    owner: str
    repo: str
    issue_number: int
    since: str = ""
    per_page: int = 0
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubSearchPullRequestsRequest:
    owner: str
    repo: str
    query: str
    first: int = 30
    after: str = ""


@dataclass(frozen=True, slots=True)
class GitHubGetMergeQueueRequest:
    owner: str
    repo: str
    branch: str
    first: int = 100


@dataclass(frozen=True, slots=True)
class GitHubListPullRequestsRequest:
    owner: str
    repo: str
    state: str = "open"
    sort: str = ""
    direction: str = ""
    base: str = ""
    head: str = ""
    per_page: int = 0
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubListPullRequestsForCommitRequest:
    owner: str
    repo: str
    commit_sha: str
    per_page: int = 0
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubListOrgMembersRequest:
    owner: str
    repo: str
    role: str = ""
    filter: str = ""
    per_page: int = 0
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubListRepoContributorsRequest:
    owner: str
    repo: str
    anon: str = ""
    per_page: int = 0
    page: int = 0


@dataclass(frozen=True, slots=True)
class GitHubGetUserRequest:
    owner: str
    repo: str
    login: str


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


@dataclass(frozen=True, slots=True)
class CreatePullRequestResult:
    commit: CommitResult
    pull_request: JsonObject


PULL_REQUEST_REVIEW_THREADS_QUERY = """
query GestaltPullRequestReviewThreads(
  $owner: String!
  $repo: String!
  $number: Int!
  $first: Int!
  $after: String
  $commentsFirst: Int!
) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      reviewThreads(first: $first, after: $after) {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          id
          isResolved
          isOutdated
          viewerCanResolve
          path
          line
          startLine
          originalLine
          originalStartLine
          diffSide
          startDiffSide
          comments(first: $commentsFirst) {
            totalCount
            nodes {
              id
              databaseId
              author {
                login
              }
              body
              createdAt
              url
            }
          }
        }
      }
    }
  }
}
""".strip()

PULL_REQUEST_REVIEW_THREAD_NODE_QUERY = """
query GestaltPullRequestReviewThreadNode($threadId: ID!) {
  node(id: $threadId) {
    __typename
    ... on PullRequestReviewThread {
      id
      isResolved
      pullRequest {
        number
        repository {
          name
          owner {
            login
          }
        }
      }
    }
  }
}
""".strip()

RESOLVE_PULL_REQUEST_REVIEW_THREAD_MUTATION = """
mutation GestaltResolvePullRequestReviewThread($threadId: ID!) {
  resolveReviewThread(input: {threadId: $threadId}) {
    thread {
      id
      isResolved
    }
  }
}
""".strip()

SEARCH_PULL_REQUESTS_QUERY = """
query GestaltSearchPullRequests($query: String!, $first: Int!, $after: String) {
  search(query: $query, type: ISSUE, first: $first, after: $after) {
    issueCount
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      cursor
      node {
        ... on PullRequest {
          number
          title
          state
          url
          createdAt
          updatedAt
          mergedAt
          author {
            login
          }
          headRefName
          baseRefName
          headRefOid
          mergeCommit {
            oid
          }
          commits(last: 1) {
            nodes {
              commit {
                committedDate
                author {
                  email
                  name
                }
              }
            }
          }
        }
      }
    }
  }
}
""".strip()

MERGE_QUEUE_QUERY = """
query GestaltMergeQueue(
  $owner: String!
  $repo: String!
  $branch: String!
  $first: Int!
) {
  repository(owner: $owner, name: $repo) {
    mergeQueue(branch: $branch) {
      entries(first: $first) {
        totalCount
        nodes {
          position
          enqueuedAt
          state
          headCommit {
            oid
          }
          pullRequest {
            number
            title
          }
        }
      }
    }
  }
}
""".strip()

REACTION_CONTENTS = frozenset(
    ("+1", "-1", "laugh", "confused", "heart", "hooray", "rocket", "eyes")
)
REACTION_ISSUE = "issue"
REACTION_PULL_REQUEST = "pull_request"
REACTION_ISSUE_COMMENT = "issue_comment"
REACTION_PULL_REQUEST_REVIEW_COMMENT = "pull_request_review_comment"
REACTION_SUBJECT_TYPES = frozenset(
    (
        REACTION_ISSUE,
        REACTION_PULL_REQUEST,
        REACTION_ISSUE_COMMENT,
        REACTION_PULL_REQUEST_REVIEW_COMMENT,
    )
)
LABEL_ISSUE = "issue"
LABEL_PULL_REQUEST = "pull_request"
LABEL_SUBJECT_TYPES = frozenset((LABEL_ISSUE, LABEL_PULL_REQUEST))
PULL_REQUEST_CREATE_PERMISSIONS = {"contents": "read", "pull_requests": "write"}
CHECK_RUN_STATUSES = frozenset(("queued", "in_progress", "completed"))
CHECK_RUN_CONCLUSIONS = frozenset(
    (
        "action_required",
        "cancelled",
        "failure",
        "neutral",
        "skipped",
        "success",
        "timed_out",
    )
)


@dataclass(frozen=True, slots=True)
class _ValidatedCommitRequest:
    owner: str
    repo: str
    message_text: str
    files: tuple[GitHubFileChange, ...]
    base_branch: str
    branch: str
    expected_head_sha: str


def commit_files(
    request: GitHubCommitRequest,
    *,
    subject: gestalt.Subject,
    pull_request_permissions: bool,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> CommitResult:
    github = github_client(client)
    validated = _validate_commit_request(request)

    message = commit_message_with_coauthors(
        validated.message_text,
        coauthors=request.coauthors,
        include_bot=request.include_bot_coauthor,
        client=github,
    )
    installation_id = scoped_installation_id(
        subject,
        owner=validated.owner,
        repo=validated.repo,
        authorization=authorization,
        client=github,
    )
    permissions = {"contents": "write"}
    if pull_request_permissions:
        permissions["pull_requests"] = "write"
    token = github.installation_token(
        installation_id, repositories=[validated.repo], permissions=permissions
    )

    return _write_commit(
        validated,
        request,
        token=token,
        installation_id=installation_id,
        message=message,
        client=github,
    )


def _validate_commit_request(request: GitHubCommitRequest) -> _ValidatedCommitRequest:
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    message_text = require_text(request.message, "message")
    files = normalize_file_changes(request.files)
    if not files:
        raise ValueError("files must contain at least one change")

    base_branch = (
        require_branch_name(request.base_branch, "base_branch")
        if request.base_branch.strip()
        else ""
    )
    branch = (
        require_branch_name(request.branch, "branch")
        if request.branch.strip()
        else generated_branch_name(request.message)
    )
    expected_head_sha = request.expected_head_sha.strip()
    if expected_head_sha and request.force:
        raise ValueError("force cannot be combined with expected_head_sha")
    return _ValidatedCommitRequest(
        owner=owner,
        repo=repo,
        message_text=message_text,
        files=files,
        base_branch=base_branch,
        branch=branch,
        expected_head_sha=expected_head_sha,
    )


def _write_commit(
    validated: _ValidatedCommitRequest,
    request: GitHubCommitRequest,
    *,
    token: str,
    installation_id: int,
    message: str,
    client: GitHubAPIClient,
) -> CommitResult:
    owner = validated.owner
    repo = validated.repo
    base_branch = validated.base_branch
    branch = validated.branch

    if not base_branch:
        base_branch = client.repository_default_branch(token, owner, repo)
    if branch == base_branch and not request.allow_base_update:
        raise ValueError(
            "branch must differ from base_branch unless allow_base_update is true"
        )

    branch_ref = client.get_branch_ref(token, owner, repo, branch)
    if validated.expected_head_sha:
        if branch_ref is None:
            raise ValueError("branch was not found for expected_head_sha")
        current_head_sha = client.object_sha(branch_ref, "branch ref")
        if current_head_sha != validated.expected_head_sha:
            raise ValueError("branch head changed since expected_head_sha")
    branch_created = branch_ref is None
    parent_ref = branch_ref or client.require_branch_ref(
        token, owner, repo, base_branch, "base_branch"
    )
    parent_sha = client.object_sha(parent_ref, "parent ref")
    parent_commit = client.github_json(
        "GET",
        repo_path(owner, repo, "git", "commits", parent_sha),
        token,
    )
    base_tree_sha = nested_str(parent_commit, "tree", "sha")
    if not base_tree_sha:
        raise GitHubAPIError(502, "GitHub commit response did not include tree.sha")

    tree_entries = [
        tree_entry_for_file(token, owner=owner, repo=repo, change=change, client=client)
        for change in validated.files
    ]
    tree = client.github_json(
        "POST",
        repo_path(owner, repo, "git", "trees"),
        token,
        {
            "base_tree": base_tree_sha,
            "tree": tree_entries,
        },
    )
    tree_sha = str_field(tree, "sha")
    if not tree_sha:
        raise GitHubAPIError(502, "GitHub tree response did not include sha")

    commit_payload: JsonObject = {
        "message": message,
        "tree": tree_sha,
        "parents": [parent_sha],
    }
    author = git_identity(request.author_name, request.author_email)
    if author:
        commit_payload["author"] = author
    committer = git_identity(request.committer_name, request.committer_email)
    if committer:
        commit_payload["committer"] = committer

    commit = client.github_json(
        "POST",
        repo_path(owner, repo, "git", "commits"),
        token,
        commit_payload,
    )
    commit_sha = str_field(commit, "sha")
    if not commit_sha:
        raise GitHubAPIError(502, "GitHub commit response did not include sha")

    if branch_created:
        client.github_json(
            "POST",
            repo_path(owner, repo, "git", "refs"),
            token,
            {"ref": f"refs/heads/{branch}", "sha": commit_sha},
        )
    else:
        client.github_json(
            "PATCH",
            repo_path(owner, repo, "git", "refs", "heads", branch, safe_last="/"),
            token,
            {"sha": commit_sha, "force": bool(request.force)},
        )

    return CommitResult(
        owner=owner,
        repo=repo,
        branch=branch,
        base_branch=base_branch,
        installation_id=installation_id,
        commit_sha=commit_sha,
        commit_url=client.commit_url(owner, repo, commit_sha),
        tree_sha=tree_sha,
        branch_created=branch_created,
        files_changed=len(validated.files),
    )


def open_pull_request(
    request: GitHubOpenPullRequestRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    title = require_text(request.title, "title")
    head = require_branch_name(request.head, "head")
    base = require_branch_name(request.base, "base")
    head_owner = optional_slug(request.head_owner, "head_owner")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id,
        repositories=[repo],
        permissions=PULL_REQUEST_CREATE_PERMISSIONS,
    )
    return create_pull_request_on_github(
        token,
        owner=owner,
        repo=repo,
        title=title,
        head=head,
        base=base,
        body=request.body,
        head_owner=head_owner,
        draft=request.draft,
        maintainer_can_modify=request.maintainer_can_modify,
        client=github,
    )


def close_pull_request(
    request: GitHubPullRequestRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id,
        repositories=[repo],
        permissions={"pull_requests": "write"},
    )
    return github.github_json(
        "PATCH",
        repo_path(owner, repo, "pulls", str(pull_number)),
        token,
        {"state": "closed"},
    )


def create_pull_request_with_files(
    request: GitHubCreatePullRequestRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> CreatePullRequestResult:
    github = github_client(client)
    title = require_text(request.title, "title")
    commit = commit_files(
        GitHubCommitRequest(
            owner=request.owner,
            repo=request.repo,
            message=request.message,
            files=request.files,
            branch=request.branch,
            base_branch=request.base,
            coauthors=request.coauthors,
            include_bot_coauthor=request.include_bot_coauthor,
            author_name=request.author_name,
            author_email=request.author_email,
            committer_name=request.committer_name,
            committer_email=request.committer_email,
            force=request.force,
            allow_base_update=False,
        ),
        subject=subject,
        pull_request_permissions=True,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        commit.installation_id,
        repositories=[commit.repo],
        permissions=PULL_REQUEST_CREATE_PERMISSIONS,
    )
    pull = create_pull_request_on_github(
        token,
        owner=commit.owner,
        repo=commit.repo,
        title=title,
        head=commit.branch,
        base=commit.base_branch,
        body=request.body,
        head_owner="",
        draft=request.draft,
        maintainer_can_modify=request.maintainer_can_modify,
        client=github,
    )
    return CreatePullRequestResult(commit=commit, pull_request=pull)


def create_pull_request_on_github(
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
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
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
    return github.github_json("POST", repo_path(owner, repo, "pulls"), token, payload)


ISSUE_STATES = frozenset({"open", "closed"})


def create_issue(
    request: GitHubCreateIssueRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    title = require_text(request.title, "title")
    body = request.body.strip()
    labels = normalize_unique_strings(request.labels, "labels", required=False)
    assignees = normalize_unique_strings(request.assignees, "assignees", required=False)
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"issues": "write"}
    )
    payload = _compact_dict(
        {
            "title": title,
            "body": body or None,
            "labels": list(labels) if labels else None,
            "assignees": list(assignees) if assignees else None,
        }
    )
    return github.github_json("POST", repo_path(owner, repo, "issues"), token, payload)


def update_issue(
    request: GitHubUpdateIssueRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    issue_number = require_positive_int(request.issue_number, "issue_number")
    payload = issue_update_payload(
        title=request.title,
        body=request.body,
        state=request.state,
        labels=request.labels,
        assignees=request.assignees,
        require_any=True,
    )
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"issues": "write"}
    )
    return github.github_json(
        "PATCH",
        repo_path(owner, repo, "issues", str(issue_number)),
        token,
        payload,
    )


def get_issue(
    request: GitHubGetIssueRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    issue_number = require_positive_int(request.issue_number, "issue_number")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"issues": "read"}
    )
    return github.github_json(
        "GET",
        repo_path(owner, repo, "issues", str(issue_number)),
        token,
        None,
    )


def list_issues(
    request: GitHubListIssuesRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    state = request.state.strip().lower() or "all"
    if state not in {"open", "closed", "all"}:
        raise ValueError("state must be open, closed, or all")
    params = pagination_params(per_page=request.per_page, page=request.page)
    params["state"] = state
    params["sort"] = "created"
    params["direction"] = "desc"
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"issues": "read"}
    )
    data = github.github_json_value(
        "GET",
        path_with_query(repo_path(owner, repo, "issues"), params),
        token,
    )
    return require_json_object_list(data, "GitHub issues response")


def create_issue_comment(
    request: GitHubCreateIssueCommentRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    issue_number = require_positive_int(request.issue_number, "issue_number")
    body = require_text(request.body, "body")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    path = repo_path(owner, repo, "issues", str(issue_number), "comments")
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"issues": "write"}
    )
    return github.github_json(
        "POST",
        path,
        token,
        {"body": body},
    )


def add_reaction(
    request: GitHubAddReactionRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    subject_type = require_subject_type(
        request.subject_type, "subject_type", REACTION_SUBJECT_TYPES
    )
    content = require_reaction_content(request.content)
    if subject_type == REACTION_ISSUE:
        issue_number = require_positive_int(request.issue_number, "issue_number")
        path = repo_path(owner, repo, "issues", str(issue_number), "reactions")
        permissions = {"issues": "write"}
    elif subject_type == REACTION_PULL_REQUEST:
        pull_number = require_positive_int(request.pull_number, "pull_number")
        path = repo_path(owner, repo, "issues", str(pull_number), "reactions")
        permissions = {"issues": "write"}
    elif subject_type == REACTION_ISSUE_COMMENT:
        comment_id = require_positive_int(request.comment_id, "comment_id")
        path = repo_path(
            owner, repo, "issues", "comments", str(comment_id), "reactions"
        )
        permissions = {"issues": "write"}
    else:
        comment_id = require_positive_int(request.comment_id, "comment_id")
        path = repo_path(owner, repo, "pulls", "comments", str(comment_id), "reactions")
        permissions = {"pull_requests": "write"}

    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions=permissions
    )
    return github.github_json("POST", path, token, {"content": content})


def add_labels(
    request: GitHubAddLabelsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    target_number, permissions = labels_target(
        request.subject_type,
        issue_number=request.issue_number,
        pull_number=request.pull_number,
    )
    labels = normalize_unique_strings(request.labels, "labels")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions=permissions
    )
    data = github.github_json_value(
        "POST",
        repo_path(owner, repo, "issues", str(target_number), "labels"),
        token,
        {"labels": list(labels)},
    )
    return require_json_object_list(data, "GitHub labels response")


def remove_labels(
    request: GitHubRemoveLabelsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> tuple[tuple[str, ...], list[JsonObject]]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    target_number, permissions = labels_target(
        request.subject_type,
        issue_number=request.issue_number,
        pull_number=request.pull_number,
    )
    labels = normalize_unique_strings(request.labels, "labels")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions=permissions
    )
    remaining: list[JsonObject] = []
    for label in labels:
        data = github.github_json_value(
            "DELETE",
            repo_path(owner, repo, "issues", str(target_number), "labels", label),
            token,
        )
        remaining = require_json_object_list(data, "GitHub labels response")
    return labels, remaining


def request_reviewers(
    request: GitHubRequestReviewersRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    reviewers = normalize_unique_strings(request.reviewers, "reviewers", required=False)
    team_reviewers = normalize_unique_strings(
        request.team_reviewers, "team_reviewers", required=False
    )
    if not reviewers and not team_reviewers:
        raise ValueError("reviewers or team_reviewers must contain at least one value")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    payload: JsonObject = {}
    if reviewers:
        payload["reviewers"] = list(reviewers)
    if team_reviewers:
        payload["team_reviewers"] = list(team_reviewers)
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"pull_requests": "write"}
    )
    return github.github_json(
        "POST",
        repo_path(owner, repo, "pulls", str(pull_number), "requested_reviewers"),
        token,
        payload,
    )


def create_pull_request_conversation_comment(
    request: GitHubCreatePullRequestConversationCommentRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    body = require_text(request.body, "body")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    path = repo_path(owner, repo, "issues", str(pull_number), "comments")
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"pull_requests": "write"}
    )
    return github.github_json(
        "POST",
        path,
        token,
        {"body": body},
    )


def create_pull_request_review(
    request: GitHubCreatePullRequestReviewRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    body = require_text(request.body, "body")
    comments = normalize_pull_request_review_comments(request.comments)
    if not comments:
        raise ValueError("comments must contain at least one comment")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    payload: JsonObject = {
        "body": body,
        "event": "COMMENT",
        "comments": [
            pull_request_review_comment_payload(comment) for comment in comments
        ],
    }
    commit_id = request.commit_id.strip()
    if commit_id:
        payload["commit_id"] = commit_id
    path = repo_path(owner, repo, "pulls", str(pull_number), "reviews")
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"pull_requests": "write"}
    )
    return github.github_json("POST", path, token, payload)


def list_pull_request_reviews(
    request: GitHubListPullRequestReviewsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    params = pagination_params(per_page=request.per_page, page=request.page)
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"pull_requests": "read"}
    )
    data = github.github_json_value(
        "GET",
        path_with_query(
            repo_path(owner, repo, "pulls", str(pull_number), "reviews"),
            params,
        ),
        token,
    )
    if not isinstance(data, list):
        raise GitHubAPIError(502, "GitHub pull request reviews response was not a list")
    return [review for review in data if isinstance(review, dict)]


def list_pull_request_review_threads(
    request: GitHubListPullRequestReviewThreadsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    first = bounded_connection_size(request.first, "first", 100, 100)
    comments_first = bounded_connection_size(
        request.comments_first, "comments_first", 20, 50
    )
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    variables: JsonObject = {
        "owner": owner,
        "repo": repo,
        "number": pull_number,
        "first": first,
        "commentsFirst": comments_first,
    }
    after = request.after.strip()
    if after:
        variables["after"] = after
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"pull_requests": "read"}
    )
    response = github.graphql_json(PULL_REQUEST_REVIEW_THREADS_QUERY, token, variables)
    repository = map_field(map_field(response, "data"), "repository")
    if not repository:
        raise GitHubAPIError(
            502, "GitHub review threads response did not include repository"
        )
    pull_request = map_field(repository, "pullRequest")
    if not pull_request:
        raise GitHubAPIError(
            502, "GitHub review threads response did not include pullRequest"
        )
    connection = map_field(pull_request, "reviewThreads")
    page_info = map_field(connection, "pageInfo")
    raw_nodes = connection.get("nodes")
    if not isinstance(raw_nodes, list) or not page_info:
        raise GitHubAPIError(
            502, "GitHub review threads response did not include reviewThreads nodes"
        )
    threads = [
        pull_request_review_thread_summary(node)
        for node in raw_nodes
        if isinstance(node, dict)
    ]
    return {
        "threads": threads,
        "pageInfo": {
            "hasNextPage": bool(page_info.get("hasNextPage")),
            "endCursor": str_field(page_info, "endCursor"),
        },
    }


def resolve_pull_request_review_thread(
    request: GitHubResolvePullRequestReviewThreadRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    thread_id = require_text(request.thread_id, "thread_id")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"pull_requests": "write"}
    )
    node_response = github.graphql_json(
        PULL_REQUEST_REVIEW_THREAD_NODE_QUERY, token, {"threadId": thread_id}
    )
    node = map_field(map_field(node_response, "data"), "node")
    if str_field(node, "__typename") != "PullRequestReviewThread":
        raise ValueError("thread_id must identify a pull request review thread")
    node_pull = map_field(node, "pullRequest")
    node_repo = map_field(node_pull, "repository")
    node_owner = nested_str(node_repo, "owner", "login")
    if (
        node_owner.lower() != owner.lower()
        or str_field(node_repo, "name").lower() != repo.lower()
        or int_field(node_pull, "number") != pull_number
    ):
        raise ValueError("thread_id does not belong to the requested pull request")
    if bool(node.get("isResolved")):
        return {"id": str_field(node, "id") or thread_id, "isResolved": True}

    mutation_response = github.graphql_json(
        RESOLVE_PULL_REQUEST_REVIEW_THREAD_MUTATION,
        token,
        {"threadId": thread_id},
    )
    thread = map_field(
        map_field(map_field(mutation_response, "data"), "resolveReviewThread"),
        "thread",
    )
    if not str_field(thread, "id"):
        raise GitHubAPIError(
            502, "GitHub resolveReviewThread response did not include thread.id"
        )
    return {"id": str_field(thread, "id"), "isResolved": bool(thread.get("isResolved"))}


def get_pull_request(
    request: GitHubPullRequestRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"pull_requests": "read"}
    )
    return github.github_json(
        "GET",
        repo_path(owner, repo, "pulls", str(pull_number)),
        token,
    )


def list_pull_request_files(
    request: GitHubListPullRequestFilesRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    params = pagination_params(per_page=request.per_page, page=request.page)
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"pull_requests": "read"}
    )
    data = github.github_json_value(
        "GET",
        path_with_query(
            repo_path(owner, repo, "pulls", str(pull_number), "files"),
            params,
        ),
        token,
    )
    if not isinstance(data, list):
        raise GitHubAPIError(502, "GitHub pull request files response was not a list")
    return [item for item in data if isinstance(item, dict)]


def list_pull_request_commits(
    request: GitHubListPullRequestCommitsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    params = pagination_params(per_page=request.per_page, page=request.page)
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"pull_requests": "read"}
    )
    data = github.github_json_value(
        "GET",
        path_with_query(
            repo_path(owner, repo, "pulls", str(pull_number), "commits"),
            params,
        ),
        token,
    )
    return require_json_object_list(data, "GitHub pull request commits response")


def get_repository(
    request: GitHubRepositoryRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"contents": "read"}
    )
    return github.github_json("GET", repo_path(owner, repo), token)


def search_code(
    request: GitHubCodeSearchRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    query = require_text(request.query, "query")
    search_path = normalize_optional_search_path(request.path)
    params = pagination_params(per_page=request.per_page, page=request.page)
    scoped_query = f"{query} repo:{owner}/{repo}"
    if search_path:
        scoped_query = f"{scoped_query} path:{search_path}"
    params["q"] = scoped_query
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"contents": "read"}
    )
    return github.github_json("GET", path_with_query("/search/code", params), token)


def list_commits(
    request: GitHubListCommitsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    params = pagination_params(per_page=request.per_page, page=request.page)
    if request.sha.strip():
        params["sha"] = request.sha.strip()
    if request.path.strip():
        params["path"] = request.path.strip()
    if request.author.strip():
        params["author"] = request.author.strip()
    if request.since.strip():
        params["since"] = request.since.strip()
    if request.until.strip():
        params["until"] = request.until.strip()
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"contents": "read"}
    )
    data = github.github_json_value(
        "GET",
        path_with_query(repo_path(owner, repo, "commits"), params),
        token,
    )
    return require_json_object_list(data, "commits response")


def compare_refs(
    request: GitHubCompareRefsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    base = require_text(request.base, "base")
    head = require_text(request.head, "head")
    compare_segment = f"{base}...{head}"
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"contents": "read"}
    )
    return github.github_json(
        "GET",
        repo_path(owner, repo, "compare", compare_segment, safe_last="/.:"),
        token,
    )


def get_file_text_at_ref(
    request: GitHubFileContentRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> str:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    path = normalize_file_content_path(request.path)
    max_bytes = max(1, int(request.max_bytes or 1))
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"contents": "read"}
    )
    ref = request.ref.strip() or github.repository_default_branch(token, owner, repo)
    data = github.github_json(
        "GET",
        path_with_query(
            repo_path(owner, repo, "contents", path, safe_last="/"),
            {"ref": ref},
        ),
        token,
    )
    if str_field(data, "type") != "file":
        raise ValueError(f"{path}: content is not a file")
    size = int_field(data, "size")
    if size > max_bytes:
        raise ValueError(f"{path}: content exceeds self-fix size limit")
    if str_field(data, "encoding") != "base64":
        raise ValueError(f"{path}: content encoding is not base64")
    raw_content = str_field(data, "content")
    try:
        decoded = base64.b64decode(raw_content, validate=False)
    except (binascii.Error, ValueError) as err:
        raise ValueError(f"{path}: content_base64 must be valid base64") from err
    if len(decoded) > max_bytes:
        raise ValueError(f"{path}: content exceeds self-fix size limit")
    try:
        return decoded.decode("utf-8")
    except UnicodeDecodeError as err:
        raise ValueError(f"{path}: content is not UTF-8 text") from err


def normalize_file_content_path(path: str) -> str:
    normalized = path.strip().lstrip("/")
    if not normalized:
        raise ValueError("file path is required")
    if normalized in {".", ".."} or "/../" in f"/{normalized}/":
        raise ValueError(f"{normalized}: path must not contain '..'")
    return normalized


def normalize_optional_search_path(path: str) -> str:
    normalized = path.strip().lstrip("/")
    if normalized.endswith("/"):
        normalized = normalized.rstrip("/")
    if normalized in {".", ".."} or "/../" in f"/{normalized}/":
        raise ValueError(f"{normalized}: path must not contain '..'")
    return normalized


def create_check_run(
    request: GitHubCreateCheckRunRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    name = require_text(request.name, "name")
    head_sha = require_text(request.head_sha, "head_sha")
    payload = check_run_payload(
        name=name,
        head_sha=head_sha,
        status=request.status,
        conclusion=request.conclusion,
        details_url=request.details_url,
        external_id=request.external_id,
        output=request.output,
    )
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"checks": "write"}
    )
    return github.github_json(
        "POST",
        repo_path(owner, repo, "check-runs"),
        token,
        payload,
    )


def update_check_run(
    request: GitHubUpdateCheckRunRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    check_run_id = require_positive_int(request.check_run_id, "check_run_id")
    payload = check_run_payload(
        name=request.name,
        status=request.status,
        conclusion=request.conclusion,
        details_url=request.details_url,
        output=request.output,
        completed_at=request.completed_at,
        require_any=True,
    )
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"checks": "write"}
    )
    return github.github_json(
        "PATCH",
        repo_path(owner, repo, "check-runs", str(check_run_id)),
        token,
        payload,
    )


def get_check_run(
    request: GitHubCheckRunRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    check_run_id = require_positive_int(request.check_run_id, "check_run_id")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"checks": "read"}
    )
    return github.github_json(
        "GET",
        repo_path(owner, repo, "check-runs", str(check_run_id)),
        token,
    )


def _check_run_list_params(
    request: GitHubListCheckSuiteCheckRunsRequest | GitHubListCommitCheckRunsRequest,
) -> dict[str, Any]:
    check_name = request.check_name.strip()
    status = request.status.strip()
    if status and status not in {
        "queued",
        "in_progress",
        "completed",
        "waiting",
        "requested",
        "pending",
    }:
        raise ValueError(
            "status must be queued, in_progress, completed, waiting, requested, or pending"
        )
    filter_value = request.filter.strip()
    if filter_value and filter_value not in {"latest", "all"}:
        raise ValueError("filter must be either 'latest' or 'all'")
    params: dict[str, Any] = {}
    if check_name:
        params["check_name"] = check_name
    if status:
        params["status"] = status
    if filter_value:
        params["filter"] = filter_value
    params.update(pagination_params(per_page=request.per_page, page=request.page))
    return params


def _list_repository_check_runs(
    request: GitHubListCheckSuiteCheckRunsRequest | GitHubListCommitCheckRunsRequest,
    *,
    path: str,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    params = _check_run_list_params(request)
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"checks": "read"}
    )
    return github.github_json("GET", path_with_query(path, params), token)


def list_check_suite_check_runs(
    request: GitHubListCheckSuiteCheckRunsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    check_suite_id = require_positive_int(request.check_suite_id, "check_suite_id")
    return _list_repository_check_runs(
        request,
        path=repo_path(owner, repo, "check-suites", str(check_suite_id), "check-runs"),
        subject=subject,
        authorization=authorization,
        client=client,
    )


def list_commit_check_runs(
    request: GitHubListCommitCheckRunsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    ref = require_text(request.ref, "ref")
    return _list_repository_check_runs(
        request,
        path=repo_path(owner, repo, "commits", ref, "check-runs"),
        subject=subject,
        authorization=authorization,
        client=client,
    )


def list_check_run_annotations(
    request: GitHubListCheckRunAnnotationsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    check_run_id = require_positive_int(request.check_run_id, "check_run_id")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"checks": "read"}
    )
    data = github.github_json_value(
        "GET",
        path_with_query(
            repo_path(owner, repo, "check-runs", str(check_run_id), "annotations"),
            pagination_params(per_page=request.per_page, page=request.page),
        ),
        token,
    )
    if not isinstance(data, list):
        raise GitHubAPIError(
            502, "GitHub check run annotations response was not a list"
        )
    return [item for item in data if isinstance(item, dict)]


def get_workflow_run(
    request: GitHubWorkflowRunRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    run_id = require_positive_int(request.run_id, "run_id")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"actions": "read"}
    )
    return github.github_json(
        "GET",
        repo_path(owner, repo, "actions", "runs", str(run_id)),
        token,
    )


def list_workflow_run_jobs(
    request: GitHubListWorkflowRunJobsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    run_id = require_positive_int(request.run_id, "run_id")
    filter_value = request.filter.strip()
    if filter_value and filter_value not in {"latest", "all"}:
        raise ValueError("filter must be either 'latest' or 'all'")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"actions": "read"}
    )
    params = pagination_params(per_page=request.per_page, page=request.page)
    if filter_value:
        params["filter"] = filter_value
    return github.github_json(
        "GET",
        path_with_query(
            repo_path(owner, repo, "actions", "runs", str(run_id), "jobs"),
            params,
        ),
        token,
    )


def list_workflow_runs(
    request: GitHubListWorkflowRunsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    params = pagination_params(per_page=request.per_page, page=request.page)
    if request.branch.strip():
        params["branch"] = request.branch.strip()
    if request.event.strip():
        params["event"] = request.event.strip()
    if request.head_sha.strip():
        params["head_sha"] = request.head_sha.strip()
    if request.status.strip():
        params["status"] = request.status.strip()
    if request.created.strip():
        params["created"] = request.created.strip()
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"actions": "read"}
    )
    workflow_id = request.workflow_id.strip()
    if workflow_id:
        path = repo_path(owner, repo, "actions", "workflows", workflow_id, "runs")
    else:
        path = repo_path(owner, repo, "actions", "runs")
    return github.github_json(
        "GET",
        path_with_query(path, params),
        token,
    )


def get_workflow_job_logs(
    request: GitHubGetWorkflowJobLogsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> str:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    job_id = require_positive_int(request.job_id, "job_id")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"actions": "read"}
    )
    return github.workflow_job_logs(token, owner, repo, job_id)


def list_issue_comments(
    request: GitHubListIssueCommentsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    issue_number = require_positive_int(request.issue_number, "issue_number")
    params = pagination_params(per_page=request.per_page, page=request.page)
    if request.since.strip():
        params["since"] = request.since.strip()
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"issues": "read"}
    )
    data = github.github_json_value(
        "GET",
        path_with_query(
            repo_path(owner, repo, "issues", str(issue_number), "comments"),
            params,
        ),
        token,
    )
    return require_json_object_list(data, "GitHub issue comments response")


def search_pull_requests(
    request: GitHubSearchPullRequestsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    query = require_text(request.query, "query")
    first = bounded_connection_size(request.first, "first", 30, 100)
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    variables: JsonObject = {"query": query, "first": first}
    after = request.after.strip()
    if after:
        variables["after"] = after
    # The search GraphQL query selects commit data (committedDate / author email),
    # which GitHub gates behind contents:read. pull_requests:read alone yields
    # "Resource not accessible by integration".
    token = github.installation_token(
        installation_id,
        repositories=[repo],
        permissions={"pull_requests": "read", "contents": "read"},
    )
    response = github.graphql_json(SEARCH_PULL_REQUESTS_QUERY, token, variables)
    search = map_field(map_field(response, "data"), "search")
    if not search:
        raise GitHubAPIError(502, "GitHub search response did not include search")
    page_info = map_field(search, "pageInfo")
    raw_edges = search.get("edges")
    if not isinstance(raw_edges, list) or not page_info:
        raise GitHubAPIError(502, "GitHub search response did not include search edges")
    pull_requests = []
    for edge in raw_edges:
        if not isinstance(edge, dict):
            continue
        node = edge.get("node")
        if not isinstance(node, dict):
            continue
        cursor = str(edge.get("cursor") or "")
        pull_requests.append(search_pull_request_summary(node, cursor=cursor))
    return {
        "issue_count": int_field(search, "issueCount"),
        "pull_requests": pull_requests,
        "pageInfo": {
            "hasNextPage": bool(page_info.get("hasNextPage")),
            "endCursor": str_field(page_info, "endCursor"),
        },
    }


def get_merge_queue(
    request: GitHubGetMergeQueueRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    branch = require_branch_name(request.branch, "branch")
    first = bounded_connection_size(request.first, "first", 100, 100)
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"contents": "read"}
    )
    response = github.graphql_json(
        MERGE_QUEUE_QUERY,
        token,
        {"owner": owner, "repo": repo, "branch": branch, "first": first},
    )
    repository = map_field(map_field(response, "data"), "repository")
    if not repository:
        raise GitHubAPIError(
            502, "GitHub merge queue response did not include repository"
        )
    merge_queue = map_field(repository, "mergeQueue")
    if not merge_queue:
        return {"total_count": 0, "entries": []}
    return merge_queue_summary(merge_queue)


def list_pull_requests(
    request: GitHubListPullRequestsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    state = request.state.strip().lower() or "open"
    if state not in {"open", "closed", "all"}:
        raise ValueError("state must be open, closed, or all")
    sort = request.sort.strip().lower()
    if sort and sort not in {"created", "updated", "popularity", "long-running"}:
        raise ValueError("sort must be created, updated, popularity, or long-running")
    direction = request.direction.strip().lower()
    if direction and direction not in {"asc", "desc"}:
        raise ValueError("direction must be asc or desc")
    params = pagination_params(per_page=request.per_page, page=request.page)
    params["state"] = state
    if sort:
        params["sort"] = sort
    if direction:
        params["direction"] = direction
    if request.base.strip():
        params["base"] = request.base.strip()
    if request.head.strip():
        params["head"] = request.head.strip()
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"pull_requests": "read"}
    )
    data = github.github_json_value(
        "GET",
        path_with_query(repo_path(owner, repo, "pulls"), params),
        token,
    )
    return require_json_object_list(data, "GitHub pull requests response")


def list_pull_requests_for_commit(
    request: GitHubListPullRequestsForCommitRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    commit_sha = require_text(request.commit_sha, "commit_sha")
    params = pagination_params(per_page=request.per_page, page=request.page)
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"pull_requests": "read"}
    )
    data = github.github_json_value(
        "GET",
        path_with_query(
            repo_path(owner, repo, "commits", commit_sha, "pulls", safe_last="/.:"),
            params,
        ),
        token,
    )
    return require_json_object_list(data, "GitHub commit pull requests response")


def list_org_members(
    request: GitHubListOrgMembersRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    role = request.role.strip().lower()
    if role and role not in {"all", "admin", "member"}:
        raise ValueError("role must be all, admin, or member")
    filter_value = request.filter.strip().lower()
    if filter_value and filter_value not in {"2fa_disabled", "2fa_insecure"}:
        raise ValueError("filter must be 2fa_disabled or 2fa_insecure")
    params = pagination_params(per_page=request.per_page, page=request.page)
    if role:
        params["role"] = role
    if filter_value:
        params["filter"] = filter_value
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"members": "read"}
    )
    data = github.github_json_value(
        "GET",
        path_with_query(org_path(owner, "members"), params),
        token,
    )
    return require_json_object_list(data, "GitHub org members response")


def list_repo_contributors(
    request: GitHubListRepoContributorsRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    anon = request.anon.strip().lower()
    if anon and anon not in {"true", "false", "1", "0"}:
        raise ValueError("anon must be true or false")
    params = pagination_params(per_page=request.per_page, page=request.page)
    if anon:
        params["anon"] = anon
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"metadata": "read"}
    )
    data = github.github_json_value(
        "GET",
        path_with_query(repo_path(owner, repo, "contributors"), params),
        token,
    )
    return require_json_object_list(data, "GitHub contributors response")


def get_user(
    request: GitHubGetUserRequest,
    *,
    subject: gestalt.Subject,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    login = require_text(request.login, "login")
    installation_id = scoped_installation_id(
        subject,
        owner=owner,
        repo=repo,
        authorization=authorization,
        client=github,
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"metadata": "read"}
    )
    return github.github_json(
        "GET",
        f"/users/{urllib.parse.quote(login, safe='')}",
        token,
    )


def tree_entry_for_file(
    token: str,
    *,
    owner: str,
    repo: str,
    change: GitHubFileChange,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    mode = "100755" if change.executable else "100644"
    if change.delete:
        return {
            "path": change.path,
            "mode": mode,
            "type": "blob",
            "sha": None,
        }
    if change.content and change.content_base64:
        raise ValueError(
            f"{change.path}: content and content_base64 are mutually exclusive"
        )
    if change.content_base64:
        blob = github.github_json(
            "POST",
            repo_path(owner, repo, "git", "blobs"),
            token,
            {
                "content": change.content_base64,
                "encoding": "base64",
            },
        )
        blob_sha = str_field(blob, "sha")
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


def normalize_file_changes(
    files: Sequence[GitHubFileChange],
) -> tuple[GitHubFileChange, ...]:
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
            raise ValueError(
                f"{path}: content and content_base64 are mutually exclusive"
            )
        if content_base64:
            try:
                base64.b64decode(content_base64, validate=True)
            except (binascii.Error, ValueError) as err:
                raise ValueError(
                    f"{path}: content_base64 must be valid base64"
                ) from err
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
    return tuple(normalized)


def normalize_pull_request_review_comments(
    comments: Sequence[GitHubPullRequestReviewComment],
) -> tuple[GitHubPullRequestReviewComment, ...]:
    normalized: list[GitHubPullRequestReviewComment] = []
    for index, item in enumerate(comments):
        path = normalize_review_comment_path(item.path)
        body = require_text(item.body, f"comments[{index}].body")
        line = optional_positive_int(item.line, f"comments[{index}].line")
        start_line = optional_positive_int(
            item.start_line, f"comments[{index}].start_line"
        )
        side = item.side.strip().upper()
        start_side = item.start_side.strip().upper()

        if line <= 0:
            raise ValueError(f"{path}: line is required")
        side = require_review_side(side, f"{path}: side")
        if start_line:
            if start_line > line:
                raise ValueError(
                    f"{path}: start_line must be less than or equal to line"
                )
            start_side = require_review_side(start_side, f"{path}: start_side")
        elif start_side:
            raise ValueError(f"{path}: start_side requires start_line")

        normalized.append(
            GitHubPullRequestReviewComment(
                path=path,
                body=body,
                line=line,
                side=side,
                start_line=start_line,
                start_side=start_side,
            )
        )
    return tuple(normalized)


def normalize_review_comment_path(path: str) -> str:
    normalized = path.strip().lstrip("/")
    if not normalized:
        raise ValueError("comment path is required")
    if normalized in {".", ".."} or "/../" in f"/{normalized}/":
        raise ValueError(f"{normalized}: path must not contain '..'")
    return normalized


def optional_positive_int(value: int, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as err:
        raise ValueError(f"{name} must be an integer") from err
    if number < 0:
        raise ValueError(f"{name} must be greater than 0 when set")
    return number


def require_review_side(value: str, name: str) -> str:
    if value not in {"LEFT", "RIGHT"}:
        raise ValueError(f"{name} must be LEFT or RIGHT")
    return value


def require_reaction_content(value: str) -> str:
    content = require_text(value, "content")
    if content not in REACTION_CONTENTS:
        raise ValueError(
            "content must be one of +1, -1, laugh, confused, heart, "
            "hooray, rocket, or eyes"
        )
    return content


def require_subject_type(value: str, name: str, allowed: frozenset[str]) -> str:
    subject_type = require_text(value, name).lower()
    if subject_type not in allowed:
        raise ValueError(f"{name} must be one of {', '.join(sorted(allowed))}")
    return subject_type


def labels_target(
    subject_type: str, *, issue_number: int, pull_number: int
) -> tuple[int, dict[str, str]]:
    normalized = require_subject_type(subject_type, "subject_type", LABEL_SUBJECT_TYPES)
    if normalized == LABEL_ISSUE:
        return (
            require_positive_int(issue_number, "issue_number"),
            {"issues": "write"},
        )
    return (
        require_positive_int(pull_number, "pull_number"),
        {"pull_requests": "write"},
    )


def normalize_unique_strings(
    values: Sequence[str], name: str, *, required: bool = True
) -> tuple[str, ...]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise ValueError(f"{name} must be a list")
    normalized: list[str] = []
    seen: set[str] = set()
    for index, value in enumerate(values):
        if not isinstance(value, str):
            raise ValueError(f"{name}[{index}] must be a string")
        trimmed = require_text(value, f"{name}[{index}]")
        if trimmed in seen:
            raise ValueError(f"{name}[{index}] duplicates {trimmed!r}")
        seen.add(trimmed)
        normalized.append(trimmed)
    if required and not normalized:
        raise ValueError(f"{name} must contain at least one value")
    return tuple(normalized)


def require_json_object_list(data: Any, name: str) -> list[JsonObject]:
    if not isinstance(data, list):
        raise GitHubAPIError(502, f"{name} was not a list")
    if not all(isinstance(item, dict) for item in data):
        raise GitHubAPIError(502, f"{name} contained a non-object item")
    return data


def pull_request_review_comment_payload(
    comment: GitHubPullRequestReviewComment,
) -> JsonObject:
    payload: JsonObject = {"path": comment.path, "body": comment.body}
    payload["line"] = comment.line
    payload["side"] = comment.side
    if comment.start_line:
        payload["start_line"] = comment.start_line
        payload["start_side"] = comment.start_side
    return payload


def scoped_installation_id(
    subject: gestalt.Subject,
    *,
    owner: str,
    repo: str,
    authorization: gestalt.Authorization | None = None,
    client: GitHubAPIClient | None = None,
) -> int:
    require_repository_authorization(
        authorization,
        subject=subject,
        owner=owner,
        repo=repo,
    )
    try:
        return github_client(client).repository_installation_id(owner, repo)
    except GitHubAPIError as err:
        if err.status == HTTPStatus.NOT_FOUND:
            raise GitHubAuthorizationError(
                "GitHub App installation could not be resolved for repository"
            ) from err
        raise


def require_repository_authorization(
    authorization: gestalt.Authorization | None,
    *,
    subject: gestalt.Subject,
    owner: str,
    repo: str,
) -> None:
    subject_id = subject.id.strip()
    if not subject_id:
        raise GitHubAuthorizationError(
            "GitHub bot operations require an authenticated subject"
        )
    try:
        decision = (authorization or gestalt.Authorization.connect()).check_access(
            gestalt.CheckAccessRequest(
                subject=gestalt.AuthorizationSubject(type="subject", id=subject_id),
                action=gestalt.AuthorizationAction(name=GITHUB_REPOSITORY_ACTION_BOT),
                resource=gestalt.AuthorizationResource(
                    type=GITHUB_REPOSITORY_RESOURCE_TYPE,
                    id=github_repository_resource_id(owner, repo),
                ),
            )
        )
    except Exception as err:
        raise GitHubAuthorizationError(
            "GitHub bot repository authorization is unavailable"
        ) from err
    if not decision.allowed:
        raise GitHubAuthorizationError(
            f"{subject_id} is not authorized for {GITHUB_REPOSITORY_ACTION_BOT} on {owner}/{repo}"
        )


def github_repository_resource_id(owner: str, repo: str) -> str:
    base_url = GITHUB_DEFAULT_WEB_BASE_URL
    try:
        base_url = get_github_config().web_base_url or base_url
    except Exception:
        base_url = GITHUB_DEFAULT_WEB_BASE_URL
    parsed = urllib.parse.urlparse(base_url)
    host = parsed.netloc or parsed.path.split("/", 1)[0]
    host = host.strip().lower() or "github.com"
    return f"{host}/{owner.strip().lower()}/{repo.strip().lower()}"


def commit_message_with_coauthors(
    message: str,
    *,
    coauthors: Sequence[GitHubCoAuthor],
    include_bot: bool,
    client: GitHubAPIClient | None = None,
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

    if include_bot:
        bot = bot_coauthor(client=client)
        if bot is not None and bot not in seen:
            seen.add(bot)
            trailers.append(f"Co-authored-by: {bot[0]} <{bot[1]}>")

    if not trailers:
        return message
    return message.rstrip() + "\n\n" + "\n".join(trailers)


def bot_coauthor(*, client: GitHubAPIClient | None = None) -> tuple[str, str] | None:
    identity = github_client(client).bot_identity_or_none()
    if identity is None or not identity.name or not identity.email:
        return None
    return identity.name, identity.email


def git_identity(name: str, email: str) -> dict[str, str] | None:
    name = name.strip()
    email = email.strip()
    if not name and not email:
        return None
    if not name or not email:
        raise ValueError(
            "git author/committer name and email must be provided together"
        )
    return {"name": name, "email": email}


def commit_result_dict(commit: CommitResult) -> dict[str, Any]:
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


def pull_request_summary(pull: Mapping[str, Any]) -> dict[str, Any]:
    head_repo = pull_request_ref_repo_summary(
        map_field(map_field(pull, "head"), "repo")
    )
    base_repo = pull_request_ref_repo_summary(
        map_field(map_field(pull, "base"), "repo")
    )
    summary: dict[str, Any] = {
        "number": int_field(pull, "number"),
        "title": str_field(pull, "title"),
        "state": str_field(pull, "state"),
        "html_url": str_field(pull, "html_url"),
        "url": str_field(pull, "url"),
        "head": nested_str(pull, "head", "ref"),
        "head_ref": nested_str(pull, "head", "ref"),
        "head_sha": nested_str(pull, "head", "sha"),
        "base": nested_str(pull, "base", "ref"),
        "base_ref": nested_str(pull, "base", "ref"),
        "base_sha": nested_str(pull, "base", "sha"),
    }
    if head_repo:
        summary["head_repo"] = head_repo
    if base_repo:
        summary["base_repo"] = base_repo
    head_full_name = str(head_repo.get("full_name", ""))
    base_full_name = str(base_repo.get("full_name", ""))
    if head_full_name and base_full_name:
        summary["head_repo_is_base_repo"] = head_full_name == base_full_name
    maintainer_can_modify = pull.get("maintainer_can_modify")
    if isinstance(maintainer_can_modify, bool):
        summary["maintainer_can_modify"] = maintainer_can_modify
    merged = pull.get("merged")
    if isinstance(merged, bool):
        summary["merged"] = merged
    merged_at = str_field(pull, "merged_at")
    if merged_at:
        summary["merged_at"] = merged_at
    created_at = str_field(pull, "created_at")
    if created_at:
        summary["created_at"] = created_at
    merge_commit_sha = str_field(pull, "merge_commit_sha")
    if merge_commit_sha:
        summary["merge_commit_sha"] = merge_commit_sha
    user_login = nested_str(pull, "user", "login")
    if user_login:
        summary["user"] = user_login
    return summary


def pull_request_ref_repo_summary(repo: Mapping[str, Any]) -> dict[str, Any]:
    data = {
        "full_name": str_field(repo, "full_name"),
        "owner": nested_str(repo, "owner", "login"),
        "name": str_field(repo, "name"),
    }
    return {key: value for key, value in data.items() if value}


def repository_summary(repo: Mapping[str, Any]) -> dict[str, Any]:
    return _compact_dict(
        {
            "id": int_field(repo, "id"),
            "name": str_field(repo, "name"),
            "full_name": str_field(repo, "full_name"),
            "owner": nested_str(repo, "owner", "login"),
            "private": bool(repo.get("private")),
            "description": str_field(repo, "description"),
            "default_branch": str_field(repo, "default_branch"),
            "html_url": str_field(repo, "html_url"),
        }
    )


def commit_summary(commit: Mapping[str, Any]) -> dict[str, Any]:
    commit_obj = map_field(commit, "commit")
    author = map_field(commit_obj, "author")
    committer = map_field(commit_obj, "committer")
    return _compact_dict(
        {
            "sha": str_field(commit, "sha"),
            "html_url": str_field(commit, "html_url"),
            "message": str_field(commit_obj, "message"),
            "author_name": str_field(author, "name"),
            "author_email": str_field(author, "email"),
            "author_date": str_field(author, "date"),
            "date": str_field(committer, "date"),
        }
    )


def commit_list_summary(response: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "commits": [
            commit_summary(item)
            for item in response
            if isinstance(item, dict)
        ],
    }


def compare_refs_summary(response: Mapping[str, Any]) -> dict[str, Any]:
    files = response.get("files")
    commits = response.get("commits")
    return _compact_dict(
        {
            "status": str_field(response, "status"),
            "ahead_by": int_field(response, "ahead_by"),
            "behind_by": int_field(response, "behind_by"),
            "total_commits": int_field(response, "total_commits"),
            "html_url": str_field(response, "html_url"),
            "permalink_url": str_field(response, "permalink_url"),
            "commits": [
                commit_summary(item)
                for item in (commits if isinstance(commits, list) else [])
                if isinstance(item, dict)
            ],
            "files": [
                pull_request_file_summary(item)
                for item in (files if isinstance(files, list) else [])
                if isinstance(item, dict)
            ],
        }
    )


def code_search_summary(response: Mapping[str, Any]) -> dict[str, Any]:
    items = response.get("items")
    return {
        "total_count": int_field(response, "total_count"),
        "incomplete_results": bool(response.get("incomplete_results")),
        "items": [
            code_search_item_summary(item)
            for item in (items if isinstance(items, list) else [])
            if isinstance(item, dict)
        ],
    }


def code_search_item_summary(item: Mapping[str, Any]) -> dict[str, Any]:
    repo = map_field(item, "repository")
    return _compact_dict(
        {
            "name": str_field(item, "name"),
            "path": str_field(item, "path"),
            "sha": str_field(item, "sha"),
            "html_url": str_field(item, "html_url"),
            "repository": str_field(repo, "full_name"),
            "score": item.get("score"),
        }
    )


def pull_request_file_summary(file: Mapping[str, Any]) -> dict[str, Any]:
    raw_patch = file.get("patch")
    patch = raw_patch if isinstance(raw_patch, str) else ""
    patch_truncated = len(patch) > MAX_GITHUB_PATCH_CHARS
    if patch_truncated:
        patch = _bounded_text(patch, MAX_GITHUB_PATCH_CHARS)
    return {
        "sha": str_field(file, "sha"),
        "filename": str_field(file, "filename"),
        "status": str_field(file, "status"),
        "previous_filename": str_field(file, "previous_filename"),
        "additions": int_field(file, "additions"),
        "deletions": int_field(file, "deletions"),
        "changes": int_field(file, "changes"),
        "blob_url": str_field(file, "blob_url"),
        "raw_url": str_field(file, "raw_url"),
        "contents_url": str_field(file, "contents_url"),
        "patch": patch,
        "patch_truncated": patch_truncated,
        "patch_limit": MAX_GITHUB_PATCH_CHARS,
    }


def issue_update_payload(
    *,
    title: str = "",
    body: str | None = None,
    state: str = "",
    labels: tuple[str, ...] | None = None,
    assignees: tuple[str, ...] | None = None,
    require_any: bool = False,
) -> JsonObject:
    payload: JsonObject = {}
    if title.strip():
        payload["title"] = require_text(title, "title")
    if body is not None:
        payload["body"] = body
    if state.strip():
        normalized_state = state.strip().lower()
        if normalized_state not in ISSUE_STATES:
            raise ValueError("state must be open or closed")
        payload["state"] = normalized_state
    if labels is not None:
        normalized_labels = normalize_unique_strings(labels, "labels", required=False)
        payload["labels"] = list(normalized_labels)
    if assignees is not None:
        normalized_assignees = normalize_unique_strings(
            assignees, "assignees", required=False
        )
        payload["assignees"] = list(normalized_assignees)
    if require_any and not payload:
        raise ValueError("at least one issue field is required")
    return payload


def issue_summary(issue: Mapping[str, Any]) -> dict[str, Any]:
    labels = issue.get("labels")
    assignees = issue.get("assignees")
    return _compact_dict(
        {
            "number": int_field(issue, "number"),
            "title": str_field(issue, "title"),
            "body": str_field(issue, "body"),
            "state": str_field(issue, "state"),
            "html_url": str_field(issue, "html_url"),
            "url": str_field(issue, "url"),
            "id": int_field(issue, "id"),
            "node_id": str_field(issue, "node_id"),
            "created_at": str_field(issue, "created_at"),
            "updated_at": str_field(issue, "updated_at"),
            "closed_at": str_field(issue, "closed_at"),
            "labels": [
                label_summary(label)
                for label in labels
                if isinstance(label, Mapping)
            ]
            if isinstance(labels, list)
            else [],
            "assignees": [
                _compact_dict({"login": str_field(assignee, "login")})
                for assignee in assignees
                if isinstance(assignee, Mapping)
            ]
            if isinstance(assignees, list)
            else [],
        }
    )


def issue_comment_summary(comment: Mapping[str, Any]) -> dict[str, Any]:
    user = map_field(comment, "user")
    return _compact_dict(
        {
            "id": int_field(comment, "id"),
            "node_id": str_field(comment, "node_id"),
            "url": str_field(comment, "url"),
            "html_url": str_field(comment, "html_url"),
            "body": str_field(comment, "body"),
            "user": _compact_dict({"login": str_field(user, "login")}),
            "created_at": str_field(comment, "created_at"),
            "updated_at": str_field(comment, "updated_at"),
        }
    )


def reaction_summary(reaction: Mapping[str, Any]) -> dict[str, Any]:
    user = map_field(reaction, "user")
    return _compact_dict(
        {
            "id": int_field(reaction, "id"),
            "node_id": str_field(reaction, "node_id"),
            "content": str_field(reaction, "content"),
            "user": _compact_dict({"login": str_field(user, "login")}),
            "created_at": str_field(reaction, "created_at"),
        }
    )


def label_summary(label: Mapping[str, Any]) -> dict[str, Any]:
    return _compact_dict(
        {
            "id": int_field(label, "id"),
            "node_id": str_field(label, "node_id"),
            "url": str_field(label, "url"),
            "name": str_field(label, "name"),
            "color": str_field(label, "color"),
            "description": str_field(label, "description"),
        }
    )


def pull_request_review_summary(review: Mapping[str, Any]) -> dict[str, Any]:
    user = map_field(review, "user")
    return _compact_dict(
        {
            "id": int_field(review, "id"),
            "node_id": str_field(review, "node_id"),
            "state": str_field(review, "state"),
            "html_url": str_field(review, "html_url"),
            "pull_request_url": str_field(review, "pull_request_url"),
            "commit_id": str_field(review, "commit_id"),
            "body": str_field(review, "body"),
            "user": _compact_dict({"login": str_field(user, "login")}),
            "submitted_at": str_field(review, "submitted_at"),
        }
    )


def pull_request_review_thread_summary(thread: Mapping[str, Any]) -> dict[str, Any]:
    comments_connection = map_field(thread, "comments")
    raw_comments = comments_connection.get("nodes")
    comment_nodes = raw_comments if isinstance(raw_comments, list) else []
    comments = [
        pull_request_review_thread_comment_summary(comment)
        for comment in comment_nodes
        if isinstance(comment, dict)
    ]
    total_count = int_field(comments_connection, "totalCount")
    return {
        "id": str_field(thread, "id"),
        "isResolved": bool(thread.get("isResolved")),
        "isOutdated": bool(thread.get("isOutdated")),
        "viewerCanResolve": bool(thread.get("viewerCanResolve")),
        "path": str_field(thread, "path"),
        "line": int_field(thread, "line"),
        "startLine": int_field(thread, "startLine"),
        "originalLine": int_field(thread, "originalLine"),
        "originalStartLine": int_field(thread, "originalStartLine"),
        "diffSide": str_field(thread, "diffSide"),
        "startDiffSide": str_field(thread, "startDiffSide"),
        "commentsCount": total_count,
        "commentsTruncated": total_count > len(comments),
        "comments": comments,
    }


def pull_request_review_thread_comment_summary(
    comment: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "id": str_field(comment, "id"),
        "databaseId": int_field(comment, "databaseId"),
        "authorLogin": nested_str(comment, "author", "login"),
        "body": str_field(comment, "body"),
        "createdAt": str_field(comment, "createdAt"),
        "url": str_field(comment, "url"),
    }


def check_run_summary(check_run: Mapping[str, Any]) -> dict[str, Any]:
    output_raw = map_field(check_run, "output")
    output_summary: dict[str, Any] | None = None
    if output_raw:
        output_summary = _compact_dict(
            {
                "title": str_field(output_raw, "title"),
                "summary": str_field(output_raw, "summary"),
            }
        )
    return _compact_dict(
        {
            "id": int_field(check_run, "id"),
            "name": str_field(check_run, "name"),
            "status": str_field(check_run, "status"),
            "conclusion": str_field(check_run, "conclusion"),
            "html_url": str_field(check_run, "html_url"),
            "details_url": str_field(check_run, "details_url"),
            "head_sha": str_field(check_run, "head_sha"),
            "external_id": str_field(check_run, "external_id"),
            "started_at": str_field(check_run, "started_at"),
            "completed_at": str_field(check_run, "completed_at"),
            "output": output_summary,
        }
    )


def check_run_annotation_summary(annotation: Mapping[str, Any]) -> dict[str, Any]:
    return _compact_dict(
        {
            "path": str_field(annotation, "path"),
            "start_line": int_field(annotation, "start_line"),
            "end_line": int_field(annotation, "end_line"),
            "annotation_level": str_field(annotation, "annotation_level"),
            "message": str_field(annotation, "message"),
            "title": str_field(annotation, "title"),
            "raw_details": str_field(annotation, "raw_details"),
        }
    )


def workflow_run_summary(workflow_run: Mapping[str, Any]) -> dict[str, Any]:
    head_ref = str_field(workflow_run, "head_ref") or str_field(workflow_run, "head_branch")
    return _compact_dict(
        {
            "id": int_field(workflow_run, "id"),
            "name": str_field(workflow_run, "name"),
            "display_title": str_field(workflow_run, "display_title"),
            "status": str_field(workflow_run, "status"),
            "conclusion": str_field(workflow_run, "conclusion"),
            "html_url": str_field(workflow_run, "html_url"),
            "run_number": int_field(workflow_run, "run_number"),
            "event": str_field(workflow_run, "event"),
            "head_branch": str_field(workflow_run, "head_branch"),
            "head_ref": head_ref,
            "head_sha": str_field(workflow_run, "head_sha"),
            "path": str_field(workflow_run, "path"),
            "workflow_id": int_field(workflow_run, "workflow_id"),
            "created_at": str_field(workflow_run, "created_at"),
            "updated_at": str_field(workflow_run, "updated_at"),
        }
    )


def workflow_job_step_summary(step: Mapping[str, Any]) -> dict[str, Any]:
    return _compact_dict(
        {
            "name": str_field(step, "name"),
            "status": str_field(step, "status"),
            "conclusion": str_field(step, "conclusion"),
            "number": int_field(step, "number"),
        }
    )


def workflow_run_job_summary(job: Mapping[str, Any]) -> dict[str, Any]:
    raw_steps = job.get("steps")
    steps = raw_steps if isinstance(raw_steps, list) else []
    step_summaries = [
        workflow_job_step_summary(step)
        for step in steps
        if isinstance(step, dict)
    ]
    return _compact_dict(
        {
            "id": int_field(job, "id"),
            "run_id": int_field(job, "run_id"),
            "name": str_field(job, "name"),
            "status": str_field(job, "status"),
            "conclusion": str_field(job, "conclusion"),
            "html_url": str_field(job, "html_url"),
            "created_at": str_field(job, "created_at"),
            "started_at": str_field(job, "started_at"),
            "completed_at": str_field(job, "completed_at"),
            "steps": step_summaries or None,
        }
    )


def workflow_runs_list_summary(response: Mapping[str, Any]) -> dict[str, Any]:
    raw_runs = response.get("workflow_runs")
    runs = raw_runs if isinstance(raw_runs, list) else []
    return {
        "total_count": int_field(response, "total_count"),
        "workflow_runs": [
            workflow_run_summary(run) for run in runs if isinstance(run, dict)
        ],
    }


def user_summary(user: Mapping[str, Any]) -> dict[str, Any]:
    return _compact_dict(
        {
            "id": int_field(user, "id"),
            "login": str_field(user, "login"),
            "name": str_field(user, "name"),
            "type": str_field(user, "type"),
            "html_url": str_field(user, "html_url"),
            "avatar_url": str_field(user, "avatar_url"),
        }
    )


def contributor_summary(contributor: Mapping[str, Any]) -> dict[str, Any]:
    return _compact_dict(
        {
            "id": int_field(contributor, "id"),
            "login": str_field(contributor, "login"),
            "contributions": int_field(contributor, "contributions"),
            "html_url": str_field(contributor, "html_url"),
            "type": str_field(contributor, "type"),
        }
    )


def search_pull_request_summary(
    node: Mapping[str, Any], *, cursor: str = ""
) -> dict[str, Any]:
    commit_node: Mapping[str, Any] | None = None
    commits = map_field(node, "commits")
    if commits:
        raw_nodes = commits.get("nodes")
        if isinstance(raw_nodes, list) and raw_nodes:
            first = raw_nodes[0]
            if isinstance(first, dict):
                mapped_commit = map_field(first, "commit")
                if mapped_commit:
                    commit_node = mapped_commit
    merge_commit = map_field(node, "mergeCommit")
    return _compact_dict(
        {
            "number": int_field(node, "number"),
            "title": str_field(node, "title"),
            "state": str_field(node, "state"),
            "url": str_field(node, "url"),
            "created_at": str_field(node, "createdAt"),
            "updated_at": str_field(node, "updatedAt"),
            "merged_at": str_field(node, "mergedAt"),
            "author_login": nested_str(node, "author", "login"),
            "head_ref": str_field(node, "headRefName"),
            "base_ref": str_field(node, "baseRefName"),
            "head_sha": str_field(node, "headRefOid"),
            "merge_commit_sha": str_field(merge_commit, "oid"),
            "committed_at": str_field(commit_node, "committedDate")
            if commit_node
            else "",
            "author_email": nested_str(commit_node, "author", "email")
            if commit_node
            else "",
            "author_name": nested_str(commit_node, "author", "name")
            if commit_node
            else "",
            "cursor": cursor.strip(),
        }
    )


def merge_queue_entry_summary(entry: Mapping[str, Any]) -> dict[str, Any]:
    head_commit = map_field(entry, "headCommit")
    pull_request = map_field(entry, "pullRequest")
    pull_request_summary: dict[str, Any] | None = None
    if pull_request:
        pull_request_summary = _compact_dict(
            {
                "number": int_field(pull_request, "number"),
                "title": str_field(pull_request, "title"),
            }
        )
    return _compact_dict(
        {
            "position": int_field(entry, "position"),
            "enqueued_at": str_field(entry, "enqueuedAt"),
            "merge_request_state": str_field(entry, "state"),
            "head_commit_oid": str_field(head_commit, "oid"),
            "pull_request": pull_request_summary,
        }
    )


def merge_queue_summary(merge_queue: Mapping[str, Any]) -> dict[str, Any]:
    entries_connection = map_field(merge_queue, "entries")
    raw_nodes = entries_connection.get("nodes")
    nodes = raw_nodes if isinstance(raw_nodes, list) else []
    return {
        "total_count": int_field(entries_connection, "totalCount"),
        "entries": [
            merge_queue_entry_summary(node) for node in nodes if isinstance(node, dict)
        ],
    }


def pagination_params(*, per_page: int, page: int) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if per_page:
        if per_page < 1 or per_page > 100:
            raise ValueError("per_page must be between 1 and 100")
        params["per_page"] = per_page
    if page:
        if page < 1:
            raise ValueError("page must be greater than 0")
        params["page"] = page
    return params


def path_with_query(path: str, params: Mapping[str, Any]) -> str:
    if not params:
        return path
    return path + "?" + urllib.parse.urlencode(params)


def check_run_payload(
    *,
    name: str = "",
    head_sha: str = "",
    status: str = "",
    conclusion: str = "",
    details_url: str = "",
    external_id: str = "",
    output: GitHubCheckRunOutput | None = None,
    completed_at: str = "",
    require_any: bool = False,
) -> JsonObject:
    payload: JsonObject = {}
    if name.strip():
        payload["name"] = name.strip()
    if head_sha.strip():
        payload["head_sha"] = head_sha.strip()
    if details_url.strip():
        payload["details_url"] = details_url.strip()
    if external_id.strip():
        payload["external_id"] = external_id.strip()
    if status.strip():
        normalized_status = status.strip()
        if normalized_status not in CHECK_RUN_STATUSES:
            raise ValueError("status must be queued, in_progress, or completed")
        payload["status"] = normalized_status
    if conclusion.strip():
        normalized_conclusion = conclusion.strip()
        if normalized_conclusion not in CHECK_RUN_CONCLUSIONS:
            raise ValueError(
                "conclusion must be action_required, cancelled, failure, neutral, "
                "skipped, success, or timed_out"
            )
        payload["conclusion"] = normalized_conclusion
        payload["status"] = "completed"
    if output is not None:
        output_payload = check_run_output_payload(output)
        if output_payload:
            payload["output"] = output_payload
    if completed_at.strip():
        payload["completed_at"] = completed_at.strip()
    if require_any and not payload:
        raise ValueError("at least one check run field must be provided")
    return payload


def check_run_output_payload(output: GitHubCheckRunOutput) -> JsonObject:
    return _compact_dict(
        {
            "title": output.title.strip(),
            "summary": output.summary.strip(),
            "text": output.text.strip(),
        }
    )


def require_positive_int(value: int, name: str) -> int:
    if isinstance(value, bool) or int(value) <= 0:
        raise ValueError(f"{name} is required")
    return int(value)


def bounded_connection_size(
    value: int,
    name: str,
    default: int,
    maximum: int,
) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as err:
        raise ValueError(f"{name} must be an integer") from err
    if parsed <= 0:
        return default
    return max(1, min(maximum, parsed))


def github_client(client: GitHubAPIClient | None) -> GitHubAPIClient:
    if client is not None:
        return client
    config = get_github_config()
    if not config.cache_enabled:
        return DEFAULT_GITHUB_CLIENT
    return CachingGitHubClient(
        DEFAULT_GITHUB_CLIENT,
        provider_name=config.provider_name,
        app_id=config.app_id,
        api_base_url=config.api_base_url,
        graphql_base_url=config.graphql_base_url,
        ttl_seconds=config.cache_ttl_seconds,
        search_pull_requests_query=SEARCH_PULL_REQUESTS_QUERY,
    )


def _compact_dict(value: dict[str, Any]) -> dict[str, Any]:
    return {
        key: nested
        for key, nested in value.items()
        if nested not in ("", 0, None, {}, [])
    }


def _bounded_text(value: str, max_chars: int) -> str:
    if len(value) <= max_chars:
        return value
    marker = "\n...<truncated>"
    if max_chars <= len(marker):
        return value[:max_chars]
    return value[: max_chars - len(marker)] + marker


def generated_branch_name(message: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", message.strip().lower())
    slug = re.sub(r"\.+", ".", slug)
    slug = slug.replace("..", ".").strip("-/.")[:48] or "changes"
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d%H%M%S")
    return f"gestalt/{slug}-{timestamp}"
