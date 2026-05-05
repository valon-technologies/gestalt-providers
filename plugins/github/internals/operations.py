from __future__ import annotations

import base64
import binascii
import datetime as dt
import re
import urllib.parse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .client import (
    DEFAULT_GITHUB_CLIENT,
    GitHubAPIClient,
    JsonObject,
    repo_path,
)
from .constants import (
    GITHUB_INSTALLATION_SUBJECT_PREFIX,
    GITHUB_REPOSITORY_SUBJECT_SEPARATOR,
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
    installation_id: int = 0
    coauthors: tuple[GitHubCoAuthor, ...] = ()
    include_bot_coauthor: bool = True
    author_name: str = ""
    author_email: str = ""
    committer_name: str = ""
    committer_email: str = ""
    force: bool = False
    allow_base_update: bool = False


@dataclass(frozen=True, slots=True)
class GitHubOpenPullRequestRequest:
    owner: str
    repo: str
    title: str
    head: str
    base: str
    body: str = ""
    installation_id: int = 0
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
    installation_id: int = 0
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
class GitHubCreateIssueCommentRequest:
    owner: str
    repo: str
    issue_number: int
    body: str
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubCreatePullRequestConversationCommentRequest:
    owner: str
    repo: str
    pull_number: int
    body: str
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubPullRequestReviewComment:
    path: str
    body: str
    line: int = 0
    side: str = ""
    start_line: int = 0
    start_side: str = ""
    position: int = 0


@dataclass(frozen=True, slots=True)
class GitHubCreatePullRequestReviewRequest:
    owner: str
    repo: str
    pull_number: int
    body: str
    comments: tuple[GitHubPullRequestReviewComment, ...]
    commit_id: str = ""
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubListPullRequestReviewThreadsRequest:
    owner: str
    repo: str
    pull_number: int
    first: int = 100
    after: str = ""
    comments_first: int = 20
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubResolvePullRequestReviewThreadRequest:
    owner: str
    repo: str
    pull_number: int
    thread_id: str
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubAddReactionRequest:
    owner: str
    repo: str
    subject_type: str
    content: str
    issue_number: int = 0
    pull_number: int = 0
    comment_id: int = 0
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubAddLabelsRequest:
    owner: str
    repo: str
    subject_type: str
    labels: tuple[str, ...]
    issue_number: int = 0
    pull_number: int = 0
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubRemoveLabelsRequest:
    owner: str
    repo: str
    subject_type: str
    labels: tuple[str, ...]
    issue_number: int = 0
    pull_number: int = 0
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubRequestReviewersRequest:
    owner: str
    repo: str
    pull_number: int
    reviewers: tuple[str, ...] = ()
    team_reviewers: tuple[str, ...] = ()
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubPullRequestRequest:
    owner: str
    repo: str
    pull_number: int
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubListPullRequestFilesRequest:
    owner: str
    repo: str
    pull_number: int
    per_page: int = 0
    page: int = 0
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubCheckRunRequest:
    owner: str
    repo: str
    check_run_id: int
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubListCheckRunAnnotationsRequest:
    owner: str
    repo: str
    check_run_id: int
    per_page: int = 0
    page: int = 0
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubWorkflowRunRequest:
    owner: str
    repo: str
    run_id: int
    installation_id: int = 0


@dataclass(frozen=True, slots=True)
class GitHubListWorkflowRunJobsRequest:
    owner: str
    repo: str
    run_id: int
    filter: str = ""
    per_page: int = 0
    page: int = 0
    installation_id: int = 0


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


@dataclass(frozen=True, slots=True)
class GitHubSubjectScope:
    installation_id: int = 0
    repository: str = ""


def commit_files(
    request: GitHubCommitRequest,
    *,
    subject: Any,
    pull_request_permissions: bool,
    client: GitHubAPIClient | None = None,
) -> CommitResult:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    message = commit_message_with_coauthors(
        require_text(request.message, "message"),
        coauthors=request.coauthors,
        include_bot=request.include_bot_coauthor,
        client=github,
    )
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

    installation_id = scoped_installation_id(
        subject, owner=owner, repo=repo, explicit=request.installation_id
    )
    permissions = {"contents": "write"}
    if pull_request_permissions:
        permissions["pull_requests"] = "write"
    token = github.installation_token(
        installation_id, repositories=[repo], permissions=permissions
    )

    if not base_branch:
        base_branch = github.repository_default_branch(token, owner, repo)
    if branch == base_branch and not request.allow_base_update:
        raise ValueError(
            "branch must differ from base_branch unless allow_base_update is true"
        )

    branch_ref = github.get_branch_ref(token, owner, repo, branch)
    branch_created = branch_ref is None
    parent_ref = branch_ref or github.require_branch_ref(
        token, owner, repo, base_branch, "base_branch"
    )
    parent_sha = github.object_sha(parent_ref, "parent ref")
    parent_commit = github.github_json(
        "GET",
        repo_path(owner, repo, "git", "commits", parent_sha),
        token,
    )
    base_tree_sha = nested_str(parent_commit, "tree", "sha")
    if not base_tree_sha:
        raise GitHubAPIError(502, "GitHub commit response did not include tree.sha")

    tree_entries = [
        tree_entry_for_file(token, owner=owner, repo=repo, change=change, client=github)
        for change in files
    ]
    tree = github.github_json(
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

    commit = github.github_json(
        "POST",
        repo_path(owner, repo, "git", "commits"),
        token,
        commit_payload,
    )
    commit_sha = str_field(commit, "sha")
    if not commit_sha:
        raise GitHubAPIError(502, "GitHub commit response did not include sha")

    if branch_created:
        github.github_json(
            "POST",
            repo_path(owner, repo, "git", "refs"),
            token,
            {"ref": f"refs/heads/{branch}", "sha": commit_sha},
        )
    else:
        github.github_json(
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
        commit_url=github.commit_url(owner, repo, commit_sha),
        tree_sha=tree_sha,
        branch_created=branch_created,
        files_changed=len(files),
    )


def open_pull_request(
    request: GitHubOpenPullRequestRequest,
    *,
    subject: Any,
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
        subject, owner=owner, repo=repo, explicit=request.installation_id
    )
    token = github.installation_token(
        installation_id,
        repositories=[repo],
        permissions={"pull_requests": "write"},
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
    subject: Any,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    installation_id = scoped_installation_id(
        subject, owner=owner, repo=repo, explicit=request.installation_id
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
    subject: Any,
    client: GitHubAPIClient | None = None,
) -> CreatePullRequestResult:
    github = github_client(client)
    commit = commit_files(
        GitHubCommitRequest(
            owner=request.owner,
            repo=request.repo,
            message=request.message,
            files=request.files,
            branch=request.branch,
            base_branch=request.base,
            installation_id=request.installation_id,
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
        client=github,
    )
    token = github.installation_token(
        commit.installation_id,
        repositories=[commit.repo],
        permissions={"pull_requests": "write"},
    )
    pull = create_pull_request_on_github(
        token,
        owner=commit.owner,
        repo=commit.repo,
        title=require_text(request.title, "title"),
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


def create_issue_comment(
    request: GitHubCreateIssueCommentRequest,
    *,
    subject: Any,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    issue_number = require_positive_int(request.issue_number, "issue_number")
    body = require_text(request.body, "body")
    installation_id = scoped_installation_id(
        subject, owner=owner, repo=repo, explicit=request.installation_id
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
    subject: Any,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    subject_type = require_subject_type(
        request.subject_type, "subject_type", REACTION_SUBJECT_TYPES
    )
    content = require_reaction_content(request.content)
    installation_id = scoped_installation_id(
        subject, owner=owner, repo=repo, explicit=request.installation_id
    )

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

    token = github.installation_token(
        installation_id, repositories=[repo], permissions=permissions
    )
    return github.github_json("POST", path, token, {"content": content})


def add_labels(
    request: GitHubAddLabelsRequest,
    *,
    subject: Any,
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
        subject, owner=owner, repo=repo, explicit=request.installation_id
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
    subject: Any,
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
        subject, owner=owner, repo=repo, explicit=request.installation_id
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
    subject: Any,
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
        subject, owner=owner, repo=repo, explicit=request.installation_id
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
    subject: Any,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    body = require_text(request.body, "body")
    installation_id = scoped_installation_id(
        subject, owner=owner, repo=repo, explicit=request.installation_id
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
    subject: Any,
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
        subject, owner=owner, repo=repo, explicit=request.installation_id
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


def list_pull_request_review_threads(
    request: GitHubListPullRequestReviewThreadsRequest,
    *,
    subject: Any,
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
        subject, owner=owner, repo=repo, explicit=request.installation_id
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
    subject: Any,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    thread_id = require_text(request.thread_id, "thread_id")
    installation_id = scoped_installation_id(
        subject, owner=owner, repo=repo, explicit=request.installation_id
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
    subject: Any,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    installation_id = scoped_installation_id(
        subject, owner=owner, repo=repo, explicit=request.installation_id
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
    subject: Any,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    pull_number = require_positive_int(request.pull_number, "pull_number")
    params = pagination_params(per_page=request.per_page, page=request.page)
    installation_id = scoped_installation_id(
        subject, owner=owner, repo=repo, explicit=request.installation_id
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


def get_check_run(
    request: GitHubCheckRunRequest,
    *,
    subject: Any,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    check_run_id = require_positive_int(request.check_run_id, "check_run_id")
    installation_id = scoped_installation_id(
        subject, owner=owner, repo=repo, explicit=request.installation_id
    )
    token = github.installation_token(
        installation_id, repositories=[repo], permissions={"checks": "read"}
    )
    return github.github_json(
        "GET",
        repo_path(owner, repo, "check-runs", str(check_run_id)),
        token,
    )


def list_check_run_annotations(
    request: GitHubListCheckRunAnnotationsRequest,
    *,
    subject: Any,
    client: GitHubAPIClient | None = None,
) -> list[JsonObject]:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    check_run_id = require_positive_int(request.check_run_id, "check_run_id")
    installation_id = scoped_installation_id(
        subject, owner=owner, repo=repo, explicit=request.installation_id
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
    subject: Any,
    client: GitHubAPIClient | None = None,
) -> JsonObject:
    github = github_client(client)
    owner = require_slug(request.owner, "owner")
    repo = require_slug(request.repo, "repo")
    run_id = require_positive_int(request.run_id, "run_id")
    installation_id = scoped_installation_id(
        subject, owner=owner, repo=repo, explicit=request.installation_id
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
    subject: Any,
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
        subject, owner=owner, repo=repo, explicit=request.installation_id
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
        position = optional_positive_int(item.position, f"comments[{index}].position")
        side = item.side.strip().upper()
        start_side = item.start_side.strip().upper()

        if position and (line or side or start_line or start_side):
            raise ValueError(
                f"{path}: position cannot be combined with line, side, "
                "start_line, or start_side"
            )
        if position:
            normalized.append(
                GitHubPullRequestReviewComment(
                    path=path,
                    body=body,
                    position=position,
                )
            )
            continue

        if line <= 0:
            raise ValueError(f"{path}: line is required unless position is set")
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
    if comment.position:
        payload["position"] = comment.position
        return payload
    payload["line"] = comment.line
    payload["side"] = comment.side
    if comment.start_line:
        payload["start_line"] = comment.start_line
        payload["start_side"] = comment.start_side
    return payload


def scoped_installation_id(
    subject: Any, *, owner: str, repo: str, explicit: int
) -> int:
    scope = github_scope_from_subject(subject)
    if scope.installation_id <= 0:
        raise GitHubAuthorizationError(
            "GitHub bot operations require a GitHub App installation service account subject"
        )
    if explicit > 0 and explicit != scope.installation_id:
        raise GitHubAuthorizationError(
            "installation_id must match the caller's GitHub App installation subject"
        )
    requested_repo = f"{owner}/{repo}".lower()
    if not scope.repository:
        raise GitHubAuthorizationError(
            "GitHub bot operations require a repository-scoped webhook service account subject"
        )
    if scope.repository.lower() != requested_repo:
        raise GitHubAuthorizationError(
            "repository must match the caller's GitHub App webhook subject"
        )
    return explicit or scope.installation_id


def github_scope_from_subject(subject: Any) -> GitHubSubjectScope:
    if subject.kind != "service_account" or subject.auth_source != "github_app_webhook":
        return GitHubSubjectScope()
    if not subject.id.startswith(GITHUB_INSTALLATION_SUBJECT_PREFIX):
        return GitHubSubjectScope()
    value = subject.id.removeprefix(GITHUB_INSTALLATION_SUBJECT_PREFIX)
    installation_text, separator, repo = value.partition(
        GITHUB_REPOSITORY_SUBJECT_SEPARATOR
    )
    if installation_text.isdigit():
        return GitHubSubjectScope(
            installation_id=int(installation_text), repository=repo if separator else ""
        )
    return GitHubSubjectScope()


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
    return {
        "number": int_field(pull, "number"),
        "title": str_field(pull, "title"),
        "state": str_field(pull, "state"),
        "html_url": str_field(pull, "html_url"),
        "url": str_field(pull, "url"),
        "head": nested_str(pull, "head", "ref"),
        "head_sha": nested_str(pull, "head", "sha"),
        "base": nested_str(pull, "base", "ref"),
        "base_sha": nested_str(pull, "base", "sha"),
    }


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
    return _compact_dict(
        {
            "id": int_field(workflow_run, "id"),
            "name": str_field(workflow_run, "name"),
            "status": str_field(workflow_run, "status"),
            "conclusion": str_field(workflow_run, "conclusion"),
            "html_url": str_field(workflow_run, "html_url"),
            "run_number": int_field(workflow_run, "run_number"),
            "event": str_field(workflow_run, "event"),
            "head_branch": str_field(workflow_run, "head_branch"),
            "head_sha": str_field(workflow_run, "head_sha"),
            "created_at": str_field(workflow_run, "created_at"),
            "updated_at": str_field(workflow_run, "updated_at"),
        }
    )


def workflow_run_job_summary(job: Mapping[str, Any]) -> dict[str, Any]:
    return _compact_dict(
        {
            "id": int_field(job, "id"),
            "run_id": int_field(job, "run_id"),
            "name": str_field(job, "name"),
            "status": str_field(job, "status"),
            "conclusion": str_field(job, "conclusion"),
            "html_url": str_field(job, "html_url"),
            "started_at": str_field(job, "started_at"),
            "completed_at": str_field(job, "completed_at"),
        }
    )


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
    return client if client is not None else DEFAULT_GITHUB_CLIENT


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
