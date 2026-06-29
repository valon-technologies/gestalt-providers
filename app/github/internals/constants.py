GITHUB_API_VERSION = "2022-11-28"
GITHUB_DEFAULT_API_BASE_URL = "https://api.github.com"
GITHUB_DEFAULT_GRAPHQL_BASE_URL = "https://api.github.com/graphql"
GITHUB_DEFAULT_WEB_BASE_URL = "https://github.com"

GITHUB_USER_RESOURCE_TYPE = "app/github/user"
GITHUB_USER_LINKED_ACTION = "linked"
GITHUB_REPOSITORY_RESOURCE_TYPE = "app/github/repository"
GITHUB_REPOSITORY_ACTION_BOT = "bot"

GITHUB_EVENT_OPERATION = "events.handle"
IDENTITY_LINK_SELF_OPERATION = "identity.linkSelf"
BOT_GET_REPOSITORY_OPERATION = "bot.getRepository"
BOT_SEARCH_CODE_OPERATION = "bot.searchCode"
BOT_GET_CONTENT_OPERATION = "bot.getContent"
BOT_LIST_COMMITS_OPERATION = "bot.listCommits"
BOT_COMPARE_REFS_OPERATION = "bot.compareRefs"
BOT_COMMIT_FILES_OPERATION = "bot.commitFiles"
BOT_OPEN_PULL_REQUEST_OPERATION = "bot.openPullRequest"
BOT_CLOSE_PULL_REQUEST_OPERATION = "bot.closePullRequest"
BOT_CREATE_PULL_REQUEST_OPERATION = "bot.createPullRequest"
BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION = "bot.createPullRequestReview"
BOT_LIST_PULL_REQUEST_REVIEWS_OPERATION = "bot.listPullRequestReviews"
BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION = "bot.listPullRequestReviewThreads"
BOT_RESOLVE_PULL_REQUEST_REVIEW_THREAD_OPERATION = "bot.resolvePullRequestReviewThread"
BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION = (
    "bot.createPullRequestConversationComment"
)
BOT_CREATE_ISSUE_OPERATION = "bot.createIssue"
BOT_UPDATE_ISSUE_OPERATION = "bot.updateIssue"
BOT_GET_ISSUE_OPERATION = "bot.getIssue"
BOT_LIST_ISSUES_OPERATION = "bot.listIssues"
BOT_CREATE_ISSUE_COMMENT_OPERATION = "bot.createIssueComment"
BOT_ADD_REACTION_OPERATION = "bot.addReaction"
BOT_ADD_LABELS_OPERATION = "bot.addLabels"
BOT_REMOVE_LABELS_OPERATION = "bot.removeLabels"
BOT_REQUEST_REVIEWERS_OPERATION = "bot.requestReviewers"
BOT_GET_PULL_REQUEST_OPERATION = "bot.getPullRequest"
BOT_LIST_PULL_REQUEST_FILES_OPERATION = "bot.listPullRequestFiles"
BOT_LIST_PULL_REQUEST_COMMITS_OPERATION = "bot.listPullRequestCommits"
BOT_GET_CHECK_RUN_OPERATION = "bot.getCheckRun"
BOT_CREATE_CHECK_RUN_OPERATION = "bot.createCheckRun"
BOT_UPDATE_CHECK_RUN_OPERATION = "bot.updateCheckRun"
BOT_LIST_CHECK_SUITE_CHECK_RUNS_OPERATION = "bot.listCheckSuiteCheckRuns"
BOT_LIST_COMMIT_CHECK_RUNS_OPERATION = "bot.listCommitCheckRuns"
BOT_LIST_CHECK_RUN_ANNOTATIONS_OPERATION = "bot.listCheckRunAnnotations"
BOT_GET_WORKFLOW_RUN_OPERATION = "bot.getWorkflowRun"
BOT_LIST_WORKFLOW_RUN_JOBS_OPERATION = "bot.listWorkflowRunJobs"
BOT_LIST_WORKFLOW_RUNS_OPERATION = "bot.listWorkflowRuns"
BOT_GET_WORKFLOW_JOB_LOGS_OPERATION = "bot.getWorkflowJobLogs"
BOT_LIST_ISSUE_COMMENTS_OPERATION = "bot.listIssueComments"
BOT_SEARCH_PULL_REQUESTS_OPERATION = "bot.searchPullRequests"
BOT_GET_MERGE_QUEUE_OPERATION = "bot.getMergeQueue"
BOT_LIST_PULL_REQUESTS_OPERATION = "bot.listPullRequests"
BOT_LIST_PULL_REQUESTS_FOR_COMMIT_OPERATION = "bot.listPullRequestsForCommit"
BOT_LIST_ORG_MEMBERS_OPERATION = "bot.listOrgMembers"
BOT_LIST_REPO_CONTRIBUTORS_OPERATION = "bot.listRepoContributors"
BOT_GET_USER_OPERATION = "bot.getUser"

GITHUB_WEBHOOK_SUBJECT_PREFIX = "service_account:github_webhook:"
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

MAX_GITHUB_PATCH_CHARS = 8192
MAX_GITHUB_TITLE_CHARS = 512
